"""
Shelfard interactive agent.

Uses LangChain to support multiple LLM backends. Model is resolved by:
  1. --model CLI flag  (explicit model name or provider shorthand)
  2. Environment auto-detection: ANTHROPIC_API_KEY → Claude, OPENAI_API_KEY → GPT-4o

Usage (via CLI):
    shelfard agent                          # auto-detect from env
    shelfard agent --model anthropic        # Claude sonnet (default)
    shelfard agent --model openai           # GPT-4o (default)
    shelfard agent --model claude-opus-4-6  # specific model
    shelfard agent --model gpt-4o-mini      # specific model
"""

import os
import sys
from typing import Optional

from langchain.agents import create_agent
from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import MemorySaver

from .registry import get_all_schemas, get_registered_schema


# ── Defaults ──────────────────────────────────────────────────────────────────

_ANTHROPIC_DEFAULT = "claude-sonnet-4-6"
_OPENAI_DEFAULT    = "gpt-4o"


# ── Tools ─────────────────────────────────────────────────────────────────────

@tool
def get_schema(schema_name: str) -> str:
    """Retrieve the latest saved schema for a named data source from the registry.
    Returns column names, types, nullability, and the version timestamp."""
    return get_registered_schema(schema_name).to_json()


@tool
def list_all_schemas() -> str:
    """List all schemas stored in the registry with summary info:
    name, version count, latest version timestamp, source, and column count."""
    return get_all_schemas().to_json()


TOOLS = [get_schema, list_all_schemas]


# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are the Shelfard schema assistant. Shelfard is a schema drift detection tool.
The registry stores versioned snapshots of data source schemas (REST APIs, databases).

You can call tools to read from the registry:
- list_all_schemas — list everything stored
- get_schema(schema_name) — inspect a specific schema in detail

Answer the user's questions about their schemas, help interpret drift, and suggest next steps.
Be concise. When showing schemas, highlight important fields (types, nullability, changes).
"""


# ── Model resolution ──────────────────────────────────────────────────────────

def _resolve_model(model_flag: Optional[str]) -> tuple[str, str]:
    """
    Return (model_id, provider) from the --model flag or env-var auto-detection.

    Priority:
      1. model_flag provided → infer provider, validate API key is set
      2. ANTHROPIC_API_KEY in env → claude-sonnet-4-6
      3. OPENAI_API_KEY in env → gpt-4o
      4. Neither → RuntimeError with helpful message
    """
    if model_flag is None:
        if os.environ.get("ANTHROPIC_API_KEY"):
            return _ANTHROPIC_DEFAULT, "anthropic"
        if os.environ.get("OPENAI_API_KEY"):
            return _OPENAI_DEFAULT, "openai"
        raise RuntimeError(
            "No API key found in the environment.\n"
            "  Set ANTHROPIC_API_KEY to use Claude, or OPENAI_API_KEY to use OpenAI.\n"
            "  Or specify a model explicitly: shelfard agent --model <model>"
        )

    if model_flag == "anthropic" or model_flag.startswith("claude"):
        provider = "anthropic"
        model_id = _ANTHROPIC_DEFAULT if model_flag == "anthropic" else model_flag
    elif model_flag == "openai" or model_flag.startswith(("gpt-", "o1", "o3")):
        provider = "openai"
        model_id = _OPENAI_DEFAULT if model_flag == "openai" else model_flag
    else:
        raise ValueError(
            f"Unknown model {model_flag!r}.\n"
            "  Use a model name like 'claude-sonnet-4-6' or 'gpt-4o',\n"
            "  or a provider shorthand: 'anthropic' or 'openai'."
        )

    key_var = "ANTHROPIC_API_KEY" if provider == "anthropic" else "OPENAI_API_KEY"
    if not os.environ.get(key_var):
        raise RuntimeError(
            f"Model '{model_id}' requires {key_var} to be set in the environment."
        )

    return model_id, provider


def _build_llm(model_id: str, provider: str):
    """Instantiate the appropriate LangChain chat model."""
    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(model=model_id, max_tokens=4096)
    else:
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model=model_id)


# ── Interactive REPL ──────────────────────────────────────────────────────────

def run_agent(model: Optional[str] = None) -> None:
    if not sys.stdin.isatty():
        print(
            "Error: 'shelfard agent' requires an interactive terminal.\n"
            "Run it in an activated shell:\n\n"
            "    conda activate shelfard\n"
            "    shelfard agent",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        model_id, provider = _resolve_model(model)
        llm = _build_llm(model_id, provider)
    except (ValueError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    agent = create_agent(
        llm,
        TOOLS,
        system_prompt=SYSTEM_PROMPT,
        checkpointer=MemorySaver(),
    )
    config = {"configurable": {"thread_id": "session"}}

    print(f"Shelfard Agent  [{model_id}]  (type 'exit' to quit)")
    print()

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if user_input.lower() in ("exit", "quit", "q") or not user_input:
            break

        try:
            state = agent.invoke(
                {"messages": [HumanMessage(content=user_input)]},
                config=config,
            )
        except Exception as e:
            print(f"Agent error: {e}")
            continue

        output = state["messages"][-1].content
        print(f"Agent: {output}\n")
