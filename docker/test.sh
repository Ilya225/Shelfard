#!/usr/bin/env bash
# Smoke test script — runs inside Dockerfile.test on every `docker run`.
# Installs shelfard fresh, exercises every CLI command against JSONPlaceholder.
set -euo pipefail

API="https://jsonplaceholder.typicode.com"

step() { echo ""; echo "── $*"; }
ok()   { echo "   ✓ $*"; }

echo "┌─────────────────────────────────────────────────┐"
echo "│   Shelfard CLI smoke tests                      │"
echo "└─────────────────────────────────────────────────┘"

# ── Install ────────────────────────────────────────────
step "Installing shelfard"
pip install git+https://github.com/Ilya225/shelfard.git -q
ok "Installed"

# ── Snapshot ───────────────────────────────────────────
step "rest snapshot  →  todos"
shelfard rest snapshot "$API/todos/1" --name todos
ok "Snapshot saved"

step "rest snapshot  →  users"
shelfard rest snapshot "$API/users/1" --name users
ok "Snapshot saved"

# ── Check (same endpoint — no drift expected) ──────────
step "rest check  →  todos  (expect: no drift, exit 0)"
shelfard rest check "$API/todos/1" --name todos
ok "No drift"

step "rest check  →  users  (expect: no drift, exit 0)"
shelfard rest check "$API/users/1" --name users
ok "No drift"

# ── Show ───────────────────────────────────────────────
step "show todos"
shelfard show todos

step "show users"
shelfard show users

# ── List schemas ───────────────────────────────────────
step "list schemas"
shelfard list schemas
ok "Listed"

# ── Subscribe ──────────────────────────────────────────
step "subscribe  →  analytics on todos  (all columns)"
shelfard subscribe todos --consumer analytics
ok "Subscribed"

step "subscribe  →  reporting on users  (projection)"
shelfard subscribe users --consumer reporting --columns email,username,phone
ok "Subscribed"

# ── List subscriptions ─────────────────────────────────
step "list subscriptions"
shelfard list subscriptions
ok "Listed"

echo ""
echo "╔═════════════════════════════════════════════════╗"
echo "║   All smoke tests passed!                       ║"
echo "╚═════════════════════════════════════════════════╝"
echo ""
