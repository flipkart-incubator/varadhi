#!/bin/sh
set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOOK_SRC="$REPO_ROOT/scripts/git-hooks/pre-commit"
HOOK_DST="$REPO_ROOT/.git/hooks/pre-commit"

if [ ! -d "$REPO_ROOT/.git" ]; then
  echo "Not a git repository: $REPO_ROOT" >&2
  exit 1
fi

cp "$HOOK_SRC" "$HOOK_DST"
chmod +x "$HOOK_DST"

# Remove Spotless pre-push hook if present (use pre-commit instead).
if [ -f "$REPO_ROOT/.git/hooks/pre-push" ]; then
  if grep -q 'SPOTLESS HOOK' "$REPO_ROOT/.git/hooks/pre-push" 2>/dev/null; then
    rm "$REPO_ROOT/.git/hooks/pre-push"
    echo "Removed Spotless pre-push hook."
  fi
fi

echo "Installed pre-commit hook -> $HOOK_DST"
