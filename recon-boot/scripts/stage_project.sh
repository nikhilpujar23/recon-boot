#!/usr/bin/env bash
set -euo pipefail

# Curated staging script for recon-boot
# Usage:
#   ./scripts/stage_project.sh [--dry-run] [--branch BRANCH] [--commit "message"]

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

DRY_RUN=0
BRANCH=""
COMMIT_MSG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --branch) BRANCH="$2"; shift 2 ;;
    --commit) COMMIT_MSG="$2"; shift 2 ;;
    -h|--help) echo "Usage: $0 [--dry-run] [--branch BRANCH] [--commit \"message\"]"; exit 0 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

echo "Repo root: $SCRIPT_DIR"

if [ ! -f .gitignore ] && [ ! -d .git ]; then
  echo "Warning: this doesn't look like the repo root (no .gitignore or .git)." >&2
fi

TO_ADD=(
  "pom.xml"
  "Dockerfile"
  "docker-compose.yaml"
  "Makefile"
  "README.md"
  "scripts/"
  "infra/"
  "migrations/"
  "eval/"
  "recon-*/pom.xml"
  "recon-*/src/"
)

echo
echo "Files/patterns that will be staged (respecting .gitignore and existing files):"
for p in "${TO_ADD[@]}"; do
  echo "  $p"
done

if [ "$DRY_RUN" -eq 1 ]; then
  echo
  echo "Dry run: showing what would be added (git add -n)":
  # Expand globs and show
  shopt -s nullglob
  for pattern in "${TO_ADD[@]}"; do
    expanded=( $pattern )
    if [ ${#expanded[@]} -eq 0 ]; then
      echo "  (no match) $pattern"
      continue
    fi
    for f in "${expanded[@]}"; do
      echo "  $f"
    done
  done
  exit 0
fi

# Optionally create or switch to branch
if [ -n "$BRANCH" ]; then
  if git rev-parse --verify --quiet "$BRANCH" >/dev/null; then
    echo "Switching to existing branch: $BRANCH"
    git checkout "$BRANCH"
  else
    echo "Creating and switching to branch: $BRANCH"
    git checkout -b "$BRANCH"
  fi
fi

echo
echo "Staging curated files..."
shopt -s nullglob
for pattern in "${TO_ADD[@]}"; do
  expanded=( $pattern )
  if [ ${#expanded[@]} -eq 0 ]; then
    echo "  (no match) $pattern"
    continue
  fi
  for f in "${expanded[@]}"; do
    echo "  git add $f"
    git add "$f"
  done
done

echo
echo "Staged files (short status):"
git status --short

if [ -n "$COMMIT_MSG" ]; then
  echo
  echo "Committing staged changes..."
  git commit -m "$COMMIT_MSG"
  echo "Committed. Run 'git push -u origin ${BRANCH:-$(git rev-parse --abbrev-ref HEAD)}' to push."
else
  echo
  echo "No commit message provided. Review staged files and run 'git commit -m "..."' when ready."
fi

echo
echo "Done."

