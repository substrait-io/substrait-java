#!/usr/bin/env bash
# shellcheck shell=bash

set -euo pipefail

curdir="$PWD"
worktree="$(mktemp -d)"
branch="$(basename "$worktree")"

git worktree add "$worktree"

function cleanup() {
  cd "$curdir" || exit 1
  git worktree remove "$worktree"
  git worktree prune
  git branch -D "$branch"
}

trap cleanup EXIT ERR

cd "$worktree" || exit 1

export GITHUB_REF="$branch"
export RELEASE_DRY_RUN=true

npx --yes \
  -p "semantic-release@25.0.5" \
  -p "@semantic-release/commit-analyzer@13.0.1" \
  -p "@semantic-release/release-notes-generator@14.1.1" \
  -p "@semantic-release/changelog@7.0.0-beta.1" \
  -p "@semantic-release/github@12.0.8" \
  -p "@semantic-release/exec@7.1.0" \
  -p "@semantic-release/git@10.0.1" \
  -p "conventional-changelog-conventionalcommits@9.3.1" \
  semantic-release \
  --ci false \
  --dry-run \
  --branches "$branch" \
  --repository-url "file://$PWD"
