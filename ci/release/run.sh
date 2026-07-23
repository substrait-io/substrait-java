#!/usr/bin/env bash
# shellcheck shell=bash

set -euo pipefail

RELEASE_DRY_RUN=false npx --yes \
  -p "semantic-release@25.0.5" \
  -p "@semantic-release/commit-analyzer@13.0.1" \
  -p "@semantic-release/release-notes-generator@14.1.1" \
  -p "@semantic-release/changelog@6.0.3" \
  -p "@semantic-release/github@12.0.8" \
  -p "@semantic-release/exec@7.1.0" \
  -p "@semantic-release/git@10.0.1" \
  -p "conventional-changelog-conventionalcommits@9.3.1" \
  semantic-release --ci
