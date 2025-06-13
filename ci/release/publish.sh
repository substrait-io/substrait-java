#!/usr/bin/env bash
# shellcheck shell=bash

set -euo pipefail

# ensure the submodule tags exist
git submodule foreach 'git fetch --unshallow || true'

gradle wrapper
./gradlew clean
./gradlew publishAllPublicationsToStagingRepository
./gradlew jreleaserDeploy
