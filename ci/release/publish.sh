#!/usr/bin/env bash
# shellcheck shell=bash

set -euo pipefail

gradle wrapper
./gradlew clean :core:publishToSonatype :isthmus:publishToSonatype closeSonatypeStagingRepository closeSonatypeStagingRepository
