#!/usr/bin/env bash
# shellcheck shell=bash

set -euo pipefail

# update gradle.properties version number
LAST=`sed -rn 's/^version = (.*)$/\1/p' gradle.properties`
echo "Upgrade version from $LAST to $1"
sed -ir "s/^version = .*/version = $1/" gradle.properties
