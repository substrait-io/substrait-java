#!/usr/bin/env bash
# shellcheck shell=bash

set -euo pipefail
export GPG_TTY=$(tty)

echo "Validate Sonatype OSSRH Credentials."
CODE=$(curl -u "$SONATYPE_USER:$SONATYPE_PASSWORD" -sSL -w '%{http_code}' -o /dev/null https://s01.oss.sonatype.org/service/local/staging/profiles)
if [[ "$CODE" =~ ^2 ]]; then
    echo "Sonatype OSSRH Credentials configured successfully."
else
    echo "Error to validate Sonatype OSSRH Credentials. Server returned HTTP code $CODE."
fi

echo "Validate Signing Private/Public Key."
echo "$SIGNING_KEY" | base64 --decode | gpg --batch --import
KEYGRIP=`gpg --with-keygrip --list-secret-keys $SIGNING_KEY_ID | sed -e '/^ *Keygrip  *=  */!d;s///;q'`
echo "allow-preset-passphrase"  >> ~/.gnupg/gpg-agent.conf
gpgconf --reload gpg-agent
"$(gpgconf --list-dirs libexecdir)/gpg-preset-passphrase" -c $KEYGRIP <<< $SIGNING_PASSWORD
echo "test_use_passphrase_from_cache" | gpg -q --batch --status-fd 1 --sign --local-user $SIGNING_KEY_ID --passphrase-fd 0 > /dev/null
if [ $? -eq 0 ]; then
  echo "Public/Private Key Credentials configured successfully."
else
  echo "Error to validate Public/Private Key Credentials."
fi
