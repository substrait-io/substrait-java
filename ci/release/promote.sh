#!/usr/bin/env bash
# shellcheck shell=bash

set -euo pipefail

getStagingProfileRepositories() {
  curl --silent --show-error --fail --request GET -u "$SONATYPE_USER:$SONATYPE_PASSWORD" https://s01.oss.sonatype.org/service/local/staging/profile_repositories \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  | jq '.data[] | select((.description | contains("'"$PACKAGE"'")) and (.type=="closed") and ((now - (.updated[:-5] | strptime("%Y-%m-%dT%H:%M:%S") | mktime))/(24*60*60)) > '"$DAYS_TO_PROMOTE"') | {status: .type, updated: .updated, stagedRepositoryIds: .repositoryId, description: .description, oldDays: ((now - (.updated[:-5] | strptime("%Y-%m-%dT%H:%M:%S") | mktime))/(24*60*60))}'
}

promoteStagingToMavenCentral() {
  stagedRepositoryIds=`echo $repositoriesToRelease | jq -jr '.stagedRepositoryIds | tojson + ", "'`
  description=$(echo $repositoriesToRelease | jq -jr '.description[-5:] + ", "')
  CODE=$(curl --request POST -u "$SONATYPE_USER:$SONATYPE_PASSWORD" \
         --url https://s01.oss.sonatype.org/service/local/staging/bulk/promote \
         -sSL --write-out '%{http_code}' -o /dev/null \
         --header 'Accept: application/json' \
         --header 'Content-Type: application/json' \
         --data '{ "data" : {"stagedRepositoryIds":['${stagedRepositoryIds::-2}'], "autoDropAfterRelease" : true, "description":"Release versions: '"${description::-2}"'." } }')
  if [[ "$CODE" =~ ^2 ]]; then
      echo "Bulk promote sending to Sonatype OSSRH. IDs: [${stagedRepositoryIds::-2}], Versions: [${description::-2}]."
  else
      echo "Error promote to Maven Central. Server returned HTTP code $CODE."
  fi
}

echo "START: Promote to Maven Central."
CODE=$(curl -u "$SONATYPE_USER:$SONATYPE_PASSWORD" -sSL -w '%{http_code}' -o /dev/null https://s01.oss.sonatype.org/service/local/staging/profiles)
if [[ "$CODE" =~ ^2 ]]; then
    echo "Get list of packages to promote."
    repositoriesToRelease=$(getStagingProfileRepositories)
    if [ -z "$repositoriesToRelease" ]; then
      echo "There aren't Staging repositories to promote to Maven Central."
    else
      echo "These are the Staging repositories that we are going to promote to Maven Central."
      echo "$repositoriesToRelease"
      echo "Promote Staging repositories to Maven Central."
      promoteStagingToMavenCentral
    fi
else
    echo "Error to get the profile. Server returned HTTP code $CODE."
fi
echo "FINISH: Promote to Maven Central."
