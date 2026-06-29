# CI CD

## Introduction

- `Semantic release` helps us to automate the whole package release workflow including:
  - Determining the next version number,
  - Generating the release notes,
  - Publishing the package.
- Published releases are available to users on Maven Central.
- Use formalized `commit message convention` to document changes in the codebase.

## Credentials

Semantic release requires several credentials in order to automate publishing actions.

### Github Token

Github workflow use this environment variable `${{ secrets.GITHUB_TOKEN }}` for [Automatic token authentication](https://docs.github.com/en/actions/security-guides/automatic-token-authentication#about-the-github_token-secret
).

### Public/Private Key

GPG keys are used to sign the release artifacts.

```shell
$ gpg --full-gen-key
$ gpg --list-keys
$ gpg --keyserver keyserver.ubuntu.com --send-keys AAACACAB1674014619E139CC1E0BFFA5E9412929
$ gpg --export-secret-keys AAACACAB1674014619E139CC1E0BFFA5E9412929 | base64
```

Configure values for:
```properties
M2_SIGNING_KEY_ID = AAACACAB1674014619E139CC1E0BFFA5E9412929
M2_SIGNING_PASSWORD = password
M2_SIGNING_KEY = gpg --export-secret-keys AAACACAB1674014619E139CC1E0BFFA5E9412929 | base64
```

### Actions Secrets

The release process uses the following organization-level secrets, shared across the
`substrait-java` and `substrait-packaging` repositories:

- `M2_CENTRAL_USERNAME` — Sonatype Central Publisher Portal username
- `M2_CENTRAL_PASSWORD` — Sonatype Central Publisher Portal password
- `M2_SIGNING_KEY_ID` — GPG signing key ID
- `M2_SIGNING_PASSWORD` — GPG signing key passphrase
- `M2_SIGNING_KEY` — base64-encoded GPG private key

## Repository Manager

The artifacts are deployed to Sonatype [Central Publishing Portal](https://central.sonatype.com/).

Documentation: https://central.sonatype.org/register/central-portal/

## Release Process

- Releases are published automatically by `semantic-release` from the `main` branch.
- The [release workflow](../../.github/workflows/release.yml) runs on a weekly schedule
  (Sundays at 02:00 UTC) and can also be triggered manually via `workflow_dispatch`.
- `semantic-release` analyzes the conventional commits since the last release to determine
  the next version, updates `gradle.properties`, generates the changelog, then builds, signs,
  and publishes the artifacts to the Sonatype Central Publisher Portal
  (`./gradlew publishAggregationToCentralPortal`).
- The Central Portal then publishes the deployment to Maven Central.
- Once a component has been published to Maven Central, it cannot be altered.

## Artifacts

Once published, the artifacts can be downloaded from the following locations:

- Github Artifacts:
  - https://github.com/substrait-io/substrait-java/releases

- Maven Central (releases):
  - https://repo1.maven.org/maven2/io/substrait/

## Branches Configuration

- Regular development of new features and functionality is done by creating PRs into the `main` branch.

  Releases are cut automatically from `main` on the weekly schedule described in
  [Release Process](#release-process) — merging a PR does not by itself publish an artifact.

`main` is the only release branch configured in `.releaserc.mjs`:

```js
  branches: [{ name: "main" }],
```

## Release Validation

### GPG Signatures

#### Getting Signature

The fingerprint of the key used to sign the artifacts is `AAACACAB1674014619E139CC1E0BFFA5E9412929`. The long-form ID is `0x1E0BFFA5E9412929`.

You can download and import it with:

````shell
$ gpg --keyserver keyserver.ubuntu.com --recv-keys AAACACAB1674014619E139CC1E0BFFA5E9412929
gpg: key 1E0BFFA5E9412929: public key "Substrait (artifact signing key for the Substrait project) <security@substrait.io>" imported
gpg: Total number processed: 1
gpg:               imported: 1
````

#### Verifying the Signature

Download Java JAR/POM files and validate the signature of them:

```shell
# Maven Central - 1.0.0 version
# JAR
$ wget https://repo1.maven.org/maven2/io/substrait/core/1.0.0/core-1.0.0.jar
$ wget https://repo1.maven.org/maven2/io/substrait/core/1.0.0/core-1.0.0.jar.asc
$ gpg --verify /Users/substrait/core-1.0.0.jar.asc
gpg: assuming signed data in '/Users/substrait/core-1.0.0.jar'
gpg: Signature made Fri Nov 18 08:52:19 2022 -05
gpg:                using EDDSA key 1E0BFFA5E9412929
gpg: Good signature from "Substrait (artifact signing key for the Substrait project) <security@substrait.io>"
# POM
$ wget https://repo1.maven.org/maven2/io/substrait/core/1.0.0/core-1.0.0.pom
$ wget https://repo1.maven.org/maven2/io/substrait/core/1.0.0/core-1.0.0.pom.asc
$ gpg --verify /Users/substrait/core-1.0.0.pom.asc
gpg: assuming signed data in '/Users/substrait/core-1.0.0.pom'
gpg: Signature made Fri Nov 18 08:52:18 2022 -05
gpg:                using EDDSA key 1E0BFFA5E9412929
gpg: Good signature from "Substrait (artifact signing key for the Substrait project) <security@substrait.io>"
```

## Q&A

#### 1. Is it possible to release a library with a custom version (i.e.: 3.2.9.RC1, 5.0.0.M1)?

No. The version is determined automatically by `semantic-release` from the conventional commit
history, so only standard semantic versions are published to Maven Central. Custom or manual
version strings are not part of the release flow.
