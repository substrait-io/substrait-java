# Contributing to Substrait Java

This page provides some orientation and recommendations on how to get the best results when engaging with the community.

1. [Commit conventions](#commit-conventions)
2. [Style Guide](#style-guide)

## Commit Conventions

Substrait Java follows [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) for commit message structure. You can use [`pre-commit`](https://pre-commit.com/) to check your messages for you, but note that you must install pre-commit using `pre-commit install --hook-type commit-msg` for this to work. CI will also lint your commit messages. Please also ensure that your PR title and initial comment together form a valid commit message; that will save us some work formatting the merge commit message when we merge your PR.

```bash
$ pre-commit install --hook-type commit-msg
pre-commit installed at .git/hooks/commit-msg
```

Examples of commit messages can be seen [here](https://www.conventionalcommits.org/en/v1.0.0/#examples).

## Style guide

Changes must adhere to the style guide and this will be verified by the continuous integration build.

* Java code style is [Google style](https://google.github.io/styleguide/javaguide.html).

Java code style is checked by [Spotless](https://github.com/diffplug/spotless)
with [google-java-format](https://github.com/google/google-java-format) during the build.

### Automatically fixing code style issues

Java code style issues can be fixed from the command line using
`./gradlew spotlessApply`.

### Configuring the Code Formatter for Intellij IDEA and Eclipse

Follow the instructions for [Eclipse](https://github.com/google/google-java-format#eclipse) or
[IntelliJ](https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides),
note the required manual actions for IntelliJ.

### Gradle & JDK 17

Run Gradle with a **JDK 17** daemon.

The compile and test tasks pin themselves to a Java 17 toolchain, so those run correctly
regardless of which JDK launches Gradle, as long as a JDK 17 is installed and discoverable.
Spotless is the exception: the `google-java-format` version it uses only runs on JDK 17 and
fails with `NoSuchMethodError` / `NoClassDefFoundError` when the Gradle daemon runs on a newer
JDK, so `./gradlew spotlessApply` (and `spotlessCheck`) require the daemon itself to be on JDK 17.
