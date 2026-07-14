# Contributing to Substrait Java

This page provides some orientation and recommendations on how to get the best results when engaging with the community.

1. [Commit conventions](#commit-conventions)
2. [Style Guide](#style-guide)
3. [Documentation](#documentation)

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

Given that the project currently uses JDK 17 features, it requires to run Gradle itself with JDK 17, which in turn requires the below settings in `~/.gradle/gradle.properties`.
Without those settings you might see issues when running `./gradlew spotlessApply`.

```
org.gradle.jvmargs=--add-exports jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED \
  --add-exports jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED \
  --add-exports jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED \
  --add-exports jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED \
  --add-exports jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED
```

## Documentation

The user-facing documentation site lives under [`docs/`](docs) and is built with
[Zensical](https://zensical.org). Python and the documentation dependencies are managed with
[pixi](https://pixi.sh), so the only prerequisite is a pixi installation.

Preview your changes locally with a live-reloading dev server, or produce the static site:

```bash
pixi run docs-serve   # live-reloading preview at http://localhost:8000
pixi run docs-build   # build the static site into ./site
```

Guidelines:

* Every page is a Markdown file under `docs/`; the navigation is defined explicitly in
  [`zensical.toml`](zensical.toml). When you add a page, add it to the `nav`.
* Keep code samples accurate — base them on the real APIs and tests rather than inventing method
  names. `pixi run docs-build` validates internal links.
* As elsewhere in the codebase, do not reference GitHub issue or PR numbers in the docs.

Documentation is built on every pull request by the `Build documentation` workflow. On each
release, the `Deploy documentation` workflow publishes a versioned copy to
<https://substrait-io.github.io/substrait-java/> (via the `mike` version manager, on the
`gh-pages` branch).
