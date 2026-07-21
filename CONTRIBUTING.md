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
* **Code samples are verified, not hand-written.** Runnable code samples are pulled from compiled,
  CI-executed tests with
  [`pymdownx.snippets`](https://facelessuser.github.io/pymdown-extensions/extensions/snippets/), so
  they cannot drift from the API. The backing test marks a region with `// --8<-- [start:name]` /
  `// --8<-- [end:name]`, and the Markdown references it inside a fenced code block, e.g.
  `--8<-- "core/src/test/java/io/substrait/docs/BuildingPlansDocTest.java:create-builder"`. Backing
  tests live in an `io.substrait…docs` package under each module's test sources (`core` and
  `isthmus` as JUnit tests, `spark` as a scalatest suite that runs a full round trip). To add or
  change a sample, edit the test — not the Markdown — then run that module's tests; `pixi run
  docs-build` fails if a referenced file or region is missing. `import` lines and similar
  illustrative context may be kept as literal text alongside the include. A few pure
  API-signature snippets (and examples that need external resources) remain inline.
* As elsewhere in the codebase, do not reference GitHub issue or PR numbers in the docs.

Documentation is built on every pull request by the `Build documentation` workflow. On each
release, the `Deploy documentation` workflow publishes a versioned copy to
<https://substrait-io.github.io/substrait-java/> (via the `mike` version manager, on the
`gh-pages` branch).
