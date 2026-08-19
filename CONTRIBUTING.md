# Contributing to Substrait Java

This page provides some orientation and recommendations on how to get the best results when engaging with the community.

1. [Contributor License Agreement](#contributor-license-agreement)
2. [The specification is the source of truth](#the-specification-is-the-source-of-truth)
3. [Claiming an issue](#claiming-an-issue)
4. [Commit conventions](#commit-conventions)
5. [Pull requests](#pull-requests)
6. [Style Guide](#style-guide)
7. [Building and testing](#building-and-testing)

## Contributor License Agreement

Substrait requires all contributors to sign the [Contributor License Agreement (CLA)](https://cla-assistant.io/substrait-io/substrait) before their contributions can be merged. A GitHub app checks this on every pull request and guides new contributors through signing it.

## The specification is the source of truth

Substrait Java is an implementation of the [Substrait specification](https://substrait.io/); it does not define Substrait semantics. Review behavioral changes against the spec — the proto comments and the spec text for the version this tree targets, which is pinned by the `substrait-packaging` version in `gradle/libs.versions.toml` and reported by `io.substrait.SubstraitVersion.VERSION`.

Where the spec is genuinely unclear, don't settle it here. Survey the ecosystem for an existing consensus first. The closest comparison is the sibling language bindings listed under [Active Libraries](https://substrait.io/community/active_libraries/) — `substrait-go`, `substrait-python`, and `substrait-rs` solve the same modeling problem at the same layer, so how they represent a construct is directly relevant; that page also marks which bindings are unmaintained, and a stale binding's choice is weaker evidence. For questions about runtime semantics rather than modeling, the engines under [Powered by Substrait](https://substrait.io/community/powered_by/) (DataFusion, DuckDB, Acero, Velox, Gluten) are the better reference.

If they agree, follow that de facto consensus and say so in the PR. If they disagree, or none of them cover the case, raise a clarification issue in [`substrait-io/substrait`](https://github.com/substrait-io/substrait/issues) or bring it to the [community](https://substrait.io/community/) channels rather than encoding a guess — and record the open question in the PR so the assumption stays reviewable.

## Claiming an issue

If you want to work on an issue, please comment on it before you start — a maintainer can only assign an issue to you once you have commented on it, and the assignment is what tells everyone else that the work is already being taken care of.

## Commit Conventions

Substrait Java follows [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) for commit message structure. You can use [`pre-commit`](https://pre-commit.com/) to check your messages for you, but note that you must install pre-commit using `pre-commit install --hook-type commit-msg` for this to work. CI will also lint your commit messages. Please also ensure that your PR title and initial comment together form a valid commit message; that will save us some work formatting the merge commit message when we merge your PR.

```bash
$ pre-commit install --hook-type commit-msg
pre-commit installed at .git/hooks/commit-msg
```

Examples of commit messages can be seen [here](https://www.conventionalcommits.org/en/v1.0.0/#examples).

## Pull requests

Pull requests are squash-merged, and the **PR title and description become the commit message** that `semantic-release` parses to build [`CHANGELOG.md`](CHANGELOG.md). The title is the subject and the description is the body, so the two together must form a valid conventional commit; CI checks both and comments on the PR when they don't. [`.github/pull_request_template.md`](.github/pull_request_template.md) restates that where you write the description.

Because the description is changelog input rather than a review scratchpad, leave out anything the diff and the CI checks already show:

* **Lists of files touched** — they are in the diff.
* **Claims that CI-verified things pass** — "tests pass", "spotless clean". If they didn't, the checks would be red.
* **Process notes that are already implicit** — "opened as draft pending review".

Do include the rationale, and for spec-tracking changes the spec version (e.g. `spec v0.88.0`).

### Breaking changes

Mark a breaking change twice: with `!` after the type and scope in the title (`feat(core)!: …`), and with a `BREAKING CHANGE:` footer in the description. The `!` drives the version bump; the footer text is what populates the ⚠ BREAKING CHANGES section of the release notes, so describe what breaks and what consumers should do instead.

Keep that footer **last, with nothing after it** — below the rationale and below any `Closes #NNN` line. The conventional-commits parser ends a `BREAKING CHANGE` note only at another footer keyword or an issue reference; anything else trailing it, whether prose, an attribution line, or a stray comment marker, is absorbed into the note and published verbatim in the release notes. (`.releaserc.mjs` strips trailing git trailers such as `Signed-off-by:`, but it matches only `Key: value` trailers, so it cannot recognize prose.) Putting the footer last also means the squash-merge message can be trimmed to just the subject and the footer in a single cut.

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

Keep the daemon on JDK 17 **consistently**. The `build-logic` convention plugins are compiled
to bytecode matching the daemon's JDK, so switching JDKs between builds can leave cached plugins
that a later daemon cannot load (`UnsupportedClassVersionError`); run `./gradlew --stop` and
rebuild to clear the stale daemon and cache.

The one exception is the `isthmus-cli` **native image**: `nativeCompile` uses whatever JDK runs
the Gradle daemon (`graalvmNative { toolchainDetection = false }` in `isthmus-cli/build.gradle.kts`),
so it needs the daemon on a **GraalVM** JDK with `native-image` (CI uses GraalVM 25). Switching
the daemon between JDK 17 and GraalVM is the most common cause of the cache churn above.

## Building and testing

`./gradlew build` builds and tests everything; see the [readme](readme.md#building) for the
high-level build and the native-image executable. Useful narrower tasks while iterating:

* **Run a module's tests:** `./gradlew :core:test`
* **A single test class:** `./gradlew :core:test --tests "io.substrait.<pkg>.<Class>"`
* **Format:** `./gradlew spotlessApply` (Google Java Format), or scope it with
  `./gradlew :core:spotlessApply`. `spotlessCheck` runs in CI and requires a JDK 17 daemon
  (see [Gradle & JDK 17](#gradle--jdk-17)).
* **Spark variants:** the Scala source is shared across `spark-3.4_2.12`, `spark-3.5_2.12`, and
  `spark-4.0_2.13`; `./gradlew :spark:build` builds all three, or compile one with
  `./gradlew :spark:spark-3.5_2.12:compileScala`. Formatting runs from the parent `:spark`.

Some checks are **not** wired into `compileJava` / `test`, so they can pass locally yet fail
CI — build the whole thing before pushing:

* **PMD** (`substrait.java-conventions`, ruleset
  `build-logic/src/main/resources/substrait-pmd.xml`) runs via `check` / `build` and fails on
  violations. Common tripwires: missing `@Override`, unused private fields/methods/locals,
  `var` (rule `UseExplicitTypes` — use explicit types), `assert` (rule `AvoidAssertStatement` —
  assertions are disabled unless the JVM runs with `-ea`, so throw `IllegalArgumentException`
  for caller-facing invariants and `IllegalStateException` for internal ones), and `public`
  JUnit 5 test classes/methods (they must be package-private).
* **javadoc** doclint fails the build, but only via `build` / `javadocJar` — run
  `./gradlew :core:javadoc` before pushing.
* **CI** runs the full `./gradlew build --rerun-tasks` plus `yamllint`, `editorconfig-checker`,
  and commitlint (all also wired as local pre-commit hooks).

`build-logic/` is an **included build**, not a normal subproject, so it does not inherit the
root `gradle.properties`; its Kotlin-compile daemon heap is set in `build-logic/gradle.properties`.
Avoid `--no-build-cache` casually — it forces the `build-logic` Kotlin plugins to recompile on
every run.
