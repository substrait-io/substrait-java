# Contributing to Substrait Java

This page provides some orientation and recommendations on how to get the best results when engaging with the community.

1. [Commit conventions](#commit-conventions)
2. [Style Guide](#style-guide)
3. [Building and testing](#building-and-testing)

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

Keep the daemon on JDK 17 **consistently**. The `build-logic` convention plugins are compiled
to bytecode matching the daemon's JDK, so switching JDKs between builds can leave cached plugins
that a later daemon cannot load (`UnsupportedClassVersionError`); run `./gradlew --stop` and
rebuild to clear the stale daemon and cache.

The one exception is the `isthmus-cli` **native image**: `nativeCompile` uses whatever JDK runs
the Gradle daemon (`graalvmNative { toolchainDetection = false }` in `isthmus-cli/build.gradle.kts`),
so it needs the daemon on a **GraalVM** JDK with `native-image` (CI uses GraalVM 25). Switching
the daemon between JDK 17 and GraalVM is the most common cause of the cache churn above.

## Building and testing

`./gradlew build` builds and tests everything; see the [README](README.md#building) for the
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
  `var` (rule `UseExplicitTypes` — use explicit types), and `public` JUnit 5 test
  classes/methods (they must be package-private).
* **javadoc** doclint fails the build, but only via `build` / `javadocJar` — run
  `./gradlew :core:javadoc` before pushing.
* **CI** runs the full `./gradlew build --rerun-tasks` plus `yamllint`, `editorconfig-checker`,
  and commitlint (all also wired as local pre-commit hooks).

`build-logic/` is an **included build**, not a normal subproject, so it does not inherit the
root `gradle.properties`; its Kotlin-compile daemon heap is set in `build-logic/gradle.properties`.
Avoid `--no-build-cache` casually — it forces the `build-logic` Kotlin plugins to recompile on
every run.
