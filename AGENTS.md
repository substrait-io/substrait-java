# AGENTS.md

Guidance for AI agents working in the `substrait-java` repository. This complements
`CONTRIBUTING.md` (commit conventions, style guide) with practical, codebase-specific
knowledge.

## What this project is

`substrait-java` is the Java implementation of [Substrait](https://substrait.io/) —
a cross-language specification for relational query plans. It provides an immutable
POJO model for plans/relations/expressions/types and bidirectional conversion to and
from the Substrait protobuf wire format, plus integrations (Isthmus → Apache Calcite,
Spark).

## Module layout

| Module | Path | Purpose |
| --- | --- | --- |
| `:core` | `core/` | POJO model, proto converters, function extension handling. The heart of the project. |
| `:isthmus` | `isthmus/` | Calcite SQL ⇄ Substrait conversion. |
| `:spark` | `spark/` + variants `spark-3.4_2.12`, `spark-3.5_2.12`, `spark-4.0_2.13` | Spark plan ⇄ Substrait (Scala). Source shared in `spark/src`; each variant compiles it against a different Spark/Scala version — see Spark notes. |
| `:examples` | `examples/isthmus-api`, `examples/substrait-spark` | Runnable examples. |
| `:isthmus-cli` | `isthmus-cli/` | CLI over `:isthmus`, compiled to a GraalVM **native image** (`nativeCompile`) — the `isthmus` binary + smoke tests. |
| `build-logic` | `build-logic/` | Gradle **included build**: Kotlin-DSL convention plugins (`substrait.java-conventions` → shared Java config + PMD). Has its own `gradle.properties`; does not inherit the root's. |

The `substrait/` directory is a **git submodule** of the upstream spec, and several
`:core` build inputs are read from it — none are vendored in this repo, so don't look
for them under `core/src`:

- **Proto**: `substrait/proto/substrait/*.proto` → generated Java in package `io.substrait.proto`.
- **ANTLR grammars**: `substrait/grammar/*.g4` (notably `SubstraitType.g4`, the type /
  function-signature grammar) → parser generated under `io.substrait.type`.
- **Standard extension YAMLs**: `substrait/extensions/*.yaml` (e.g. `functions_arithmetic.yaml`)
  → packaged into `:core` resources at `substrait/extensions/`.
- **Validation schemas**: `substrait/text/simple_extensions_schema.yaml` and
  `dialect_schema.yaml` → validate extensions/dialects (mainly in tests).

`:core:submodulesUpdate` (submodule init/update) therefore gates proto and grammar
generation. These files are owned by the upstream spec — to change them, update
`substrait` upstream and bump the submodule pin, don't edit them in-tree.

## Core architecture (the pattern most changes follow)

The POJO model uses **Immutables** (`org.immutables:value`):

- Interfaces/abstract classes are annotated `@Value.Immutable`; the enclosing type
  (e.g. `Expression`) is `@Value.Enclosing`. The build generates
  `ImmutableExpression.Foo` etc. **These generated classes do not exist until you
  compile**, so your IDE/diagnostics will show "cannot be resolved" errors for
  `ImmutableExpression.*` and new `builder()` methods until `:core:compileJava` runs.
  This is expected — compile to confirm.
- Each POJO exposes a static `builder()` delegating to the generated immutable.
- Immutables copies an accessor's Javadoc **verbatim** into the generated
  `ImmutableXxx` (which lives in the same package as the abstract type). A
  `{@link}`/`@see` targeting a type in a *different* package resolves in the source
  file (which imports it) but NOT in the generated file, so `:core:javadoc` fails with
  `reference not found`. **Fully-qualify** cross-package `{@link}` targets in the
  Javadoc of any `@Value.Immutable` accessor.

Expressions are visited via a double-dispatch **visitor**:

- `ExpressionVisitor` (interface) — one `visit(...)` overload per concrete expression
  type. Direct implementors **must** implement every method.
- `AbstractExpressionVisitor` — provides `visitFallback`-based defaults for every
  method, so subclasses only override what they need. Implementors that extend this
  (e.g. isthmus `ExpressionRexConverter`, spark `DefaultExpressionVisitor`) do **not**
  break when a new expression type is added.
- Adding a new expression type therefore means: add the POJO, add a `visit` method to
  `ExpressionVisitor`, add a default to `AbstractExpressionVisitor`, and update the
  **direct** implementors (`ExpressionProtoConverter`, `ExpressionCopyOnWriteVisitor`,
  examples `ExpressionStringify`).

The same double-dispatch pattern recurs across the model, so the "add a case → update the
direct implementors" rule applies well beyond expressions:

- **Relations**: `RelVisitor` / `AbstractRelVisitor` — same `*Visitor` + `Abstract*Visitor`
  shape as expressions — plus copy-on-write transformers `RelCopyOnWriteVisitor` and
  `ExpressionCopyOnWriteVisitor`.
- **Types**: `TypeVisitor`, extended by `ParameterizedTypeVisitor` / `TypeExpressionVisitor`
  for function-signature and derived-type expressions. Interface-only (no `Abstract*Visitor`
  fallback), so implementors must handle every type kind.
- **Function arguments**: a `FunctionArg` is an `Expression`, `Type`, or `EnumArg`, dispatched
  via `FunctionArg.accept(fnDef, argIdx, FuncArgVisitor, ctx)`; the nested `FuncArgVisitor`
  exposes `visitExpr` / `visitType` / `visitEnumArg` instead of per-POJO overloads. (Function
  *invocations* are `Expression` subtypes, so `ExpressionVisitor` already covers them.)
- Expression, relation, and function-argument visitors thread a `VisitationContext` (`C`) type
  parameter; the type visitors do not.

Proto conversion is split into two directions, and the class name tells you which:
**`<Thing>ProtoConverter` = POJO → proto**, **`Proto<Thing>Converter` = proto → POJO**.

- **POJO → proto** (`<Thing>ProtoConverter`) is a **visitor** over the POJO model —
  `ExpressionProtoConverter` (an `ExpressionVisitor`), `RelProtoConverter` (a `RelVisitor`),
  `TypeProtoConverter` (a `TypeExpressionVisitor`). Reuse `toProto(io.substrait.type.Type)`
  to get a wrapped proto `Type`, then extract the needed sub-message.
- **proto → POJO** (`Proto<Thing>Converter`) is a `switch` on the proto `oneof` case enum —
  `ProtoExpressionConverter`, `ProtoRelConverter`, `ProtoTypeConverter`.
- The same pair exists for each model layer: expressions, types, relations, `Plan`
  (`PlanProtoConverter` / `ProtoPlanConverter`, which delegate to the rel converters),
  function extensions (`ExtensionProtoConverter`), extended expressions, and field masks.
  Changing or adding a proto message means updating **both** converters of the affected
  layer, plus a round-trip test.
- **Enum ↔ proto-enum** mapping lives in the POJO enum itself, not in these converters:
  each constant is built with its generated proto counterpart and exposes `toProto()` +
  static `fromProto(proto)` (e.g. `Set.SetOp`, `Join.JoinType`, `Expression.WindowBoundsType`,
  `Plan.VariableEvaluationMode`); the message converters just call those. `fromProto` scans
  `values()` and throws on an unrecognized proto value — so when several POJO constants map
  to one proto value, order the deprecated ones **last** so the reverse lookup returns the
  canonical one (see `Join.JoinType`). Adding an enum value is an edit to the enum, not the
  converters.
- POJO types are created with `TypeCreator.REQUIRED` / `TypeCreator.NULLABLE`.

## Building, testing, formatting

- Build/run a module's tests: `./gradlew :core:test`
- Single test class: `./gradlew :core:test --tests "io.substrait.<pkg>.<Class>"`
- Format code: `./gradlew spotlessApply` (Google Java Format); scope to a module with
  `./gradlew :core:spotlessApply` when iterating. `spotlessCheck` runs in CI.
- Static analysis: `substrait.java-conventions` applies **PMD** (custom ruleset at
  `build-logic/src/main/resources/substrait-pmd.xml`) to the Java modules, and `check`/
  `build` **fail** on violations. Easy ones to trip: missing `@Override`, unused private
  fields/methods/locals, `var` (rule `UseExplicitTypes` — use explicit types), and
  `public` JUnit 5 test classes/methods (they must be package-private).
- Spark subprojects are nested: compile with
  `./gradlew :spark:spark-3.5_2.12:compileScala`.
- Requires running Gradle with JDK 17 and the `--add-exports` flags in
  `~/.gradle/gradle.properties` (see `CONTRIBUTING.md`), or `spotlessApply` may fail.
  Keep the daemon on JDK 17 *consistently*: `build-logic`'s convention plugins are
  compiled to bytecode matching the daemon's JDK, so switching JDKs between builds can
  leave cached plugins a later daemon can't load (`UnsupportedClassVersionError`) —
  `./gradlew --stop` then rebuild clears the stale daemon/cache.
- The `isthmus-cli` **native image** is the exception to JDK 17: `nativeCompile` uses
  whatever JDK runs the Gradle daemon (`graalvmNative { toolchainDetection = false }` in
  `isthmus-cli/build.gradle.kts`), so it needs the daemon on a **GraalVM** JDK with
  `native-image` — CI uses GraalVM **25**, while everything else declares a JDK 17
  toolchain. Switching the daemon between the two is a common cause of the daemon/cache
  churn noted above.
- `build-logic/` is an **included build**, not a normal subproject: it does **not**
  inherit the root `gradle.properties`, so its Kotlin-compile daemon heap is set in
  `build-logic/gradle.properties` (raising the root heap does nothing for it). Avoid
  `--no-build-cache` casually — it forces `build-logic`'s Kotlin plugins to recompile
  every run.
- CI (`.github/workflows/pr.yml`) runs the **full** `./gradlew build --rerun-tasks`,
  plus `yamllint`, `editorconfig-checker`, and commitlint (all also wired as local
  pre-commit hooks). Narrower local tasks pass while the PR fails — build the whole
  thing before pushing.
- `javadoc` runs doclint that **fails the build**, but only via `build`/`javadocJar` —
  not via `compileJava` / `test` / `spotlessCheck`. So Javadoc errors surface only on
  the PR; run `./gradlew :core:javadoc` before pushing.
- The `substrait` proto submodule needs its **tags** fetched: the generated
  `SubstraitVersion` derives from `git describe --tags` on the submodule, so a
  shallow/tagless checkout yields a malformed version that throws at *runtime* (not
  build time). CI fetches them explicitly because `actions/checkout` doesn't.

When you add to the public expression/visitor API, verify the dependent modules still
compile — they have their own visitor implementors:
`./gradlew :core:spotlessCheck :isthmus:compileJava :spark:spark-3.5_2.12:compileScala :examples:substrait-spark:compileJava`

## Isthmus (Calcite conversion) notes

- Build Calcite `RelBuilder`s for Substrait→Calcite conversion with
  `.typeSystem(SubstraitTypeSystem.TYPE_SYSTEM)` (see `ConverterProvider.getRelBuilder`).
  Calcite's default type system caps decimal precision at 19 while converted
  expressions carry precision-38 types; the mismatch makes `RexSimplify` (run by
  `RelBuilder.project`) re-derive decimal arithmetic at precision 19 and wrap the result
  in a truncating `CAST(… AS DECIMAL(19,0))`. `SubstraitTypeSystem` keeps a public
  no-arg constructor because Frameworks/Avatica reinstantiates it from its class name
  via reflection — don't remove it.

## Spark (multi-version) notes

- The Scala source is **shared, not duplicated**: it lives in `spark/src/main/scala`
  (+ `spark/src/test/scala`). The three subprojects (`spark-3.4_2.12`, `spark-3.5_2.12`,
  `spark-4.0_2.13`) have **no source of their own** — each sets its source set to the
  shared dir and compiles it against a different version: Spark 3.4.4 / 3.5.4 on Scala
  **2.12**, Spark 4.0.2 on Scala **2.13**. Edit the shared source once; don't copy
  changes across variants. `spark/` itself is just an orchestrator (`buildAllVariants`).
- Version-specific code goes in overlay source dirs `spark/src/main/spark-<major.minor>`
  (`spark-3.4`, `spark-3.5`, `spark-4.0`) — that's how per-version Spark API differences
  are handled without `#if`-style branching.
- Because Spark 4.0 compiles on Scala **2.13** and the others on 2.12, a shared-source
  change that compiles on one variant can still break another. Compiling only
  `:spark:spark-3.5_2.12` misses 2.13 breaks — also run
  `./gradlew :spark:spark-4.0_2.13:compileScala`, or build all three with `:spark:build`.
- Formatting is done by the **parent** `:spark` project (`./gradlew :spark:spotlessApply`);
  spotless is disabled in the variant subprojects.

## Testing conventions

- Proto round-trip tests extend `io.substrait.TestBase` and call
  `verifyRoundTrip(Expression)` / `verifyRoundTrip(Rel)` to assert POJO → proto → POJO
  fidelity. See `core/src/test/java/io/substrait/type/proto/DynamicParameterRoundtripTest.java`
  for the canonical pattern.

## Documentation

User-facing documentation lives in `docs/` (Markdown, one file per page) and is built with
**Zensical** (config in `zensical.toml`). Python and the doc tooling are managed with **pixi**,
independent of the Gradle build — the only prerequisite is a `pixi` install.

- Preview locally with `pixi run docs-serve` (live reload); build the static site with
  `pixi run docs-build` (output to `site/`, gitignored). `docs-build` validates internal links,
  so run it before pushing doc changes.
- Pages are grouped under `docs/{core,isthmus,isthmus-cli,spark}/` plus a landing page and
  getting-started. Navigation is **explicit** in `zensical.toml` (`nav`) — when you add a page,
  add it to `nav`.
- **Keep the guide in sync with the code as substrait-java evolves.** When you add or change
  user-facing behavior — a new expression/relation type, a `SubstraitBuilder`/`ExpressionCreator`
  factory, a newly supported function, an isthmus/spark capability, or a CLI flag — update the
  matching `docs/` page in the same PR. (As in source) keep GitHub issue/PR numbers out of the docs.
- **Runnable code samples are pulled from compiled, CI-run tests via `pymdownx.snippets`** — they
  are not hand-written in the Markdown, so they can't silently drift from the API. Backing tests
  live in an `io.substrait…docs` package under each module's test sources: `core`
  (`core/src/test/java/io/substrait/docs/`) and `isthmus`
  (`isthmus/src/test/java/io/substrait/isthmus/docs/`) as JUnit tests, and `spark`
  (`spark/src/test/scala/io/substrait/spark/docs/DocExamplesSuite.scala`, a full round-trip run on
  every variant). A test marks a region with `// --8<-- [start:name]` / `[end:name]`; the Markdown
  references it inside a fenced block, e.g.
  `--8<-- "core/src/test/java/io/substrait/docs/TypesDocTest.java:aliases"`. Edit the **test**, not
  the Markdown, to change a sample; `pixi run docs-build` fails on a missing file or region
  (`check_paths`), and the backing test failing to compile/run fails the Gradle build. `import`
  lines stay literal alongside the include; pure API-signature and resource-dependent snippets
  remain inline.
- CI: `.github/workflows/docs.yml` build-checks docs on every PR and push to `main`;
  `.github/workflows/docs-deploy.yml` publishes versioned docs to the `gh-pages` branch via the
  Zensical-compatible `mike` fork on release tags (the bare `X.Y.Z` tag publishes the minor
  series `X.Y` and updates the `latest` alias). Site: <https://substrait-io.github.io/substrait-java/>.

## Conventions & workflow

- **Conventional commits** are required (CI lints them, and PR title + body must form a
  valid commit message). Scope tags seen in history: `feat(core)`, `feat(pojo)`,
  `feat(isthmus)`, `feat(extensions)`, `build(deps)`, `chore(release)`. A `!` marks a
  breaking change.
- **No GitHub issue/PR references in source** (comments or Javadoc) — they belong in
  commit messages and PR descriptions. `Closes #NNN` in the commit/PR body is fine;
  in the code, describe behavior and spec version (e.g. `spec v0.88.0`) instead.
- Many features track upstream Substrait spec releases (see epic-style issues); a new
  proto message usually needs: POJO + visitor wiring + both proto converters + a
  round-trip test, and often `ExpressionCreator` factories and `dsl/SubstraitBuilder`
  helpers for ergonomics — plus a matching update to the user guide under `docs/`
  (see [Documentation](#documentation)).
- When monitoring PR checks, budget for a long tail: the **macOS `Build Isthmus Native
  Image`** job is the long pole — it `needs:` the `java` + `integration` jobs (so it
  starts late), then AOT-compiles for ~15–20 min on a slower macOS runner. A PR staying
  yellow after the other checks pass usually just means that job is still running, not
  that it's stuck or failing.
