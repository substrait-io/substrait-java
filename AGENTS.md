# AGENTS.md

Entry point for AI agents working in the `substrait-java` repository. Read the shared,
human-facing docs first, then keep the codebase-specific notes below in mind.

## Start here

- **[`readme.md`](readme.md)** — what the project is, the module overview, and how to build
  and run it.
- **[`CONTRIBUTING.md`](CONTRIBUTING.md)** — commit conventions, the style guide, and the
  build / test / format / PMD command mechanics plus the JDK 17 daemon and GraalVM
  native-image setup.

For GitHub work (issues, PRs), use the `gh` CLI.

## Module layout

| Module | Path | Purpose |
| --- | --- | --- |
| `:core` | `core/` | POJO model, proto converters, function extension handling. The heart of the project. |
| `:isthmus` | `isthmus/` | Calcite SQL ⇄ Substrait conversion. |
| `:spark` | `spark/` + variants `spark-3.4_2.12`, `spark-3.5_2.12`, `spark-4.0_2.13` | Spark plan ⇄ Substrait (Scala). Source shared in `spark/src`; each variant compiles it against a different Spark/Scala version — see Spark notes. |
| `:examples` | `examples/isthmus-api`, `examples/substrait-spark` | Runnable examples. |
| `:isthmus-cli` | `isthmus-cli/` | CLI over `:isthmus`, compiled to a GraalVM **native image** (`nativeCompile`) — the `isthmus` binary + smoke tests. |
| `build-logic` | `build-logic/` | Gradle **included build**: Kotlin-DSL convention plugins (`substrait.java-conventions` → shared Java config + PMD). Has its own `gradle.properties`; does not inherit the root's. |

The spec inputs `:core` needs are **not** generated or vendored in this repo — they come
from the `substrait-packaging` Maven artifacts (`io.substrait:{protobuf,antlr,extensions}`),
pinned via the `substrait-packaging` version in `gradle/libs.versions.toml`. Don't look for
these under `core/src`:

- **Proto**: compiled `io.substrait.proto.*` bindings ship in the `protobuf` artifact
  (`api(libs.substrait.protobuf)` in `core/build.gradle.kts`).
- **ANTLR parsers**: the generated `io.substrait.antlr` parser ships in the `antlr`
  artifact (shadowed so its ANTLR runtime is relocated).
- **Standard extension YAMLs**, **function test cases**, **validation schemas**
  (`simple_extensions_schema.yaml`, `dialect_schema.yaml`), and the **per-section dialect
  fixtures**: all ship in the `extensions` artifact as classpath resources under
  `substrait/` (e.g. `/substrait/extensions/functions_arithmetic.yaml`,
  `/substrait/text/dialect_schema.yaml`, `/substrait/dialects/tests/*_test.yaml`).

These resources are owned by the upstream spec — to change them, update the spec and cut a
new `substrait-packaging` release, then bump the `substrait-packaging` version in the
catalog. The spec version reported by `io.substrait.SubstraitVersion.VERSION` is derived
from that same catalog version (with any `-SNAPSHOT` suffix stripped) in
`core/build.gradle.kts`, so it stays in lockstep with the artifacts.

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
- **`RelCommon` data** (emit mapping, hint, rel anchor, common advanced extension) is converted by
  one helper per direction: `ProtoRelConverter.applyRelCommon(rel, relCommon)` on the way in and
  `RelProtoConverter.common(Rel)` on the way out. Both are still *called* per relation, so both
  halves are easy to forget: a new `newXxx` must route its result through `applyRelCommon`, and a
  new `visit` must call `.setCommon(common(rel))`. `RelCommonRoundtripTest` round-trips every
  relation in the shared `RelSamples` fixture (see Testing conventions), so a relation added without
  a sample fails `RelSamplesTest` — but that fixture is keyed on POJO types, not on proto `oneof`
  cases, so a new case mapping to an existing POJO type slips through. `applyRelCommon`
  runs *after* `build()`, so a relation with a `@Value.Check` on one of these fields must also set
  it on its builder (see `newLateralJoin`). Every modeled relation message has a `common` field;
  `ReferenceRel` does not, but no POJO models it.
- POJO types are created with `TypeCreator.REQUIRED` / `TypeCreator.NULLABLE`.

## Building and testing

Run `./gradlew build` and make sure it passes before pushing — narrower tasks skip checks that
CI runs, and a `:core` change can break the visitor implementors in the other modules. See
[`CONTRIBUTING.md`](CONTRIBUTING.md#building-and-testing).

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
- A test that has to cover *every* relation rather than a hand-picked few uses the shared
  `core/src/test/java/io/substrait/utils/RelSamples.java` fixture — one sample per `RelVisitor`
  type, plus a `relTypes()` reflecting over the visitor's `visit` overloads so `RelSamplesTest`
  fails when a new relation has no sample. Its details are `StringHolder`s, so a consumer extends
  `StringHolderRoundtripTestBase` rather than `TestBase` directly. Layer the per-relation data under
  test onto the samples (`Rel.withXxx`, or the `AdvancedExtension` slots
  `withAdvancedExtensions(...)` fills in) and assert that they carry it, so the round trip cannot
  pass vacuously.

## Conventions & workflow

- **Keep PR descriptions high-signal.** The PR title and body together become the
  squash-merge commit message that `semantic-release` uses to build `CHANGELOG.md`, so they
  must together form a valid conventional commit (see
  [`CONTRIBUTING.md`](CONTRIBUTING.md#commit-conventions)). Beyond that, leave out the noise
  agents tend to add:
  - **Lists of files touched** — they're in the diff.
  - **Claims that CI-verified things pass** — e.g. "tests pass", "spotless clean". If they
    didn't, the checks would be red.
  - **Process notes that are already implicit** — e.g. "opened as draft pending review".

  Do include the rationale, and for spec-tracking changes the spec version (e.g.
  `spec v0.88.0`). Keep commit bodies free of git trailers (`Signed-off-by`,
  `Co-authored-by`, tool-attribution lines) — `semantic-release` builds the changelog from
  the commit message and history here doesn't carry them.
- **No GitHub issue/PR references in source** (comments or Javadoc) — they belong in
  commit messages and PR descriptions. `Closes #NNN` in the commit/PR body is fine;
  in the code, describe behavior and spec version (e.g. `spec v0.88.0`) instead.
- Many features track upstream Substrait spec releases (see epic-style issues); a new
  proto message usually needs: POJO + visitor wiring + both proto converters + a
  round-trip test, and often `ExpressionCreator` factories and `dsl/SubstraitBuilder`
  helpers for ergonomics.
- The macOS native image is not built on PRs (only Linux is), so macOS-specific native
  regressions surface on `main` or the weekly `native-image-macos.yml` run, not on the PR.
