# Isthmus CLI

The Isthmus CLI is a native command-line tool, named `isthmus`, that drives the
[Isthmus](../isthmus/index.md) library from the shell. It uses the Apache Calcite SQL
compiler to convert:

- **SQL queries** into a Substrait [`Plan`](https://substrait.io/serialization/binary_serialization/), and
- **SQL expressions** into a Substrait [`ExtendedExpression`](https://substrait.io/expressions/extended_expression/).

The binary is a [GraalVM](https://www.graalvm.org/) native image built with
[picocli](https://picocli.info/), so it starts instantly and has no JVM or classpath to
manage. It is ideal for experimenting with Substrait, inspecting the plan a given SQL
statement produces, and scripting conversions in CI or shell pipelines.

## CLI vs. the Isthmus Java API

The CLI is a thin wrapper around the same conversion classes documented under
[Isthmus](../isthmus/index.md) (`SqlToSubstrait` and `SqlExpressionToSubstrait`). Choose
based on how you want to work:

- **Use the CLI** when you want a quick, zero-setup way to turn SQL into Substrait from a
  terminal or shell script, to eyeball the generated plan, or to generate fixtures for
  tests and documentation.
- **Use the [Isthmus Java API](../isthmus/index.md)** when you are embedding SQL-to-Substrait
  conversion in an application, need the resulting POJO model for further processing, or
  want to customize the conversion (custom type systems, function extension collections,
  and so on).

Both paths produce the same Substrait output for the same input.

## On this page set

| Page | What it covers |
| --- | --- |
| [Install & build](install.md) | Download a prebuilt release binary or build `isthmus` yourself with GraalVM. |
| [Usage](usage.md) | Every flag and argument, defaults, and how the tool decides between plan and expression conversion. |
| [Examples](examples.md) | Worked commands with (abbreviated) output, plus the runnable smoke-test scripts. |
