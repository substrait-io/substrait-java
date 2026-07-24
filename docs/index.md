# substrait-java

**substrait-java** is the Java implementation of [Substrait](https://substrait.io/) — a
cross-language specification for relational query plans. It gives you an immutable Java object
model for plans, relations, expressions, and types, bidirectional conversion to and from the
Substrait protobuf wire format, and integrations with Apache Calcite (SQL) and Apache Spark.

If you are new here, start with [Getting started](getting-started.md), then dive into the module
that matches your use case.

## What is Substrait?

Substrait defines a standard, engine-agnostic representation of data-manipulation operations
(scans, filters, projections, joins, aggregations, and so on) together with a catalogue of
functions and types. A plan produced by one system can be consumed by another. substrait-java
lets you produce and consume those plans from the JVM without hand-assembling protobuf.

## Modules

The project is organized into four modules. Each has its own section in this documentation.

### Core

The heart of the project: an immutable POJO model for plans, relations, expressions, and types,
plus converters to and from the Substrait protobuf format. Build plans directly in Java with the
`SubstraitBuilder` DSL, then serialize them for any Substrait-aware engine.

→ [Core documentation](core/index.md)

### Isthmus

SQL ⇄ Substrait conversion built on Apache Calcite. Turn a SQL query into a Substrait plan, turn
a SQL expression into a Substrait extended expression, and convert plans back to SQL or to Calcite
relational trees.

→ [Isthmus documentation](isthmus/index.md)

### Isthmus CLI

A command-line tool — the `isthmus` native binary — that converts SQL queries and expressions to
Substrait from the shell, without writing any Java.

→ [Isthmus CLI documentation](isthmus-cli/index.md)

### Spark

Convert Apache Spark logical plans to and from Substrait. Produce a portable plan from a Spark
query and execute a Substrait plan on Spark. Published for multiple Spark and Scala versions.

→ [Spark documentation](spark/index.md)

## Getting help

- [Substrait specification](https://substrait.io/) — the parent project.
- [Substrait community](https://substrait.io/community/) — how to get involved.
- [substrait-java on GitHub](https://github.com/substrait-io/substrait-java) — source, issues,
  and releases.
