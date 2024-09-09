# Substrait Java

Substrait Java is a project that makes it easier to build [Substrait](https://substrait.io/) plans through Java. The project has two main parts:
1) **Core** is the module that supports building Substrait plans directly through Java. This is much easier than manipulating the Substrait protobuf directly. It has no direct support for going from SQL to Substrait (that's covered by the second part)
2) **Isthmus** is the module that allows going from SQL to a Substrait plan. Both Java APIs and a top level script for conversion are present. Not all SQL is supported yet by this module, but a lot is. For example, all of the TPC-H queries and all but a few of the TPC-DS queries are translatable.
3) **Spark** is the module that provides an API for translating a Substrait plan to and from a Spark query plan.  The most commonly used logical relations are supported, including those generated from all of the TPC-H queries, but there are currently some gaps in support that prevent all of the TPC-DS queries from being translatable.

## Building
After you've cloned the project through git, Substrait Java is built with a tool called [Gradle](https://gradle.org/). To build, execute the following:
```
./gradlew build
```

To build the Isthmus executable that enables Substrait plans to be generated for a SQL statement, execute the following:
```
./gradlew nativeImage
```

## Getting Started
A good way to get started is to experiment with building Substrait plans for your own SQL. To do that, you can use the isthmus executable as described [here](https://github.com/substrait-io/substrait-java/blob/main/isthmus/README.md).

Another way to get an idea of what Substrait plans look like is to use our script that generates Substrait plans for all the TPC-H queries:
```
./isthmus-cli/src/test/script/tpch_smoke.sh
```

## Logging
This project uses the [SLF4J](https://www.slf4j.org/) logging API. If you are using the Substrait Java core component as a dependency in your own project, you should consider including an appropriate [SLF4J logging provider](https://www.slf4j.org/manual.html#swapping) in your runtime classpath. If you do not include a logging provider in your classpath, the code will still work correctly but you will not receive any logging output and might see the following warning in your standard error output:

```
SLF4J(W): No SLF4J providers were found.
SLF4J(W): Defaulting to no-operation (NOP) logger implementation
SLF4J(W): See https://www.slf4j.org/codes.html#noProviders for further details.
```

## Examples

The [examples](./examples) folder contains examples on using Substrait with Java; please check each example for specific details of the requirements and how to run. The examples are aimed to be tested within the github workflow; depending on the setup required it might be only possible to validate compilation.

- [Substrait-Spark](./examples/subtrait-spark/README.md) Using Substrait to produce and consume plans within Apache Spark

## Getting Involved
To learn more, head over [Substrait](https://substrait.io/), our parent project and join our [community](https://substrait.io/community/)
