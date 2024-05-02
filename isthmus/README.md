# Isthmus

## Overview

Substrait Isthmus is a Java library which enables serializing SQL queries to [Substrait Protobuf](https://substrait.io/serialization/binary_serialization/) and SQL expressions to [Extended Expressions](https://substrait.io/expressions/extended_expression/) using
the Calcite SQL compiler. Optionally, you can leverage the Calcite RelNode to Substrait Plan translator as an IR translation.

The capability provided by this library can be accessed using a command-line interface, provided by [isthmus-cli](../isthmus-cli).
