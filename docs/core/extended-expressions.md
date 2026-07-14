# Extended expressions

Not every use case needs a whole plan. Sometimes you want to describe one or more
expressions — a computed column, a filter predicate, an aggregate — to be
evaluated against a known input schema, without a surrounding relational tree.
Substrait models this with an **extended expression**, and the API lives in
`io.substrait.extendedexpression`.

An `ExtendedExpression` bundles:

- a list of **referred expressions**, each named for its output; and
- a **base schema** (a `NamedStruct`) that the expressions reference.

## The POJO

`ExtendedExpression` is built through its `builder()`. Each entry is an
`ExpressionReferenceBase`, of which there are two concrete kinds:

- `ExpressionReference` — wraps a regular `Expression`.
- `AggregateFunctionReference` — wraps an `Aggregate.Measure`.

```java
import io.substrait.expression.ExpressionCreator;
import io.substrait.extendedexpression.ImmutableExpressionReference;

// an expression referring to the base schema, named "new-column"
ImmutableExpressionReference literalRef =
    ImmutableExpressionReference.builder()
        .expression(ExpressionCreator.i32(false, 76))
        .addOutputNames("new-column")
        .build();
```

A field reference works the same way — it just points into the base schema:

```java
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.type.TypeCreator;

ImmutableExpressionReference fieldRef =
    ImmutableExpressionReference.builder()
        .expression(
            ImmutableFieldReference.builder()
                .addSegments(FieldReference.StructField.of(0))
                .type(TypeCreator.REQUIRED.decimal(10, 2))
                .build())
        .addOutputNames("new-column")
        .build();
```

An aggregate is expressed as an `AggregateFunctionReference` wrapping a measure:

```java
import io.substrait.extendedexpression.ImmutableAggregateFunctionReference;
import io.substrait.relation.Aggregate;

ImmutableAggregateFunctionReference aggRef =
    ImmutableAggregateFunctionReference.builder()
        .measure(measure)              // an Aggregate.Measure
        .addOutputNames("new-column")
        .build();
```

## Assembling with a base schema

Provide the base schema as a `NamedStruct` and add the references:

```java
import io.substrait.extendedexpression.ImmutableExtendedExpression;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;

NamedStruct baseSchema =
    NamedStruct.builder()
        .addNames("N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT")
        .struct(
            Type.Struct.builder()
                .nullable(false)
                .addFields(
                    TypeCreator.REQUIRED.decimal(10, 2),
                    TypeCreator.REQUIRED.STRING,
                    TypeCreator.REQUIRED.decimal(10, 2),
                    TypeCreator.REQUIRED.STRING)
                .build())
        .build();

ImmutableExtendedExpression extendedExpression =
    ImmutableExtendedExpression.builder()
        .referredExpressions(List.of(literalRef))
        .baseSchema(baseSchema)
        .build();
```

## Serialization

Extended expressions have their own converter pair, mirroring the plan
converters described in [Serialization](serialization.md):

- `ExtendedExpressionProtoConverter.toProto(...)` — POJO to
  `io.substrait.proto.ExtendedExpression`.
- `ProtoExtendedExpressionConverter.from(...)` — proto back to POJO.

```java
import io.substrait.extendedexpression.ExtendedExpressionProtoConverter;
import io.substrait.extendedexpression.ProtoExtendedExpressionConverter;

io.substrait.proto.ExtendedExpression proto =
    new ExtendedExpressionProtoConverter().toProto(extendedExpression);

ExtendedExpression roundTripped =
    new ProtoExtendedExpressionConverter().from(proto);
```

Both converters default to `DefaultExtensionCatalog.DEFAULT_COLLECTION`; pass a
custom `SimpleExtension.ExtensionCollection` to
`ProtoExtendedExpressionConverter` when the expressions reference custom
functions. The proto message can then be encoded to bytes or JSON exactly like a
`Plan`.

## Related

- Build the expressions and measures used here in
  [Expressions & literals](expressions.md).
- Resolve function declarations in [Function & type extensions](extensions.md).
