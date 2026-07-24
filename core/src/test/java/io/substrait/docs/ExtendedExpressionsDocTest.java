package io.substrait.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.TestBase;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.extendedexpression.ExtendedExpression;
import io.substrait.extendedexpression.ExtendedExpressionProtoConverter;
import io.substrait.extendedexpression.ImmutableAggregateFunctionReference;
import io.substrait.extendedexpression.ImmutableExpressionReference;
import io.substrait.extendedexpression.ImmutableExtendedExpression;
import io.substrait.extendedexpression.ProtoExtendedExpressionConverter;
import io.substrait.relation.Aggregate;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Backs the code samples in {@code docs/core/extended-expressions.md}. Regions marked with {@code
 * // --8<-- [start:name]} / {@code [end:name]} are pulled into the docs via {@code --8<--} snippet
 * includes.
 */
class ExtendedExpressionsDocTest extends TestBase {

  @Test
  void extendedExpressionExample() throws Exception {
    // --8<-- [start:expression-reference]
    // an expression referring to the base schema, named "new-column"
    ImmutableExpressionReference literalRef =
        ImmutableExpressionReference.builder()
            .expression(ExpressionCreator.i32(false, 76))
            .addOutputNames("new-column")
            .build();
    // --8<-- [end:expression-reference]
    assertNotNull(literalRef);

    // --8<-- [start:field-reference]
    ImmutableExpressionReference fieldRef =
        ImmutableExpressionReference.builder()
            .expression(
                ImmutableFieldReference.builder()
                    .addSegments(FieldReference.StructField.of(0))
                    .type(TypeCreator.REQUIRED.decimal(10, 2))
                    .build())
            .addOutputNames("new-column")
            .build();
    // --8<-- [end:field-reference]
    assertNotNull(fieldRef);

    Aggregate.Measure measure = sb.countStar();
    // --8<-- [start:aggregate-reference]
    ImmutableAggregateFunctionReference aggRef =
        ImmutableAggregateFunctionReference.builder()
            .measure(measure) // an Aggregate.Measure
            .addOutputNames("new-column")
            .build();
    // --8<-- [end:aggregate-reference]
    assertNotNull(aggRef);

    // --8<-- [start:assemble]
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
    // --8<-- [end:assemble]
    assertNotNull(extendedExpression);

    // --8<-- [start:serialize]
    io.substrait.proto.ExtendedExpression proto =
        new ExtendedExpressionProtoConverter().toProto(extendedExpression);

    ExtendedExpression roundTripped = new ProtoExtendedExpressionConverter().from(proto);
    // --8<-- [end:serialize]
    assertNotNull(roundTripped);
  }
}
