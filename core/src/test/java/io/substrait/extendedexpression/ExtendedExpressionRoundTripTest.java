package io.substrait.extendedexpression;

import io.substrait.TestBase;
import io.substrait.expression.*;
import io.substrait.relation.Aggregate;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ExtendedExpressionRoundTripTest extends TestBase {
  static final String NAMESPACE = "/functions_arithmetic_decimal.yaml";

  private static Stream<Arguments> expressionReferenceProvider() {
    return Stream.of(
        Arguments.of(getI32LiteralExpression()),
        Arguments.of(getFieldReferenceExpression()),
        Arguments.of(getScalarFunctionExpression()),
        Arguments.of(getImmutableAggregateFunctionReference()));
  }

  @ParameterizedTest
  @MethodSource("expressionReferenceProvider")
  public void testRoundTrip(ImmutableExpressionReference expressionReference) throws IOException {
    List<ExtendedExpression.ExpressionReferenceBase> expressionReferences = new ArrayList<>();
    expressionReferences.add(expressionReference);
    ImmutableNamedStruct namedStruct = getImmutableNamedStruct();
    assertExtendedExpressionOperation(expressionReferences, namedStruct);
  }

  private static ImmutableExpressionReference getI32LiteralExpression() {
    return ImmutableExpressionReference.builder()
        .expression(ExpressionCreator.i32(false, 76))
        .addOutputNames("new-column")
        .build();
  }

  private static ImmutableExpressionReference getFieldReferenceExpression() {
    return ImmutableExpressionReference.builder()
        .expression(
            ImmutableFieldReference.builder()
                .addSegments(FieldReference.StructField.of(0))
                .type(TypeCreator.REQUIRED.decimal(10, 2))
                .build())
        .addOutputNames("new-column")
        .build();
  }

  private static ImmutableExpressionReference getScalarFunctionExpression() {
    Expression.ScalarFunctionInvocation scalarFunctionInvocation =
        b.scalarFn(
            NAMESPACE,
            "add:dec_dec",
            TypeCreator.REQUIRED.BOOLEAN,
            ImmutableFieldReference.builder()
                .addSegments(FieldReference.StructField.of(0))
                .type(TypeCreator.REQUIRED.decimal(10, 2))
                .build(),
            ExpressionCreator.i32(false, 183));

    return ImmutableExpressionReference.builder()
        .expression(scalarFunctionInvocation)
        .addOutputNames("new-column")
        .build();
  }

  private static ImmutableAggregateFunctionReference getImmutableAggregateFunctionReference() {
    Aggregate.Measure measure =
        Aggregate.Measure.builder()
            .function(
                AggregateFunctionInvocation.builder()
                    .arguments(Collections.emptyList())
                    .declaration(defaultExtensionCollection.aggregateFunctions().get(0))
                    .outputType(TypeCreator.of(false).I64)
                    .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                    .invocation(Expression.AggregationInvocation.ALL)
                    .build())
            .build();

    return ImmutableAggregateFunctionReference.builder()
        .measure(measure)
        .addOutputNames("new-column")
        .build();
  }

  private static ImmutableNamedStruct getImmutableNamedStruct() {
    return ImmutableNamedStruct.builder()
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
  }

  private static void assertExtendedExpressionOperation(
      List<ExtendedExpression.ExpressionReferenceBase> expressionReferences,
      ImmutableNamedStruct namedStruct)
      throws IOException {

    // initial pojo
    ExtendedExpression extendedExpressionPojoInitial =
        ImmutableExtendedExpression.builder()
            .referredExpressions(expressionReferences)
            .baseSchema(namedStruct)
            .build();

    // proto
    io.substrait.proto.ExtendedExpression extendedExpressionProto =
        new ExtendedExpressionProtoConverter().toProto(extendedExpressionPojoInitial);

    // get pojo from proto
    ExtendedExpression extendedExpressionPojoFinal =
        new ProtoExtendedExpressionConverter().from(extendedExpressionProto);

    Assertions.assertEquals(extendedExpressionPojoInitial, extendedExpressionPojoFinal);
  }
}
