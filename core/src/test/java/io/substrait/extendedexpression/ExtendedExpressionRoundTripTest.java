package io.substrait.extendedexpression;

import io.substrait.TestBase;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.*;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.relation.Aggregate;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ExtendedExpressionRoundTripTest extends TestBase {

  private static Stream<Arguments> expressionReferenceProvider() {
    return Stream.of(
        Arguments.of(getI32LiteralExpression()),
        Arguments.of(getFieldReferenceExpression()),
        Arguments.of(getScalarFunctionExpression()),
        Arguments.of(getAggregateFunctionReference()));
  }

  @ParameterizedTest
  @MethodSource("expressionReferenceProvider")
  public void testRoundTrip(ExtendedExpression.ExpressionReferenceBase expressionReference) {
    List<ExtendedExpression.ExpressionReferenceBase> expressionReferences = new ArrayList<>();
    expressionReferences.add(expressionReference);
    NamedStruct namedStruct = getImmutableNamedStruct();
    assertExtendedExpressionOperation(expressionReferences, namedStruct);
  }

  @Test
  public void getNoExpressionDefined() {
    IllegalStateException illegalStateException =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> ImmutableExpressionReference.builder().addOutputNames("new-column").build());
    Assertions.assertTrue(
        illegalStateException
            .getMessage()
            .contains(
                "Cannot build ExpressionReference, some of required attributes are not set [expression]"));
  }

  @Test
  public void getNoAggregateFunctionDefined() {
    IllegalStateException illegalStateException =
        Assertions.assertThrows(
            IllegalStateException.class,
            () ->
                ImmutableAggregateFunctionReference.builder().addOutputNames("new-column").build());
    Assertions.assertTrue(
        illegalStateException
            .getMessage()
            .contains(
                "Cannot build AggregateFunctionReference, some of required attributes are not set [measure]"));
  }

  private static ImmutableExpressionReference getI32LiteralExpression() {
    return io.substrait.extendedexpression.ImmutableExpressionReference.builder()
        .expression(ExpressionCreator.i32(false, 76))
        .addOutputNames("new-column")
        .build();
  }

  private static ImmutableExpressionReference getFieldReferenceExpression() {
    return io.substrait.extendedexpression.ImmutableExpressionReference.builder()
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
        new SubstraitBuilder(defaultExtensionCollection)
            .scalarFn(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL,
                "add:dec_dec",
                TypeCreator.REQUIRED.BOOLEAN,
                ImmutableFieldReference.builder()
                    .addSegments(FieldReference.StructField.of(0))
                    .type(TypeCreator.REQUIRED.decimal(10, 2))
                    .build(),
                ExpressionCreator.i32(false, 183));

    return io.substrait.extendedexpression.ImmutableExpressionReference.builder()
        .expression(scalarFunctionInvocation)
        .addOutputNames("new-column")
        .build();
  }

  private static ImmutableAggregateFunctionReference getAggregateFunctionReference() {
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

  private static NamedStruct getImmutableNamedStruct() {
    return NamedStruct.builder()
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
      NamedStruct namedStruct) {

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
