package io.substrait.extendedexpression;

import io.substrait.TestBase;
import io.substrait.expression.*;
import io.substrait.relation.Aggregate;
import io.substrait.relation.AggregateFunctionProtoController;
import io.substrait.relation.ImmutableMeasure;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExtendedExpressionRoundTripTest extends TestBase {
  static final String NAMESPACE = "/functions_arithmetic_decimal.yaml";

  @Test
  public void expressionRoundTrip() throws IOException {
    // create predefined POJO extended expression
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

    ImmutableExpressionReference expressionReference =
        ImmutableExpressionReference.builder()
            .expressionType(
                ImmutableExpressionType.builder().expression(scalarFunctionInvocation).build())
            .addOutputNames("new-column")
            .build();

    List<io.substrait.extendedexpression.ExtendedExpression.ExpressionReference>
        expressionReferences = new ArrayList<>();
    expressionReferences.add(expressionReference);

    ImmutableNamedStruct namedStruct =
        ImmutableNamedStruct.builder()
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

    // pojo initial extended expression
    ImmutableExtendedExpression extendedExpressionPojoInitial =
        ImmutableExtendedExpression.builder()
            .referredExpressions(expressionReferences)
            .baseSchema(namedStruct)
            .build();

    // proto extended expression
    io.substrait.proto.ExtendedExpression extendedExpressionProto =
        new ExtendedExpressionProtoConverter().toProto(extendedExpressionPojoInitial);

    // pojo final extended expression
    io.substrait.extendedexpression.ExtendedExpression extendedExpressionPojoFinal =
        new ProtoExtendedExpressionConverter().from(extendedExpressionProto);

    // validate extended expression pojo initial equals to final roundtrip
    Assertions.assertEquals(extendedExpressionPojoInitial, extendedExpressionPojoFinal);
  }

  @Test
  public void aggregationRoundTrip() throws IOException {
    // create predefined POJO aggregation function
    ImmutableMeasure measure =
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

    ImmutableAggregateFunctionType aggregateFunctionType =
        ImmutableAggregateFunctionType.builder()
            .measure(new AggregateFunctionProtoController(functionCollector).toProto(measure))
            .build();

    ImmutableExpressionReference expressionReference =
        ImmutableExpressionReference.builder()
            .expressionType(aggregateFunctionType)
            .addOutputNames("new-column")
            .build();

    List<io.substrait.extendedexpression.ExtendedExpression.ExpressionReference>
        expressionReferences = new ArrayList<>();
    expressionReferences.add(expressionReference);

    ImmutableNamedStruct namedStruct =
        ImmutableNamedStruct.builder()
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

    // pojo initial aggregation function
    ImmutableExtendedExpression extendedExpressionPojoInitial =
        ImmutableExtendedExpression.builder()
            .referredExpressions(expressionReferences)
            .baseSchema(namedStruct)
            .build();

    // proto aggregation function
    io.substrait.proto.ExtendedExpression extendedExpressionProto =
        new ExtendedExpressionProtoConverter().toProto(extendedExpressionPojoInitial);

    // pojo final aggregation function
    io.substrait.extendedexpression.ExtendedExpression extendedExpressionPojoFinal =
        new ProtoExtendedExpressionConverter().from(extendedExpressionProto);

    // validate aggregation function pojo initial equals to final roundtrip
    Assertions.assertEquals(extendedExpressionPojoInitial, extendedExpressionPojoFinal);
  }

  @Test
  public void expressionAndAggregationRoundTrip() throws IOException {
    // POJO 01
    // create predefined POJO extended expression
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

    ImmutableExpressionReference expressionReferenceExpression =
        ImmutableExpressionReference.builder()
            .expressionType(
                ImmutableExpressionType.builder().expression(scalarFunctionInvocation).build())
            .addOutputNames("new-column")
            .build();

    List<io.substrait.extendedexpression.ExtendedExpression.ExpressionReference>
        expressionReferences = new ArrayList<>();

    // POJO 02
    // create predefined POJO aggregation function
    ImmutableMeasure measure =
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

    ImmutableAggregateFunctionType aggregateFunctionType =
        ImmutableAggregateFunctionType.builder()
            .measure(new AggregateFunctionProtoController(functionCollector).toProto(measure))
            .build();

    ImmutableExpressionReference expressionReferenceAggregation =
        ImmutableExpressionReference.builder()
            .expressionType(aggregateFunctionType)
            .addOutputNames("new-column")
            .build();

    // adding expression
    expressionReferences.add(expressionReferenceExpression);
    // adding aggregation function
    expressionReferences.add(expressionReferenceAggregation);

    ImmutableNamedStruct namedStruct =
        ImmutableNamedStruct.builder()
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

    // pojo initial extended expression + aggregation
    ImmutableExtendedExpression extendedExpressionPojoInitial =
        ImmutableExtendedExpression.builder()
            .referredExpressions(expressionReferences)
            .baseSchema(namedStruct)
            .build();

    // proto extended expression + aggregation
    io.substrait.proto.ExtendedExpression extendedExpressionProto =
        new ExtendedExpressionProtoConverter().toProto(extendedExpressionPojoInitial);

    // pojo final extended expression + aggregation
    io.substrait.extendedexpression.ExtendedExpression extendedExpressionPojoFinal =
        new ProtoExtendedExpressionConverter().from(extendedExpressionProto);

    // validate extended expression + aggregation pojo initial equals to final roundtrip
    Assertions.assertEquals(extendedExpressionPojoInitial, extendedExpressionPojoFinal);
  }
}
