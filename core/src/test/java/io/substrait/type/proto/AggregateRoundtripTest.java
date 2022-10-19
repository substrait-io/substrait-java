package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.proto.FunctionCollector;
import io.substrait.function.SimpleExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.relation.Aggregate;
import io.substrait.relation.ImmutableAggregate;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.RelProtoConverter;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class AggregateRoundtripTest {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AggregateRoundtripTest.class);

  private void assertAggregateRoundtrip(AggregateFunction.AggregationInvocation invocation)
      throws IOException {
    Expression.DecimalLiteral expression = ExpressionCreator.decimal(false, BigDecimal.TEN, 10, 2);
    Expression.StructLiteral literal =
        ImmutableExpression.StructLiteral.builder().from(expression).build();
    io.substrait.relation.ImmutableVirtualTableScan input =
        VirtualTableScan.builder().addRows(literal).build();
    FunctionCollector functionCollector = new FunctionCollector();
    RelProtoConverter to = new RelProtoConverter(functionCollector);
    SimpleExtension.ExtensionCollection extensions = SimpleExtension.loadDefaults();
    ProtoRelConverter from = new ProtoRelConverter(functionCollector, extensions);

    io.substrait.relation.ImmutableMeasure measure =
        Aggregate.Measure.builder()
            .function(
                AggregateFunctionInvocation.builder()
                    .arguments(Collections.emptyList())
                    .declaration(extensions.aggregateFunctions().get(0))
                    .outputType(TypeCreator.of(false).I64)
                    .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                    .invocation(invocation)
                    .build())
            .build();

    ImmutableAggregate aggRel =
        ImmutableAggregate.builder().input(input).measures(Arrays.asList(measure)).build();
    io.substrait.proto.Rel protoAggRel = to.toProto(aggRel);
    assertEquals(
        protoAggRel.getAggregate().getMeasuresList().get(0).getMeasure().getInvocation(),
        invocation);
    assertEquals(protoAggRel, to.toProto(from.from(protoAggRel)));
  }

  @Test
  void aggregateInvocationRoundtrip() throws IOException {
    for (AggregateFunction.AggregationInvocation invocation :
        AggregateFunction.AggregationInvocation.values()) {
      if (invocation != AggregateFunction.AggregationInvocation.UNRECOGNIZED) {
        assertAggregateRoundtrip(invocation);
      }
    }
  }
}
