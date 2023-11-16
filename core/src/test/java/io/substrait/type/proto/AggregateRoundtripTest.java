package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import io.substrait.extension.ExtensionCollector;
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

public class AggregateRoundtripTest extends TestBase {

  private void assertAggregateRoundtrip(Expression.AggregationInvocation invocation) {
    var expression = ExpressionCreator.decimal(false, BigDecimal.TEN, 10, 2);
    Expression.StructLiteral literal =
        ImmutableExpression.StructLiteral.builder().from(expression).build();
    var input = VirtualTableScan.builder().addRows(literal).build();
    ExtensionCollector functionCollector = new ExtensionCollector();
    var to = new RelProtoConverter(functionCollector);
    var extensions = defaultExtensionCollection;
    var from = new ProtoRelConverter(functionCollector, extensions);

    var measure =
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

    var aggRel = ImmutableAggregate.builder().input(input).measures(Arrays.asList(measure)).build();
    var protoAggRel = to.toProto(aggRel);
    assertEquals(
        protoAggRel.getAggregate().getMeasuresList().get(0).getMeasure().getInvocation(),
        invocation.toProto());
    assertEquals(protoAggRel, to.toProto(from.from(protoAggRel)));
  }

  @Test
  void aggregateInvocationRoundtrip() throws IOException {
    for (var invocation : Expression.AggregationInvocation.values()) {
      assertAggregateRoundtrip(invocation);
    }
  }
}
