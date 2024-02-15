package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.ImmutableWindowBound;
import io.substrait.extension.ExtensionCollector;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.relation.ImmutableConsistentPartitionWindow;
import io.substrait.relation.ImmutableVirtualTableScan;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.RelProtoConverter;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class WindowFunctionRoundtripTest extends TestBase {

  private void assertWindowFunctionRoundtrip(Expression.AggregationInvocation invocation) {
    Expression.DecimalLiteral expression = ExpressionCreator.decimal(false, BigDecimal.TEN, 10, 2);
    Expression.StructLiteral literal =
        ImmutableExpression.StructLiteral.builder().from(expression).build();
    ImmutableVirtualTableScan input = VirtualTableScan.builder().addRows(literal).build();
    ExtensionCollector functionCollector = new ExtensionCollector();
    RelProtoConverter to = new RelProtoConverter(functionCollector);
    ProtoRelConverter from = new ProtoRelConverter(functionCollector, defaultExtensionCollection);

    ImmutableConsistentPartitionWindow aggRel =
        ImmutableConsistentPartitionWindow.builder()
            .input(input)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .declaration(defaultExtensionCollection.windowFunctions().get(0))
                        .arguments(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .outputType(TypeCreator.of(true).I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(invocation)
                        .lowerBound(ImmutableWindowBound.Unbounded.UNBOUNDED)
                        .upperBound(ImmutableWindowBound.Following.CURRENT_ROW)
                        .boundsType(Expression.WindowBoundsType.RANGE)
                        .build()))
            .partitionExpressions(Arrays.asList(expression))
            .sorts(
                Arrays.asList(
                    Expression.SortField.builder()
                        .expr(expression)
                        .from(b.sortField(expression, Expression.SortDirection.ASC_NULLS_FIRST))
                        .build()))
            .build();

    io.substrait.proto.Rel protoRel = to.toProto(aggRel);
    io.substrait.relation.Rel aggRel2 = from.from(protoRel);
    assertEquals(aggRel, aggRel2);
  }

  @Test
  void aggregateInvocationRoundtrip() throws IOException {
    for (Expression.AggregationInvocation invocation : Expression.AggregationInvocation.values()) {
      assertWindowFunctionRoundtrip(invocation);
    }
  }
}
