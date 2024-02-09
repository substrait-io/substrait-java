package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.ImmutableWindowBound;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.Rel;
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
    SimpleExtension.ExtensionCollection extensions = defaultExtensionCollection;
    ProtoRelConverter from = new ProtoRelConverter(functionCollector, extensions);

    ImmutableConsistentPartitionWindow aggRel =
        ImmutableConsistentPartitionWindow.builder()
            .input(input)
            .windowFunctions(
                Arrays.asList(
                    Expression.WindowRelFunctionInvocation.builder()
                        .declaration(defaultExtensionCollection.windowFunctions().get(0))
                        .arguments(Collections.emptyList())
                        .options(Collections.emptyMap())
                        .outputType(TypeCreator.of(false).I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(invocation)
                        .lowerBound(ImmutableWindowBound.Unbounded.builder().build())
                        .upperBound(ImmutableWindowBound.Unbounded.builder().build())
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

    Rel protoWindowRel = to.toProto(aggRel);
    assertEquals(protoWindowRel, to.toProto(from.from(protoWindowRel)));
    assertEquals(
        protoWindowRel.getWindow().getWindowFunctions(0).getInvocation(), invocation.toProto());
  }

  @Test
  void aggregateInvocationRoundtrip() throws IOException {
    for (Expression.AggregationInvocation invocation : Expression.AggregationInvocation.values()) {
      assertWindowFunctionRoundtrip(invocation);
    }
  }
}
