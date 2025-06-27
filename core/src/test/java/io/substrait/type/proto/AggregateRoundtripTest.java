package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionOption;
import io.substrait.extension.ExtensionCollector;
import io.substrait.relation.Aggregate;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.RelProtoConverter;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class AggregateRoundtripTest extends TestBase {

  private void assertAggregateRoundtrip(Expression.AggregationInvocation invocation) {
    var expression = ExpressionCreator.decimal(false, BigDecimal.TEN, 10, 2);
    Expression.StructLiteral literal =
        Expression.StructLiteral.builder().addFields(expression).build();
    var input =
        VirtualTableScan.builder()
            .initialSchema(NamedStruct.of(Arrays.asList("decimal"), R.struct(R.decimal(10, 2))))
            .addRows(literal)
            .build();
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
                    .options(
                        Arrays.asList(
                            FunctionOption.builder()
                                .name("option")
                                .addValues("VALUE1", "VALUE2")
                                .build()))
                    .sort(
                        Arrays.asList(
                            Expression.SortField.builder()
                                // SORT BY decimal
                                .expr(b.fieldReference(input, 0))
                                .direction(Expression.SortDirection.ASC_NULLS_LAST)
                                .build()))
                    .build())
            .build();

    var aggRel = Aggregate.builder().input(input).measures(Arrays.asList(measure)).build();
    var protoAggRel = to.toProto(aggRel);
    assertEquals(
        protoAggRel.getAggregate().getMeasuresList().get(0).getMeasure().getInvocation(),
        invocation.toProto());
    assertEquals(protoAggRel, to.toProto(from.from(protoAggRel)));
  }

  @Test
  void aggregateInvocationRoundtrip() {
    for (var invocation : Expression.AggregationInvocation.values()) {
      assertAggregateRoundtrip(invocation);
    }
  }
}
