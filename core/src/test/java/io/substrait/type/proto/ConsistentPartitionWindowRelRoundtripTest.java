package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ImmutableWindowBound;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.relation.ImmutableConsistentPartitionWindow;
import io.substrait.relation.Rel;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class ConsistentPartitionWindowRelRoundtripTest extends TestBase {

  @Test
  void consistentPartitionWindowRoundtrip() {
    var windowFunctionDeclaration =
        defaultExtensionCollection.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input =
        b.namedScan(
            Arrays.asList("test"),
            Arrays.asList("a", "b", "c"),
            Arrays.asList(R.I64, R.I16, R.I32));
    Rel rel1 =
        ImmutableConsistentPartitionWindow.builder()
            .input(input)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .declaration(windowFunctionDeclaration)
                        // lead(a)
                        .arguments(Arrays.asList(b.fieldReference(input, 0)))
                        .options(Collections.emptyMap())
                        .outputType(R.I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(Expression.AggregationInvocation.ALL)
                        .lowerBound(ImmutableWindowBound.Unbounded.UNBOUNDED)
                        .upperBound(ImmutableWindowBound.Following.CURRENT_ROW)
                        .boundsType(Expression.WindowBoundsType.RANGE)
                        .build()))
            // PARTITION BY b
            .partitionExpressions(Arrays.asList(b.fieldReference(input, 1)))
            .sorts(
                Arrays.asList(
                    Expression.SortField.builder()
                        // SORT BY c
                        .expr(b.fieldReference(input, 2))
                        .direction(Expression.SortDirection.ASC_NULLS_FIRST)
                        .build()))
            .build();

    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel1);
    io.substrait.relation.Rel rel2 = protoRelConverter.from(protoRel);
    assertEquals(rel1, rel2);
  }
}
