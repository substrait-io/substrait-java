package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionOption;
import io.substrait.expression.WindowBound;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.relation.Rel;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class ConsistentPartitionWindowRelRoundtripTest extends TestBase {

  @Test
  void consistentPartitionWindowRoundtripSingle() {
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
        ConsistentPartitionWindow.builder()
            .input(input)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .declaration(windowFunctionDeclaration)
                        // lead(a)
                        .arguments(Arrays.asList(b.fieldReference(input, 0)))
                        .options(
                            Arrays.asList(
                                FunctionOption.builder()
                                    .name("option")
                                    .addValues("VALUE1", "VALUE2")
                                    .build()))
                        .outputType(R.I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(Expression.AggregationInvocation.ALL)
                        .lowerBound(WindowBound.Unbounded.UNBOUNDED)
                        .upperBound(WindowBound.Following.CURRENT_ROW)
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

    // Make sure that the record types match I64, I16, I32 and then the I64 from the window
    // function.
    assertEquals(rel2.getRecordType().fields(), Arrays.asList(R.I64, R.I16, R.I32, R.I64));
  }

  @Test
  void consistentPartitionWindowRoundtripMulti() {
    var windowFunctionLeadDeclaration =
        defaultExtensionCollection.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    var windowFunctionLagDeclaration =
        defaultExtensionCollection.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input =
        b.namedScan(
            Arrays.asList("test"),
            Arrays.asList("a", "b", "c"),
            Arrays.asList(R.I64, R.I16, R.I32));
    Rel rel1 =
        ConsistentPartitionWindow.builder()
            .input(input)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .declaration(windowFunctionLeadDeclaration)
                        // lead(a)
                        .arguments(Arrays.asList(b.fieldReference(input, 0)))
                        .options(
                            Arrays.asList(
                                FunctionOption.builder()
                                    .name("option")
                                    .addValues("VALUE1", "VALUE2")
                                    .build()))
                        .outputType(R.I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(Expression.AggregationInvocation.ALL)
                        .lowerBound(WindowBound.Unbounded.UNBOUNDED)
                        .upperBound(WindowBound.Following.CURRENT_ROW)
                        .boundsType(Expression.WindowBoundsType.RANGE)
                        .build(),
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .declaration(windowFunctionLagDeclaration)
                        // lag(a)
                        .arguments(Arrays.asList(b.fieldReference(input, 0)))
                        .options(
                            Arrays.asList(
                                FunctionOption.builder()
                                    .name("option")
                                    .addValues("VALUE1", "VALUE2")
                                    .build()))
                        .outputType(R.I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(Expression.AggregationInvocation.ALL)
                        .lowerBound(WindowBound.Unbounded.UNBOUNDED)
                        .upperBound(WindowBound.Following.CURRENT_ROW)
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

    // Make sure that the record types match I64, I16, I32 and then the I64 and I64 from the window
    // functions.
    assertEquals(rel2.getRecordType().fields(), Arrays.asList(R.I64, R.I16, R.I32, R.I64, R.I64));
  }
}
