package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionOption;
import io.substrait.expression.WindowBound;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.ConsistentPartitionWindowRel;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.relation.Rel;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class ConsistentPartitionWindowRelRoundtripTest extends TestBase {

  @Test
  void consistentPartitionWindowRoundtripSingle() {
    SimpleExtension.WindowFunctionVariant windowFunctionDeclaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input =
        sb.namedScan(
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
                        .arguments(Arrays.asList(sb.fieldReference(input, 0)))
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
            .partitionExpressions(Arrays.asList(sb.fieldReference(input, 1)))
            .sorts(
                Arrays.asList(
                    Expression.SortField.builder()
                        // SORT BY c
                        .expr(sb.fieldReference(input, 2))
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
    SimpleExtension.WindowFunctionVariant windowFunctionLeadDeclaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    SimpleExtension.WindowFunctionVariant windowFunctionLagDeclaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input =
        sb.namedScan(
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
                        .arguments(Arrays.asList(sb.fieldReference(input, 0)))
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
                        .arguments(Arrays.asList(sb.fieldReference(input, 0)))
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
            .partitionExpressions(Arrays.asList(sb.fieldReference(input, 1)))
            .sorts(
                Arrays.asList(
                    Expression.SortField.builder()
                        // SORT BY c
                        .expr(sb.fieldReference(input, 2))
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

  @Test
  void consistentPartitionWindowRoundtripWithNonLiteralOffsetExpr() {
    SimpleExtension.WindowFunctionVariant windowFunctionDeclaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input =
        sb.namedScan(
            Arrays.asList("test"),
            Arrays.asList("a", "b", "c"),
            Arrays.asList(R.I64, R.I16, R.I32));
    // A non-literal distance, only representable via offset_expr.
    Rel rel1 =
        ConsistentPartitionWindow.builder()
            .input(input)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .declaration(windowFunctionDeclaration)
                        .arguments(Arrays.asList(sb.fieldReference(input, 0)))
                        .outputType(R.I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(Expression.AggregationInvocation.ALL)
                        .lowerBound(WindowBound.Preceding.of(sb.fieldReference(input, 0)))
                        .upperBound(WindowBound.Following.CURRENT_ROW)
                        .boundsType(Expression.WindowBoundsType.RANGE)
                        .build()))
            .partitionExpressions(Arrays.asList(sb.fieldReference(input, 1)))
            .sorts(
                Arrays.asList(
                    Expression.SortField.builder()
                        .expr(sb.fieldReference(input, 2))
                        .direction(Expression.SortDirection.ASC_NULLS_FIRST)
                        .build()))
            .build();

    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel1);
    ConsistentPartitionWindowRel.WindowRelFunction protoWindowFunction =
        protoRel.getWindow().getWindowFunctions(0);
    assertTrue(protoWindowFunction.getLowerBound().getPreceding().hasOffsetExpr());
    assertEquals(0, protoWindowFunction.getLowerBound().getPreceding().getOffset());

    io.substrait.relation.Rel rel2 = protoRelConverter.from(protoRel);
    assertEquals(rel1, rel2);
  }

  @Test
  void precedingOffsetExprRoundtripsThroughLiteralOffsetForBackwardCompat() {
    // A literal offset_expr must also populate the legacy field, per the spec's migration note.
    io.substrait.proto.Expression.WindowFunction.Bound proto =
        ExpressionProtoConverter.BoundConverter.convert(
            WindowBound.Preceding.of(ExpressionCreator.i64(false, 5)),
            relProtoConverter.getExpressionProtoConverter());

    assertTrue(proto.getPreceding().hasOffsetExpr());
    assertEquals(5, proto.getPreceding().getOffset());
  }

  @Test
  void boundsTypeUnspecifiedWithRealBoundIsRejected() {
    SimpleExtension.WindowFunctionVariant windowFunctionDeclaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I64));
    // bounds_type is required whenever a bound isn't Unbounded; UNSPECIFIED here must be rejected.
    Rel rel =
        ConsistentPartitionWindow.builder()
            .input(input)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .declaration(windowFunctionDeclaration)
                        .arguments(Arrays.asList(sb.fieldReference(input, 0)))
                        .outputType(R.I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(Expression.AggregationInvocation.ALL)
                        .lowerBound(WindowBound.Preceding.of(5))
                        .upperBound(WindowBound.CURRENT_ROW)
                        .boundsType(Expression.WindowBoundsType.UNSPECIFIED)
                        .build()))
            .build();

    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
    assertThrows(IllegalArgumentException.class, () -> protoRelConverter.from(protoRel));
  }

  @Test
  void boundsTypeUnspecifiedWithUnboundedBoundsIsAccepted() {
    SimpleExtension.WindowFunctionVariant windowFunctionDeclaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I64));
    // Both bounds Unbounded needs no bounds_type — this is Spark's UnspecifiedFrame case.
    Rel rel =
        ConsistentPartitionWindow.builder()
            .input(input)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .declaration(windowFunctionDeclaration)
                        .arguments(Arrays.asList(sb.fieldReference(input, 0)))
                        .outputType(R.I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(Expression.AggregationInvocation.ALL)
                        .lowerBound(WindowBound.UNBOUNDED)
                        .upperBound(WindowBound.UNBOUNDED)
                        .boundsType(Expression.WindowBoundsType.UNSPECIFIED)
                        .build()))
            .build();

    verifyRoundTrip(rel);
  }

  @Test
  void offsetExprFunctionCallSharesFunctionAnchorWithRestOfPlan() {
    // Proves BoundConverter reuses the caller's extensionCollector rather than an isolated one.
    SimpleExtension.WindowFunctionVariant windowFunctionDeclaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    SimpleExtension.ScalarFunctionVariant addDeclaration =
        extensions.scalarFunctions().stream()
            .filter(s -> s.name().equalsIgnoreCase("add"))
            .findFirst()
            .orElseThrow(AssertionError::new);
    SimpleExtension.ScalarFunctionVariant subtractDeclaration =
        extensions.scalarFunctions().stream()
            .filter(s -> s.name().equalsIgnoreCase("subtract"))
            .findFirst()
            .orElseThrow(AssertionError::new);
    // Consumes the first anchor so a coincidental match with an isolated collector can't hide.
    int decoyAnchor = functionCollector.getFunctionReference(subtractDeclaration);

    Rel input = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I64));

    Expression.ScalarFunctionInvocation argAdd =
        ExpressionCreator.scalarFunction(
            addDeclaration, R.I64, sb.fieldReference(input, 0), ExpressionCreator.i64(false, 1));
    Expression.ScalarFunctionInvocation offsetAdd =
        ExpressionCreator.scalarFunction(
            addDeclaration, R.I64, sb.fieldReference(input, 0), ExpressionCreator.i64(false, 2));

    Rel rel =
        ConsistentPartitionWindow.builder()
            .input(input)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .declaration(windowFunctionDeclaration)
                        .arguments(Arrays.asList(argAdd))
                        .outputType(R.I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(Expression.AggregationInvocation.ALL)
                        .lowerBound(WindowBound.Preceding.of(offsetAdd))
                        .upperBound(WindowBound.CURRENT_ROW)
                        .boundsType(Expression.WindowBoundsType.RANGE)
                        .build()))
            .build();

    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
    ConsistentPartitionWindowRel.WindowRelFunction protoWindowFunction =
        protoRel.getWindow().getWindowFunctions(0);
    int argFunctionRef =
        protoWindowFunction.getArguments(0).getValue().getScalarFunction().getFunctionReference();
    int offsetFunctionRef =
        protoWindowFunction
            .getLowerBound()
            .getPreceding()
            .getOffsetExpr()
            .getScalarFunction()
            .getFunctionReference();
    assertEquals(argFunctionRef, offsetFunctionRef);
    assertNotEquals(decoyAnchor, offsetFunctionRef);

    verifyRoundTrip(rel);
  }
}
