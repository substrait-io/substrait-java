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
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.ConsistentPartitionWindowRel;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.relation.ImmutableConsistentPartitionWindow;
import io.substrait.relation.Rel;
import java.util.Arrays;
import java.util.Collections;
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
                        .upperBound(WindowBound.CURRENT_ROW)
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

    verifyRoundTrip(rel1);
  }

  @Test
  void windowFunctionInvocationRoundtripWithNonLiteralOffsetExpr() {
    SimpleExtension.WindowFunctionVariant windowFunctionDeclaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    // Unlike the relation-level fixture above, this bare expression has no enclosing relation to
    // resolve field references against, so the offset is a scalar function call over literals.
    // A RANGE bound with a Preceding side requires exactly one ordering expression, carried here
    // directly on the invocation since there is no enclosing relation to hold it.
    Expression.WindowFunctionInvocation wfi =
        Expression.WindowFunctionInvocation.builder()
            .declaration(windowFunctionDeclaration)
            .arguments(Collections.emptyList())
            .partitionBy(Collections.emptyList())
            .sort(
                Collections.singletonList(
                    Expression.SortField.builder()
                        .expr(sb.i64(1))
                        .direction(Expression.SortDirection.ASC_NULLS_FIRST)
                        .build()))
            .outputType(R.I64)
            .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .lowerBound(WindowBound.Preceding.of(sb.add(sb.i64(2), sb.i64(3))))
            .upperBound(WindowBound.CURRENT_ROW)
            .boundsType(Expression.WindowBoundsType.RANGE)
            .build();

    verifyRoundTrip(wfi);
  }

  @Test
  void precedingOffsetExprRoundtripsThroughLiteralOffsetForBackwardCompat() {
    // We populate the legacy field for compatibility: the spec says producers may set both, and
    // its "must" only constrains the value when both are set.
    io.substrait.proto.Expression.WindowFunction.Bound proto =
        expressionProtoConverter.toProto(WindowBound.Preceding.of(ExpressionCreator.i64(false, 5)));

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
    // bounds_type is required whenever a bound isn't Unbounded; UNSPECIFIED here must be
    // rejected. The check runs in a @Value.Check, so it fires at construction time rather than
    // only when a plan is later read back from proto.
    ImmutableConsistentPartitionWindow.WindowRelFunctionInvocation.Builder invocationBuilder =
        ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
            .declaration(windowFunctionDeclaration)
            .arguments(Arrays.asList(sb.fieldReference(input, 0)))
            .outputType(R.I64)
            .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .lowerBound(WindowBound.Preceding.of(5))
            .upperBound(WindowBound.CURRENT_ROW)
            .boundsType(Expression.WindowBoundsType.UNSPECIFIED);

    assertThrows(IllegalArgumentException.class, invocationBuilder::build);
  }

  @Test
  void rangePrecedingWithoutASingleOrderingExpressionIsRejected() {
    SimpleExtension.WindowFunctionVariant windowFunctionDeclaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I64));
    // A RANGE bound with a Preceding side requires exactly one ordering expression on the
    // enclosing relation; this fixture has none. The check runs in a @Value.Check, so it fires at
    // construction time rather than only when a plan is later read back from proto.
    ImmutableConsistentPartitionWindow.Builder relBuilder =
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
                        .boundsType(Expression.WindowBoundsType.RANGE)
                        .build()));

    assertThrows(IllegalArgumentException.class, relBuilder::build);
  }

  @Test
  void rangePrecedingWithTwoOrderingExpressionsIsRejected() {
    SimpleExtension.WindowFunctionVariant windowFunctionDeclaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input =
        sb.namedScan(Arrays.asList("test"), Arrays.asList("a", "b"), Arrays.asList(R.I64, R.I64));
    // A RANGE bound with a Preceding side requires exactly one ordering expression; this fixture
    // has two.
    ImmutableConsistentPartitionWindow.Builder relBuilder =
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
                        .boundsType(Expression.WindowBoundsType.RANGE)
                        .build()))
            .sorts(sb.sortFields(input, 0, 1));

    assertThrows(IllegalArgumentException.class, relBuilder::build);
  }

  @Test
  void rangePrecedingWithClusteredOrderingIsRejected() {
    SimpleExtension.WindowFunctionVariant windowFunctionDeclaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I64));
    // A RANGE bound with a Preceding side cannot use SORT_DIRECTION_CLUSTERED for its ordering
    // expression.
    ImmutableConsistentPartitionWindow.Builder relBuilder =
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
                        .boundsType(Expression.WindowBoundsType.RANGE)
                        .build()))
            .sorts(
                Arrays.asList(
                    Expression.SortField.builder()
                        .expr(sb.fieldReference(input, 0))
                        .direction(Expression.SortDirection.CLUSTERED)
                        .build()));

    assertThrows(IllegalArgumentException.class, relBuilder::build);
  }

  @Test
  void rangePrecedingWithTwoOrderingExpressionsOnAnInvocationIsRejected() {
    SimpleExtension.WindowFunctionVariant declaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Expression.SortField sort =
        Expression.SortField.builder()
            .expr(sb.i64(1))
            .direction(Expression.SortDirection.ASC_NULLS_FIRST)
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            Expression.WindowFunctionInvocation.builder()
                .declaration(declaration)
                .arguments(Collections.emptyList())
                .partitionBy(Collections.emptyList())
                .sort(Arrays.asList(sort, sort))
                .outputType(R.I64)
                .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                .invocation(Expression.AggregationInvocation.ALL)
                .lowerBound(WindowBound.Preceding.of(5))
                .upperBound(WindowBound.CURRENT_ROW)
                .boundsType(Expression.WindowBoundsType.RANGE)
                .build());
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
    SimpleExtension.ScalarFunctionVariant subtractDeclaration =
        extensions.getScalarFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "subtract:i64_i64"));
    // Consumes the first anchor so a coincidental match with an isolated collector can't hide.
    int decoyAnchor = functionCollector.getFunctionReference(subtractDeclaration);

    Rel input = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I64));

    Expression.ScalarFunctionInvocation argAdd =
        sb.add(sb.fieldReference(input, 0), ExpressionCreator.i64(false, 1));
    Expression.ScalarFunctionInvocation offsetAdd =
        sb.add(sb.fieldReference(input, 0), ExpressionCreator.i64(false, 2));

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
            .sorts(
                Arrays.asList(
                    Expression.SortField.builder()
                        .expr(sb.fieldReference(input, 0))
                        .direction(Expression.SortDirection.ASC_NULLS_FIRST)
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
