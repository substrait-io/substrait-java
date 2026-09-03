package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.WindowBound;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.util.EmptyVisitationContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RelCopyOnWriteVisitorTest extends TestBase {

  /** Rewrites every i32 literal to its negation, leaving all other literals alone. */
  private static RelCopyOnWriteVisitor<RuntimeException> negateI32LiteralsVisitor() {
    return new RelCopyOnWriteVisitor<>(
        relVisitor ->
            new ExpressionCopyOnWriteVisitor<RuntimeException>(relVisitor) {
              @Override
              public Optional<Expression> visitLiteral(Expression.Literal literal) {
                if (!(literal instanceof Expression.I32Literal)) {
                  return Optional.empty();
                }
                Expression.I32Literal i32Literal = (Expression.I32Literal) literal;
                return Optional.of(
                    Expression.I32Literal.builder()
                        .from(i32Literal)
                        .value(-i32Literal.value())
                        .build());
              }
            });
  }

  @Test
  void consistentPartitionWindowBoundOffsetsAreRewritten() {
    SimpleExtension.WindowFunctionVariant declaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I64));
    ConsistentPartitionWindow window =
        ConsistentPartitionWindow.builder()
            .input(input)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .declaration(declaration)
                        .arguments(Collections.emptyList())
                        .outputType(R.I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(Expression.AggregationInvocation.ALL)
                        .lowerBound(WindowBound.Preceding.of(sb.i32(5)))
                        .upperBound(WindowBound.Following.of(sb.i32(7)))
                        .boundsType(Expression.WindowBoundsType.RANGE)
                        .build()))
            .build();

    Optional<Rel> rewritten =
        window.accept(negateI32LiteralsVisitor(), EmptyVisitationContext.INSTANCE);

    ConsistentPartitionWindow expected =
        ConsistentPartitionWindow.builder()
            .from(window)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .from(window.getWindowFunctions().get(0))
                        .lowerBound(WindowBound.Preceding.of(sb.i32(-5)))
                        .upperBound(WindowBound.Following.of(sb.i32(-7)))
                        .build()))
            .build();
    assertEquals(Optional.of(expected), rewritten);
  }

  @Test
  void consistentPartitionWindowWithUnchangedBoundsIsNotCopied() {
    SimpleExtension.WindowFunctionVariant declaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel input = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I64));
    ConsistentPartitionWindow window =
        ConsistentPartitionWindow.builder()
            .input(input)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .declaration(declaration)
                        .arguments(Collections.emptyList())
                        .outputType(R.I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(Expression.AggregationInvocation.ALL)
                        .lowerBound(WindowBound.UNBOUNDED)
                        .upperBound(WindowBound.CURRENT_ROW)
                        .boundsType(Expression.WindowBoundsType.RANGE)
                        .build()))
            .build();

    assertEquals(
        Optional.empty(),
        window.accept(negateI32LiteralsVisitor(), EmptyVisitationContext.INSTANCE));
  }

  /**
   * The window relation's own lists are not the only thing below it. A rewrite that applies to the
   * input has nowhere else to be found: a window relation is the only single-input relation in this
   * visitor that used to decide "unchanged" without asking its input.
   */
  @Test
  void consistentPartitionWindowRewritesItsInput() {
    SimpleExtension.WindowFunctionVariant declaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Rel scan = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I32));
    Rel input = sb.project(in -> Arrays.asList(sb.i32(5)), sb.remap(1), scan);
    ConsistentPartitionWindow window =
        ConsistentPartitionWindow.builder()
            .input(input)
            .windowFunctions(
                Arrays.asList(
                    ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                        .declaration(declaration)
                        .arguments(Collections.emptyList())
                        .outputType(R.I64)
                        .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                        .invocation(Expression.AggregationInvocation.ALL)
                        .lowerBound(WindowBound.UNBOUNDED)
                        .upperBound(WindowBound.CURRENT_ROW)
                        .boundsType(Expression.WindowBoundsType.RANGE)
                        .build()))
            .build();

    Optional<Rel> rewritten =
        window.accept(negateI32LiteralsVisitor(), EmptyVisitationContext.INSTANCE);

    ConsistentPartitionWindow expected =
        ConsistentPartitionWindow.builder()
            .from(window)
            .input(sb.project(in -> Arrays.asList(sb.i32(-5)), sb.remap(1), scan))
            .build();
    assertEquals(Optional.of(expected), rewritten);
  }
}
