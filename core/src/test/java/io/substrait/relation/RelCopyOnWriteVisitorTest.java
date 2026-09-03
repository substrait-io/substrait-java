package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.WindowBound;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.physical.MultiBucketExchange;
import io.substrait.util.EmptyVisitationContext;
import io.substrait.utils.RelSamples;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

  /** A window over {@code input} whose one function carries the given bounds. */
  private ConsistentPartitionWindow windowOver(Rel input, WindowBound lower, WindowBound upper) {
    SimpleExtension.WindowFunctionVariant declaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    return ConsistentPartitionWindow.builder()
        .input(input)
        .windowFunctions(
            Arrays.asList(
                ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
                    .declaration(declaration)
                    .outputType(R.I64)
                    .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
                    .invocation(Expression.AggregationInvocation.ALL)
                    .lowerBound(lower)
                    .upperBound(upper)
                    .boundsType(Expression.WindowBoundsType.RANGE)
                    .build()))
        .build();
  }

  @Test
  void consistentPartitionWindowBoundOffsetsAreRewritten() {
    Rel input = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I64));
    ConsistentPartitionWindow window =
        windowOver(input, WindowBound.Preceding.of(sb.i32(5)), WindowBound.Following.of(sb.i32(7)));

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
    Rel input = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I64));
    ConsistentPartitionWindow window =
        windowOver(input, WindowBound.UNBOUNDED, WindowBound.CURRENT_ROW);

    assertEquals(
        Optional.empty(),
        window.accept(negateI32LiteralsVisitor(), EmptyVisitationContext.INSTANCE));
  }

  /** A rewrite that applies only below a window relation comes back with the input replaced. */
  @Test
  void consistentPartitionWindowRewritesItsInput() {
    Rel scan = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I32));
    Rel input = sb.project(in -> Arrays.asList(sb.i32(5)), sb.remap(1), scan);
    ConsistentPartitionWindow window =
        windowOver(input, WindowBound.UNBOUNDED, WindowBound.CURRENT_ROW);

    Optional<Rel> rewritten =
        window.accept(negateI32LiteralsVisitor(), EmptyVisitationContext.INSTANCE);

    ConsistentPartitionWindow expected =
        ConsistentPartitionWindow.builder()
            .from(window)
            .input(sb.project(in -> Arrays.asList(sb.i32(-5)), sb.remap(1), scan))
            .build();
    assertEquals(Optional.of(expected), rewritten);
  }

  /**
   * The same guard, one relation over: a rewrite that touches only the exchange's own expression
   * comes back, where the input alone used to decide whether anything changed.
   */
  @Test
  void multiBucketExchangeRewritesItsExpression() {
    Rel scan = sb.namedScan(Arrays.asList("test"), Arrays.asList("a"), Arrays.asList(R.I32));
    MultiBucketExchange exchange =
        MultiBucketExchange.builder()
            .input(scan)
            .expression(sb.i32(5))
            .constrainedToCount(true)
            .partitionCount(1)
            .build();

    Optional<Rel> rewritten =
        exchange.accept(negateI32LiteralsVisitor(), EmptyVisitationContext.INSTANCE);

    assertEquals(
        Optional.of(MultiBucketExchange.builder().from(exchange).expression(sb.i32(-5)).build()),
        rewritten);
  }

  /**
   * Every relation that has inputs hands back a rewrite made below it, so a relation added without
   * visiting its input fails here rather than dropping rewrites silently. Driven by the shared
   * samples, whose own test keeps them exhaustive over the model.
   */
  @Test
  void everyRelationWithInputsPropagatesARewriteBelowIt() {
    // The two the visitor refuses outright rather than descending into.
    List<Class<?>> notVisited = Arrays.asList(Expand.class, ExtensionWrite.class);
    RelCopyOnWriteVisitor<RuntimeException> renameScans =
        new RelCopyOnWriteVisitor<RuntimeException>() {
          @Override
          public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext context) {
            return Optional.of(
                NamedScan.builder()
                    .from(namedScan)
                    .names(Collections.singletonList("renamed"))
                    .build());
          }
        };

    new RelSamples(sb, extensions)
        .samples()
        .forEach(
            (type, rel) -> {
              if (rel.getInputs().isEmpty() || notVisited.contains(type)) {
                return;
              }
              assertTrue(
                  rel.accept(renameScans, EmptyVisitationContext.INSTANCE).isPresent(),
                  type.getSimpleName());
            });
  }
}
