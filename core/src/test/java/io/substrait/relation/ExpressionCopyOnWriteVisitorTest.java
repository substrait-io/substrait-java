package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.WindowBound;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.util.EmptyVisitationContext;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ExpressionCopyOnWriteVisitorTest extends TestBase {

  /** Rewrites every i32 literal to its negation, leaving all other literals alone. */
  private static class NegateI32Literals extends ExpressionCopyOnWriteVisitor<RuntimeException> {
    NegateI32Literals() {
      super(new RelCopyOnWriteVisitor<>());
    }

    @Override
    public Optional<Expression> visitLiteral(Expression.Literal literal) {
      if (!(literal instanceof Expression.I32Literal)) {
        return Optional.empty();
      }
      Expression.I32Literal i32Literal = (Expression.I32Literal) literal;
      return Optional.of(
          Expression.I32Literal.builder().from(i32Literal).value(-i32Literal.value()).build());
    }
  }

  @Test
  void nestedMapKeysAndValuesAreRewritten() {
    Expression.NestedMap nestedMap =
        Expression.NestedMap.builder()
            .nullable(true)
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.i32(1), sb.i32(10)))
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.i32(2), sb.i32(20)))
            .build();

    assertEquals(
        Optional.of(
            Expression.NestedMap.builder()
                .nullable(true)
                .addKeyValues(Expression.NestedMap.KeyValue.of(sb.i32(-1), sb.i32(-10)))
                .addKeyValues(Expression.NestedMap.KeyValue.of(sb.i32(-2), sb.i32(-20)))
                .build()),
        nestedMap.accept(new NegateI32Literals(), EmptyVisitationContext.INSTANCE));
  }

  @Test
  void unchangedNestedMapIsNotCopied() {
    Expression.NestedMap nestedMap =
        Expression.NestedMap.builder()
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("a"), sb.str("b")))
            .build();

    assertEquals(
        Optional.empty(),
        nestedMap.accept(new NegateI32Literals(), EmptyVisitationContext.INSTANCE));
  }

  @Test
  void visitKeyValueCanBeOverridden() {
    // Rewriting whole pairs only requires overriding visitKeyValue, not visit(NestedMap).
    ExpressionCopyOnWriteVisitor<RuntimeException> swapKeysAndValues =
        new ExpressionCopyOnWriteVisitor<RuntimeException>(new RelCopyOnWriteVisitor<>()) {
          @Override
          protected Optional<Expression.NestedMap.KeyValue> visitKeyValue(
              Expression.NestedMap.KeyValue keyValue, EmptyVisitationContext context) {
            return Optional.of(Expression.NestedMap.KeyValue.of(keyValue.value(), keyValue.key()));
          }
        };

    Expression.NestedMap nestedMap =
        Expression.NestedMap.builder()
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.i32(1), sb.i32(10)))
            .build();

    assertEquals(
        Optional.of(
            Expression.NestedMap.builder()
                .addKeyValues(Expression.NestedMap.KeyValue.of(sb.i32(10), sb.i32(1)))
                .build()),
        nestedMap.accept(swapKeysAndValues, EmptyVisitationContext.INSTANCE));
  }

  @Test
  void windowFunctionBoundOffsetsAreRewritten() {
    SimpleExtension.WindowFunctionVariant declaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Expression.WindowFunctionInvocation wfi =
        Expression.WindowFunctionInvocation.builder()
            .declaration(declaration)
            .arguments(Collections.emptyList())
            .partitionBy(Collections.emptyList())
            .sort(
                Collections.singletonList(
                    Expression.SortField.builder()
                        .expr(sb.i32(1))
                        .direction(Expression.SortDirection.ASC_NULLS_FIRST)
                        .build()))
            .outputType(R.I64)
            .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .lowerBound(WindowBound.Preceding.of(sb.i32(5)))
            .upperBound(WindowBound.Following.of(sb.i32(7)))
            .boundsType(Expression.WindowBoundsType.RANGE)
            .build();

    Optional<Expression> rewritten =
        wfi.accept(new NegateI32Literals(), EmptyVisitationContext.INSTANCE);

    assertEquals(
        Optional.of(
            Expression.WindowFunctionInvocation.builder()
                .from(wfi)
                .sort(
                    Collections.singletonList(
                        Expression.SortField.builder()
                            .expr(sb.i32(-1))
                            .direction(Expression.SortDirection.ASC_NULLS_FIRST)
                            .build()))
                .lowerBound(WindowBound.Preceding.of(sb.i32(-5)))
                .upperBound(WindowBound.Following.of(sb.i32(-7)))
                .build()),
        rewritten);
  }

  @Test
  void windowFunctionWithUnchangedBoundsIsNotCopied() {
    SimpleExtension.WindowFunctionVariant declaration =
        extensions.getWindowFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "lead:any"));
    Expression.WindowFunctionInvocation wfi =
        Expression.WindowFunctionInvocation.builder()
            .declaration(declaration)
            .arguments(Collections.emptyList())
            .partitionBy(Collections.emptyList())
            .sort(Collections.emptyList())
            .outputType(R.I64)
            .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .lowerBound(WindowBound.UNBOUNDED)
            .upperBound(WindowBound.CURRENT_ROW)
            .boundsType(Expression.WindowBoundsType.RANGE)
            .build();

    assertEquals(
        Optional.empty(), wfi.accept(new NegateI32Literals(), EmptyVisitationContext.INSTANCE));
  }
}
