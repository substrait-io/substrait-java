package io.substrait.relation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.util.EmptyVisitationContext;
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
}
