package io.substrait.isthmus;

import io.substrait.expression.Expression;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

/**
 * Functional interface for converting Calcite {@link RexCall} expressions into Substrait {@link
 * Expression}s.
 *
 * <p>Implementations should return an {@link Optional} containing the converted expression, or
 * {@link Optional#empty()} if the call is not handled.
 */
@FunctionalInterface
public interface CallConverter {

  /**
   * Converts a Calcite {@link RexCall} into a Substrait {@link Expression}.
   *
   * @param call the Calcite function/operator call to convert
   * @param topLevelConverter a function for converting nested {@link RexNode} operands
   * @return an {@link Optional} containing the converted expression, or empty if not applicable
   */
  Optional<Expression> convert(RexCall call, Function<RexNode, Expression> topLevelConverter);
}
