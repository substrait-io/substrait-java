package io.substrait.extended.expression;

import io.substrait.expression.Expression;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExpressionReference {
  public abstract Expression getExpression();

  public abstract String getOutputNames();
}
