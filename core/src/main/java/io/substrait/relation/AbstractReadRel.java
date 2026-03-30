package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.expression.MaskExpression;
import io.substrait.expression.MaskExpressionTypeProjector;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.Optional;

public abstract class AbstractReadRel extends ZeroInputRel implements HasExtension {

  public abstract NamedStruct getInitialSchema();

  public abstract Optional<Expression> getFilter();

  public abstract Optional<Expression> getBestEffortFilter();

  public abstract Optional<MaskExpression.MaskExpr> getProjection();

  @Override
  protected final Type.Struct deriveRecordType() {
    Type.Struct base = getInitialSchema().struct();
    return getProjection()
        .map(projection -> MaskExpressionTypeProjector.project(projection, base))
        .orElse(base);
  }
}
