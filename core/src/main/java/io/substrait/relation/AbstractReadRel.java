package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.Optional;

public abstract class AbstractReadRel extends ZeroInputRel implements HasExtension {

  public abstract NamedStruct getInitialSchema();

  public abstract Optional<Expression> getFilter();

  public abstract Optional<Expression> getBestEffortFilter();

  // TODO:
  // public abstract Optional<MaskExpression>

  @Override
  protected final Type.Struct deriveRecordType() {
    return getInitialSchema().struct();
  }
}
