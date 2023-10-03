package io.substrait.relation;

import io.substrait.expression.Expression;
import java.util.Optional;

public abstract class AbstractJoin extends BiRel implements HasExtension {

  public abstract Optional<Expression> getPostJoinFilter();
}
