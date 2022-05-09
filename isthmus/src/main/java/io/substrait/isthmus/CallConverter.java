package io.substrait.isthmus;

import io.substrait.expression.Expression;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

@FunctionalInterface
public interface CallConverter {
  Optional<Expression> convert(RexCall call, Function<RexNode, Expression> topLevelConverter);
}
