package io.substrait.extended.expression;

import io.substrait.expression.Expression;
import io.substrait.proto.AdvancedExtension;
import io.substrait.type.NamedStruct;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ExtendedExpression {
  public abstract Map<Integer, Expression> getReferredExpr();

  public abstract NamedStruct getBaseSchema();

  public abstract List<String> getExpectedTypeUrls();

  public abstract Optional<AdvancedExtension> getAdvancedExtension();
}
