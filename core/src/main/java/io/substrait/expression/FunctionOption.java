package io.substrait.expression;

import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class FunctionOption {

  public abstract String getName();

  public abstract List<String> values();
}
