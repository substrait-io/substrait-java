package io.substrait.expression;

import java.util.List;
import org.immutables.value.Value;

/** A named option that configures the behavior of a function invocation. */
@Value.Immutable
public abstract class FunctionOption {

  /**
   * Returns the name of the option.
   *
   * @return the option name
   */
  public abstract String getName();

  /**
   * Returns the preferred option values, in order of preference.
   *
   * @return the option values
   */
  public abstract List<String> values();

  /**
   * Creates a builder for {@link FunctionOption}.
   *
   * @return a new builder
   */
  public static ImmutableFunctionOption.Builder builder() {
    return ImmutableFunctionOption.builder();
  }
}
