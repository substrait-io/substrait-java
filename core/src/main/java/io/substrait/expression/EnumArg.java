package io.substrait.expression;

import io.substrait.extension.SimpleExtension;
import io.substrait.util.VisitationContext;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Captures the {@link SimpleExtension.EnumArgument} option value in a {@link
 * SimpleExtension.Function} invocation.
 *
 * @see io.substrait.expression.Expression.ScalarFunctionInvocation
 * @see AggregateFunctionInvocation
 */
@Value.Immutable
public interface EnumArg extends FunctionArg {
  /** Constant representing an unspecified enum argument with no value. */
  EnumArg UNSPECIFIED_ENUM_ARG = builder().value(Optional.empty()).build();

  /**
   * Returns the enum option value.
   *
   * @return the option value, if present
   */
  Optional<String> value();

  @Override
  default <R, C extends VisitationContext, E extends Throwable> R accept(
      SimpleExtension.Function fnDef, int argIdx, FuncArgVisitor<R, C, E> fnArgVisitor, C context)
      throws E {
    return fnArgVisitor.visitEnumArg(fnDef, argIdx, this, context);
  }

  /**
   * Creates an EnumArg with the specified option value, validating it against the enum argument
   * definition.
   *
   * @param enumArg the enum argument definition
   * @param option the option value to use
   * @return a new EnumArg instance
   * @throws IllegalArgumentException if the option is not valid for the enum argument
   */
  static EnumArg of(SimpleExtension.EnumArgument enumArg, String option) {
    if (!enumArg.options().contains(option)) {
      throw new IllegalArgumentException(
          String.format("EnumArg value %s not valid for options: %s", option, enumArg.options()));
    }
    return builder().value(Optional.of(option)).build();
  }

  /**
   * Creates an EnumArg with the specified value without validation.
   *
   * @param value the enum value
   * @return a new EnumArg instance
   */
  static EnumArg of(String value) {
    return builder().value(Optional.of(value)).build();
  }

  /**
   * Creates a new builder for constructing an EnumArg.
   *
   * @return a new builder instance
   */
  static ImmutableEnumArg.Builder builder() {
    return ImmutableEnumArg.builder();
  }
}
