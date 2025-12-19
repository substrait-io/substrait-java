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
  EnumArg UNSPECIFIED_ENUM_ARG = builder().value(Optional.empty()).build();

  Optional<String> value();

  @Override
  default <R, C extends VisitationContext, E extends Throwable> R accept(
      SimpleExtension.Function fnDef, int argIdx, FuncArgVisitor<R, C, E> fnArgVisitor, C context)
      throws E {
    return fnArgVisitor.visitEnumArg(fnDef, argIdx, this, context);
  }

  static EnumArg of(SimpleExtension.EnumArgument enumArg, String option) {
    if (!enumArg.options().contains(option)) {
      throw new IllegalArgumentException(
          String.format("EnumArg value %s not valid for options: %s", option, enumArg.options()));
    }
    return builder().value(Optional.of(option)).build();
  }

  static EnumArg of(String value) {
    return builder().value(Optional.of(value)).build();
  }

  static ImmutableEnumArg.Builder builder() {
    return ImmutableEnumArg.builder();
  }
}
