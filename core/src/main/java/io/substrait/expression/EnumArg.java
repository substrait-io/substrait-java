package io.substrait.expression;

import io.substrait.function.SimpleExtension;
import org.immutables.value.Value;

@Value.Immutable
/**
 * Capture the enum value and the associated `SimpleExtension.EnumArgument` TODO: probably better to
 * capture `SimpleExtension.Function` also.
 */
public interface EnumArg extends FunctionArg {
  String option();

  SimpleExtension.EnumArgument enumArg();

  @Override
  default <R, E extends Throwable> R acceptFuncArgVis(FuncArgVisitor<R, E> fnArgVisitor) throws E {
    return fnArgVisitor.visitEnumArg(this);
  }

  static <T extends Enum> EnumArg of(SimpleExtension.EnumArgument enumArg, String option) {
    return ImmutableEnumArg.<T>builder().enumArg(enumArg).option(option).build();
  }
}
