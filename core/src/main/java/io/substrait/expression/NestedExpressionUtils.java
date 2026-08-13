package io.substrait.expression;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.stream.Collectors;

/** Provides common utilities for the nested expression types. */
final class NestedExpressionUtils {

  private NestedExpressionUtils() {}

  /**
   * Returns the type the given expressions have in common, disregarding nullability: the type of
   * the first expression, made nullable if any of the expressions is nullable.
   *
   * <p>Expressions that differ only in nullability are accepted because SQL collection constructors
   * do not cast their operands to a common type: {@code ARRAY[not_null_column, nullable_column]}
   * yields values that differ only in nullability. The common type of such a collection is the
   * nullable one, since it holds a null.
   *
   * @param description what the expressions are, used in the exception message
   * @param expressions the expressions to reduce to a common type, at least one
   * @return the common type
   * @throws IllegalArgumentException if the expressions do not all have the same type once
   *     nullability is disregarded
   */
  static Type commonType(String description, List<? extends Expression> expressions) {
    List<Type> types = expressions.stream().map(Expression::getType).collect(Collectors.toList());
    if (types.stream().map(TypeCreator::asNullable).distinct().limit(2).count() > 1) {
      throw new IllegalArgumentException(
          String.format(
              "%s must all have the same type, disregarding nullability, but found %s",
              description, types));
    }
    Type first = types.get(0);
    return types.stream().anyMatch(Type::nullable) ? TypeCreator.asNullable(first) : first;
  }
}
