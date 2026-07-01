package io.substrait.function;

import io.substrait.type.TypeVisitor;

/**
 * Visitor over the concrete {@link ParameterizedType} kinds, extending {@link TypeVisitor} with the
 * parameterized variants.
 *
 * @param <R> the result type produced by the visitor
 * @param <E> the exception type that may be thrown
 */
public interface ParameterizedTypeVisitor<R, E extends Throwable> extends TypeVisitor<R, E> {
  /**
   * Visits a parameterized fixed-length character type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.FixedChar expr) throws E;

  /**
   * Visits a parameterized variable-length character type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.VarChar expr) throws E;

  /**
   * Visits a parameterized fixed-length binary type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.FixedBinary expr) throws E;

  /**
   * Visits a parameterized decimal type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.Decimal expr) throws E;

  /**
   * Visits a parameterized day-time interval type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.IntervalDay expr) throws E;

  /**
   * Visits a parameterized compound interval type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.IntervalCompound expr) throws E;

  /**
   * Visits a parameterized precision-time type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.PrecisionTime expr) throws E;

  /**
   * Visits a parameterized precision-timestamp (without timezone) type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.PrecisionTimestamp expr) throws E;

  /**
   * Visits a parameterized precision-timestamp with timezone type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.PrecisionTimestampTZ expr) throws E;

  /**
   * Visits a parameterized struct type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.Struct expr) throws E;

  /**
   * Visits a parameterized list type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.ListType expr) throws E;

  /**
   * Visits a parameterized map type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.Map expr) throws E;

  /**
   * Visits a string-literal type parameter.
   *
   * @param stringLiteral the parameter being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.StringLiteral stringLiteral) throws E;

  /**
   * Visits a parameterized function type.
   *
   * @param expr the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(ParameterizedType.Func expr) throws E;

  /**
   * Base {@link ParameterizedTypeVisitor} that throws {@link UnsupportedOperationException} for
   * every type, allowing subclasses to override only the visit methods they support.
   *
   * @param <R> the result type produced by the visitor
   * @param <E> the exception type that may be thrown
   */
  abstract class ParameterizedTypeThrowsVisitor<R, E extends Throwable>
      extends TypeVisitor.TypeThrowsVisitor<R, E> implements ParameterizedTypeVisitor<R, E> {

    /**
     * Creates a visitor that throws with the given message for unsupported types.
     *
     * @param unsupportedMessage the message used for unsupported types
     */
    protected ParameterizedTypeThrowsVisitor(String unsupportedMessage) {
      super(unsupportedMessage);
    }

    @Override
    public R visit(ParameterizedType.FixedChar expr) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.VarChar expr) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.FixedBinary expr) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.Decimal expr) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.PrecisionTime expr) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.PrecisionTimestamp expr) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.PrecisionTimestampTZ expr) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.IntervalDay expr) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.IntervalCompound expr) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.Struct expr) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.ListType expr) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.Map expr) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.StringLiteral stringLiteral) throws E {
      throw t();
    }

    @Override
    public R visit(ParameterizedType.Func expr) throws E {
      throw t();
    }
  }
}
