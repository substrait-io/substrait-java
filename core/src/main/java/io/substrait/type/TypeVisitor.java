package io.substrait.type;

/**
 * Visitor over the concrete {@link Type} kinds.
 *
 * @param <R> the result type produced by the visitor
 * @param <E> the exception type that may be thrown
 */
public interface TypeVisitor<R, E extends Throwable> {
  /**
   * Visits a boolean type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.Bool type) throws E;

  /**
   * Visits an 8-bit integer type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.I8 type) throws E;

  /**
   * Visits a 16-bit integer type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.I16 type) throws E;

  /**
   * Visits a 32-bit integer type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.I32 type) throws E;

  /**
   * Visits a 64-bit integer type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.I64 type) throws E;

  /**
   * Visits a 32-bit floating point type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.FP32 type) throws E;

  /**
   * Visits a 64-bit floating point type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.FP64 type) throws E;

  /**
   * Visits a string type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.Str type) throws E;

  /**
   * Visits a binary type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.Binary type) throws E;

  /**
   * Visits a date type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.Date type) throws E;

  /**
   * Visits a precision-time type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.PrecisionTime type) throws E;

  /**
   * Visits a precision-timestamp (without timezone) type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.PrecisionTimestamp type) throws E;

  /**
   * Visits a precision-timestamp with timezone type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.PrecisionTimestampTZ type) throws E;

  /**
   * Visits a year-month interval type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.IntervalYear type) throws E;

  /**
   * Visits a day-time interval type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.IntervalDay type) throws E;

  /**
   * Visits a compound interval type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.IntervalCompound type) throws E;

  /**
   * Visits a UUID type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.UUID type) throws E;

  /**
   * Visits a fixed-length character type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.FixedChar type) throws E;

  /**
   * Visits a variable-length character type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.VarChar type) throws E;

  /**
   * Visits a fixed-length binary type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.FixedBinary type) throws E;

  /**
   * Visits a decimal type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.Decimal type) throws E;

  /**
   * Visits a function type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.Func type) throws E;

  /**
   * Visits a struct type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.Struct type) throws E;

  /**
   * Visits a list type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.ListType type) throws E;

  /**
   * Visits a map type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.Map type) throws E;

  /**
   * Visits a user-defined type.
   *
   * @param type the type being visited
   * @return the visit result
   * @throws E if the visit fails
   */
  R visit(Type.UserDefined type) throws E;

  /**
   * Base {@link TypeVisitor} that throws {@link UnsupportedOperationException} for every type,
   * allowing subclasses to override only the visit methods they support.
   *
   * @param <R> the result type produced by the visitor
   * @param <E> the exception type that may be thrown
   */
  abstract class TypeThrowsVisitor<R, E extends Throwable> implements TypeVisitor<R, E> {

    private final String unsupportedMessage;

    /**
     * Creates a visitor that throws with the given message for unsupported types.
     *
     * @param unsupportedMessage the message used for unsupported types
     */
    protected TypeThrowsVisitor(String unsupportedMessage) {
      this.unsupportedMessage = unsupportedMessage;
    }

    /**
     * Throws an {@link UnsupportedOperationException} with the configured message.
     *
     * @return never returns; always throws
     */
    protected final UnsupportedOperationException t() {
      throw new UnsupportedOperationException(unsupportedMessage);
    }

    @Override
    public R visit(Type.Bool type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.I8 type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.I16 type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.I32 type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.I64 type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.FP32 type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.FP64 type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.Str type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.Binary type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.Date type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.IntervalYear type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.IntervalDay type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.IntervalCompound type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.UUID type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.FixedChar type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.VarChar type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.FixedBinary type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.Decimal type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.PrecisionTimestamp type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.PrecisionTime type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.PrecisionTimestampTZ type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.Func type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.Struct type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.ListType type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.Map type) throws E {
      throw t();
    }

    @Override
    public R visit(Type.UserDefined type) throws E {
      throw t();
    }
  }
}
