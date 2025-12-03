package io.substrait.function;

import io.substrait.type.TypeVisitor;

public interface ParameterizedTypeVisitor<R, E extends Throwable> extends TypeVisitor<R, E> {
  R visit(ParameterizedType.FixedChar expr) throws E;

  R visit(ParameterizedType.VarChar expr) throws E;

  R visit(ParameterizedType.FixedBinary expr) throws E;

  R visit(ParameterizedType.Decimal expr) throws E;

  R visit(ParameterizedType.IntervalDay expr) throws E;

  R visit(ParameterizedType.IntervalCompound expr) throws E;

  R visit(ParameterizedType.PrecisionTime expr) throws E;

  R visit(ParameterizedType.PrecisionTimestamp expr) throws E;

  R visit(ParameterizedType.PrecisionTimestampTZ expr) throws E;

  R visit(ParameterizedType.Struct expr) throws E;

  R visit(ParameterizedType.ListType expr) throws E;

  R visit(ParameterizedType.Map expr) throws E;

  R visit(ParameterizedType.StringLiteral stringLiteral) throws E;

  abstract class ParameterizedTypeThrowsVisitor<R, E extends Throwable>
      extends TypeVisitor.TypeThrowsVisitor<R, E> implements ParameterizedTypeVisitor<R, E> {

    protected ParameterizedTypeThrowsVisitor(final String unsupportedMessage) {
      super(unsupportedMessage);
    }

    @Override
    public R visit(final ParameterizedType.FixedChar expr) throws E {
      throw t();
    }

    @Override
    public R visit(final ParameterizedType.VarChar expr) throws E {
      throw t();
    }

    @Override
    public R visit(final ParameterizedType.FixedBinary expr) throws E {
      throw t();
    }

    @Override
    public R visit(final ParameterizedType.Decimal expr) throws E {
      throw t();
    }

    @Override
    public R visit(final ParameterizedType.PrecisionTime expr) throws E {
      throw t();
    }

    @Override
    public R visit(final ParameterizedType.PrecisionTimestamp expr) throws E {
      throw t();
    }

    @Override
    public R visit(final ParameterizedType.PrecisionTimestampTZ expr) throws E {
      throw t();
    }

    @Override
    public R visit(final ParameterizedType.IntervalDay expr) throws E {
      throw t();
    }

    @Override
    public R visit(final ParameterizedType.IntervalCompound expr) throws E {
      throw t();
    }

    @Override
    public R visit(final ParameterizedType.Struct expr) throws E {
      throw t();
    }

    @Override
    public R visit(final ParameterizedType.ListType expr) throws E {
      throw t();
    }

    @Override
    public R visit(final ParameterizedType.Map expr) throws E {
      throw t();
    }

    @Override
    public R visit(final ParameterizedType.StringLiteral stringLiteral) throws E {
      throw t();
    }
  }
}
