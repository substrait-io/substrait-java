package io.substrait.examples.util;

import io.substrait.type.Type;
import io.substrait.type.Type.Binary;
import io.substrait.type.Type.Bool;
import io.substrait.type.Type.Date;
import io.substrait.type.Type.Decimal;
import io.substrait.type.Type.FP32;
import io.substrait.type.Type.FP64;
import io.substrait.type.Type.FixedBinary;
import io.substrait.type.Type.FixedChar;
import io.substrait.type.Type.I16;
import io.substrait.type.Type.I32;
import io.substrait.type.Type.I64;
import io.substrait.type.Type.I8;
import io.substrait.type.Type.IntervalCompound;
import io.substrait.type.Type.IntervalDay;
import io.substrait.type.Type.IntervalYear;
import io.substrait.type.Type.ListType;
import io.substrait.type.Type.Map;
import io.substrait.type.Type.PrecisionTime;
import io.substrait.type.Type.Str;
import io.substrait.type.Type.Struct;
import io.substrait.type.Type.Time;
import io.substrait.type.Type.Timestamp;
import io.substrait.type.Type.TimestampTZ;
import io.substrait.type.Type.UUID;
import io.substrait.type.Type.UserDefined;
import io.substrait.type.Type.VarChar;
import io.substrait.type.TypeVisitor;

/** TypeStringify produces a simple debug string of Substrait types */
public class TypeStringify extends ParentStringify
    implements TypeVisitor<String, RuntimeException> {

  protected TypeStringify(final int indent) {
    super(indent);
  }

  @Override
  public String visit(final I64 type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final Bool type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final I8 type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final I16 type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final I32 type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final FP32 type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final FP64 type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final Str type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final Binary type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final Date type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final Time type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  @Deprecated
  public String visit(final TimestampTZ type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  @Deprecated
  public String visit(final Timestamp type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final Type.PrecisionTimestamp type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final Type.PrecisionTimestampTZ type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final IntervalYear type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final IntervalDay type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final UUID type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final FixedChar type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final VarChar type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final FixedBinary type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final Decimal type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final Struct type) throws RuntimeException {
    final StringBuffer sb = new StringBuffer(type.getClass().getSimpleName());
    type.fields()
        .forEach(
            f -> {
              sb.append(f.accept(this));
            });
    return sb.toString();
  }

  @Override
  public String visit(final ListType type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final Map type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final UserDefined type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final PrecisionTime type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(final IntervalCompound type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }
}
