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

/** TypeStringify produces a simple debug string of Substrait types. */
public class TypeStringify extends ParentStringify
    implements TypeVisitor<String, RuntimeException> {

  /**
   * Constructor.
   *
   * @param indent numver of idents to use
   */
  protected TypeStringify(int indent) {
    super(indent);
  }

  @Override
  public String visit(I64 type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(Bool type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(I8 type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(I16 type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(I32 type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(FP32 type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(FP64 type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(Str type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(Binary type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(Date type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(Time type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  @Deprecated
  public String visit(TimestampTZ type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  @Deprecated
  public String visit(Timestamp type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(Type.PrecisionTimestamp type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(Type.PrecisionTimestampTZ type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(IntervalYear type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(IntervalDay type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(UUID type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(FixedChar type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(VarChar type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(FixedBinary type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(Decimal type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(Struct type) throws RuntimeException {
    StringBuffer sb = new StringBuffer(type.getClass().getSimpleName());
    type.fields()
        .forEach(
            f -> {
              sb.append(f.accept(this));
            });
    return sb.toString();
  }

  @Override
  public String visit(ListType type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(Map type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(UserDefined type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(PrecisionTime type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }

  @Override
  public String visit(IntervalCompound type) throws RuntimeException {
    return type.getClass().getSimpleName();
  }
}
