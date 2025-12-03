package io.substrait.isthmus.expression;

import io.substrait.function.ParameterizedType;
import io.substrait.function.ParameterizedTypeVisitor;
import io.substrait.type.Type;

public class IgnoreNullableAndParameters
    implements ParameterizedTypeVisitor<Boolean, RuntimeException> {

  private final ParameterizedType typeToMatch;

  public IgnoreNullableAndParameters(final ParameterizedType typeToMatch) {
    this.typeToMatch = typeToMatch;
  }

  @Override
  public Boolean visit(final Type.Bool type) {
    return typeToMatch instanceof Type.Bool;
  }

  @Override
  public Boolean visit(final Type.I8 type) {
    return typeToMatch instanceof Type.I8;
  }

  @Override
  public Boolean visit(final Type.I16 type) {
    return typeToMatch instanceof Type.I16;
  }

  @Override
  public Boolean visit(final Type.I32 type) {
    return typeToMatch instanceof Type.I32;
  }

  @Override
  public Boolean visit(final Type.I64 type) {
    return typeToMatch instanceof Type.I64;
  }

  @Override
  public Boolean visit(final Type.FP32 type) {
    return typeToMatch instanceof Type.FP32;
  }

  @Override
  public Boolean visit(final Type.FP64 type) {
    return typeToMatch instanceof Type.FP64;
  }

  @Override
  public Boolean visit(final Type.Str type) {
    return typeToMatch instanceof Type.Str;
  }

  @Override
  public Boolean visit(final Type.Binary type) {
    return typeToMatch instanceof Type.Binary;
  }

  @Override
  public Boolean visit(final Type.Date type) {
    return typeToMatch instanceof Type.Date;
  }

  @Override
  public Boolean visit(final Type.Time type) {
    return typeToMatch instanceof Type.Time;
  }

  @Override
  public Boolean visit(final Type.TimestampTZ type) {
    return typeToMatch instanceof Type.TimestampTZ;
  }

  @Override
  public Boolean visit(final Type.Timestamp type) {
    return typeToMatch instanceof Type.Timestamp;
  }

  @Override
  public Boolean visit(final Type.IntervalYear type) {
    return typeToMatch instanceof Type.IntervalYear;
  }

  @Override
  public Boolean visit(final Type.IntervalDay type) {
    return typeToMatch instanceof Type.IntervalDay
        || typeToMatch instanceof ParameterizedType.IntervalDay;
  }

  @Override
  public Boolean visit(final Type.IntervalCompound type) {
    return typeToMatch instanceof Type.IntervalCompound
        || typeToMatch instanceof ParameterizedType.IntervalCompound;
  }

  @Override
  public Boolean visit(final Type.UUID type) {
    return typeToMatch instanceof Type.UUID;
  }

  @Override
  public Boolean visit(final Type.UserDefined type) throws RuntimeException {
    // Two user-defined types are equal if they have the same uri AND name
    return typeToMatch.equals(type);
  }

  @Override
  public Boolean visit(final Type.FixedChar type) {
    return typeToMatch instanceof Type.FixedChar
        || typeToMatch instanceof ParameterizedType.FixedChar;
  }

  @Override
  public Boolean visit(final Type.VarChar type) {
    return typeToMatch instanceof Type.VarChar || typeToMatch instanceof ParameterizedType.VarChar;
  }

  @Override
  public Boolean visit(final Type.FixedBinary type) {
    return typeToMatch instanceof Type.FixedBinary
        || typeToMatch instanceof ParameterizedType.FixedBinary;
  }

  @Override
  public Boolean visit(final Type.Decimal type) {
    return typeToMatch instanceof Type.Decimal || typeToMatch instanceof ParameterizedType.Decimal;
  }

  @Override
  public Boolean visit(final Type.PrecisionTime type) {
    return typeToMatch instanceof Type.PrecisionTime
        || typeToMatch instanceof ParameterizedType.PrecisionTime;
  }

  @Override
  public Boolean visit(final Type.PrecisionTimestamp type) {
    return typeToMatch instanceof Type.PrecisionTimestamp
        || typeToMatch instanceof ParameterizedType.PrecisionTimestamp;
  }

  @Override
  public Boolean visit(final Type.PrecisionTimestampTZ type) {
    return typeToMatch instanceof Type.PrecisionTimestampTZ
        || typeToMatch instanceof ParameterizedType.PrecisionTimestampTZ;
  }

  @Override
  public Boolean visit(final Type.Struct type) {
    return typeToMatch instanceof Type.Struct || typeToMatch instanceof ParameterizedType.Struct;
  }

  @Override
  public Boolean visit(final Type.ListType type) {
    return typeToMatch instanceof Type.ListType
        || typeToMatch instanceof ParameterizedType.ListType;
  }

  @Override
  public Boolean visit(final Type.Map type) {
    return typeToMatch instanceof Type.Map || typeToMatch instanceof ParameterizedType.Map;
  }

  @Override
  public Boolean visit(final ParameterizedType.FixedChar expr) throws RuntimeException {
    return typeToMatch instanceof Type.FixedChar
        || typeToMatch instanceof ParameterizedType.FixedChar;
  }

  @Override
  public Boolean visit(final ParameterizedType.VarChar expr) throws RuntimeException {
    return typeToMatch instanceof Type.VarChar || typeToMatch instanceof ParameterizedType.VarChar;
  }

  @Override
  public Boolean visit(final ParameterizedType.FixedBinary expr) throws RuntimeException {
    return typeToMatch instanceof Type.FixedBinary
        || typeToMatch instanceof ParameterizedType.FixedBinary;
  }

  @Override
  public Boolean visit(final ParameterizedType.Decimal expr) throws RuntimeException {
    return typeToMatch instanceof Type.Decimal || typeToMatch instanceof ParameterizedType.Decimal;
  }

  @Override
  public Boolean visit(final ParameterizedType.IntervalDay expr) throws RuntimeException {
    return typeToMatch instanceof Type.IntervalDay
        || typeToMatch instanceof ParameterizedType.IntervalDay;
  }

  @Override
  public Boolean visit(final ParameterizedType.IntervalCompound expr) throws RuntimeException {
    return typeToMatch instanceof Type.IntervalCompound
        || typeToMatch instanceof ParameterizedType.IntervalCompound;
  }

  @Override
  public Boolean visit(final ParameterizedType.PrecisionTime expr) throws RuntimeException {
    return typeToMatch instanceof Type.PrecisionTime
        || typeToMatch instanceof ParameterizedType.PrecisionTime;
  }

  @Override
  public Boolean visit(final ParameterizedType.PrecisionTimestamp expr) throws RuntimeException {
    return typeToMatch instanceof Type.PrecisionTimestamp
        || typeToMatch instanceof ParameterizedType.PrecisionTimestamp;
  }

  @Override
  public Boolean visit(final ParameterizedType.PrecisionTimestampTZ expr) throws RuntimeException {
    return typeToMatch instanceof Type.PrecisionTimestampTZ
        || typeToMatch instanceof ParameterizedType.PrecisionTimestampTZ;
  }

  @Override
  public Boolean visit(final ParameterizedType.Struct expr) throws RuntimeException {
    return typeToMatch instanceof Type.Struct || typeToMatch instanceof ParameterizedType.Struct;
  }

  @Override
  public Boolean visit(final ParameterizedType.ListType expr) throws RuntimeException {
    return typeToMatch instanceof Type.ListType
        || typeToMatch instanceof ParameterizedType.ListType;
  }

  @Override
  public Boolean visit(final ParameterizedType.Map expr) throws RuntimeException {
    return typeToMatch instanceof Type.Map || typeToMatch instanceof ParameterizedType.Map;
  }

  @Override
  public Boolean visit(final ParameterizedType.StringLiteral stringLiteral)
      throws RuntimeException {
    return false;
  }
}
