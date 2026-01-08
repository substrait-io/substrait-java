package io.substrait.isthmus.expression;

import io.substrait.function.ParameterizedType;
import io.substrait.function.ParameterizedTypeVisitor;
import io.substrait.type.Type;

public class IgnoreNullableAndParameters
    implements ParameterizedTypeVisitor<Boolean, RuntimeException> {

  private final ParameterizedType typeToMatch;

  public IgnoreNullableAndParameters(ParameterizedType typeToMatch) {
    this.typeToMatch = typeToMatch;
  }

  @Override
  public Boolean visit(Type.Bool type) {
    return typeToMatch instanceof Type.Bool;
  }

  @Override
  public Boolean visit(Type.I8 type) {
    return typeToMatch instanceof Type.I8;
  }

  @Override
  public Boolean visit(Type.I16 type) {
    return typeToMatch instanceof Type.I16;
  }

  @Override
  public Boolean visit(Type.I32 type) {
    return typeToMatch instanceof Type.I32;
  }

  @Override
  public Boolean visit(Type.I64 type) {
    return typeToMatch instanceof Type.I64;
  }

  @Override
  public Boolean visit(Type.FP32 type) {
    return typeToMatch instanceof Type.FP32;
  }

  @Override
  public Boolean visit(Type.FP64 type) {
    return typeToMatch instanceof Type.FP64;
  }

  @Override
  public Boolean visit(Type.Str type) {
    // Treat all string types as compatible: Str, VarChar, and FixedChar
    return typeToMatch instanceof Type.Str
        || typeToMatch instanceof Type.VarChar
        || typeToMatch instanceof Type.FixedChar
        || typeToMatch instanceof ParameterizedType.VarChar
        || typeToMatch instanceof ParameterizedType.FixedChar;
  }

  @Override
  public Boolean visit(Type.Binary type) {
    return typeToMatch instanceof Type.Binary;
  }

  @Override
  public Boolean visit(Type.Date type) {
    return typeToMatch instanceof Type.Date;
  }

  @Override
  public Boolean visit(Type.Time type) {
    return typeToMatch instanceof Type.Time;
  }

  @Override
  public Boolean visit(Type.TimestampTZ type) {
    return typeToMatch instanceof Type.TimestampTZ;
  }

  @Override
  public Boolean visit(Type.Timestamp type) {
    return typeToMatch instanceof Type.Timestamp;
  }

  @Override
  public Boolean visit(Type.IntervalYear type) {
    return typeToMatch instanceof Type.IntervalYear;
  }

  @Override
  public Boolean visit(Type.IntervalDay type) {
    return typeToMatch instanceof Type.IntervalDay
        || typeToMatch instanceof ParameterizedType.IntervalDay;
  }

  @Override
  public Boolean visit(Type.IntervalCompound type) {
    return typeToMatch instanceof Type.IntervalCompound
        || typeToMatch instanceof ParameterizedType.IntervalCompound;
  }

  @Override
  public Boolean visit(Type.UUID type) {
    return typeToMatch instanceof Type.UUID;
  }

  @Override
  public Boolean visit(Type.UserDefined type) throws RuntimeException {
    // Two user-defined types are equal if they have the same uri AND name
    return typeToMatch.equals(type);
  }

  @Override
  public Boolean visit(Type.FixedChar type) {
    // Treat all string types as compatible: Str, VarChar, and FixedChar
    return typeToMatch instanceof Type.FixedChar
        || typeToMatch instanceof ParameterizedType.FixedChar
        || typeToMatch instanceof Type.Str
        || typeToMatch instanceof Type.VarChar
        || typeToMatch instanceof ParameterizedType.VarChar;
  }

  @Override
  public Boolean visit(Type.VarChar type) {
    // Treat all string types as compatible: Str, VarChar, and FixedChar
    return typeToMatch instanceof Type.VarChar
        || typeToMatch instanceof ParameterizedType.VarChar
        || typeToMatch instanceof Type.Str
        || typeToMatch instanceof Type.FixedChar
        || typeToMatch instanceof ParameterizedType.FixedChar;
  }

  @Override
  public Boolean visit(Type.FixedBinary type) {
    return typeToMatch instanceof Type.FixedBinary
        || typeToMatch instanceof ParameterizedType.FixedBinary;
  }

  @Override
  public Boolean visit(Type.Decimal type) {
    return typeToMatch instanceof Type.Decimal || typeToMatch instanceof ParameterizedType.Decimal;
  }

  @Override
  public Boolean visit(Type.PrecisionTime type) {
    return typeToMatch instanceof Type.PrecisionTime
        || typeToMatch instanceof Type.Time
        || typeToMatch instanceof ParameterizedType.PrecisionTime;
  }

  @Override
  public Boolean visit(Type.PrecisionTimestamp type) {
    return typeToMatch instanceof Type.PrecisionTimestamp
        || typeToMatch instanceof Type.Timestamp
        || typeToMatch instanceof ParameterizedType.PrecisionTimestamp;
  }

  @Override
  public Boolean visit(Type.PrecisionTimestampTZ type) {
    return typeToMatch instanceof Type.PrecisionTimestampTZ
        || typeToMatch instanceof Type.TimestampTZ
        || typeToMatch instanceof ParameterizedType.PrecisionTimestampTZ;
  }

  @Override
  public Boolean visit(Type.Struct type) {
    return typeToMatch instanceof Type.Struct || typeToMatch instanceof ParameterizedType.Struct;
  }

  @Override
  public Boolean visit(Type.ListType type) {
    return typeToMatch instanceof Type.ListType
        || typeToMatch instanceof ParameterizedType.ListType;
  }

  @Override
  public Boolean visit(Type.Map type) {
    return typeToMatch instanceof Type.Map || typeToMatch instanceof ParameterizedType.Map;
  }

  @Override
  public Boolean visit(ParameterizedType.FixedChar expr) throws RuntimeException {
    // Treat all string types as compatible: Str, VarChar, and FixedChar
    return typeToMatch instanceof Type.FixedChar
        || typeToMatch instanceof ParameterizedType.FixedChar
        || typeToMatch instanceof Type.Str
        || typeToMatch instanceof Type.VarChar
        || typeToMatch instanceof ParameterizedType.VarChar;
  }

  @Override
  public Boolean visit(ParameterizedType.VarChar expr) throws RuntimeException {
    // Treat all string types as compatible: Str, VarChar, and FixedChar
    return typeToMatch instanceof Type.VarChar
        || typeToMatch instanceof ParameterizedType.VarChar
        || typeToMatch instanceof Type.Str
        || typeToMatch instanceof Type.FixedChar
        || typeToMatch instanceof ParameterizedType.FixedChar;
  }

  @Override
  public Boolean visit(ParameterizedType.FixedBinary expr) throws RuntimeException {
    return typeToMatch instanceof Type.FixedBinary
        || typeToMatch instanceof ParameterizedType.FixedBinary;
  }

  @Override
  public Boolean visit(ParameterizedType.Decimal expr) throws RuntimeException {
    return typeToMatch instanceof Type.Decimal || typeToMatch instanceof ParameterizedType.Decimal;
  }

  @Override
  public Boolean visit(ParameterizedType.IntervalDay expr) throws RuntimeException {
    return typeToMatch instanceof Type.IntervalDay
        || typeToMatch instanceof ParameterizedType.IntervalDay;
  }

  @Override
  public Boolean visit(ParameterizedType.IntervalCompound expr) throws RuntimeException {
    return typeToMatch instanceof Type.IntervalCompound
        || typeToMatch instanceof ParameterizedType.IntervalCompound;
  }

  @Override
  public Boolean visit(ParameterizedType.PrecisionTime expr) throws RuntimeException {
    return typeToMatch instanceof Type.PrecisionTime
        || typeToMatch instanceof Type.Time
        || typeToMatch instanceof ParameterizedType.PrecisionTime;
  }

  @Override
  public Boolean visit(ParameterizedType.PrecisionTimestamp expr) throws RuntimeException {
    return typeToMatch instanceof Type.PrecisionTimestamp
        || typeToMatch instanceof Type.Timestamp
        || typeToMatch instanceof ParameterizedType.PrecisionTimestamp;
  }

  @Override
  public Boolean visit(ParameterizedType.PrecisionTimestampTZ expr) throws RuntimeException {
    return typeToMatch instanceof Type.PrecisionTimestampTZ
        || typeToMatch instanceof Type.TimestampTZ
        || typeToMatch instanceof ParameterizedType.PrecisionTimestampTZ;
  }

  @Override
  public Boolean visit(ParameterizedType.Struct expr) throws RuntimeException {
    return typeToMatch instanceof Type.Struct || typeToMatch instanceof ParameterizedType.Struct;
  }

  @Override
  public Boolean visit(ParameterizedType.ListType expr) throws RuntimeException {
    return typeToMatch instanceof Type.ListType
        || typeToMatch instanceof ParameterizedType.ListType;
  }

  @Override
  public Boolean visit(ParameterizedType.Map expr) throws RuntimeException {
    return typeToMatch instanceof Type.Map || typeToMatch instanceof ParameterizedType.Map;
  }

  @Override
  public Boolean visit(ParameterizedType.StringLiteral stringLiteral) throws RuntimeException {
    return false;
  }
}
