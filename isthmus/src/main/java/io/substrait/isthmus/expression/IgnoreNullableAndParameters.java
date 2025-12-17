package io.substrait.isthmus.expression;

import io.substrait.function.ParameterizedType;
import io.substrait.function.ParameterizedTypeVisitor;
import io.substrait.type.Type;

/**
 * Visitor that compares a given {@link ParameterizedType} against visited types, ignoring
 * nullability and (where applicable) type parameters.
 *
 * <p>Intended for structural type compatibility checks where exact parameter values are not
 * required.
 */
public class IgnoreNullableAndParameters
    implements ParameterizedTypeVisitor<Boolean, RuntimeException> {

  private final ParameterizedType typeToMatch;

  /**
   * Creates a visitor that will compare visited types against {@code typeToMatch}.
   *
   * @param typeToMatch the target type to compare against
   */
  public IgnoreNullableAndParameters(ParameterizedType typeToMatch) {
    this.typeToMatch = typeToMatch;
  }

  /**
   * Compares {@link Type.Bool} ignoring nullability.
   *
   * @param type boolean type
   * @return {@code true} if {@code typeToMatch} is {@link Type.Bool}
   */
  @Override
  public Boolean visit(Type.Bool type) {
    return typeToMatch instanceof Type.Bool;
  }

  /**
   * Compares {@link Type.I8} ignoring nullability.
   *
   * @param type 8-bit integer type
   * @return {@code true} if {@code typeToMatch} is {@link Type.I8}
   */
  @Override
  public Boolean visit(Type.I8 type) {
    return typeToMatch instanceof Type.I8;
  }

  /**
   * Compares {@link Type.I16} ignoring nullability.
   *
   * @param type 16-bit integer type
   * @return {@code true} if {@code typeToMatch} is {@link Type.I16}
   */
  @Override
  public Boolean visit(Type.I16 type) {
    return typeToMatch instanceof Type.I16;
  }

  /**
   * Compares {@link Type.I32} ignoring nullability.
   *
   * @param type 32-bit integer type
   * @return {@code true} if {@code typeToMatch} is {@link Type.I32}
   */
  @Override
  public Boolean visit(Type.I32 type) {
    return typeToMatch instanceof Type.I32;
  }

  /**
   * Compares {@link Type.I64} ignoring nullability.
   *
   * @param type 64-bit integer type
   * @return {@code true} if {@code typeToMatch} is {@link Type.I64}
   */
  @Override
  public Boolean visit(Type.I64 type) {
    return typeToMatch instanceof Type.I64;
  }

  /**
   * Compares {@link Type.FP32} ignoring nullability.
   *
   * @param type 32-bit floating point type
   * @return {@code true} if {@code typeToMatch} is {@link Type.FP32}
   */
  @Override
  public Boolean visit(Type.FP32 type) {
    return typeToMatch instanceof Type.FP32;
  }

  /**
   * Compares {@link Type.FP64} ignoring nullability.
   *
   * @param type 64-bit floating point type
   * @return {@code true} if {@code typeToMatch} is {@link Type.FP64}
   */
  @Override
  public Boolean visit(Type.FP64 type) {
    return typeToMatch instanceof Type.FP64;
  }

  /**
   * Compares {@link Type.Str} ignoring nullability.
   *
   * @param type string type
   * @return {@code true} if {@code typeToMatch} is {@link Type.Str}
   */
  @Override
  public Boolean visit(Type.Str type) {
    return typeToMatch instanceof Type.Str;
  }

  /**
   * Compares {@link Type.Binary} ignoring nullability.
   *
   * @param type binary (var-length) type
   * @return {@code true} if {@code typeToMatch} is {@link Type.Binary}
   */
  @Override
  public Boolean visit(Type.Binary type) {
    return typeToMatch instanceof Type.Binary;
  }

  /**
   * Compares {@link Type.Date} ignoring nullability.
   *
   * @param type date type
   * @return {@code true} if {@code typeToMatch} is {@link Type.Date}
   */
  @Override
  public Boolean visit(Type.Date type) {
    return typeToMatch instanceof Type.Date;
  }

  /**
   * Compares {@link Type.Time} ignoring nullability.
   *
   * @param type time type
   * @return {@code true} if {@code typeToMatch} is {@link Type.Time}
   */
  @Override
  public Boolean visit(Type.Time type) {
    return typeToMatch instanceof Type.Time;
  }

  /**
   * Compares {@link Type.TimestampTZ} ignoring nullability.
   *
   * @param type timestamp-with-time-zone type
   * @return {@code true} if {@code typeToMatch} is {@link Type.TimestampTZ}
   */
  @Override
  public Boolean visit(Type.TimestampTZ type) {
    return typeToMatch instanceof Type.TimestampTZ;
  }

  /**
   * Compares {@link Type.Timestamp} ignoring nullability.
   *
   * @param type timestamp type
   * @return {@code true} if {@code typeToMatch} is {@link Type.Timestamp}
   */
  @Override
  public Boolean visit(Type.Timestamp type) {
    return typeToMatch instanceof Type.Timestamp;
  }

  /**
   * Compares {@link Type.IntervalYear} ignoring nullability.
   *
   * @param type year-month interval type
   * @return {@code true} if {@code typeToMatch} is {@link Type.IntervalYear}
   */
  @Override
  public Boolean visit(Type.IntervalYear type) {
    return typeToMatch instanceof Type.IntervalYear;
  }

  /**
   * Compares {@link Type.IntervalDay} ignoring nullability and parameters.
   *
   * @param type day-time interval type
   * @return {@code true} if {@code typeToMatch} is {@link Type.IntervalDay} or {@link
   *     ParameterizedType.IntervalDay}
   */
  @Override
  public Boolean visit(Type.IntervalDay type) {
    return typeToMatch instanceof Type.IntervalDay
        || typeToMatch instanceof ParameterizedType.IntervalDay;
  }

  /**
   * Compares {@link Type.IntervalCompound} ignoring nullability and parameters.
   *
   * @param type compound interval type
   * @return {@code true} if {@code typeToMatch} is {@link Type.IntervalCompound} or {@link
   *     ParameterizedType.IntervalCompound}
   */
  @Override
  public Boolean visit(Type.IntervalCompound type) {
    return typeToMatch instanceof Type.IntervalCompound
        || typeToMatch instanceof ParameterizedType.IntervalCompound;
  }

  /**
   * Compares {@link Type.UUID} ignoring nullability.
   *
   * @param type UUID type
   * @return {@code true} if {@code typeToMatch} is {@link Type.UUID}
   */
  @Override
  public Boolean visit(Type.UUID type) {
    return typeToMatch instanceof Type.UUID;
  }

  /**
   * Compares {@link Type.UserDefined} for exact equality (URI and name).
   *
   * @param type user-defined type
   * @return {@code true} if {@code typeToMatch} equals {@code type}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(Type.UserDefined type) throws RuntimeException {
    // Two user-defined types are equal if they have the same uri AND name
    return typeToMatch.equals(type);
  }

  /**
   * Compares {@link Type.FixedChar} ignoring parameters and nullability.
   *
   * @param type fixed CHAR type
   * @return {@code true} if {@code typeToMatch} is {@link Type.FixedChar} or {@link
   *     ParameterizedType.FixedChar}
   */
  @Override
  public Boolean visit(Type.FixedChar type) {
    return typeToMatch instanceof Type.FixedChar
        || typeToMatch instanceof ParameterizedType.FixedChar;
  }

  /**
   * Compares {@link Type.VarChar} ignoring parameters and nullability.
   *
   * @param type variable CHAR type
   * @return {@code true} if {@code typeToMatch} is {@link Type.VarChar} or {@link
   *     ParameterizedType.VarChar}
   */
  @Override
  public Boolean visit(Type.VarChar type) {
    return typeToMatch instanceof Type.VarChar || typeToMatch instanceof ParameterizedType.VarChar;
  }

  /**
   * Compares {@link Type.FixedBinary} ignoring parameters and nullability.
   *
   * @param type fixed BINARY type
   * @return {@code true} if {@code typeToMatch} is {@link Type.FixedBinary} or {@link
   *     ParameterizedType.FixedBinary}
   */
  @Override
  public Boolean visit(Type.FixedBinary type) {
    return typeToMatch instanceof Type.FixedBinary
        || typeToMatch instanceof ParameterizedType.FixedBinary;
  }

  /**
   * Compares {@link Type.Decimal} ignoring parameters and nullability.
   *
   * @param type DECIMAL type
   * @return {@code true} if {@code typeToMatch} is {@link Type.Decimal} or {@link
   *     ParameterizedType.Decimal}
   */
  @Override
  public Boolean visit(Type.Decimal type) {
    return typeToMatch instanceof Type.Decimal || typeToMatch instanceof ParameterizedType.Decimal;
  }

  /**
   * Compares {@link Type.PrecisionTime} ignoring parameters and nullability.
   *
   * @param type precision TIME type
   * @return {@code true} if {@code typeToMatch} is {@link Type.PrecisionTime} or {@link
   *     ParameterizedType.PrecisionTime}
   */
  @Override
  public Boolean visit(Type.PrecisionTime type) {
    return typeToMatch instanceof Type.PrecisionTime
        || typeToMatch instanceof ParameterizedType.PrecisionTime;
  }

  /**
   * Compares {@link Type.PrecisionTimestamp} ignoring parameters and nullability.
   *
   * @param type precision TIMESTAMP type
   * @return {@code true} if {@code typeToMatch} is {@link Type.PrecisionTimestamp} or {@link
   *     ParameterizedType.PrecisionTimestamp}
   */
  @Override
  public Boolean visit(Type.PrecisionTimestamp type) {
    return typeToMatch instanceof Type.PrecisionTimestamp
        || typeToMatch instanceof ParameterizedType.PrecisionTimestamp;
  }

  /**
   * Compares {@link Type.PrecisionTimestampTZ} ignoring parameters and nullability.
   *
   * @param type precision TIMESTAMP WITH LOCAL TIME ZONE type
   * @return {@code true} if {@code typeToMatch} is {@link Type.PrecisionTimestampTZ} or {@link
   *     ParameterizedType.PrecisionTimestampTZ}
   */
  @Override
  public Boolean visit(Type.PrecisionTimestampTZ type) {
    return typeToMatch instanceof Type.PrecisionTimestampTZ
        || typeToMatch instanceof ParameterizedType.PrecisionTimestampTZ;
  }

  /**
   * Compares {@link Type.Struct} ignoring parameters and nullability.
   *
   * @param type STRUCT type
   * @return {@code true} if {@code typeToMatch} is {@link Type.Struct} or {@link
   *     ParameterizedType.Struct}
   */
  @Override
  public Boolean visit(Type.Struct type) {
    return typeToMatch instanceof Type.Struct || typeToMatch instanceof ParameterizedType.Struct;
  }

  /**
   * Compares {@link Type.ListType} ignoring parameters and nullability.
   *
   * @param type LIST type
   * @return {@code true} if {@code typeToMatch} is {@link Type.ListType} or {@link
   *     ParameterizedType.ListType}
   */
  @Override
  public Boolean visit(Type.ListType type) {
    return typeToMatch instanceof Type.ListType
        || typeToMatch instanceof ParameterizedType.ListType;
  }

  /**
   * Compares {@link Type.Map} ignoring parameters and nullability.
   *
   * @param type MAP type
   * @return {@code true} if {@code typeToMatch} is {@link Type.Map} or {@link
   *     ParameterizedType.Map}
   */
  @Override
  public Boolean visit(Type.Map type) {
    return typeToMatch instanceof Type.Map || typeToMatch instanceof ParameterizedType.Map;
  }

  /**
   * Compares parameterized {@link ParameterizedType.FixedChar} ignoring parameters.
   *
   * @param expr fixed CHAR parameterized type
   * @return {@code true} if {@code typeToMatch} is {@link Type.FixedChar} or {@link
   *     ParameterizedType.FixedChar}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(ParameterizedType.FixedChar expr) throws RuntimeException {
    return typeToMatch instanceof Type.FixedChar
        || typeToMatch instanceof ParameterizedType.FixedChar;
  }

  /**
   * Compares parameterized {@link ParameterizedType.VarChar} ignoring parameters.
   *
   * @param expr VARCHAR parameterized type
   * @return {@code true} if {@code typeToMatch} is {@link Type.VarChar} or {@link
   *     ParameterizedType.VarChar}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(ParameterizedType.VarChar expr) throws RuntimeException {
    return typeToMatch instanceof Type.VarChar || typeToMatch instanceof ParameterizedType.VarChar;
  }

  /**
   * Compares parameterized {@link ParameterizedType.FixedBinary} ignoring parameters.
   *
   * @param expr fixed BINARY parameterized type
   * @return {@code true} if {@code typeToMatch} is {@link Type.FixedBinary} or {@link
   *     ParameterizedType.FixedBinary}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(ParameterizedType.FixedBinary expr) throws RuntimeException {
    return typeToMatch instanceof Type.FixedBinary
        || typeToMatch instanceof ParameterizedType.FixedBinary;
  }

  /**
   * Compares parameterized {@link ParameterizedType.Decimal} ignoring parameters.
   *
   * @param expr DECIMAL parameterized type
   * @return {@code true} if {@code typeToMatch} is {@link Type.Decimal} or {@link
   *     ParameterizedType.Decimal}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(ParameterizedType.Decimal expr) throws RuntimeException {
    return typeToMatch instanceof Type.Decimal || typeToMatch instanceof ParameterizedType.Decimal;
  }

  /**
   * Compares parameterized {@link ParameterizedType.IntervalDay} ignoring parameters.
   *
   * @param expr day-time interval parameterized type
   * @return {@code true} if {@code typeToMatch} is {@link Type.IntervalDay} or {@link
   *     ParameterizedType.IntervalDay}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(ParameterizedType.IntervalDay expr) throws RuntimeException {
    return typeToMatch instanceof Type.IntervalDay
        || typeToMatch instanceof ParameterizedType.IntervalDay;
  }

  /**
   * Compares parameterized {@link ParameterizedType.IntervalCompound} ignoring parameters.
   *
   * @param expr compound interval parameterized type
   * @return {@code true} if {@code typeToMatch} is {@link Type.IntervalCompound} or {@link
   *     ParameterizedType.IntervalCompound}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(ParameterizedType.IntervalCompound expr) throws RuntimeException {
    return typeToMatch instanceof Type.IntervalCompound
        || typeToMatch instanceof ParameterizedType.IntervalCompound;
  }

  /**
   * Compares parameterized {@link ParameterizedType.PrecisionTime} ignoring parameters.
   *
   * @param expr precision TIME parameterized type
   * @return {@code true} if {@code typeToMatch} is {@link Type.PrecisionTime} or {@link
   *     ParameterizedType.PrecisionTime}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(ParameterizedType.PrecisionTime expr) throws RuntimeException {
    return typeToMatch instanceof Type.PrecisionTime
        || typeToMatch instanceof ParameterizedType.PrecisionTime;
  }

  /**
   * Compares parameterized {@link ParameterizedType.PrecisionTimestamp} ignoring parameters.
   *
   * @param expr precision TIMESTAMP parameterized type
   * @return {@code true} if {@code typeToMatch} is {@link Type.PrecisionTimestamp} or {@link
   *     ParameterizedType.PrecisionTimestamp}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(ParameterizedType.PrecisionTimestamp expr) throws RuntimeException {
    return typeToMatch instanceof Type.PrecisionTimestamp
        || typeToMatch instanceof ParameterizedType.PrecisionTimestamp;
  }

  /**
   * Compares parameterized {@link ParameterizedType.PrecisionTimestampTZ} ignoring parameters.
   *
   * @param expr precision TIMESTAMP WITH LOCAL TIME ZONE parameterized type
   * @return {@code true} if {@code typeToMatch} is {@link Type.PrecisionTimestampTZ} or {@link
   *     ParameterizedType.PrecisionTimestampTZ}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(ParameterizedType.PrecisionTimestampTZ expr) throws RuntimeException {
    return typeToMatch instanceof Type.PrecisionTimestampTZ
        || typeToMatch instanceof ParameterizedType.PrecisionTimestampTZ;
  }

  /**
   * Compares parameterized {@link ParameterizedType.Struct} ignoring parameters.
   *
   * @param expr STRUCT parameterized type
   * @return {@code true} if {@code typeToMatch} is {@link Type.Struct} or {@link
   *     ParameterizedType.Struct}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(ParameterizedType.Struct expr) throws RuntimeException {
    return typeToMatch instanceof Type.Struct || typeToMatch instanceof ParameterizedType.Struct;
  }

  /**
   * Compares parameterized {@link ParameterizedType.ListType} ignoring parameters.
   *
   * @param expr LIST parameterized type
   * @return {@code true} if {@code typeToMatch} is {@link Type.ListType} or {@link
   *     ParameterizedType.ListType}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(ParameterizedType.ListType expr) throws RuntimeException {
    return typeToMatch instanceof Type.ListType
        || typeToMatch instanceof ParameterizedType.ListType;
  }

  /**
   * Compares parameterized {@link ParameterizedType.Map} ignoring parameters.
   *
   * @param expr MAP parameterized type
   * @return {@code true} if {@code typeToMatch} is {@link Type.Map} or {@link
   *     ParameterizedType.Map}
   * @throws RuntimeException if comparison cannot be performed
   */
  @Override
  public Boolean visit(ParameterizedType.Map expr) throws RuntimeException {
    return typeToMatch instanceof Type.Map || typeToMatch instanceof ParameterizedType.Map;
  }

  /**
   * String literal parameterized types are not considered a match in this visitor.
   *
   * @param stringLiteral string literal parameterized type
   * @return always {@code false}
   * @throws RuntimeException never thrown in current implementation
   */
  @Override
  public Boolean visit(ParameterizedType.StringLiteral stringLiteral) throws RuntimeException {
    return false;
  }
}
