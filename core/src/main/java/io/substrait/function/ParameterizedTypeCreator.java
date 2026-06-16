package io.substrait.function;

import io.substrait.type.TypeCreator;

/** Creates {@link ParameterizedType}s whose parameters are named string-literal placeholders. */
public class ParameterizedTypeCreator extends TypeCreator
    implements ExtendedTypeCreator<ParameterizedType, String> {

  /** Creator producing non-nullable parameterized types. */
  public static final ParameterizedTypeCreator REQUIRED = new ParameterizedTypeCreator(false);

  /** Creator producing nullable parameterized types. */
  public static final ParameterizedTypeCreator NULLABLE = new ParameterizedTypeCreator(true);

  /**
   * Creates a parameterized-type creator.
   *
   * @param nullable whether produced types are nullable
   */
  protected ParameterizedTypeCreator(boolean nullable) {
    super(nullable);
  }

  private static ParameterizedType.StringLiteral parameter(String literal, boolean nullable) {
    return ParameterizedType.StringLiteral.builder().nullable(nullable).value(literal).build();
  }

  /**
   * Creates a string-literal parameter placeholder with this creator's nullability.
   *
   * @param literal the parameter name
   * @return the string-literal parameter
   */
  public ParameterizedType.StringLiteral parameter(String literal) {
    return parameter(literal, nullable);
  }

  @Override
  public ParameterizedType fixedCharE(String len) {
    return ParameterizedType.FixedChar.builder()
        .nullable(nullable)
        .length(parameter(len, false))
        .build();
  }

  @Override
  public ParameterizedType varCharE(String len) {
    return ParameterizedType.VarChar.builder()
        .nullable(nullable)
        .length(parameter(len, false))
        .build();
  }

  @Override
  public ParameterizedType fixedBinaryE(String len) {
    return ParameterizedType.FixedBinary.builder()
        .nullable(nullable)
        .length(parameter(len, false))
        .build();
  }

  @Override
  public ParameterizedType decimalE(String precision, String scale) {
    return ParameterizedType.Decimal.builder()
        .nullable(nullable)
        .precision(parameter(precision, false))
        .scale(parameter(scale, false))
        .build();
  }

  /**
   * Creates an interval-day type with a parameterized precision.
   *
   * @param precision the precision parameter
   * @return the interval-day type
   */
  public ParameterizedType intervalDayE(String precision) {
    return ParameterizedType.IntervalDay.builder()
        .nullable(nullable)
        .precision(parameter(precision, false))
        .build();
  }

  /**
   * Creates an interval-compound type with a parameterized precision.
   *
   * @param precision the precision parameter
   * @return the interval-compound type
   */
  public ParameterizedType intervalCompoundE(String precision) {
    return ParameterizedType.IntervalCompound.builder()
        .nullable(nullable)
        .precision(parameter(precision, false))
        .build();
  }

  /**
   * Creates a precision-time type with a parameterized precision.
   *
   * @param precision the precision parameter
   * @return the precision-time type
   */
  public ParameterizedType precisionTimeE(String precision) {
    return ParameterizedType.PrecisionTime.builder()
        .nullable(nullable)
        .precision(parameter(precision, false))
        .build();
  }

  /**
   * Creates a precision-timestamp type with a parameterized precision.
   *
   * @param precision the precision parameter
   * @return the precision-timestamp type
   */
  public ParameterizedType precisionTimestampE(String precision) {
    return ParameterizedType.PrecisionTimestamp.builder()
        .nullable(nullable)
        .precision(parameter(precision, false))
        .build();
  }

  /**
   * Creates a precision-timestamp-with-timezone type with a parameterized precision.
   *
   * @param precision the precision parameter
   * @return the precision-timestamp-tz type
   */
  public ParameterizedType precisionTimestampTZE(String precision) {
    return ParameterizedType.PrecisionTimestampTZ.builder()
        .nullable(nullable)
        .precision(parameter(precision, false))
        .build();
  }

  @Override
  public ParameterizedType structE(ParameterizedType... types) {
    return ParameterizedType.Struct.builder().nullable(nullable).addFields(types).build();
  }

  @Override
  public ParameterizedType structE(Iterable<? extends ParameterizedType> types) {
    return ParameterizedType.Struct.builder().nullable(nullable).addAllFields(types).build();
  }

  @Override
  public ParameterizedType listE(ParameterizedType type) {
    return ParameterizedType.ListType.builder().nullable(nullable).name(type).build();
  }

  @Override
  public ParameterizedType funcE(
      Iterable<? extends ParameterizedType> parameterTypes, ParameterizedType returnType) {
    return ParameterizedType.Func.builder()
        .nullable(nullable)
        .addAllParameterTypes(parameterTypes)
        .returnType(returnType)
        .build();
  }

  @Override
  public ParameterizedType mapE(ParameterizedType key, ParameterizedType value) {
    return ParameterizedType.Map.builder().nullable(nullable).key(key).value(value).build();
  }
}
