package io.substrait.function;

import io.substrait.type.TypeCreator;

public class ParameterizedTypeCreator extends TypeCreator
    implements ExtendedTypeCreator<ParameterizedType, String> {

  public static final ParameterizedTypeCreator REQUIRED = new ParameterizedTypeCreator(false);
  public static final ParameterizedTypeCreator NULLABLE = new ParameterizedTypeCreator(true);

  protected ParameterizedTypeCreator(boolean nullable) {
    super(nullable);
  }

  private static ParameterizedType.StringLiteral parameter(String literal, boolean nullable) {
    return ParameterizedType.StringLiteral.builder().nullable(nullable).value(literal).build();
  }

  public ParameterizedType.StringLiteral parameter(String literal) {
    return parameter(literal, nullable);
  }

  public ParameterizedType fixedCharE(String len) {
    return ParameterizedType.FixedChar.builder()
        .nullable(nullable)
        .length(parameter(len, false))
        .build();
  }

  public ParameterizedType varCharE(String len) {
    return ParameterizedType.VarChar.builder()
        .nullable(nullable)
        .length(parameter(len, false))
        .build();
  }

  public ParameterizedType fixedBinaryE(String len) {
    return ParameterizedType.FixedBinary.builder()
        .nullable(nullable)
        .length(parameter(len, false))
        .build();
  }

  public ParameterizedType decimalE(String precision, String scale) {
    return ParameterizedType.Decimal.builder()
        .nullable(nullable)
        .precision(parameter(precision, false))
        .scale(parameter(scale, false))
        .build();
  }

  public ParameterizedType precisionTimestampE(String precision) {
    return ParameterizedType.PrecisionTimestamp.builder()
        .nullable(nullable)
        .precision(parameter(precision, false))
        .build();
  }

  public ParameterizedType precisionTimestampTZE(String precision) {
    return ParameterizedType.PrecisionTimestampTZ.builder()
        .nullable(nullable)
        .precision(parameter(precision, false))
        .build();
  }

  public ParameterizedType structE(ParameterizedType... types) {
    return ParameterizedType.Struct.builder().nullable(nullable).addFields(types).build();
  }

  public ParameterizedType structE(Iterable<? extends ParameterizedType> types) {
    return ParameterizedType.Struct.builder().nullable(nullable).addAllFields(types).build();
  }

  public ParameterizedType listE(ParameterizedType type) {
    return ParameterizedType.ListType.builder().nullable(nullable).name(type).build();
  }

  public ParameterizedType mapE(ParameterizedType key, ParameterizedType value) {
    return ParameterizedType.Map.builder().nullable(nullable).key(key).value(value).build();
  }
}
