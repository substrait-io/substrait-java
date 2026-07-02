package io.substrait.type;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Factory for {@link Type}s of a fixed nullability, exposing constants and parameterized builders.
 */
public class TypeCreator {

  /** Creator producing non-nullable types. */
  public static final TypeCreator REQUIRED = new TypeCreator(false);

  /** Creator producing nullable types. */
  public static final TypeCreator NULLABLE = new TypeCreator(true);

  /** Whether the types produced by this creator are nullable. */
  protected final boolean nullable;

  /** The boolean type. */
  public final Type BOOLEAN;

  /** The 8-bit integer type. */
  public final Type I8;

  /** The 16-bit integer type. */
  public final Type I16;

  /** The 32-bit integer type. */
  public final Type I32;

  /** The 64-bit integer type. */
  public final Type I64;

  /** The 32-bit floating point type. */
  public final Type FP32;

  /** The 64-bit floating point type. */
  public final Type FP64;

  /** The string type. */
  public final Type STRING;

  /** The binary type. */
  public final Type BINARY;

  /** The date type. */
  public final Type DATE;

  /** The year-month interval type. */
  public final Type INTERVAL_YEAR;

  /** The UUID type. */
  public final Type UUID;

  /**
   * Creates a type creator.
   *
   * @param nullable whether the produced types are nullable
   */
  protected TypeCreator(boolean nullable) {
    this.nullable = nullable;
    BOOLEAN = Type.Bool.builder().nullable(nullable).build();
    I8 = Type.I8.builder().nullable(nullable).build();
    I16 = Type.I16.builder().nullable(nullable).build();
    I32 = Type.I32.builder().nullable(nullable).build();
    I64 = Type.I64.builder().nullable(nullable).build();
    FP32 = Type.FP32.builder().nullable(nullable).build();
    FP64 = Type.FP64.builder().nullable(nullable).build();
    STRING = Type.Str.builder().nullable(nullable).build();
    BINARY = Type.Binary.builder().nullable(nullable).build();
    DATE = Type.Date.builder().nullable(nullable).build();
    INTERVAL_YEAR = Type.IntervalYear.builder().nullable(nullable).build();
    UUID = Type.UUID.builder().nullable(nullable).build();
  }

  /**
   * Creates a fixed-length character type.
   *
   * @param len the length
   * @return the fixed-char type
   */
  public Type fixedChar(int len) {
    return Type.FixedChar.builder().nullable(nullable).length(len).build();
  }

  /**
   * Creates a variable-length character type.
   *
   * @param len the maximum length
   * @return the varchar type
   */
  public final Type varChar(int len) {
    return Type.VarChar.builder().nullable(nullable).length(len).build();
  }

  /**
   * Creates a fixed-length binary type.
   *
   * @param len the length
   * @return the fixed-binary type
   */
  public final Type fixedBinary(int len) {
    return Type.FixedBinary.builder().nullable(nullable).length(len).build();
  }

  /**
   * Creates a decimal type.
   *
   * @param precision the total number of digits
   * @param scale the number of digits to the right of the decimal point
   * @return the decimal type
   */
  public final Type decimal(int precision, int scale) {
    return Type.Decimal.builder().nullable(nullable).precision(precision).scale(scale).build();
  }

  /**
   * Creates a struct type from the given field types.
   *
   * @param types the field types
   * @return the struct type
   */
  public final Type.Struct struct(Type... types) {
    return Type.Struct.builder().nullable(nullable).addFields(types).build();
  }

  /**
   * Creates a precision-time type.
   *
   * @param precision the fractional-second precision
   * @return the precision-time type
   */
  public final Type precisionTime(int precision) {
    return Type.PrecisionTime.builder().nullable(nullable).precision(precision).build();
  }

  /**
   * Creates a precision-timestamp (without timezone) type.
   *
   * @param precision the fractional-second precision
   * @return the precision-timestamp type
   */
  public final Type precisionTimestamp(int precision) {
    return Type.PrecisionTimestamp.builder().nullable(nullable).precision(precision).build();
  }

  /**
   * Creates a precision-timestamp with timezone type.
   *
   * @param precision the fractional-second precision
   * @return the precision-timestamp-tz type
   */
  public final Type precisionTimestampTZ(int precision) {
    return Type.PrecisionTimestampTZ.builder().nullable(nullable).precision(precision).build();
  }

  /**
   * Creates a day-time interval type.
   *
   * @param precision the fractional-second precision
   * @return the interval-day type
   */
  public final Type intervalDay(int precision) {
    return Type.IntervalDay.builder().nullable(nullable).precision(precision).build();
  }

  /**
   * Creates a compound interval type.
   *
   * @param precision the fractional-second precision
   * @return the interval-compound type
   */
  public final Type intervalCompound(int precision) {
    return Type.IntervalCompound.builder().nullable(nullable).precision(precision).build();
  }

  /**
   * Creates a function type.
   *
   * @param parameterTypes the parameter types
   * @param returnType the return type
   * @return the function type
   */
  public Type.Func func(java.util.List<Type> parameterTypes, Type returnType) {
    return Type.Func.builder()
        .nullable(nullable)
        .parameterTypes(parameterTypes)
        .returnType(returnType)
        .build();
  }

  /**
   * Creates a struct type from the given field types.
   *
   * @param types the field types
   * @return the struct type
   */
  public Type.Struct struct(Iterable<? extends Type> types) {
    return Type.Struct.builder().nullable(nullable).addAllFields(types).build();
  }

  /**
   * Creates a struct type from the given stream of field types.
   *
   * @param types the field types
   * @return the struct type
   */
  public Type.Struct struct(Stream<? extends Type> types) {
    return Type.Struct.builder()
        .nullable(nullable)
        .addAllFields(types.collect(Collectors.toList()))
        .build();
  }

  /**
   * Creates a list type with the given element type.
   *
   * @param type the element type
   * @return the list type
   */
  public Type.ListType list(Type type) {
    return Type.ListType.builder().nullable(nullable).elementType(type).build();
  }

  /**
   * Creates a map type with the given key and value types.
   *
   * @param key the key type
   * @param value the value type
   * @return the map type
   */
  public Type.Map map(Type key, Type value) {
    return Type.Map.builder().nullable(nullable).key(key).value(value).build();
  }

  /**
   * Creates a reference to a user-defined type with the default type variation.
   *
   * @param urn the extension URN declaring the type
   * @param name the type name
   * @return the user-defined type
   */
  public Type userDefined(String urn, String name) {
    return userDefined(urn, name, 0);
  }

  /**
   * Creates a reference to a user-defined type with a specific type variation.
   *
   * @param urn the extension URN declaring the type
   * @param name the type name
   * @param typeVariationReference the type variation reference
   * @return the user-defined type
   */
  public Type userDefined(String urn, String name, int typeVariationReference) {
    return Type.UserDefined.builder()
        .nullable(nullable)
        .urn(urn)
        .name(name)
        .typeVariationReference(typeVariationReference)
        .build();
  }

  /**
   * Returns the shared creator for the given nullability.
   *
   * @param nullability whether nullable types are desired
   * @return {@link #NULLABLE} if {@code nullability} is true, otherwise {@link #REQUIRED}
   */
  public static TypeCreator of(boolean nullability) {
    return nullability ? NULLABLE : REQUIRED;
  }

  /**
   * Make the given type NULLABLE.
   *
   * @param type the type to make nullable
   * @return the nullable variant of the type
   */
  public static Type asNullable(Type type) {
    return type.withNullable(true);
  }

  /**
   * Make the given type NOT NULLABLE.
   *
   * @param type the type to make non-nullable
   * @return the non-nullable variant of the type
   */
  public static Type asNotNullable(Type type) {
    return type.withNullable(false);
  }
}
