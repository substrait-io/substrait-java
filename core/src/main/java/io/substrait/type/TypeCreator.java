package io.substrait.type;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TypeCreator {

  public static final TypeCreator REQUIRED = new TypeCreator(false);
  public static final TypeCreator NULLABLE = new TypeCreator(true);

  protected final boolean nullable;
  public final Type BOOLEAN;
  public final Type I8;
  public final Type I16;
  public final Type I32;
  public final Type I64;
  public final Type FP32;
  public final Type FP64;
  public final Type STRING;
  public final Type BINARY;
  public final Type TIMESTAMP;
  public final Type TIMESTAMP_TZ;
  public final Type DATE;
  public final Type TIME;
  public final Type INTERVAL_DAY;
  public final Type INTERVAL_YEAR;
  public final Type UUID;

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
    TIMESTAMP = Type.Timestamp.builder().nullable(nullable).build();
    TIMESTAMP_TZ = Type.TimestampTZ.builder().nullable(nullable).build();
    DATE = Type.Date.builder().nullable(nullable).build();
    TIME = Type.Time.builder().nullable(nullable).build();
    INTERVAL_DAY = Type.IntervalDay.builder().nullable(nullable).build();
    INTERVAL_YEAR = Type.IntervalYear.builder().nullable(nullable).build();
    UUID = Type.UUID.builder().nullable(nullable).build();
  }

  public Type fixedChar(int len) {
    return Type.FixedChar.builder().nullable(nullable).length(len).build();
  }

  public final Type varChar(int len) {
    return Type.VarChar.builder().nullable(nullable).length(len).build();
  }

  public final Type fixedBinary(int len) {
    return Type.FixedBinary.builder().nullable(nullable).length(len).build();
  }

  public final Type decimal(int precision, int scale) {
    return Type.Decimal.builder().nullable(nullable).precision(precision).scale(scale).build();
  }

  public final Type.Struct struct(Type... types) {
    return Type.Struct.builder().nullable(nullable).addFields(types).build();
  }

  public final Type precisionTimestamp(int precision) {
    return Type.PrecisionTimestamp.builder().nullable(nullable).precision(precision).build();
  }

  public final Type precisionTimestampTZ(int precision) {
    return Type.PrecisionTimestampTZ.builder().nullable(nullable).precision(precision).build();
  }

  public Type.Struct struct(Iterable<? extends Type> types) {
    return Type.Struct.builder().nullable(nullable).addAllFields(types).build();
  }

  public Type.Struct struct(Stream<? extends Type> types) {
    return Type.Struct.builder()
        .nullable(nullable)
        .addAllFields(types.collect(Collectors.toList()))
        .build();
  }

  public Type.ListType list(Type type) {
    return Type.ListType.builder().nullable(nullable).elementType(type).build();
  }

  public Type map(Type key, Type value) {
    return Type.Map.builder().nullable(nullable).key(key).value(value).build();
  }

  public Type userDefined(String uri, String name) {
    return Type.UserDefined.builder().nullable(nullable).uri(uri).name(name).build();
  }

  public static TypeCreator of(boolean nullability) {
    return nullability ? NULLABLE : REQUIRED;
  }

  public static Type asNullable(Type type) {
    return type.nullable() ? type : type.accept(NULLABLE_TRUE_VISITOR);
  }

  public static Type asNotNullable(Type type) {
    return type.nullable() ? type.accept(NULLABLE_FALSE_VISITOR) : type;
  }

  private static NullableSettingTypeVisitor NULLABLE_TRUE_VISITOR =
      new NullableSettingTypeVisitor(true);
  private static NullableSettingTypeVisitor NULLABLE_FALSE_VISITOR =
      new NullableSettingTypeVisitor(false);

  private static final class NullableSettingTypeVisitor
      implements TypeVisitor<Type, RuntimeException> {

    private final boolean nullability;

    NullableSettingTypeVisitor(boolean nullability) {
      this.nullability = nullability;
    }

    @Override
    public Type visit(Type.Bool type) throws RuntimeException {
      return Type.Bool.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.I8 type) throws RuntimeException {
      return Type.I8.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.I16 type) throws RuntimeException {
      return Type.I16.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.I32 type) throws RuntimeException {
      return Type.I32.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.I64 type) throws RuntimeException {
      return Type.I64.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.FP32 type) throws RuntimeException {
      return Type.FP32.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.FP64 type) throws RuntimeException {
      return Type.FP64.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.Str type) throws RuntimeException {
      return Type.Str.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.Binary type) throws RuntimeException {
      return Type.Binary.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.Date type) throws RuntimeException {
      return Type.Date.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.Time type) throws RuntimeException {
      return Type.Time.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.TimestampTZ type) throws RuntimeException {
      return Type.TimestampTZ.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.Timestamp type) throws RuntimeException {
      return Type.Timestamp.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.IntervalYear type) throws RuntimeException {
      return Type.IntervalYear.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.IntervalDay type) throws RuntimeException {
      return Type.IntervalDay.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.UUID type) throws RuntimeException {
      return Type.UUID.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.FixedChar type) throws RuntimeException {
      return Type.FixedChar.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.VarChar type) throws RuntimeException {
      return Type.VarChar.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.FixedBinary type) throws RuntimeException {
      return Type.FixedBinary.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.Decimal type) throws RuntimeException {
      return Type.Decimal.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.PrecisionTimestamp type) throws RuntimeException {
      return Type.PrecisionTimestamp.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.PrecisionTimestampTZ type) throws RuntimeException {
      return Type.PrecisionTimestampTZ.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.Struct type) throws RuntimeException {
      return Type.Struct.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.ListType type) throws RuntimeException {
      return Type.ListType.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.Map type) throws RuntimeException {
      return Type.Map.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(Type.UserDefined type) throws RuntimeException {
      return Type.UserDefined.builder().from(type).nullable(nullability).build();
    }
  }
}
