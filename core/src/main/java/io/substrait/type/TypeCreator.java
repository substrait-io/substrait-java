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
  public final Type INTERVAL_YEAR;
  public final Type UUID;

  private static NullableSettingTypeVisitor NULLABLE_TRUE_VISITOR =
      new NullableSettingTypeVisitor(true);
  private static NullableSettingTypeVisitor NULLABLE_FALSE_VISITOR =
      new NullableSettingTypeVisitor(false);

  protected TypeCreator(final boolean nullable) {
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
    INTERVAL_YEAR = Type.IntervalYear.builder().nullable(nullable).build();
    UUID = Type.UUID.builder().nullable(nullable).build();
  }

  public Type fixedChar(final int len) {
    return Type.FixedChar.builder().nullable(nullable).length(len).build();
  }

  public final Type varChar(final int len) {
    return Type.VarChar.builder().nullable(nullable).length(len).build();
  }

  public final Type fixedBinary(final int len) {
    return Type.FixedBinary.builder().nullable(nullable).length(len).build();
  }

  public final Type decimal(final int precision, final int scale) {
    return Type.Decimal.builder().nullable(nullable).precision(precision).scale(scale).build();
  }

  public final Type.Struct struct(final Type... types) {
    return Type.Struct.builder().nullable(nullable).addFields(types).build();
  }

  public final Type precisionTime(final int precision) {
    return Type.PrecisionTime.builder().nullable(nullable).precision(precision).build();
  }

  public final Type precisionTimestamp(final int precision) {
    return Type.PrecisionTimestamp.builder().nullable(nullable).precision(precision).build();
  }

  public final Type precisionTimestampTZ(final int precision) {
    return Type.PrecisionTimestampTZ.builder().nullable(nullable).precision(precision).build();
  }

  public final Type intervalDay(final int precision) {
    return Type.IntervalDay.builder().nullable(nullable).precision(precision).build();
  }

  public final Type intervalCompound(final int precision) {
    return Type.IntervalCompound.builder().nullable(nullable).precision(precision).build();
  }

  public Type.Struct struct(final Iterable<? extends Type> types) {
    return Type.Struct.builder().nullable(nullable).addAllFields(types).build();
  }

  public Type.Struct struct(final Stream<? extends Type> types) {
    return Type.Struct.builder()
        .nullable(nullable)
        .addAllFields(types.collect(Collectors.toList()))
        .build();
  }

  public Type.ListType list(final Type type) {
    return Type.ListType.builder().nullable(nullable).elementType(type).build();
  }

  public Type.Map map(final Type key, final Type value) {
    return Type.Map.builder().nullable(nullable).key(key).value(value).build();
  }

  public Type userDefined(final String urn, final String name) {
    return Type.UserDefined.builder().nullable(nullable).urn(urn).name(name).build();
  }

  public static TypeCreator of(final boolean nullability) {
    return nullability ? NULLABLE : REQUIRED;
  }

  public static Type asNullable(final Type type) {
    return type.nullable() ? type : type.accept(NULLABLE_TRUE_VISITOR);
  }

  public static Type asNotNullable(final Type type) {
    return type.nullable() ? type.accept(NULLABLE_FALSE_VISITOR) : type;
  }

  private static final class NullableSettingTypeVisitor
      implements TypeVisitor<Type, RuntimeException> {

    private final boolean nullability;

    NullableSettingTypeVisitor(final boolean nullability) {
      this.nullability = nullability;
    }

    @Override
    public Type visit(final Type.Bool type) throws RuntimeException {
      return Type.Bool.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.I8 type) throws RuntimeException {
      return Type.I8.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.I16 type) throws RuntimeException {
      return Type.I16.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.I32 type) throws RuntimeException {
      return Type.I32.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.I64 type) throws RuntimeException {
      return Type.I64.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.FP32 type) throws RuntimeException {
      return Type.FP32.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.FP64 type) throws RuntimeException {
      return Type.FP64.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.Str type) throws RuntimeException {
      return Type.Str.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.Binary type) throws RuntimeException {
      return Type.Binary.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.Date type) throws RuntimeException {
      return Type.Date.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.Time type) throws RuntimeException {
      return Type.Time.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.TimestampTZ type) throws RuntimeException {
      return Type.TimestampTZ.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.Timestamp type) throws RuntimeException {
      return Type.Timestamp.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.IntervalYear type) throws RuntimeException {
      return Type.IntervalYear.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.IntervalDay type) throws RuntimeException {
      return Type.IntervalDay.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.IntervalCompound type) throws RuntimeException {
      return Type.IntervalCompound.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.UUID type) throws RuntimeException {
      return Type.UUID.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.FixedChar type) throws RuntimeException {
      return Type.FixedChar.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.VarChar type) throws RuntimeException {
      return Type.VarChar.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.FixedBinary type) throws RuntimeException {
      return Type.FixedBinary.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.Decimal type) throws RuntimeException {
      return Type.Decimal.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.PrecisionTime type) throws RuntimeException {
      return Type.PrecisionTime.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.PrecisionTimestamp type) throws RuntimeException {
      return Type.PrecisionTimestamp.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.PrecisionTimestampTZ type) throws RuntimeException {
      return Type.PrecisionTimestampTZ.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.Struct type) throws RuntimeException {
      return Type.Struct.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.ListType type) throws RuntimeException {
      return Type.ListType.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.Map type) throws RuntimeException {
      return Type.Map.builder().from(type).nullable(nullability).build();
    }

    @Override
    public Type visit(final Type.UserDefined type) throws RuntimeException {
      return Type.UserDefined.builder().from(type).nullable(nullability).build();
    }
  }
}
