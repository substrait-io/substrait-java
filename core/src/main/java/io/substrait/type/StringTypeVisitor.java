package io.substrait.type;

import java.util.stream.Collectors;

public class StringTypeVisitor implements TypeVisitor<String, RuntimeException> {

  private String n(final Type type) {
    return type.nullable() ? "?" : "";
  }

  @Override
  public String visit(final Type.Bool type) throws RuntimeException {
    return "boolean" + n(type);
  }

  @Override
  public String visit(final Type.I8 type) throws RuntimeException {
    return "i8" + n(type);
  }

  @Override
  public String visit(final Type.I16 type) throws RuntimeException {
    return "i16" + n(type);
  }

  @Override
  public String visit(final Type.I32 type) throws RuntimeException {
    return "i32" + n(type);
  }

  @Override
  public String visit(final Type.I64 type) throws RuntimeException {
    return "i64" + n(type);
  }

  @Override
  public String visit(final Type.FP32 type) throws RuntimeException {
    return "fp32" + n(type);
  }

  @Override
  public String visit(final Type.FP64 type) throws RuntimeException {
    return "fp64" + n(type);
  }

  @Override
  public String visit(final Type.Str type) throws RuntimeException {
    return "string" + n(type);
  }

  @Override
  public String visit(final Type.Binary type) throws RuntimeException {
    return "binary" + n(type);
  }

  @Override
  public String visit(final Type.Date type) throws RuntimeException {
    return "date" + n(type);
  }

  @Override
  public String visit(final Type.Time type) throws RuntimeException {
    return "time" + n(type);
  }

  @Override
  public String visit(final Type.TimestampTZ type) throws RuntimeException {
    return "timestamp_tz" + n(type);
  }

  @Override
  public String visit(final Type.Timestamp type) throws RuntimeException {
    return "timestamp" + n(type);
  }

  @Override
  public String visit(final Type.IntervalYear type) throws RuntimeException {
    return "interval_year" + n(type);
  }

  @Override
  public String visit(final Type.IntervalDay type) throws RuntimeException {
    return "interval_day" + n(type);
  }

  @Override
  public String visit(final Type.IntervalCompound type) throws RuntimeException {
    return "interval_compound" + n(type);
  }

  @Override
  public String visit(final Type.UUID type) throws RuntimeException {
    return "uuid" + n(type);
  }

  @Override
  public String visit(final Type.FixedChar type) throws RuntimeException {
    return String.format("char<%d>%s", type.length(), n(type));
  }

  @Override
  public String visit(final Type.VarChar type) throws RuntimeException {
    return String.format("varchar<%d>%s", type.length(), n(type));
  }

  @Override
  public String visit(final Type.FixedBinary type) throws RuntimeException {
    return String.format("fixedbinary<%d>%s", type.length(), n(type));
  }

  @Override
  public String visit(final Type.Decimal type) throws RuntimeException {
    return String.format("decimal<%d,%d>%s", type.precision(), type.scale(), n(type));
  }

  @Override
  public String visit(final Type.PrecisionTime type) throws RuntimeException {
    return String.format("precision_time<%d>%s", type.precision(), n(type));
  }

  @Override
  public String visit(final Type.PrecisionTimestamp type) throws RuntimeException {
    return String.format("precision_timestamp<%d>%s", type.precision(), n(type));
  }

  @Override
  public String visit(final Type.PrecisionTimestampTZ type) throws RuntimeException {
    return String.format("precision_timestamp_tz<%d>%s", type.precision(), n(type));
  }

  @Override
  public String visit(final Type.Struct type) throws RuntimeException {
    return String.format(
        "struct<%s>%s",
        type.fields().stream().map(t -> t.accept(this)).collect(Collectors.joining(", ")), n(type));
  }

  @Override
  public String visit(final Type.ListType type) throws RuntimeException {
    return String.format("list<%s>%s", type.elementType().accept(this), n(type));
  }

  @Override
  public String visit(final Type.Map type) throws RuntimeException {
    return String.format(
        "map<%s,%s>%s", type.key().accept(this), type.value().accept(this), n(type));
  }

  @Override
  public String visit(final Type.UserDefined type) throws RuntimeException {
    return String.format("u!%s%s", type.name(), n(type));
  }
}
