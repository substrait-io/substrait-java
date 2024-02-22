package io.substrait.function;

import io.substrait.type.Type;

public class ToTypeString
    extends ParameterizedTypeVisitor.ParameterizedTypeThrowsVisitor<String, RuntimeException> {

  public static final ToTypeString INSTANCE = new ToTypeString();

  public static String apply(Type type) {
    return type.accept(INSTANCE);
  }

  private ToTypeString() {
    super("Only type literals and parameterized types can be used in functions.");
  }

  @Override
  public String visit(final Type.Bool expr) {
    return "bool";
  }

  @Override
  public String visit(final Type.I8 expr) {
    return "i8";
  }

  @Override
  public String visit(final Type.I16 expr) {
    return "i16";
  }

  @Override
  public String visit(final Type.I32 expr) {
    return "i32";
  }

  @Override
  public String visit(final Type.I64 expr) {
    return "i64";
  }

  @Override
  public String visit(final Type.FP32 expr) {
    return "fp32";
  }

  @Override
  public String visit(final Type.FP64 expr) {
    return "fp64";
  }

  @Override
  public String visit(final Type.Str expr) {
    return "str";
  }

  @Override
  public String visit(final Type.Binary expr) {
    return "binary";
  }

  @Override
  public String visit(final Type.Date expr) {
    return "date";
  }

  @Override
  public String visit(final Type.Time expr) {
    return "time";
  }

  @Override
  public String visit(final Type.TimestampTZ expr) {
    return "tstz";
  }

  @Override
  public String visit(final Type.Timestamp expr) {
    return "ts";
  }

  @Override
  public String visit(final Type.IntervalYear expr) {
    return "year";
  }

  @Override
  public String visit(final Type.IntervalDay expr) {
    return "day";
  }

  @Override
  public String visit(final Type.UUID expr) {
    return "uuid";
  }

  @Override
  public String visit(final Type.FixedChar expr) {
    return "fchar";
  }

  @Override
  public String visit(final Type.VarChar expr) {
    return "vchar";
  }

  @Override
  public String visit(final Type.FixedBinary expr) {
    return "fbinary";
  }

  @Override
  public String visit(final Type.Decimal expr) {
    return "dec";
  }

  @Override
  public String visit(final Type.Struct expr) {
    return "struct";
  }

  @Override
  public String visit(final Type.ListType expr) {
    return "list";
  }

  @Override
  public String visit(final Type.Map expr) {
    return "map";
  }

  @Override
  public String visit(final Type.UserDefined expr) {
    return String.format("u!%s", expr.name());
  }

  @Override
  public String visit(ParameterizedType.FixedChar expr) throws RuntimeException {
    return "fchar";
  }

  @Override
  public String visit(ParameterizedType.VarChar expr) throws RuntimeException {
    return "vchar";
  }

  @Override
  public String visit(ParameterizedType.FixedBinary expr) throws RuntimeException {
    return "fbinary";
  }

  @Override
  public String visit(ParameterizedType.Decimal expr) throws RuntimeException {
    return "dec";
  }

  @Override
  public String visit(ParameterizedType.Struct expr) throws RuntimeException {
    return "struct";
  }

  @Override
  public String visit(ParameterizedType.ListType expr) throws RuntimeException {
    return "list";
  }

  @Override
  public String visit(ParameterizedType.Map expr) throws RuntimeException {
    return "map";
  }

  @Override
  public String visit(ParameterizedType.StringLiteral expr) throws RuntimeException {
    if (expr.value().toLowerCase().startsWith("any")) {
      return "any";
    } else {
      return super.visit(expr);
    }
  }

  /**
   * Subclass of ToTypeString that doesn't lose the context on the wildcard being used (for example,
   * that can return any1, any2, etc, instead of only any, any).
   */
  public static class ToTypeLiteralStringLossless extends ToTypeString {

    public static final ToTypeLiteralStringLossless INSTANCE = new ToTypeLiteralStringLossless();

    private ToTypeLiteralStringLossless() {}

    @Override
    public String visit(ParameterizedType.StringLiteral expr) throws RuntimeException {
      return expr.value().toLowerCase();
    }
  }
}
