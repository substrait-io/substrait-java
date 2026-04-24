package io.substrait.type;

/**
 * Counts the number of field names required for a {@link Type} using Substrait's depth-first naming
 * rules.
 *
 * <p>This is the same counting scheme used by {@link NamedStruct#names()}: top-level struct fields
 * contribute one name each, and nested struct fields inside structs, lists, and maps also
 * contribute names in depth-first order. Scalar types and other non-structural types do not
 * contribute additional names.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code struct<i64, i64>} requires 2 names
 *   <li>{@code list<struct<i64, i64>>} requires 2 names
 *   <li>{@code map<struct<i64, i64>, struct<i64, i64, i64>>} requires 5 names
 * </ul>
 *
 * <p>This utility is used anywhere the library needs to validate or reason about name counts
 * without carrying the names themselves, such as {@code Plan.Root} and {@code VirtualTableScan}
 * validation.
 */
public final class NamedFieldCountingTypeVisitor implements TypeVisitor<Integer, RuntimeException> {

  private static final NamedFieldCountingTypeVisitor VISITOR = new NamedFieldCountingTypeVisitor();

  private NamedFieldCountingTypeVisitor() {}

  /**
   * Returns the number of names required to describe {@code type} in Substrait's depth-first naming
   * order.
   *
   * <p>For a top-level struct, this includes both the top-level field names and any nested struct
   * field names required by compound child types.
   *
   * @param type the type to inspect
   * @return the number of required names
   */
  public static int countNames(Type type) {
    return type.accept(VISITOR);
  }

  @Override
  public Integer visit(Type.Bool type) {
    return 0;
  }

  @Override
  public Integer visit(Type.I8 type) {
    return 0;
  }

  @Override
  public Integer visit(Type.I16 type) {
    return 0;
  }

  @Override
  public Integer visit(Type.I32 type) {
    return 0;
  }

  @Override
  public Integer visit(Type.I64 type) {
    return 0;
  }

  @Override
  public Integer visit(Type.FP32 type) {
    return 0;
  }

  @Override
  public Integer visit(Type.FP64 type) {
    return 0;
  }

  @Override
  public Integer visit(Type.Str type) {
    return 0;
  }

  @Override
  public Integer visit(Type.Binary type) {
    return 0;
  }

  @Override
  public Integer visit(Type.Date type) {
    return 0;
  }

  @Override
  public Integer visit(Type.Time type) {
    return 0;
  }

  @Override
  public Integer visit(Type.TimestampTZ type) {
    return 0;
  }

  @Override
  public Integer visit(Type.Timestamp type) {
    return 0;
  }

  @Override
  public Integer visit(Type.PrecisionTime type) {
    return 0;
  }

  @Override
  public Integer visit(Type.PrecisionTimestamp type) {
    return 0;
  }

  @Override
  public Integer visit(Type.PrecisionTimestampTZ type) {
    return 0;
  }

  @Override
  public Integer visit(Type.IntervalYear type) {
    return 0;
  }

  @Override
  public Integer visit(Type.IntervalDay type) {
    return 0;
  }

  @Override
  public Integer visit(Type.IntervalCompound type) {
    return 0;
  }

  @Override
  public Integer visit(Type.UUID type) {
    return 0;
  }

  @Override
  public Integer visit(Type.FixedChar type) {
    return 0;
  }

  @Override
  public Integer visit(Type.VarChar type) {
    return 0;
  }

  @Override
  public Integer visit(Type.FixedBinary type) {
    return 0;
  }

  @Override
  public Integer visit(Type.Decimal type) {
    return 0;
  }

  @Override
  public Integer visit(Type.Func type) {
    return 0;
  }

  @Override
  public Integer visit(Type.Struct type) {
    // Each struct field contributes its own name, plus any nested names required by that field's
    // type.
    return type.fields().stream().mapToInt(field -> 1 + countNames(field)).sum();
  }

  @Override
  public Integer visit(Type.ListType type) {
    // Lists do not add a name themselves, but list elements may contain nested structs.
    return countNames(type.elementType());
  }

  @Override
  public Integer visit(Type.Map type) {
    // Maps do not add names themselves; any required names come from struct keys and/or values.
    return countNames(type.key()) + countNames(type.value());
  }

  @Override
  public Integer visit(Type.UserDefined type) {
    return 0;
  }
}
