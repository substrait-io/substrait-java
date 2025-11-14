package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.type.TypeVisitor;
import io.substrait.util.VisitationContext;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Value;

@Value.Immutable
public abstract class VirtualTableScan extends AbstractReadRel {

  public abstract List<Expression.StructNested> getRows();

  /**
   *
   *
   * <ul>
   *   <li>non-empty rowset
   *   <li>non-null field-names
   *   <li>no null rows
   *   <li>row shape must match field-list
   * </ul>
   */
  @Value.Check
  protected void check() {
    List<String> names = getInitialSchema().names();

    assert names.size()
        == NamedFieldCountingTypeVisitor.countNames(this.getInitialSchema().struct());
    List<Expression.StructNested> rows = getRows();

    /**
     * In the codebase, we use `StructNested` to represent two subtly distinct cases: - an instance
     * of the Expression proto with the particular case of `rex_type` set to `Nested` (and then
     * `Struct`) - access to the raw proto of `Expression.Struct.Nested` itself
     *
     * <p>Here we are using the second case, and thus we are carrying around a nullable field as a
     * consequence of the first option, but we enforce it to be false to ensure a user doesn't
     * accidentally pass around meaningless additional context.
     */
    for (Expression.StructNested row : rows) {
      assert !row.nullable();
    }

    assert !rows.isEmpty()
        && names.stream().noneMatch(Objects::isNull)
        && rows.stream().noneMatch(Objects::isNull)
        && rows.stream()
            .allMatch(r -> NamedFieldCountingTypeVisitor.countNames(r.getType()) == names.size());
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableVirtualTableScan.Builder builder() {
    return ImmutableVirtualTableScan.builder();
  }

  private static class NamedFieldCountingTypeVisitor
      implements TypeVisitor<Integer, RuntimeException> {

    private static final NamedFieldCountingTypeVisitor VISITOR =
        new NamedFieldCountingTypeVisitor();

    private static Integer countNames(Type type) {
      return type.accept(VISITOR);
    }

    @Override
    public Integer visit(Type.Bool type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.I8 type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.I16 type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.I32 type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.I64 type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.FP32 type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.FP64 type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.Str type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.Binary type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.Date type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.Time type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.TimestampTZ type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.Timestamp type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.PrecisionTimestamp type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.PrecisionTime type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.PrecisionTimestampTZ type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.IntervalYear type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.IntervalDay type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.IntervalCompound type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.UUID type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.FixedChar type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.VarChar type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.FixedBinary type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.Decimal type) throws RuntimeException {
      return 0;
    }

    @Override
    public Integer visit(Type.Struct type) throws RuntimeException {
      // Only struct fields have names - the top level column names are also
      // captured by this since the whole schema is wrapped in a Struct type
      return type.fields().stream().mapToInt(field -> 1 + field.accept(this)).sum();
    }

    @Override
    public Integer visit(Type.ListType type) throws RuntimeException {
      return type.elementType().accept(this);
    }

    @Override
    public Integer visit(Type.Map type) throws RuntimeException {
      return type.key().accept(this) + type.value().accept(this);
    }

    @Override
    public Integer visit(Type.UserDefined type) throws RuntimeException {
      return 0;
    }
  }
}
