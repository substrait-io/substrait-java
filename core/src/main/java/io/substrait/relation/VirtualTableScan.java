package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.type.TypeVisitor;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class VirtualTableScan extends AbstractReadRel {

  public abstract List<Expression.StructLiteral> getRows();

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
    var names = getInitialSchema().names();

    assert names.size()
        == NamedFieldCountingTypeVisitor.countNames(this.getInitialSchema().struct());
    var rows = getRows();

    assert rows.size() > 0
        && names.stream().noneMatch(s -> s == null)
        && rows.stream().noneMatch(r -> r == null)
        && rows.stream()
            .allMatch(r -> NamedFieldCountingTypeVisitor.countNames(r.getType()) == names.size());
  }

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
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
