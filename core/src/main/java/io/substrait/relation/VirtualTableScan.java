package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class VirtualTableScan extends AbstractReadRel {

  public abstract List<String> getDfsNames();

  public abstract List<Expression.StructLiteral> getRows();

  /**
   *
   * <li>non-empty rowset
   * <li>non-null field-names
   * <li>no null rows
   * <li>row shape must match field-list
   */
  @Value.Check
  protected void check() {
    List<String> names = getDfsNames();
    List<Expression.StructLiteral> rows = getRows();

    assert rows.size() > 0
        && names.stream().noneMatch(s -> s == null)
        && rows.stream().noneMatch(r -> r == null || r.fields().size() != names.size());
  }

  @Override
  public final NamedStruct getInitialSchema() {
    return NamedStruct.of(getDfsNames(), (Type.Struct) getRows().get(0).getType());
  }

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableVirtualTableScan.Builder builder() {
    return ImmutableVirtualTableScan.builder();
  }
}
