package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.NamedFieldCountingTypeVisitor;
import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Value;

@Value.Immutable
public abstract class VirtualTableScan extends AbstractReadRel {

  public abstract List<Expression.NestedStruct> getRows();

  /**
   * Checks the following invariants when construction a VirtualTableScan
   *
   * <ul>
   *   <li>no null field names
   *   <li>no null rows
   *   <li>row shape must match field-list
   *   <li>row field types must match schema types
   * </ul>
   */
  @Value.Check
  protected void check() {
    List<String> names = getInitialSchema().names();

    assert names.size()
        == NamedFieldCountingTypeVisitor.countNames(this.getInitialSchema().struct());
    List<Expression.NestedStruct> rows = getRows();

    // At the PROTOBUF layer, the Nested.Struct message does not carry nullability information.
    // Nullability is attached to the Nested message, which can contain a Nested.Struct.
    // The NestedStruct POJO flattens the Nested and Nested.Struct messages together, allowing the
    // nullability of a NestedStruct to be set directly.
    //
    // HOWEVER, the VirtualTable message contains a list of Nested.Struct messages, and as such
    // the nullability cannot be set at the protobuf layer. To avoid users attaching meaningless
    // nullability information in the POJOs, we restrict the nullability of NestedStructs to false
    // when used in VirtualTableScans.
    for (Expression.NestedStruct row : rows) {
      assert !row.nullable();
    }

    assert names.stream().noneMatch(Objects::isNull)
        && rows.stream().noneMatch(Objects::isNull)
        && rows.stream()
            .allMatch(r -> NamedFieldCountingTypeVisitor.countNames(r.getType()) == names.size());

    for (Expression.NestedStruct row : rows) {
      validateRowConformsToSchema(row);
    }
  }

  /**
   * Validates that a row's field types conform to the table's schema.
   *
   * @param row the row to validate
   * @throws AssertionError if the row does not conform to the schema
   */
  private void validateRowConformsToSchema(Expression.NestedStruct row) {
    Type.Struct schemaStruct = getInitialSchema().struct();
    List<Type> schemaFieldTypes = schemaStruct.fields();
    List<Expression> rowFields = row.fields();

    assert rowFields.size() == schemaFieldTypes.size()
        : String.format(
            "Row field count (%d) does not match schema field count (%d)",
            rowFields.size(), schemaFieldTypes.size());

    for (int i = 0; i < rowFields.size(); i++) {
      Type rowFieldType = rowFields.get(i).getType();
      Type schemaFieldType = schemaFieldTypes.get(i);

      assert rowFieldType.equals(schemaFieldType)
          : String.format(
              "Row field type (%s) does not match schema field type (%s)",
              rowFieldType, schemaFieldType);
    }
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableVirtualTableScan.Builder builder() {
    return ImmutableVirtualTableScan.builder();
  }
}
