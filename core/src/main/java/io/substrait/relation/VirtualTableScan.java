package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.NamedFieldCountingTypeVisitor;
import io.substrait.type.Type;
import io.substrait.util.VisitationContext;
import java.util.List;
import org.immutables.value.Value;

/** A read relation that produces an inline table from a fixed list of literal rows. */
@Value.Immutable
public abstract class VirtualTableScan extends AbstractReadRel {

  /**
   * Returns the rows of the inline table, each represented as a nested struct of field values.
   *
   * @return the literal rows
   */
  public abstract List<Expression.NestedStruct> getRows();

  /**
   * Checks the following invariants when construction a VirtualTableScan
   *
   * <ul>
   *   <li>row shape must match field-list
   *   <li>row field types must match schema types
   * </ul>
   *
   * @throws IllegalArgumentException if any of these invariants is violated
   */
  @Value.Check
  protected void check() {
    List<String> names = getInitialSchema().names();

    int schemaNameCount =
        NamedFieldCountingTypeVisitor.countNames(this.getInitialSchema().struct());
    if (names.size() != schemaNameCount) {
      throw new IllegalArgumentException(
          String.format(
              "VirtualTableScan schema names count (%d) does not match the depth-first named-field count of the schema struct (%d)",
              names.size(), schemaNameCount));
    }

    List<Type> schemaFieldTypes = getInitialSchema().struct().fields();

    for (Expression.NestedStruct row : getRows()) {
      // At the PROTOBUF layer, the Nested.Struct message does not carry nullability information.
      // Nullability is attached to the Nested message, which can contain a Nested.Struct.
      // The NestedStruct POJO flattens the Nested and Nested.Struct messages together, allowing
      // the nullability of a NestedStruct to be set directly.
      //
      // HOWEVER, the VirtualTable message contains a list of Nested.Struct messages, and as such
      // the nullability cannot be set at the protobuf layer. To avoid users attaching meaningless
      // nullability information in the POJOs, we restrict the nullability of NestedStructs to
      // false when used in VirtualTableScans.
      if (row.nullable()) {
        throw new IllegalArgumentException(
            "VirtualTableScan rows must not be nullable; nullability cannot be represented for the Nested.Struct messages of a VirtualTable");
      }

      int rowNameCount = NamedFieldCountingTypeVisitor.countNames(row.getType());
      if (rowNameCount != names.size()) {
        throw new IllegalArgumentException(
            String.format(
                "Row named-field count (%d) does not match schema names count (%d)",
                rowNameCount, names.size()));
      }

      validateRowConformsToSchema(row, schemaFieldTypes);
    }
  }

  /**
   * Validates that a row's field types conform to the table's schema.
   *
   * @param row the row to validate
   * @param schemaFieldTypes the field types of the table's schema
   * @throws IllegalArgumentException if the row does not conform to the schema
   */
  private static void validateRowConformsToSchema(
      Expression.NestedStruct row, List<Type> schemaFieldTypes) {
    List<Expression> rowFields = row.fields();

    if (rowFields.size() != schemaFieldTypes.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Row field count (%d) does not match schema field count (%d)",
              rowFields.size(), schemaFieldTypes.size()));
    }

    for (int i = 0; i < rowFields.size(); i++) {
      Type rowFieldType = rowFields.get(i).getType();
      Type schemaFieldType = schemaFieldTypes.get(i);

      if (!rowFieldType.equals(schemaFieldType)) {
        throw new IllegalArgumentException(
            String.format(
                "Row field type (%s) does not match schema field type (%s)",
                rowFieldType, schemaFieldType));
      }
    }
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link VirtualTableScan}.
   *
   * @return a new builder
   */
  public static ImmutableVirtualTableScan.Builder builder() {
    return ImmutableVirtualTableScan.builder();
  }
}
