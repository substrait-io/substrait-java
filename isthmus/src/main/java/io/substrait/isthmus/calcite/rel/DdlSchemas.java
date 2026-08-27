package io.substrait.isthmus.calcite.rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/** The schema checks the DDL relations share. */
final class DdlSchemas {

  private DdlSchemas() {}

  /**
   * Returns the given schema, having checked that the given input can fill it: a schema is a struct
   * of columns, and its leaf fields are the ones the input produces. The names and the types are
   * the statement's to declare -- a CTAS may name a column something the input does not and give it
   * a type the input is cast to -- so only their number is checked here.
   *
   * @param schema the declared schema of the object to create
   * @param input the relation filling it
   * @param what the object being created, for the failure message
   * @return the schema
   * @throws IllegalArgumentException if the schema is not a struct or the two do not align
   */
  static RelDataType requireFilledBy(RelDataType schema, RelNode input, String what) {
    if (!schema.isStruct()) {
      throw new IllegalArgumentException(
          "The schema of the " + what + " to create must be a struct, but got " + schema);
    }
    int declared = leafFieldCount(schema);
    int produced = leafFieldCount(input.getRowType());
    if (declared != produced) {
      throw new IllegalArgumentException(
          "The schema of the "
              + what
              + " to create has "
              + declared
              + " leaf fields, but the input filling it produces "
              + produced);
    }
    return schema;
  }

  private static int leafFieldCount(RelDataType type) {
    if (!type.isStruct()) {
      return 1;
    }
    int count = 0;
    for (RelDataTypeField field : type.getFieldList()) {
      count += leafFieldCount(field.getType());
    }
    return count;
  }
}
