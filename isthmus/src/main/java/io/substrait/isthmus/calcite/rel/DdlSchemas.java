package io.substrait.isthmus.calcite.rel;

import java.util.HashSet;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/** The schema checks the DDL relations share. */
final class DdlSchemas {

  private DdlSchemas() {}

  /**
   * Returns the given schema, having checked that the given input can fill it: a schema is a struct
   * of distinctly named columns, and its leaf fields are the ones the input produces. Which names
   * and which types is the statement's to declare -- a CTAS may name a column something the input
   * does not and give it a type the input is cast to -- so beyond telling its columns apart, only
   * their number is checked here.
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
    requireDistinctNames(schema, what);
    return schema;
  }

  /**
   * Checks that no two fields of a struct are named the same, at every level of it: Calcite reads a
   * row type as a scope, and a repeated name there is one a reference cannot reach.
   *
   * @param type the type to check
   * @param what the object being created, for the failure message
   * @throws IllegalArgumentException if two fields of one struct share a name
   */
  private static void requireDistinctNames(RelDataType type, String what) {
    if (!type.isStruct()) {
      return;
    }
    List<String> names = type.getFieldNames();
    if (new HashSet<>(names).size() != names.size()) {
      throw new IllegalArgumentException(
          "The schema of the " + what + " to create names two fields the same: " + names);
    }
    for (RelDataTypeField field : type.getFieldList()) {
      requireDistinctNames(field.getType(), what);
    }
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
