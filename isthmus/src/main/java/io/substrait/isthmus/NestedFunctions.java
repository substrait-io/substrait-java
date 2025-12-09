package io.substrait.isthmus;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Substrait-specific extension function to map back to the Expression NestedList type in Substrait.
 */
public class NestedFunctions {

  public static final SqlFunction NESTED_LIST =
      new SqlFunction(
          "nested_list",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.TO_ARRAY_NULLABLE,
          null,
          OperandTypes.ANY,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
}
