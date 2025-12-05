package io.substrait.isthmus;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;

/** Substrait-specific extension function for the Expression Nested List type */
public class NestedFunctions {

  public static final SqlFunction NESTED_LIST =
      new SqlFunction(
          "nested_list",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN,
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
}
