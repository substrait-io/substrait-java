package io.substrait.isthmus.expression;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.fun.SqlMultisetSetOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Utility class for listing all standard Calcite {@link SqlOperator} functions defined in {@link
 * SqlStdOperatorTable}, excluding set operators and multiset operators.
 *
 * <p>The {@code main} method uses reflection to:
 *
 * <ul>
 *   <li>Collect all public static fields of type {@link SqlOperator}
 *   <li>Filter out {@link SqlSetOperator} and {@link SqlMultisetSetOperator}
 *   <li>Print the operator names and total count to standard output
 * </ul>
 *
 * <p>This is primarily intended for debugging or inspection of available SQL operators.
 */
public class ListSqlOperatorFunctions {

  /**
   * Entry point for listing Calcite SQL operators.
   *
   * <p>Prints all operator names and their total count to standard output.
   *
   * @param args command-line arguments (not used)
   */
  public static void main(String[] args) {
    Map<String, SqlOperator> operators =
        Arrays.stream(SqlStdOperatorTable.class.getFields())
            .filter(
                f -> {
                  if (!SqlOperator.class.isAssignableFrom(f.getType())) {
                    return false;
                  }

                  if (SqlSetOperator.class.isAssignableFrom(f.getType())
                      || SqlMultisetSetOperator.class.isAssignableFrom(f.getType())) {
                    return false;
                  }

                  return true;
                })
            .filter(f -> Modifier.isStatic(f.getModifiers()) && Modifier.isPublic(f.getModifiers()))
            .collect(Collectors.toMap(Field::getName, ListSqlOperatorFunctions::toOp));

    operators.keySet().forEach(System.out::println);
    System.out.println("Operator count: " + operators.size());
  }

  /**
   * Retrieves the {@link SqlOperator} instance from a given {@link Field}.
   *
   * @param f the field representing a Calcite SQL operator
   * @return the {@link SqlOperator} instance
   * @throws IllegalStateException if the field cannot be accessed
   */
  private static SqlOperator toOp(Field f) {
    try {
      return (SqlOperator) f.get(null);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }
}
