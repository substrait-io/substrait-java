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

public class ListSqlOperatorFunctions {

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

  private static SqlOperator toOp(Field f) {
    try {
      return (SqlOperator) f.get(null);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }
}
