package io.substrait.isthmus.utils;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.substrait.relation.Set;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

public class Utils {
  /**
   * Generate a query using set operators
   *
   * @param op the set operator to use
   * @param multi whether to use more than two relations
   * @return a sql query
   */
  public static String getSetQuery(Set.SetOp op, boolean multi) {
    String opString =
        switch (op) {
          case MINUS_PRIMARY -> "EXCEPT";
          case MINUS_MULTISET -> "EXCEPT ALL";
          case INTERSECTION_PRIMARY -> "INTERSECT";
          case INTERSECTION_MULTISET -> "INTERSECT ALL";
          case UNION_DISTINCT -> "UNION";
          case UNION_ALL -> "UNION ALL";
          case UNKNOWN -> throw new UnsupportedOperationException(
              "Unknown set operation is not supported");
        };

    StringBuilder query = new StringBuilder();
    query.append("select p_partkey as partkey from part where p_partkey > cast(100 as bigint)\n");
    query.append(opString + "\n");
    query.append(
        "select l_partkey as partkey from lineitem where l_orderkey > cast(100 as bigint)\n");
    if (!multi) {
      return query.toString();
    } else {
      // check with 3 relations
      query.append(opString + "\n");
      query.append("select ps_partkey as partkey from partsupp");
      return query.toString();
    }
  }

  // Generate all combinations excluding the UNKNOWN operator
  public static Stream<Arguments> setTestConfig() {
    return Arrays.stream(Set.SetOp.values())
        .filter(op -> op != Set.SetOp.UNKNOWN)
        .flatMap(op -> Stream.of(arguments(op, false), arguments(op, true)));
  }
}
