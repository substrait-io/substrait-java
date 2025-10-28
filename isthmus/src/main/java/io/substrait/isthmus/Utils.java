package io.substrait.isthmus;

import static java.util.Collections.unmodifiableList;

import com.google.common.collect.Streams;
import io.substrait.isthmus.calcite.SubstraitSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.apache.calcite.jdbc.CalciteSchema;

public class Utils {
  /**
   * Compute the cartesian product for n lists.
   *
   * <p>Based on <a
   * href="https://thomas.preissler.me/blog/2020/12/29/permutations-using-java-streams">Soln by
   * Thomas Preissler</a>
   */
  public static <T> Stream<List<T>> crossProduct(List<List<T>> lists) {

    /**
     * @param list [a, b]
     * @param element 1
     * @return [a, b, 1]
     */
    BiFunction<List<T>, T, List<T>> appendElementToList =
        (list, element) -> {
          int capacity = list.size() + 1;
          ArrayList<T> newList = new ArrayList<>(capacity);
          newList.addAll(list);
          newList.add(element);
          return unmodifiableList(newList);
        };

    /*
     * ([a, b], [1, 2]) -> [a, b, 1], [a, b, 2]
     */
    BiFunction<List<T>, List<T>, Stream<List<T>>> appendAndGen =
        (list, elemsToAppend) ->
            elemsToAppend.stream().map(element -> appendElementToList.apply(list, element));

    /** ([[a, b], [c, d]], [1, 2]) -> [a, b, 1], [a, b, 2], [c, d, 1], [c, d, 2] */
    BiFunction<Stream<List<T>>, List<T>, Stream<List<T>>> appendAndGenLists =
        (products, toJoin) -> products.flatMap(product -> appendAndGen.apply(product, toJoin));

    if (lists.isEmpty()) {
      return Stream.empty();
    }

    lists = new ArrayList<>(lists);
    List<T> firstListToJoin = lists.remove(0);
    Stream<List<T>> startProduct = appendAndGen.apply(new ArrayList<T>(), firstListToJoin);

    return lists.stream() //
        .filter(Objects::nonNull) //
        .filter(list -> !list.isEmpty()) //
        .reduce(startProduct, appendAndGenLists, (s1, s2) -> Streams.concat(s1, s2)) //
    ;
  }

  /**
   * Traverses all names identifying a table, creating any schemas if they are not present in the
   * root schema, returning the final sub schema.
   *
   * @param rootSchema the root schema to add the missing schemas to
   * @param names the list of hierarchical schema and table names ordered from parent to child with
   *     the last element containing the table name
   * @return the final sub schema
   * @see io.substrait.isthmus.sql.SubstraitCreateStatementParser#processCreateStatementsToSchema
   * @see io.substrait.isthmus.SchemaCollector#toSchema
   */
  public static CalciteSchema createCalciteSchemaFromNames(
      CalciteSchema rootSchema, List<String> names) {
    CalciteSchema schema = rootSchema;
    for (String schemaName : names.subList(0, names.size() - 1)) {
      CalciteSchema subSchema = schema.getSubSchema(schemaName, false);
      if (subSchema != null) {
        schema = subSchema;
      } else {
        SubstraitSchema newSubSchema = new SubstraitSchema();
        schema = schema.add(schemaName, newSubSchema);
      }
    }

    return schema;
  }
}
