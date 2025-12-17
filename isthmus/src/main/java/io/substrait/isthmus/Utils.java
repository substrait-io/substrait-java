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
import org.jspecify.annotations.NonNull;

/**
 * Utility helpers for Substrait conversions and Calcite schema management.
 *
 * <p>Includes helpers for computing cartesian products and building hierarchical Calcite schemas.
 */
public class Utils {
  /**
   * Compute the cartesian product for n lists.
   *
   * <p>Based on <a
   * href="https://thomas.preissler.me/blog/2020/12/29/permutations-using-java-streams">Soln by
   * Thomas Preissler</a>
   *
   * @param <T> element type contained within each list.
   * @param lists A list of lists whose cross product is computed. Null or empty inner lists are
   *     skipped.
   * @return A stream of lists representing the cartesian product (each output list has one element
   *     from each input list), or an empty stream if {@code lists} is empty.
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
   * Traverses a list of hierarchical schema names, creating any schemas if they are not present in
   * the root schema, returning the final sub schema or the root schema if the list of names is
   * empty.
   *
   * @param rootSchema the root schema to add the missing schemas to
   * @param names the list of hierarchical schema names ordered from parent to child
   * @return the final sub schema or the root schema
   * @see io.substrait.isthmus.sql.SubstraitCreateStatementParser#processCreateStatementsToSchema
   * @see io.substrait.isthmus.SchemaCollector#toSchema
   */
  public static CalciteSchema createCalciteSchemaFromNames(
      @NonNull final CalciteSchema rootSchema, @NonNull final List<String> names) {
    CalciteSchema schema = rootSchema;
    for (final String schemaName : names) {
      final CalciteSchema subSchema = schema.getSubSchema(schemaName, false);
      if (subSchema != null) {
        schema = subSchema;
      } else {
        final SubstraitSchema newSubSchema = new SubstraitSchema();
        schema = schema.add(schemaName, newSubSchema);
      }
    }

    return schema;
  }
}
