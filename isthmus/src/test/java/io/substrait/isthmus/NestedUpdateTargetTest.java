package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.AbstractUpdate;
import io.substrait.relation.ImmutableNamedUpdate;
import io.substrait.relation.NamedUpdate;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexLiteral;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class NestedUpdateTargetTest {

  private static int valueFor(int index) {
    return 11 * (index + 1);
  }

  static Stream<Arguments> schemasAndTargets() {
    return Stream.of(
        Arguments.of("x INTEGER, n INTEGER", List.of("N"), List.of(1)),
        Arguments.of("s ROW(inner1 INTEGER), x INTEGER, n INTEGER", List.of("N"), List.of(2)),
        Arguments.of("s ROW(inner1 INTEGER), x INTEGER, n INTEGER", List.of("X"), List.of(1)),
        Arguments.of(
            "s ROW(inner1 INTEGER), x INTEGER, n INTEGER", List.of("N", "X"), List.of(2, 1)),
        Arguments.of(
            "x INTEGER, s ROW(inner1 INTEGER), n INTEGER", List.of("X", "N"), List.of(0, 2)),
        Arguments.of(
            "s ROW(inner1 ROW(deep1 INTEGER), inner2 INTEGER), x INTEGER, n INTEGER",
            List.of("N", "X"),
            List.of(2, 1)),
        Arguments.of("s ROW(inner1 INTEGER, inner2 INTEGER), n INTEGER", List.of("N"), List.of(1)));
  }

  @ParameterizedTest
  @MethodSource("schemasAndTargets")
  void preservesTopLevelUpdateTargets(
      String schema, List<String> targets, List<Integer> columnTargets) throws Exception {
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "CREATE TABLE src (" + schema + ")");
    String assignments =
        IntStream.range(0, targets.size())
            .mapToObj(i -> targets.get(i) + " = " + valueFor(i))
            .collect(Collectors.joining(", "));
    Plan plan = new SqlToSubstrait().convert("UPDATE src SET " + assignments, catalog);
    Plan decoded = new ProtoPlanConverter().from(new PlanProtoConverter().toProto(plan));
    NamedUpdate update = assertInstanceOf(NamedUpdate.class, decoded.getRoots().get(0).getInput());
    assertEquals(
        columnTargets,
        update.getTransformations().stream()
            .map(AbstractUpdate.TransformExpression::getColumnTarget)
            .collect(Collectors.toList()));

    TableModify converted =
        assertInstanceOf(
            TableModify.class,
            new SubstraitToCalcite(ConverterProvider.DEFAULT, catalog).convert(update));

    assertEquals(targets, converted.getUpdateColumnList());
    Map<String, Integer> expected = new LinkedHashMap<>();
    IntStream.range(0, targets.size()).forEach(i -> expected.put(targets.get(i), valueFor(i)));
    Map<String, Integer> actual = new LinkedHashMap<>();
    for (int i = 0; i < converted.getUpdateColumnList().size(); i++) {
      RexLiteral value =
          assertInstanceOf(RexLiteral.class, converted.getSourceExpressionList().get(i));
      actual.put(converted.getUpdateColumnList().get(i), value.getValueAs(Integer.class));
    }
    assertEquals(expected, actual);
  }

  @Test
  void doesNotConvertUntouchedSchemaFields() throws Exception {
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "CREATE TABLE src (u INTEGER, n INTEGER)");
    Plan plan = new SqlToSubstrait().convert("UPDATE src SET n = 11", catalog);
    NamedUpdate update = assertInstanceOf(NamedUpdate.class, plan.getRoots().get(0).getInput());
    NamedStruct declaredSchema =
        NamedStruct.of(
            List.of("U", "N"),
            TypeCreator.NULLABLE.struct(TypeCreator.NULLABLE.UUID, TypeCreator.NULLABLE.I32));
    NamedUpdate foreignUpdate = ImmutableNamedUpdate.copyOf(update).withTableSchema(declaredSchema);

    TableModify converted =
        assertInstanceOf(
            TableModify.class,
            new SubstraitToCalcite(ConverterProvider.DEFAULT, catalog).convert(foreignUpdate));
    assertEquals(List.of("N"), converted.getUpdateColumnList());
  }
}
