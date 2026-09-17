package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.NamedUpdate;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.core.TableModify;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class NestedUpdateTargetTest {

  static Stream<Arguments> schemasAndTargets() {
    return Stream.of(
        Arguments.of("x INTEGER, n INTEGER", List.of("N")),
        Arguments.of("s ROW(x INTEGER), x INTEGER, n INTEGER", List.of("N")),
        Arguments.of("s ROW(x INTEGER), x INTEGER, n INTEGER", List.of("X")),
        Arguments.of("s ROW(x INTEGER), x INTEGER, n INTEGER", List.of("N", "X")),
        Arguments.of("x INTEGER, s ROW(n INTEGER), n INTEGER", List.of("X", "N")),
        Arguments.of("s ROW(a ROW(x INTEGER), n INTEGER), x INTEGER, n INTEGER", List.of("N", "X")),
        Arguments.of("s ROW(a INTEGER, b INTEGER), n INTEGER", List.of("N")));
  }

  @ParameterizedTest
  @MethodSource("schemasAndTargets")
  void preservesTopLevelUpdateTargets(String schema, List<String> targets) throws Exception {
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "CREATE TABLE src (" + schema + ")");
    String assignments =
        targets.stream().map(name -> name + " = 99").collect(Collectors.joining(", "));
    Plan plan = new SqlToSubstrait().convert("UPDATE src SET " + assignments, catalog);
    Plan decoded = new ProtoPlanConverter().from(new PlanProtoConverter().toProto(plan));
    NamedUpdate update = assertInstanceOf(NamedUpdate.class, decoded.getRoots().get(0).getInput());

    TableModify converted =
        assertInstanceOf(
            TableModify.class,
            new SubstraitToCalcite(ConverterProvider.DEFAULT, catalog).convert(update));

    assertEquals(targets, converted.getUpdateColumnList());
    assertEquals(targets.size(), converted.getSourceExpressionList().size());
  }
}
