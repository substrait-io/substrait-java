package io.substrait.isthmus;

import static io.substrait.isthmus.SqlConverterBase.EXTENSION_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class NameRoundtripTest extends PlanTestBase {

  @Test
  void outputNamesShouldBeConsistent() throws Exception {
    List<String> creates = List.of("CREATE TABLE foo(a BIGINT, b BIGINT)");

    SqlToSubstrait s = new SqlToSubstrait();
    var substraitToCalcite = new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory);

    String query = """
      SELECT "a", "B" FROM foo GROUP BY a, b
      """;
    List<String> expectedNames = List.of("a", "B");

    List<org.apache.calcite.rel.RelRoot> calciteRelRoots = s.sqlToRelNode(query, creates);
    assertEquals(1, calciteRelRoots.size());

    org.apache.calcite.rel.RelRoot calciteRelRoot = calciteRelRoots.get(0);
    assertEquals(expectedNames, calciteRelRoot.validatedRowType.getFieldNames());

    io.substrait.relation.Rel substraitRel =
        SubstraitRelVisitor.convert(calciteRelRoot, EXTENSION_COLLECTION);
    org.apache.calcite.rel.RelNode relNode = substraitToCalcite.convert(substraitRel);
    assertEquals(expectedNames, relNode.getRowType().getFieldNames());
  }
}
