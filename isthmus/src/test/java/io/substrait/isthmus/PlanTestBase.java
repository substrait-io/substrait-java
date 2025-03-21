package io.substrait.isthmus;

import static io.substrait.isthmus.SqlConverterBase.EXTENSION_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.annotations.Beta;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Assertions;

public class PlanTestBase {
  protected final SimpleExtension.ExtensionCollection extensions = SimpleExtension.loadDefaults();
  protected final RelCreator creator = new RelCreator();
  protected final RelBuilder builder = creator.createRelBuilder();
  protected final RexBuilder rex = creator.rex();
  protected final RelDataTypeFactory typeFactory = creator.typeFactory();
  protected final SubstraitBuilder substraitBuilder = new SubstraitBuilder(extensions);
  protected static final TypeCreator R = TypeCreator.of(false);
  protected static final TypeCreator N = TypeCreator.of(true);

  public static String asString(String resource) throws IOException {
    return Resources.toString(Resources.getResource(resource), Charsets.UTF_8);
  }

  public static List<String> tpchSchemaCreateStatements() throws IOException {
    String[] values = asString("tpch/schema.sql").split(";");
    return Arrays.stream(values)
        .filter(t -> !t.trim().isBlank())
        .collect(java.util.stream.Collectors.toList());
  }

  protected Plan assertProtoPlanRoundrip(String query) throws IOException, SqlParseException {
    return assertProtoPlanRoundrip(query, new SqlToSubstrait());
  }

  protected Plan assertProtoPlanRoundrip(String query, SqlToSubstrait s)
      throws IOException, SqlParseException {
    return assertProtoPlanRoundrip(query, s, tpchSchemaCreateStatements());
  }

  protected Plan assertProtoPlanRoundrip(String query, SqlToSubstrait s, List<String> creates)
      throws SqlParseException {
    io.substrait.proto.Plan protoPlan1 = s.execute(query, creates);
    Plan plan = new ProtoPlanConverter(EXTENSION_COLLECTION).from(protoPlan1);
    io.substrait.proto.Plan protoPlan2 = new PlanProtoConverter().toProto(plan);
    assertEquals(protoPlan1, protoPlan2);
    var rootRels = s.sqlToRelNode(query, creates);
    assertEquals(rootRels.size(), plan.getRoots().size());
    for (int i = 0; i < rootRels.size(); i++) {
      var rootRel = SubstraitRelVisitor.convert(rootRels.get(i), EXTENSION_COLLECTION);
      assertEquals(rootRel.getRecordType(), plan.getRoots().get(i).getInput().getRecordType());
    }
    return plan;
  }

  protected void assertPlanRoundtrip(Plan plan) {
    io.substrait.proto.Plan protoPlan1 = new PlanProtoConverter().toProto(plan);
    io.substrait.proto.Plan protoPlan2 =
        new PlanProtoConverter().toProto(new ProtoPlanConverter().from(protoPlan1));
    assertEquals(protoPlan1, protoPlan2);
  }

  protected List<RelNode> assertSqlSubstraitRelRoundTrip(String query) throws Exception {
    return assertSqlSubstraitRelRoundTrip(query, tpchSchemaCreateStatements());
  }

  protected List<RelNode> assertSqlSubstraitRelRoundTrip(String query, List<String> creates)
      throws Exception {
    // sql <--> substrait round trip test.
    // Assert (sql -> substrait) and (sql -> substrait -> calcite rel -> substrait) are same.
    // Return list of sql -> substrait rel -> Calcite rel.
    List<RelNode> relNodeList = new ArrayList<>();

    // 1. sql -> substrait rel
    SqlToSubstrait s = new SqlToSubstrait();
    for (RelRoot relRoot : s.sqlToRelNode(query, creates)) {
      Rel pojoRel = SubstraitRelVisitor.convert(relRoot, EXTENSION_COLLECTION);

      // 2. substrait rel -> Calcite Rel
      RelNode relnodeRoot = new SubstraitToSql().substraitRelToCalciteRel(pojoRel, creates);

      relNodeList.add(relnodeRoot);

      // 3. Calcite Rel -> substrait rel
      Rel pojoRel2 =
          SubstraitRelVisitor.convert(
              RelRoot.of(relnodeRoot, SqlKind.SELECT), EXTENSION_COLLECTION);

      Assertions.assertEquals(pojoRel, pojoRel2);
    }
    return relNodeList;
  }

  @Beta
  protected void assertFullRoundTrip(String query) throws IOException, SqlParseException {
    assertFullRoundTrip(query, tpchSchemaCreateStatements());
  }

  /**
   * Verifies that the given query can be converted from its Calcite form to the Substrait Proto
   * representation and back.
   *
   * <p>For the given transformations: <code>
   *   SQL -> Calcite 1 -> Substrait POJO 1 -> Substrait Proto -> Substrait POJO 2 -> Calcite 2 -> Substrait POJO 3
   * </code> this code also checks that:
   *
   * <ul>
   *   <li>Substrait POJO 1 == Substrait POJO 2
   *   <li>Substrait POJO 2 == Substrait POJO 3
   * </ul>
   */
  protected void assertFullRoundTrip(String sqlQuery, List<String> createStatements)
      throws SqlParseException {
    SqlToSubstrait sqlConverter = new SqlToSubstrait();
    List<RelRoot> relRoots = sqlConverter.sqlToRelNode(sqlQuery, createStatements);

    for (RelRoot calcite1 : relRoots) {
      var extensionCollector = new ExtensionCollector();

      // Calcite 1 -> Substrait POJO 1
      io.substrait.relation.Rel pojo1 = SubstraitRelVisitor.convert(calcite1, EXTENSION_COLLECTION);

      // Substrait POJO 1 -> Substrait Proto
      io.substrait.proto.Rel proto = new RelProtoConverter(extensionCollector).toProto(pojo1);

      // Substrait Proto -> Substrait Pojo 2
      io.substrait.relation.Rel pojo2 =
          new ProtoRelConverter(extensionCollector, EXTENSION_COLLECTION).from(proto);

      // Verify that POJOs are the same
      assertEquals(pojo1, pojo2);

      // Substrait POJO 2 -> Calcite 2
      RelNode calcite2 = new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory).convert(pojo2);
      // It would be ideal to compare calcite1 and calcite2, however there isn't a good mechanism to
      // do so
      assertNotNull(calcite2);

      // Calcite 2 -> Substrait POJO 3
      io.substrait.relation.Rel pojo3 =
          SubstraitRelVisitor.convert(RelRoot.of(calcite2, calcite1.kind), EXTENSION_COLLECTION);

      // Verify that POJOs are the same
      assertEquals(pojo1, pojo3);
    }
  }

  /**
   * Verifies that the given POJO can be converted:
   *
   * <ul>
   *   <li>From POJO to Proto and back
   *   <li>From POJO to Calcite and back
   * </ul>
   */
  protected void assertFullRoundTrip(Rel pojo1) {
    var extensionCollector = new ExtensionCollector();

    // Substrait POJO 1 -> Substrait Proto
    io.substrait.proto.Rel proto = new RelProtoConverter(extensionCollector).toProto(pojo1);

    // Substrait Proto -> Substrait Pojo 2
    io.substrait.relation.Rel pojo2 =
        new ProtoRelConverter(extensionCollector, EXTENSION_COLLECTION).from(proto);

    // Verify that POJOs are the same
    assertEquals(pojo1, pojo2);

    // Substrait POJO 2 -> Calcite
    RelNode calcite = new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory).convert(pojo2);

    // Calcite -> Substrait POJO 3
    io.substrait.relation.Rel pojo3 =
        // SqlKind.SELECT is used because the majority of our tests are SELECT queries
        SubstraitRelVisitor.convert(RelRoot.of(calcite, SqlKind.SELECT), EXTENSION_COLLECTION);

    // Verify that POJOs are the same
    assertEquals(pojo1, pojo3);
  }

  protected void assertRowMatch(RelDataType actual, Type... expected) {
    assertRowMatch(actual, Arrays.asList(expected));
  }

  protected void assertRowMatch(RelDataType actual, List<Type> expected) {
    Type type = TypeConverter.DEFAULT.toSubstrait(actual);
    assertInstanceOf(Type.Struct.class, type);
    Type.Struct struct = (Type.Struct) type;
    assertEquals(expected, struct.fields());
  }
}
