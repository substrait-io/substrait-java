package io.substrait.isthmus;

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
import io.substrait.plan.Plan.Root;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelBuilder;

public class PlanTestBase {
  protected final SimpleExtension.ExtensionCollection extensions =
      SqlConverterBase.EXTENSION_COLLECTION;
  protected final RelCreator creator = new RelCreator();
  protected final RelBuilder builder = creator.createRelBuilder();
  protected final RelDataTypeFactory typeFactory = creator.typeFactory();
  protected final SubstraitBuilder substraitBuilder = new SubstraitBuilder(extensions);
  protected static final TypeCreator R = TypeCreator.of(false);
  protected static final TypeCreator N = TypeCreator.of(true);

  public static String asString(String resource) throws IOException {
    return Resources.toString(Resources.getResource(resource), Charsets.UTF_8);
  }

  /** Holder class to load TPC-H create statements only once on first access. */
  private static final class TpchCreateStatementsHolder {
    static final List<String> createStatements;

    static {
      final String[] values;
      try {
        values = asString("tpch/schema.sql").split(";");
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      createStatements =
          Arrays.stream(values)
              .map(String::trim)
              .filter(s -> !s.isBlank())
              .collect(Collectors.toList());
    }
  }

  public static List<String> tpchSchemaCreateStatements() {
    return TpchCreateStatementsHolder.createStatements;
  }

  protected Plan assertProtoPlanRoundrip(String query) throws SqlParseException {
    return assertProtoPlanRoundrip(query, new SqlToSubstrait());
  }

  protected Plan assertProtoPlanRoundrip(String query, SqlToSubstrait s) throws SqlParseException {
    return assertProtoPlanRoundrip(query, s, tpchSchemaCreateStatements());
  }

  protected Plan assertProtoPlanRoundrip(String query, SqlToSubstrait s, List<String> creates)
      throws SqlParseException {
    io.substrait.proto.Plan protoPlan1 = s.execute(query, creates);
    Plan plan = new ProtoPlanConverter(extensions).from(protoPlan1);
    io.substrait.proto.Plan protoPlan2 = new PlanProtoConverter().toProto(plan);
    assertEquals(protoPlan1, protoPlan2);
    var rootRels = s.sqlToRelNode(query, creates);
    assertEquals(rootRels.size(), plan.getRoots().size());
    for (int i = 0; i < rootRels.size(); i++) {
      Plan.Root rootRel = SubstraitRelVisitor.convert(rootRels.get(i), extensions);
      assertEquals(
          rootRel.getInput().getRecordType(), plan.getRoots().get(i).getInput().getRecordType());
    }
    return plan;
  }

  protected void assertPlanRoundtrip(Plan plan) {
    io.substrait.proto.Plan protoPlan1 = new PlanProtoConverter().toProto(plan);
    io.substrait.proto.Plan protoPlan2 =
        new PlanProtoConverter().toProto(new ProtoPlanConverter().from(protoPlan1));
    assertEquals(protoPlan1, protoPlan2);
  }

  protected RelRoot assertSqlSubstraitRelRoundTrip(String query) throws Exception {
    return assertSqlSubstraitRelRoundTrip(query, tpchSchemaCreateStatements());
  }

  protected RelRoot assertSqlSubstraitRelRoundTrip(String query, List<String> creates)
      throws Exception {
    // sql <--> substrait round trip test.
    // Assert (sql -> calcite -> substrait) and (sql -> substrait -> calcite -> substrait) are same.
    // Return list of sql -> Substrait rel -> Calcite rel.

    var substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);

    SqlToSubstrait s = new SqlToSubstrait();

    // 1. SQL -> Calcite RelRoot
    List<RelRoot> relRoots = s.sqlToRelNode(query, creates);
    assertEquals(1, relRoots.size());
    RelRoot relRoot1 = relRoots.get(0);

    // 2. Calcite RelRoot  -> Substrait Rel
    Plan.Root pojo1 = SubstraitRelVisitor.convert(relRoot1, extensions);

    // 3. Substrait Rel -> Calcite RelNode
    RelRoot relRoot2 = substraitToCalcite.convert(pojo1);

    // 4. Calcite RelNode -> Substrait Rel
    Plan.Root pojo2 = SubstraitRelVisitor.convert(relRoot2, extensions);

    assertEquals(pojo1, pojo2);
    return relRoot2;
  }

  @Beta
  protected void assertFullRoundTrip(String query) throws SqlParseException {
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
    ExtensionCollector extensionCollector = new ExtensionCollector();

    // SQL -> Calcite 1
    List<RelRoot> relRoots = sqlConverter.sqlToRelNode(sqlQuery, createStatements);
    assertEquals(1, relRoots.size());
    RelRoot calcite1 = relRoots.get(0);

    // Calcite 1 -> Substrait POJO 1
    Plan.Root pojo1 = SubstraitRelVisitor.convert(calcite1, extensions);

    // Substrait POJO 1 -> Substrait Proto
    io.substrait.proto.RelRoot proto = new RelProtoConverter(extensionCollector).toProto(pojo1);

    // Substrait Proto -> Substrait Pojo 2
    Plan.Root pojo2 = new ProtoRelConverter(extensionCollector, extensions).from(proto);

    // Verify that POJOs are the same
    assertEquals(pojo1, pojo2);

    // Substrait POJO 2 -> Calcite 2
    RelRoot calcite2 = new SubstraitToCalcite(extensions, typeFactory).convert(pojo2);
    // It would be ideal to compare calcite1 and calcite2, however there isn't a good mechanism to
    // do so
    assertNotNull(calcite2);

    // Calcite 2 -> Substrait POJO 3
    Plan.Root pojo3 = SubstraitRelVisitor.convert(calcite2, extensions);

    // Verify that POJOs are the same
    assertEquals(pojo1, pojo3);
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
    // TODO: reuse the Plan.Root based assertFullRoundTrip by generating names
    var extensionCollector = new ExtensionCollector();

    // Substrait POJO 1 -> Substrait Proto
    io.substrait.proto.Rel proto = new RelProtoConverter(extensionCollector).toProto(pojo1);

    // Substrait Proto -> Substrait Pojo 2
    io.substrait.relation.Rel pojo2 =
        new ProtoRelConverter(extensionCollector, extensions).from(proto);

    // Verify that POJOs are the same
    assertEquals(pojo1, pojo2);

    // Substrait POJO 2 -> Calcite
    RelNode calcite = new SubstraitToCalcite(extensions, typeFactory).convert(pojo2);

    // Calcite -> Substrait POJO 3
    io.substrait.relation.Rel pojo3 = SubstraitRelVisitor.convert(calcite, extensions);

    // Verify that POJOs are the same
    assertEquals(pojo1, pojo3);
  }

  /**
   * Verifies that the given POJO can be converted:
   *
   * <ul>
   *   <li>From POJO to Proto and back
   *   <li>From POJO to Calcite and back
   * </ul>
   */
  protected void assertFullRoundTrip(Plan.Root pojo1) {
    var extensionCollector = new ExtensionCollector();

    // Substrait POJO 1 -> Substrait Proto
    io.substrait.proto.RelRoot proto = new RelProtoConverter(extensionCollector).toProto(pojo1);

    // Substrait Proto -> Substrait Pojo 2
    io.substrait.plan.Plan.Root pojo2 =
        new ProtoRelConverter(extensionCollector, extensions).from(proto);

    // Verify that POJOs are the same
    assertEquals(pojo1, pojo2);

    // Substrait POJO 2 -> Calcite
    RelRoot calcite = new SubstraitToCalcite(extensions, typeFactory).convert(pojo2);

    // Calcite -> Substrait POJO 3
    io.substrait.plan.Plan.Root pojo3 = SubstraitRelVisitor.convert(calcite, extensions);

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

  protected io.substrait.proto.Plan toSubstraitPlan(String sql, CalciteCatalogReader catalog)
      throws SqlParseException {
    return new SqlToSubstrait().execute(sql, catalog);
  }

  protected String toSql(io.substrait.proto.Plan protoPlan) {
    Plan plan = new ProtoPlanConverter(extensions).from(protoPlan);
    return toSql(plan);
  }

  protected String toSql(Plan plan) {
    List<Root> roots = plan.getRoots();
    assertEquals(1, roots.size(), "number of roots");

    Root root = roots.get(0);
    RelRoot relRoot = new SubstraitToCalcite(extensions, typeFactory).convert(root);
    RelNode project = relRoot.project(true);
    return SubstraitToSql.toSql(project);
  }
}
