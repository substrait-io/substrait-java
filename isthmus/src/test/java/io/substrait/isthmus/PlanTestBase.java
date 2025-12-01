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
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.isthmus.sql.SubstraitSqlDialect;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
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
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
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

  protected static final CalciteCatalogReader TPCH_CATALOG;

  static {
    try {
      String tpchCreateStatements = asString("tpch/schema.sql");
      TPCH_CATALOG =
          SubstraitCreateStatementParser.processCreateStatementsToCatalog(tpchCreateStatements);
    } catch (IOException | SqlParseException e) {
      throw new IllegalStateException(e);
    }
  }

  private static final TpcdsSchema TPCDS_SCHEMA = new TpcdsSchema(1.0);
  protected static CalciteCatalogReader TPCDS_CATALOG =
      PlanTestBase.schemaToCatalog("tpcds", TPCDS_SCHEMA);

  public static String asString(String resource) throws IOException {
    return Resources.toString(Resources.getResource(resource), Charsets.UTF_8);
  }

  protected Plan assertProtoPlanRoundrip(String query) throws SqlParseException {
    return assertProtoPlanRoundrip(query, new SqlToSubstrait());
  }

  protected Plan assertProtoPlanRoundrip(String query, SqlToSubstrait s) throws SqlParseException {
    return assertProtoPlanRoundrip(query, s, TPCH_CATALOG);
  }

  protected Plan assertProtoPlanRoundrip(String query, SqlToSubstrait s, String createStatements)
      throws SqlParseException {
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(createStatements);
    return assertProtoPlanRoundrip(query, s, catalog);
  }

  protected Plan assertProtoPlanRoundrip(
      String query, SqlToSubstrait s, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    Plan plan1 = s.convert(query, catalogReader);
    io.substrait.proto.Plan protoPlan1 = toProto(plan1);

    Plan plan2 = new ProtoPlanConverter(extensions).from(protoPlan1);
    io.substrait.proto.Plan protoPlan2 = toProto(plan2);
    assertEquals(protoPlan1, protoPlan2);

    assertEquals(plan1.getRoots().size(), plan2.getRoots().size());
    for (int i = 0; i < plan1.getRoots().size(); i++) {
      assertEquals(
          plan1.getRoots().get(i).getInput().getRecordType(),
          plan2.getRoots().get(i).getInput().getRecordType());
    }

    return plan2;
  }

  protected void assertPlanRoundtrip(Plan plan) {
    io.substrait.proto.Plan protoPlan1 = toProto(plan);
    io.substrait.proto.Plan protoPlan2 = toProto(new ProtoPlanConverter().from(protoPlan1));
    assertEquals(protoPlan1, protoPlan2);
  }

  /** Use {@link #assertFullRoundTrip(String)} when possible, as it makes stronger assertions */
  protected RelRoot assertSqlSubstraitRelRoundTrip(String query) throws Exception {
    return assertSqlSubstraitRelRoundTrip(query, TPCH_CATALOG);
  }

  /**
   * Use {@link #assertFullRoundTrip(String, Prepare.CatalogReader)} when possible, as it makes
   * stronger assertions
   */
  protected RelRoot assertSqlSubstraitRelRoundTrip(
      String query, Prepare.CatalogReader catalogReader) throws Exception {
    // sql <--> substrait round trip test.
    // Assert (sql -> calcite -> substrait) and (sql -> substrait -> calcite -> substrait) are same.
    // Return list of sql -> Substrait rel -> Calcite rel.

    SqlToSubstrait s2s = new SqlToSubstrait();
    SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);

    // 1. SQL -> Substrait Plan
    Plan plan1 = s2s.convert(query, catalogReader);

    // 2. Substrait Plan  -> Substrait Rel
    Plan.Root pojo1 = plan1.getRoots().get(0);

    // 3. Substrait Rel -> Calcite RelNode
    RelRoot relRoot2 = substraitToCalcite.convert(pojo1);

    // 4. Calcite RelNode -> Substrait Rel
    Plan.Root pojo2 = SubstraitRelVisitor.convert(relRoot2, extensions);

    assertEquals(pojo1, pojo2);
    return relRoot2;
  }

  @Beta
  protected void assertFullRoundTrip(String query) throws SqlParseException {
    assertFullRoundTrip(query, TPCH_CATALOG);
  }

  @Beta
  protected void assertFullRoundTrip(String query, String createStatements)
      throws SqlParseException {
    CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(createStatements);
    assertFullRoundTrip(query, catalogReader);
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
  protected void assertFullRoundTrip(String sqlQuery, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    ExtensionCollector extensionCollector = new ExtensionCollector();

    // SQL -> Calcite 1
    RelRoot calcite1 = SubstraitSqlToCalcite.convertQuery(sqlQuery, catalogReader);

    // Calcite 1 -> Substrait POJO 1
    Plan.Root root1 = SubstraitRelVisitor.convert(calcite1, extensions);

    // Substrait Root 1 -> Substrait Proto
    io.substrait.proto.RelRoot proto = new RelProtoConverter(extensionCollector).toProto(root1);

    // Substrait Proto -> Substrait Root 2
    Plan.Root root2 = new ProtoRelConverter(extensionCollector, extensions).from(proto);

    // Verify that roots are the same
    assertEquals(root1, root2);

    // Substrait Root 2 -> Calcite 2
    final SubstraitToCalcite substraitToCalcite =
        new SubstraitToCalcite(extensions, typeFactory, catalogReader);

    RelRoot calcite2 = substraitToCalcite.convert(root2);
    // It would be ideal to compare calcite1 and calcite2, however there isn't a good mechanism to
    // do so
    assertNotNull(calcite2);

    // Calcite 2 -> Substrait Root 3
    Plan.Root root3 = SubstraitRelVisitor.convert(calcite2, extensions);

    // Verify that POJOs are the same
    assertEquals(root1, root3);
  }

  /**
   * Verifies that the given query can be converted from its Calcite representation to Substrait
   * proto and back. Due to the various ways in which Calcite plans are produced, some plans contain
   * identity projections and others do not. Fully removing this behaviour is quite tricky. As a
   * workaround, this method prepares the plan such that identity projections are removed.
   *
   * <p>In the long-term, we should work to remove this test method.
   *
   * <p>Preparation: <code>
   *   SQL -> Calcite 0 -> Substrait POJO 0 -> Substrait Proto 0 -> Substrait POJO 1 -> Calcite 1
   * </code> this code also checks that: Main cycle:
   *
   * <ul>
   *   <li>Substrait POJO 0 == Substrait POJO 1
   * </ul>
   *
   * Calcite 1 -> Substrait POJO 2 -> Substrait Proto 2 -> Substrait POJO 3 -> Calcite 2 ->
   * Substrait POJO 4
   *
   * <ul>
   *   <li>Substrait POJO 2 == Substrait POJO 4
   * </ul>
   */
  protected void assertFullRoundTripWithIdentityProjectionWorkaround(
      String sqlQuery, Prepare.CatalogReader catalogReader) throws SqlParseException {
    ExtensionCollector extensionCollector = new ExtensionCollector();

    // Preparation
    // SQL -> Calcite 0
    RelRoot calcite0 = SubstraitSqlToCalcite.convertQuery(sqlQuery, catalogReader);

    // Calcite 0 -> Substrait POJO 0
    Plan.Root root0 = SubstraitRelVisitor.convert(calcite0, extensions);

    // Substrait POJO 0 -> Substrait Proto 0
    io.substrait.proto.RelRoot proto0 = new RelProtoConverter(extensionCollector).toProto(root0);

    // Substrait Proto -> Substrait POJO 1
    Plan.Root root1 = new ProtoRelConverter(extensionCollector, extensions).from(proto0);

    // Verify that POJOs are the same
    assertEquals(root0, root1);

    final SubstraitToCalcite substraitToCalcite =
        new SubstraitToCalcite(extensions, typeFactory, catalogReader);

    // Substrait POJO 1 -> Calcite 1
    RelRoot calcite1 = substraitToCalcite.convert(root1);

    // End Preparation

    // Calcite 1 -> Substrait POJO 2
    Plan.Root root2 = SubstraitRelVisitor.convert(calcite1, extensions);

    // Substrait POJO 2 -> Substrait Proto 1
    io.substrait.proto.RelRoot proto1 = new RelProtoConverter(extensionCollector).toProto(root2);

    // Substrait Proto1 -> Substrait POJO 3
    Plan.Root root3 = new ProtoRelConverter(extensionCollector, extensions).from(proto1);

    // Substrait POJO 3 -> Calcite 2
    RelRoot calcite2 = substraitToCalcite.convert(root3);
    // Calcite 2 -> Substrait POJO 4
    Plan.Root root4 = SubstraitRelVisitor.convert(calcite2, extensions);

    // Verify that POJOs are the same
    assertEquals(root2, root4);
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
    ExtensionCollector extensionCollector = new ExtensionCollector();

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
    ExtensionCollector extensionCollector = new ExtensionCollector();

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

  protected Plan toSubstraitPlan(String sql, CalciteCatalogReader catalog)
      throws SqlParseException {
    return new SqlToSubstrait().convert(sql, catalog);
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
    return SubstraitSqlDialect.toSql(project).getSql();
  }

  protected io.substrait.proto.Plan toProto(Plan plan) {
    return new PlanProtoConverter().toProto(plan);
  }

  protected static CalciteCatalogReader schemaToCatalog(String schemaName, Schema schema) {
    CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
    rootSchema.add(schemaName, schema);
    List<String> defaultSchema = List.of(schemaName);
    return new CalciteCatalogReader(
        rootSchema,
        defaultSchema,
        SubstraitTypeSystem.TYPE_FACTORY,
        SqlConverterBase.CONNECTION_CONFIG);
  }
}
