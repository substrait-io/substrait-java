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
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.Plan;
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
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
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

  protected static CalciteCatalogReader TPCH_CATALOG;

  static {
    try {
      String tpchCreateStatements = asString("tpch/schema.sql");
      TPCH_CATALOG =
          SubstraitCreateStatementParser.processCreateStatementsToCatalog(tpchCreateStatements);
    } catch (IOException | SqlParseException e) {
      throw new RuntimeException(e);
    }
  }

  protected Plan assertProtoPlanRoundrip(String query) throws IOException, SqlParseException {
    return assertProtoPlanRoundrip(query, new SqlToSubstrait());
  }

  protected Plan assertProtoPlanRoundrip(String query, SqlToSubstrait s)
      throws IOException, SqlParseException {
    return assertProtoPlanRoundrip(query, s, TPCH_CATALOG);
  }

  protected Plan assertProtoPlanRoundrip(
      String query, SqlToSubstrait s, Prepare.CatalogReader catalogReader)
      throws SqlParseException {
    io.substrait.proto.Plan protoPlan1 = s.execute(query, catalogReader);
    Plan plan = new ProtoPlanConverter(EXTENSION_COLLECTION).from(protoPlan1);
    io.substrait.proto.Plan protoPlan2 = new PlanProtoConverter().toProto(plan);
    assertEquals(protoPlan1, protoPlan2);
    var rootRels = SubstraitSqlToCalcite.convertSelects(query, catalogReader);
    assertEquals(rootRels.size(), plan.getRoots().size());
    for (int i = 0; i < rootRels.size(); i++) {
      Plan.Root rootRel = CalciteToSubstraitVisitor.convert(rootRels.get(i), EXTENSION_COLLECTION);
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
    return assertSqlSubstraitRelRoundTrip(query, TPCH_CATALOG);
  }

  protected RelRoot assertSqlSubstraitRelRoundTrip(String query, List<String> createStatements)
      throws Exception {
    CalciteCatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            String.join(";", createStatements));
    return assertSqlSubstraitRelRoundTrip(query, catalogReader);
  }

  protected RelRoot assertSqlSubstraitRelRoundTrip(
      String query, Prepare.CatalogReader catalogReader) throws Exception {
    // sql <--> substrait round trip test.
    // Assert (sql -> calcite -> substrait) and (sql -> substrait -> calcite -> substrait) are same.
    // Return list of sql -> Substrait rel -> Calcite rel.

    var substraitToCalcite = new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory);

    SqlToSubstrait s = new SqlToSubstrait();

    // 1. SQL -> Calcite RelRoot
    RelRoot relRoot1 = SubstraitSqlToCalcite.convertSelect(query, catalogReader);

    // 2. Calcite RelRoot  -> Substrait Rel
    Plan.Root pojo1 = CalciteToSubstraitVisitor.convert(relRoot1, EXTENSION_COLLECTION);

    // 3. Substrait Rel -> Calcite RelNode
    RelRoot relRoot2 = substraitToCalcite.convert(pojo1);

    // 4. Calcite RelNode -> Substrait Rel
    Plan.Root pojo2 = CalciteToSubstraitVisitor.convert(relRoot2, EXTENSION_COLLECTION);

    Assertions.assertEquals(pojo1, pojo2);
    return relRoot2;
  }

  @Beta
  protected void assertFullRoundTrip(String query) throws IOException, SqlParseException {
    assertFullRoundTrip(query, TPCH_CATALOG);
  }

  @Beta
  protected void assertFullRoundTrip(String query, String createStatements)
      throws IOException, SqlParseException {
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
    RelRoot calcite1 = SubstraitSqlToCalcite.convertSelect(sqlQuery, catalogReader);

    // Calcite 1 -> Substrait POJO 1
    Plan.Root pojo1 = CalciteToSubstraitVisitor.convert(calcite1, EXTENSION_COLLECTION);

    // Substrait POJO 1 -> Substrait Proto
    io.substrait.proto.RelRoot proto = new RelProtoConverter(extensionCollector).toProto(pojo1);

    // Substrait Proto -> Substrait Pojo 2
    Plan.Root pojo2 = new ProtoRelConverter(extensionCollector, EXTENSION_COLLECTION).from(proto);

    // Verify that POJOs are the same
    assertEquals(pojo1, pojo2);

    // Substrait POJO 2 -> Calcite 2
    RelRoot calcite2 = new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory).convert(pojo2);
    // It would be ideal to compare calcite1 and calcite2, however there isn't a good mechanism to
    // do so
    assertNotNull(calcite2);

    // Calcite 2 -> Substrait POJO 3
    Plan.Root pojo3 = CalciteToSubstraitVisitor.convert(calcite2, EXTENSION_COLLECTION);

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
        new ProtoRelConverter(extensionCollector, EXTENSION_COLLECTION).from(proto);

    // Verify that POJOs are the same
    assertEquals(pojo1, pojo2);

    // Substrait POJO 2 -> Calcite
    RelNode calcite = new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory).convert(pojo2);

    // Calcite -> Substrait POJO 3
    io.substrait.relation.Rel pojo3 =
        CalciteToSubstraitVisitor.convert(calcite, EXTENSION_COLLECTION);

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
        new ProtoRelConverter(extensionCollector, EXTENSION_COLLECTION).from(proto);

    // Verify that POJOs are the same
    assertEquals(pojo1, pojo2);

    // Substrait POJO 2 -> Calcite
    RelRoot calcite = new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory).convert(pojo2);

    // Calcite -> Substrait POJO 3
    io.substrait.plan.Plan.Root pojo3 =
        CalciteToSubstraitVisitor.convert(calcite, EXTENSION_COLLECTION);

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
