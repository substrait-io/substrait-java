package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.calcite.rel.CreateTable;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.AbstractWriteRel.CreateMode;
import io.substrait.relation.NamedWrite;
import java.util.List;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class CtasCreateModeTest {
  private final ConverterProvider provider = ConverterProvider.DEFAULT;
  // An existing target must not turn CREATE or IF NOT EXISTS into a replacement request.
  private final Prepare.CatalogReader catalog =
      SubstraitCreateStatementParser.processCreateStatementsToCatalog(
          provider, "CREATE TABLE dst (v INTEGER)");

  CtasCreateModeTest() throws SqlParseException {}

  @ParameterizedTest
  @CsvSource({
    "CREATE TABLE dst AS SELECT 99 AS v, ERROR_IF_EXISTS",
    "CREATE TABLE IF NOT EXISTS dst AS SELECT 99 AS v, IGNORE_IF_EXISTS",
    "CREATE OR REPLACE TABLE dst AS SELECT 99 AS v, REPLACE_IF_EXISTS",
    "CREATE TABLE dst(v INTEGER) AS SELECT 99 AS v, ERROR_IF_EXISTS",
    "CREATE TABLE IF NOT EXISTS dst(v INTEGER) AS SELECT 99 AS v, IGNORE_IF_EXISTS",
    "CREATE OR REPLACE TABLE dst(v INTEGER) AS SELECT 99 AS v, REPLACE_IF_EXISTS"
  })
  void preservesCreationModeThroughSqlProtoAndCalcite(String sql, CreateMode expected)
      throws SqlParseException {
    Plan plan = new SqlToSubstrait(provider).convert(sql, catalog);
    NamedWrite write = assertInstanceOf(NamedWrite.class, plan.getRoots().get(0).getInput());
    assertEquals(expected, write.getCreateMode());

    io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(plan);
    assertEquals(
        expected.toProto(), proto.getRelations(0).getRoot().getInput().getWrite().getCreateMode());
    Plan decoded = new ProtoPlanConverter().from(proto);
    CreateTable calcite =
        assertInstanceOf(
            CreateTable.class,
            new SubstraitToCalcite(provider, catalog)
                .convert(decoded.getRoots().get(0).getInput()));
    assertEquals(expected, calcite.getCreateMode());

    CreateTable copied =
        assertInstanceOf(
            CreateTable.class, calcite.copy(calcite.getTraitSet(), List.of(calcite.getInput())));
    NamedWrite roundTripped =
        assertInstanceOf(NamedWrite.class, SubstraitRelVisitor.convert(copied, provider));
    assertEquals(expected, roundTripped.getCreateMode());
  }

  @Test
  void rejectsConflictingCreationPolicies() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new SqlToSubstrait(provider)
                    .convert(
                        "CREATE OR REPLACE TABLE IF NOT EXISTS dst AS SELECT 99 AS v", catalog));
    assertTrue(error.getMessage().contains("cannot combine OR REPLACE and IF NOT EXISTS"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"CREATE TABLE", "CREATE TABLE IF NOT EXISTS", "CREATE OR REPLACE TABLE"})
  void createWithoutAQueryRemainsUnsupported(String prefix) {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SqlToSubstrait(provider).convert(prefix + " dst(v INTEGER)", catalog));
  }

  @Test
  void existingConstructorsRetainTheirCreationPolicy() throws SqlParseException {
    RelNode input = SubstraitSqlToCalcite.convertQuery("SELECT 99 AS v", catalog, provider).rel;
    assertEquals(
        CreateMode.REPLACE_IF_EXISTS, new CreateTable(List.of("DST"), input).getCreateMode());
    assertEquals(
        CreateMode.REPLACE_IF_EXISTS,
        new CreateTable(List.of("DST"), input.getRowType(), input).getCreateMode());
  }

  @Test
  void differentCreationPoliciesHaveDifferentPlannerDigests() throws SqlParseException {
    RelNode input = SubstraitSqlToCalcite.convertQuery("SELECT 99 AS v", catalog, provider).rel;
    CreateTable plain = new CreateTable(List.of("DST"), input, CreateMode.ERROR_IF_EXISTS);
    CreateTable replace = new CreateTable(List.of("DST"), input, CreateMode.REPLACE_IF_EXISTS);
    assertNotEquals(plain.getDigest(), replace.getDigest());
  }

  @ParameterizedTest
  @EnumSource(
      value = CreateMode.class,
      names = {"UNSPECIFIED", "APPEND_IF_EXISTS"})
  void unsupportedCreationModesAreRefused(CreateMode mode) throws SqlParseException {
    Plan plan = new SqlToSubstrait(provider).convert("CREATE TABLE dst AS SELECT 99 AS v", catalog);
    NamedWrite write = assertInstanceOf(NamedWrite.class, plan.getRoots().get(0).getInput());
    NamedWrite unsupported = NamedWrite.builder().from(write).createMode(mode).build();
    assertThrows(
        UnsupportedOperationException.class,
        () -> new SubstraitToCalcite(provider, catalog).convert(unsupported));
  }
}
