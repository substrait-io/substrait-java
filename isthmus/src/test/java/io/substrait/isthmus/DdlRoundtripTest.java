package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.Plan;
import io.substrait.relation.AbstractDdlRel;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class DdlRoundtripTest extends PlanTestBase {
  final Prepare.CatalogReader catalogReader =
      SubstraitCreateStatementParser.processCreateStatementsToCatalog(
          "create table src1 (intcol int, charcol varchar(10))",
          "create table src2 (intcol int, charcol varchar(10))");

  public DdlRoundtripTest() throws SqlParseException {
    super();
  }

  @Test
  void testCreateTable() throws Exception {
    String sql = "create table dst1 as select * from src1";
    assertFullRoundTrip(sql, catalogReader);
  }

  @Test
  void testCreateView() throws Exception {
    String sql = "create view dst1 as select * from src1";
    assertFullRoundTrip(sql, catalogReader);
  }

  /**
   * The schema a CTAS declares is the one the table gets, and it is not the row type of the columns
   * filling it: a projection names its own columns whatever Calcite derives, here $f2 and $f3, and
   * the declared types here are ones the input does not produce either.
   */
  @Test
  void createTableKeepsTheSchemaItDeclares() {
    NamedWrite ctas =
        NamedWrite.builder()
            .input(computedColumns())
            .names(List.of("dst1"))
            .tableSchema(declaredSchema())
            .operation(AbstractWriteRel.WriteOp.CTAS)
            .createMode(AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS)
            .outputMode(AbstractWriteRel.OutputMode.NO_OUTPUT)
            .build();

    assertFullRoundTrip(ctas);
  }

  /** The same for a view: its definition fills the columns, the statement declares them. */
  @Test
  void createViewKeepsTheSchemaItDeclares() {
    NamedDdl createView =
        NamedDdl.builder()
            .viewDefinition(computedColumns())
            .names(List.of("dst1"))
            .tableSchema(declaredSchema())
            .tableDefaults(ExpressionCreator.struct(false))
            .operation(AbstractDdlRel.DdlOp.CREATE)
            .object(AbstractDdlRel.DdlObject.VIEW)
            .build();

    // Not assertFullRoundTrip: converting a NamedDdl without a catalog collects the schema by
    // walking the plan with RelCopyOnWriteVisitor, whose visit(NamedDdl) throws.
    Rel converted =
        SubstraitRelVisitor.convert(
            new SubstraitToCalcite(converterProvider, catalogReader).convert(createView),
            converterProvider);

    assertEquals(declaredSchema(), ((NamedDdl) converted).getTableSchema());
  }

  /**
   * A Calcite CreateView has nowhere to put the default values a DDL relation reports, and the spec
   * has that field report a full list of them, so a plan carrying any is refused rather than
   * converted into one that has lost them.
   */
  @Test
  void aViewWithDefaultValuesIsRefused() {
    NamedDdl withDefaults =
        NamedDdl.builder()
            .viewDefinition(computedColumns())
            .names(List.of("dst1"))
            .tableSchema(declaredSchema())
            .tableDefaults(
                ExpressionCreator.struct(
                    false, ExpressionCreator.i32(false, 0), ExpressionCreator.i32(false, 0)))
            .operation(AbstractDdlRel.DdlOp.CREATE)
            .object(AbstractDdlRel.DdlObject.VIEW)
            .build();

    assertThrows(
        UnsupportedOperationException.class,
        () -> new SubstraitToCalcite(converterProvider, catalogReader).convert(withDefaults));
  }

  @Test
  void createTableTakesTheColumnsTheStatementNames() throws SqlParseException {
    assertEquals(
        List.of("TOTAL", "DOUBLED"),
        schemaOf("create table dst1 (total, doubled) as select intcol + 1, intcol + 2 from src1")
            .names());
  }

  @Test
  void createTableTakesTheColumnTypesTheStatementDeclares() throws SqlParseException {
    NamedStruct schema =
        schemaOf(
            "create table dst1 (total bigint, doubled bigint not null)"
                + " as select intcol + 1, intcol + 2 from src1");

    assertEquals(List.of("TOTAL", "DOUBLED"), schema.names());
    assertEquals(TypeCreator.REQUIRED.struct(N.I64, R.I64), schema.struct());
  }

  @Test
  void createViewTakesTheColumnsTheStatementNames() throws SqlParseException {
    assertEquals(
        List.of("TOTAL", "DOUBLED"),
        schemaOf("create view dst1 (total, doubled) as select intcol + 1, intcol + 2 from src1")
            .names());
  }

  @Test
  void aStatementWithoutAColumnListKeepsTheNamesTheQueryProduces() throws SqlParseException {
    assertEquals(
        List.of("TOTAL"),
        schemaOf("create table dst1 as select intcol + 1 as total from src1").names());
  }

  /**
   * A statement naming two columns the same declares a schema no object can have, and Calcite
   * builds the row type it is given without uniquifying it, so the conversion refuses instead.
   */
  @Test
  void aStatementThatNamesTwoColumnsTheSameIsRefused() {
    assertThrows(
        IllegalArgumentException.class,
        () -> schemaOf("create table d (a, a) as select intcol, intcol from src1"));
  }

  /**
   * A DDL node produces the object it creates, not the query filling it, so the root over it is
   * named by the declared schema. The two disagree here: the query names its columns EXPR$0 and
   * EXPR$1.
   */
  @Test
  void theRootOverACreateStatementIsNamedByTheDeclaredSchema() throws SqlParseException {
    RelRoot root =
        SubstraitSqlToCalcite.convertQueries(
                "create table dst1 (total, doubled) as select intcol + 1, intcol + 2 from src1",
                catalogReader,
                converterProvider)
            .get(0);

    Plan.Root converted = SubstraitRelVisitor.convert(root, converterProvider);

    assertEquals(List.of("TOTAL", "DOUBLED"), converted.getNames());
  }

  /**
   * Neither a write nor a DDL relation gives an emit mapping columns to select from: isthmus
   * converts a write to a TableModify whose row type is a single ROWCOUNT column, and has nowhere
   * to put a projection between a CreateView's definition and the view it creates. A mapping over
   * either is refused rather than dropped.
   */
  @Test
  void anEmitMappingOnAWriteOrADdlIsRefused() {
    NamedWrite ctas =
        NamedWrite.builder()
            .input(computedColumns())
            .names(List.of("dst1"))
            .tableSchema(declaredSchema())
            .operation(AbstractWriteRel.WriteOp.CTAS)
            .createMode(AbstractWriteRel.CreateMode.REPLACE_IF_EXISTS)
            .outputMode(AbstractWriteRel.OutputMode.NO_OUTPUT)
            .remap(Rel.Remap.of(List.of(0)))
            .build();
    NamedDdl createView =
        NamedDdl.builder()
            .viewDefinition(computedColumns())
            .names(List.of("dst1"))
            .tableSchema(declaredSchema())
            .tableDefaults(ExpressionCreator.struct(false))
            .operation(AbstractDdlRel.DdlOp.CREATE)
            .object(AbstractDdlRel.DdlObject.VIEW)
            .remap(Rel.Remap.of(List.of(0)))
            .build();
    SubstraitToCalcite converter = new SubstraitToCalcite(converterProvider, catalogReader);

    assertAll(
        () ->
            assertTrue(
                assertThrows(UnsupportedOperationException.class, () -> converter.convert(ctas))
                    .getMessage()
                    .contains("Emit mapping on a NamedWrite is not supported")),
        () ->
            assertTrue(
                assertThrows(
                        UnsupportedOperationException.class, () -> converter.convert(createView))
                    .getMessage()
                    .contains("Emit mapping on a NamedDdl is not supported")));
  }

  /** The schema of the object a single DDL statement creates, as Substrait records it. */
  private NamedStruct schemaOf(String sql) throws SqlParseException {
    RelRoot root =
        SubstraitSqlToCalcite.convertQueries(sql, catalogReader, converterProvider).get(0);
    Rel converted = SubstraitRelVisitor.convert(root.rel, converterProvider);
    return converted instanceof NamedWrite
        ? ((NamedWrite) converted).getTableSchema()
        : ((NamedDdl) converted).getTableSchema();
  }

  private NamedStruct declaredSchema() {
    return NamedStruct.of(List.of("total", "doubled"), TypeCreator.REQUIRED.struct(R.I64, N.I32));
  }

  private Rel computedColumns() {
    Rel scan =
        sb.namedScan(List.of("SRC1"), List.of("INTCOL", "CHARCOL"), List.of(N.I32, N.varChar(10)));
    return Project.builder()
        .input(scan)
        .remap(Rel.Remap.offset(2, 2))
        .addExpressions(
            sb.add(sb.fieldReference(scan, 0), sb.i32(1)),
            sb.add(sb.fieldReference(scan, 0), sb.i32(2)))
        .build();
  }
}
