package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
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
import org.apache.calcite.rel.RelNode;
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
   * The names a CTAS declares are the ones the table gets, and they are not the names of the
   * columns filling it: a projection names its own columns whatever Calcite derives, here $f2 and
   * $f3.
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

    RelNode calcite = new SubstraitToCalcite(converterProvider, catalogReader).convert(ctas);
    Rel converted = SubstraitRelVisitor.convert(copied(calcite), converterProvider);

    assertEquals(declaredSchema(), ((NamedWrite) converted).getTableSchema());
  }

  /** The same for a view: its definition fills the columns, the statement names them. */
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

    RelNode calcite = new SubstraitToCalcite(converterProvider, catalogReader).convert(createView);
    Rel converted = SubstraitRelVisitor.convert(copied(calcite), converterProvider);

    assertEquals(declaredSchema(), ((NamedDdl) converted).getTableSchema());
  }

  /**
   * Copies the node the way a planner rewriting its input does, so that the schema has to be
   * carried rather than derived when the conversion reads it back.
   */
  private RelNode copied(RelNode node) {
    return node.copy(node.getTraitSet(), node.getInputs());
  }

  private NamedStruct declaredSchema() {
    return NamedStruct.of(List.of("total", "doubled"), TypeCreator.REQUIRED.struct(N.I32, N.I32));
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
