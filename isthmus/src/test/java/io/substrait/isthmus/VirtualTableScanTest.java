package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.MaskExpression;
import io.substrait.hint.Hint;
import io.substrait.relation.Rel;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class VirtualTableScanTest extends PlanTestBase {

  @Test
  void literalOnlyVirtualTable() {
    NamedStruct schema =
        NamedStruct.of(List.of("col1", "col2", "col3"), R.struct(R.I32, R.FP64, R.STRING));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema,
            List.of(sb.i32(2), sb.fp64(4), sb.str("a")),
            List.of(sb.i32(6), sb.fp64(8.8), sb.str("b")));

    // Check the specific Calcite encoding
    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalValues(type=[RecordType(INTEGER col1, DOUBLE col2, VARCHAR col3)], tuples=[[{ 2, 4.0E0, 'a' }, { 6, 8.8E0, 'b' }]])\n",
        explain(relNode));

    // Check full roundtrip conversion
    assertFullRoundTrip(virtualTableScan);
  }

  @Test
  void expressionContainingVirtualTable() {
    NamedStruct schema = NamedStruct.of(List.of("col1", "col2"), R.struct(R.I32, R.FP64));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema,
            List.of(sb.i32(2), sb.add(sb.fp64(4.4), sb.fp64(4.5))),
            List.of(sb.multiply(sb.i32(6), sb.i32(2)), sb.fp64(8.8)));

    // Check the specific Calcite encoding
    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalProject(inputs=[0..1])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(exprs=[[2, +(4.4E0:DOUBLE, 4.5E0:DOUBLE)]])\n"
            + "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n"
            + "    LogicalProject(exprs=[[*(6, 2), 8.8E0:DOUBLE]])\n"
            + "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n",
        explain(relNode));
  }

  @Test
  void emptyVirtualTableScan() {
    NamedStruct schema = NamedStruct.of(List.of(), R.struct());
    assertDoesNotThrow(() -> createVirtualTableScan(schema, new ArrayList<>()));
  }

  @Test
  void emptyTableNonEmptySchema() {
    NamedStruct schema = NamedStruct.of(List.of("col1"), R.struct(R.I32));
    assertDoesNotThrow(() -> createVirtualTableScan(schema));
  }

  @Test
  void emptySchemaNonEmptyTable() {
    NamedStruct schema = NamedStruct.of(List.of(), R.struct());
    assertThrows(
        IllegalArgumentException.class,
        () -> createVirtualTableScan(schema, List.of(sb.i32(3), sb.fp64(8))));
  }

  @Test
  void nullableFieldRoundTrip() {
    NamedStruct schema = NamedStruct.of(List.of("col1", "col2"), R.struct(N.I32, R.FP64));
    Expression nullableI32 = ExpressionCreator.i32(true, 6);
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(schema, List.of(nullableI32, sb.fp64(8)));
    assertFullRoundTrip(virtualTableScan);
  }

  @Test
  void nullLiteralRoundTrip() {
    NamedStruct schema = NamedStruct.of(List.of("col1", "col2"), R.struct(N.I32, N.FP64));
    Expression nullI32 = ExpressionCreator.typedNull(N.I32);
    Expression nullFp64 = ExpressionCreator.typedNull(N.FP64);
    VirtualTableScan virtualTableScan = createVirtualTableScan(schema, List.of(nullI32, nullFp64));
    assertFullRoundTrip(virtualTableScan);
  }

  @Test
  void mixedNullabilityRoundTrip() {
    NamedStruct schema =
        NamedStruct.of(List.of("col1", "col2", "col3"), R.struct(N.I32, R.FP64, N.STRING));
    Expression nullI32 = ExpressionCreator.typedNull(N.I32);
    Expression nullString = ExpressionCreator.typedNull(N.STRING);
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(schema, List.of(nullI32, sb.fp64(8), nullString));
    assertFullRoundTrip(virtualTableScan);
  }

  @Test
  void valuesLiteralUsesSchemaType() {
    RelDataType requiredI32 = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType nullableI32 = typeFactory.createTypeWithNullability(requiredI32, true);
    RelDataType rowType =
        typeFactory.builder().add("required", requiredI32).add("nullable", nullableI32).build();
    RelDataType narrowType = typeFactory.createSqlType(SqlTypeName.TINYINT);
    RexLiteral one = builder.getRexBuilder().makeExactLiteral(BigDecimal.ONE, narrowType);
    RexLiteral nullValue =
        builder
            .getRexBuilder()
            .makeNullLiteral(typeFactory.createTypeWithNullability(narrowType, true));
    RexLiteral five = builder.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(5), narrowType);
    LogicalValues values =
        LogicalValues.create(
            builder.getCluster(),
            rowType,
            ImmutableList.of(ImmutableList.of(one, nullValue), ImmutableList.of(one, five)));

    VirtualTableScan converted =
        assertInstanceOf(
            VirtualTableScan.class, SubstraitRelVisitor.convert(values, converterProvider));
    assertEquals(List.of(R.I32, N.I32), converted.getInitialSchema().struct().fields());
    assertEquals(
        List.of(ExpressionCreator.i32(false, 1), ExpressionCreator.typedNull(N.I32)),
        converted.getRows().get(0).fields());
    assertEquals(
        List.of(ExpressionCreator.i32(false, 1), ExpressionCreator.i32(true, 5)),
        converted.getRows().get(1).fields());
  }

  /**
   * A struct column takes the projection encoding whatever it holds. Calcite has a row literal, but
   * it orders Values tuples by casting each value to {@link Comparable} and a row literal's value
   * is a list of {@link RexLiteral}s, which are not -- so a second row is enough to make one throw,
   * here and in whatever the consumer's planner does with the relation afterwards.
   */
  @Test
  void structColumnConverts() {
    NamedStruct schema =
        NamedStruct.of(List.of("outer", "a", "b"), R.struct(R.struct(R.I32, R.FP64)));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema, List.of(ExpressionCreator.struct(false, sb.i32(1), sb.fp64(2.0))));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalProject(inputs=[0])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(exprs=[[ROW(1, 2.0E0:DOUBLE)]])\n"
            + "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n",
        explain(relNode));
  }

  /** The row after the first is where a row literal in a tuple would be compared to another. */
  @Test
  void twoStructRowsConvert() {
    NamedStruct schema =
        NamedStruct.of(List.of("outer", "a", "b"), R.struct(R.struct(R.I32, R.FP64)));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema,
            List.of(ExpressionCreator.struct(false, sb.i32(1), sb.fp64(2.0))),
            List.of(ExpressionCreator.struct(false, sb.i32(3), sb.fp64(4.0))));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalProject(inputs=[0])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(exprs=[[ROW(1, 2.0E0:DOUBLE)]])\n"
            + "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n"
            + "    LogicalProject(exprs=[[ROW(3, 4.0E0:DOUBLE)]])\n"
            + "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n",
        explain(relNode));
  }

  /**
   * A schema struct that is itself nullable says nothing about the relation: a row type describes
   * the columns, and Calcite derives one NOT NULL everywhere else. What the nullability does reach
   * is the columns -- Calcite makes a struct's fields nullable along with the struct -- which is
   * the same row type the conversion built before it was given the schema's names.
   */
  @Test
  void nullableSchemaStructGivesANotNullRowType() {
    NamedStruct schema = NamedStruct.of(List.of("col1"), N.struct(R.I32));
    VirtualTableScan virtualTableScan = createVirtualTableScan(schema, List.of(sb.i32(1)));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals("RecordType(INTEGER col1) NOT NULL", relNode.getRowType().getFullTypeString());
  }

  /**
   * Two struct columns, so that the names cannot be recovered by taking the first columns worth of
   * the schema's flattened list: the second column's name sits at index 3, not at index 1.
   */
  @Test
  void severalStructColumnsConvert() {
    NamedStruct schema =
        NamedStruct.of(
            List.of("first", "a", "b", "second", "c"),
            R.struct(R.struct(R.I32, R.FP64), R.struct(R.STRING)));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema,
            List.of(
                ExpressionCreator.struct(false, sb.i32(1), sb.fp64(2.0)),
                ExpressionCreator.struct(false, sb.str("x"))));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalProject(inputs=[0..1])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(exprs=[[ROW(1, 2.0E0:DOUBLE), ROW('x':VARCHAR)]])\n"
            + "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n",
        explain(relNode));
  }

  /**
   * A struct column whose fields are not all literals is a row of expressions like any other, so it
   * takes the projection encoding -- with the schema's names on it, which is what makes the
   * projection's declared type acceptable to Calcite.
   */
  @Test
  void structColumnWithAComputedField() {
    NamedStruct schema =
        NamedStruct.of(List.of("outer", "a", "b"), R.struct(R.struct(R.I32, R.FP64)));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema,
            List.of(
                ExpressionCreator.nestedStruct(
                    false, sb.multiply(sb.i32(6), sb.i32(2)), sb.fp64(2.0))));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalProject(inputs=[0])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(exprs=[[ROW(*(6, 2), 2.0E0:DOUBLE)]])\n"
            + "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n",
        explain(relNode));
  }

  /**
   * A list literal converts to Calcite's array constructor, a call rather than a literal, so a list
   * column takes the projection encoding too. It used to be handed to a Values tuple, which holds
   * nothing but literals, and the conversion died on the cast.
   */
  @Test
  void listColumnConverts() {
    NamedStruct schema = NamedStruct.of(List.of("col1"), R.struct(R.list(R.I32)));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema, List.of(ExpressionCreator.list(false, sb.i32(1), sb.i32(2))));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalProject(inputs=[0])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(exprs=[[ARRAY(1, 2)]])\n"
            + "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n",
        explain(relNode));
  }

  /**
   * Calcite has no nullable literal of a row, so a struct value in a nullable column cannot be a
   * Values tuple whatever it holds, and the row takes the projection encoding. Pinned as a
   * conversion rather than a round trip because a nullable struct does not survive one: Calcite
   * pushes a row's nullability down into its fields, so the schema comes back with nullable fields.
   */
  @Test
  void nullableStructColumnConverts() {
    NamedStruct schema =
        NamedStruct.of(List.of("outer", "a", "b"), R.struct(N.struct(R.I32, R.FP64)));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema, List.of(ExpressionCreator.struct(true, sb.i32(1), sb.fp64(2.0))));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalProject(inputs=[0])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(exprs=[[ROW(1, 2.0E0:DOUBLE)]])\n"
            + "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n",
        explain(relNode));
  }

  /** A null struct has no fields to rename, but the tuple still needs it at the column's type. */
  @Test
  void nullStructColumnConverts() {
    NamedStruct schema =
        NamedStruct.of(List.of("outer", "a", "b"), R.struct(N.struct(R.I32, R.FP64)));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema, List.of(ExpressionCreator.typedNull(N.struct(R.I32, R.FP64))));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalValues(type=[RecordType(RecordType(INTEGER a, DOUBLE b) outer)], tuples=[[{ null }]])\n",
        explain(relNode));
  }

  /**
   * A value that cannot take the type its column is declared at is reported here. Calcite types a
   * scalar subquery nullable -- no row means no value -- whatever the Substrait expression says its
   * type is, so this row cannot stand in a NOT NULL column. Without the check the relation is built
   * at the declared type regardless, and Calcite reports the mismatch through an {@code assert},
   * which says nothing unless assertions are on.
   */
  @Test
  void aValueThatCannotTakeItsColumnsTypeIsReported() {
    VirtualTableScan inner =
        createVirtualTableScan(NamedStruct.of(List.of("a"), R.struct(R.I32)), List.of(sb.i32(7)));
    Expression subquery = Expression.ScalarSubquery.builder().input(inner).type(R.I32).build();
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(NamedStruct.of(List.of("col1"), R.struct(R.I32)), List.of(subquery));

    IllegalArgumentException reported =
        assertThrows(
            IllegalArgumentException.class, () -> substraitToCalcite.convert(virtualTableScan));
    assertTrue(
        reported.getMessage().contains("is not the INTEGER NOT NULL its column is declared at"),
        reported.getMessage());
  }

  /**
   * A list and a map convert to a constructor call, except when they are null: then they are a
   * literal, carrying the placeholder name Calcite derives, and the tuple needs them at the
   * column's own type just as a struct does.
   */
  @Test
  void nullListColumnConverts() {
    NamedStruct schema = NamedStruct.of(List.of("col1"), R.struct(N.list(R.I32)));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(schema, List.of(ExpressionCreator.typedNull(N.list(R.I32))));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalValues(type=[RecordType(INTEGER ARRAY col1)], tuples=[[{ null }]])\n",
        explain(relNode));

    assertFullRoundTrip(virtualTableScan);
  }

  @Test
  void nullMapColumnConverts() {
    NamedStruct schema = NamedStruct.of(List.of("col1"), R.struct(N.map(R.STRING, R.I32)));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema, List.of(ExpressionCreator.typedNull(N.map(R.STRING, R.I32))));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalValues(type=[RecordType((VARCHAR, INTEGER) MAP col1)], tuples=[[{ null }]])\n",
        explain(relNode));

    assertFullRoundTrip(virtualTableScan);
  }

  /**
   * A struct one level down, inside a list column: the names the schema gives it have to reach it
   * there too, or the projection that holds the row is rejected on them.
   */
  @Test
  void structInListColumnConverts() {
    NamedStruct schema = NamedStruct.of(List.of("col1", "a"), R.struct(R.list(R.struct(R.I32))));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema,
            List.of(ExpressionCreator.list(false, ExpressionCreator.struct(false, sb.i32(1)))));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalProject(inputs=[0])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(exprs=[[ARRAY(ROW(1))]])\n"
            + "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n",
        explain(relNode));
  }

  /** The same one level down inside a map, where the names reach a key and a value alike. */
  @Test
  void structInMapColumnConverts() {
    NamedStruct schema =
        NamedStruct.of(List.of("col1", "a"), R.struct(R.map(R.STRING, R.struct(R.I32))));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema,
            List.of(
                ExpressionCreator.map(
                    false, Map.of(sb.str("k"), ExpressionCreator.struct(false, sb.i32(1))))));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalProject(inputs=[0])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(exprs=[[MAP('k':VARCHAR, ROW(1))]])\n"
            + "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n",
        explain(relNode));
  }

  /**
   * A nullable struct nested in a column: renaming it gives back a ROW call rather than a literal,
   * so the struct around it cannot be rebuilt as a literal either.
   */
  @Test
  void nullableStructInsideStructColumnConverts() {
    NamedStruct schema =
        NamedStruct.of(List.of("outer", "inner", "a"), R.struct(R.struct(N.struct(R.I32))));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema,
            List.of(ExpressionCreator.struct(false, ExpressionCreator.struct(true, sb.i32(1)))));

    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalProject(inputs=[0])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalProject(exprs=[[ROW(ROW(1))]])\n"
            + "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n",
        explain(relNode));
  }

  /**
   * The emit mapping of a virtual table selects its columns like any other relation's: the scan
   * produces the whole table and a projection drops what the mapping leaves out. That projection is
   * what converts back, so such a scan returns as a projection over one -- the same shape every
   * relation with a mapping comes back as.
   */
  @Test
  void anEmitMappingSelectsTheColumnsItNames() {
    NamedStruct schema = NamedStruct.of(List.of("col1", "col2"), R.struct(R.I32, R.STRING));
    VirtualTableScan table =
        VirtualTableScan.builder()
            .from(createVirtualTableScan(schema, List.of(sb.i32(2), sb.str("a"))))
            .remap(Rel.Remap.of(List.of(1)))
            .build();

    RelNode relNode = substraitToCalcite.convert(table);

    assertEquals(List.of("col2"), relNode.getRowType().getFieldNames());
    assertEquals(
        List.of(R.STRING),
        SubstraitRelVisitor.convert(relNode, extensions).getRecordType().fields());
  }

  /**
   * Without a mapping there is no projection to name, and a virtual table's own schema already
   * names its columns, so the hint is left where it is rather than rebuilding the table around it.
   */
  @Test
  void outputNamesWithoutAMappingAreLeftAlone() {
    NamedStruct schema = NamedStruct.of(List.of("col1", "col2"), R.struct(R.I32, R.STRING));
    VirtualTableScan table =
        VirtualTableScan.builder()
            .from(createVirtualTableScan(schema, List.of(sb.i32(2), sb.str("a"))))
            .hint(Hint.builder().addOutputNames("x", "y").build())
            .build();

    RelNode relNode = substraitToCalcite.convert(table);

    assertInstanceOf(LogicalValues.class, relNode);
    assertEquals(List.of("col1", "col2"), relNode.getRowType().getFieldNames());
  }

  /**
   * A projection masks a read relation's columns before anything else selects from them -- {@link
   * io.substrait.relation.AbstractReadRel#deriveRecordType()} applies it to the initial schema --
   * so an emit mapping's indices count the columns it leaves. Isthmus builds the row type from the
   * unmasked schema and reads the projection nowhere, so a scan carrying one is refused rather than
   * converted against the wrong columns.
   */
  @Test
  void aProjectionOnAVirtualTableIsRefused() {
    NamedStruct schema = NamedStruct.of(List.of("col1", "col2"), R.struct(R.I32, R.STRING));
    VirtualTableScan table =
        VirtualTableScan.builder()
            .from(createVirtualTableScan(schema, List.of(sb.i32(2), sb.str("a"))))
            .projection(
                MaskExpression.builder()
                    .select(
                        MaskExpression.StructSelect.builder()
                            .addStructItems(MaskExpression.StructItem.of(1))
                            .build())
                    .build())
            .build();

    assertTrue(
        assertThrows(UnsupportedOperationException.class, () -> substraitToCalcite.convert(table))
            .getMessage()
            .contains("Projection on a VirtualTableScan is not supported"));
  }

  /**
   * A virtual table's row type carries the names its schema gives it, which nothing uniquifies, so
   * the mapping has to select its columns by index: resolving a field by name would give the
   * projection the type of the first column sharing it.
   */
  @Test
  void anEmitMappingSelectsByIndexWhereTwoColumnsShareAName() {
    NamedStruct schema = NamedStruct.of(List.of("c", "c"), R.struct(R.I32, R.STRING));
    VirtualTableScan table =
        VirtualTableScan.builder()
            .from(createVirtualTableScan(schema, List.of(sb.i32(2), sb.str("a"))))
            .remap(Rel.Remap.of(List.of(1)))
            .build();

    RelNode relNode = substraitToCalcite.convert(table);

    assertEquals(
        List.of(R.STRING),
        SubstraitRelVisitor.convert(relNode, extensions).getRecordType().fields());
  }

  /**
   * The same as {@link #outputNamesWithoutAMappingAreLeftAlone()} for a table whose rows are
   * computed. That one comes back under a projection of its own, which is not one a mapping added
   * and so not one the names are for.
   */
  @Test
  void outputNamesWithoutAMappingAreLeftAloneOnAComputedTable() {
    NamedStruct schema = NamedStruct.of(List.of("col1", "col2"), R.struct(R.I32, R.FP64));
    VirtualTableScan table =
        VirtualTableScan.builder()
            .from(
                createVirtualTableScan(
                    schema, List.of(sb.i32(2), sb.add(sb.fp64(4.4), sb.fp64(4.5)))))
            .hint(Hint.builder().addOutputNames("x", "y").build())
            .build();

    RelNode relNode = substraitToCalcite.convert(table);

    assertEquals(List.of("col1", "col2"), relNode.getRowType().getFieldNames());
  }

  /** The names of its hint reach the projection the mapping adds, as they do elsewhere. */
  @Test
  void outputNamesReachTheProjectionTheMappingAdds() {
    NamedStruct schema = NamedStruct.of(List.of("col1", "col2"), R.struct(R.I32, R.STRING));
    VirtualTableScan table =
        VirtualTableScan.builder()
            .from(createVirtualTableScan(schema, List.of(sb.i32(2), sb.str("a"))))
            .remap(Rel.Remap.of(List.of(1)))
            .hint(Hint.builder().addOutputNames("label").build())
            .build();

    assertEquals(List.of("label"), substraitToCalcite.convert(table).getRowType().getFieldNames());
  }

  @SafeVarargs
  private VirtualTableScan createVirtualTableScan(NamedStruct schema, List<Expression>... rows) {
    List<Expression.NestedStruct> structs =
        Arrays.stream(rows)
            .map(row -> Expression.NestedStruct.builder().addAllFields(row).build())
            .collect(Collectors.toList());

    return VirtualTableScan.builder().initialSchema(schema).addAllRows(structs).build();
  }

  private String explain(RelNode relNode) {
    // Setting DIGEST_ATTRIBUTES in order to verify types in tests
    StringWriter sw = new StringWriter();
    RelWriter planWriter =
        new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.DIGEST_ATTRIBUTES, false);
    relNode.explain(planWriter);
    return sw.toString();
  }
}
