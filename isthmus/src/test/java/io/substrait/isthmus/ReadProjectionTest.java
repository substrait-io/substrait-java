package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.ImmutableMaskExpression;
import io.substrait.expression.MaskExpression;
import io.substrait.hint.Hint;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.junit.jupiter.api.Test;

/**
 * The projection a read relation carries masks the columns of its initial schema, and the read
 * produces the ones the mask leaves.
 */
class ReadProjectionTest extends PlanTestBase {

  private final NamedScan scan =
      (NamedScan)
          sb.namedScan(List.of("t"), List.of("a", "b", "c"), List.of(R.I64, N.STRING, R.FP64));

  /** A mask selecting whole columns, by the index each has in the initial schema. */
  private static MaskExpression columns(int... fields) {
    ImmutableMaskExpression.StructSelect.Builder select = MaskExpression.StructSelect.builder();
    for (int field : fields) {
      select.addStructItems(MaskExpression.StructItem.of(field));
    }
    return MaskExpression.builder().select(select.build()).build();
  }

  @Test
  void aProjectionMasksTheColumnsANamedScanReads() {
    NamedScan masked = NamedScan.builder().from(scan).projection(columns(0, 2)).build();

    RelNode relNode = substraitToCalcite.convert(masked);

    assertEquals(List.of("a", "c"), relNode.getRowType().getFieldNames());
    assertEquals(
        masked.getRecordType(),
        SubstraitRelVisitor.convert(relNode, converterProvider).getRecordType());
  }

  /**
   * A mask that lists its columns out of schema order produces them in the order it lists. That is
   * the order the model derives the record type in, and the node has to carry the columns the
   * relation says it carries. Spec v0.102.0 describes a mask as removing columns and asks whether
   * reordering should be supported at all, so this pins what the model already derives rather than
   * a rule the specification settles.
   */
  @Test
  void theColumnsComeOutInTheOrderTheMaskListsThem() {
    NamedScan reordered = NamedScan.builder().from(scan).projection(columns(2, 0)).build();

    RelNode relNode = substraitToCalcite.convert(reordered);

    assertEquals(List.of("c", "a"), relNode.getRowType().getFieldNames());
    assertEquals(
        reordered.getRecordType(),
        SubstraitRelVisitor.convert(relNode, converterProvider).getRecordType());
  }

  /**
   * An emit mapping selects from the columns the mask leaves, not from the schema it masked: on
   * this scan index 1 is the second column the mask keeps, which is the schema's third.
   */
  @Test
  void anEmitMappingSelectsFromTheMaskedColumns() {
    NamedScan masked =
        NamedScan.builder().from(scan).projection(columns(0, 2)).remap(sb.remap(1)).build();

    RelNode relNode = substraitToCalcite.convert(masked);

    assertEquals(List.of("c"), relNode.getRowType().getFieldNames());
    assertEquals(
        masked.getRecordType(),
        SubstraitRelVisitor.convert(relNode, converterProvider).getRecordType());
  }

  /**
   * So does a field reference a parent relation makes against the read: index 1 is the fp64 column
   * the mask leaves there, and not the string the schema has at that index.
   */
  @Test
  void aParentRelationReferencesTheMaskedColumns() {
    NamedScan masked = NamedScan.builder().from(scan).projection(columns(0, 2)).build();
    Filter filter = sb.filter(input -> sb.equal(sb.fieldReference(input, 1), sb.fp64(5)), masked);

    RelNode relNode = substraitToCalcite.convert(filter);

    Rel converted = SubstraitRelVisitor.convert(relNode, converterProvider);
    assertEquals(filter.getRecordType(), converted.getRecordType());
    assertEquals(filter.getCondition(), assertInstanceOf(Filter.class, converted).getCondition());
  }

  /** A mask that selects every column in order leaves the scan the columns it already reads. */
  @Test
  void aProjectionSelectingEveryColumnLeavesTheScanAlone() {
    NamedScan masked = NamedScan.builder().from(scan).projection(columns(0, 1, 2)).build();

    assertInstanceOf(TableScan.class, substraitToCalcite.convert(masked));
  }

  /**
   * The masked columns are the ones the relation produces, so they are the ones an output name from
   * a hint names -- and the projection the mask adds is where those names can go.
   */
  @Test
  void anOutputNameFromAHintNamesAMaskedColumn() {
    NamedScan masked =
        NamedScan.builder()
            .from(scan)
            .projection(columns(0, 2))
            .hint(Hint.builder().addOutputNames("x", "y").build())
            .build();

    assertEquals(
        List.of("x", "y"), substraitToCalcite.convert(masked).getRowType().getFieldNames());
  }

  /**
   * A mask selects by index: a schema's names are not uniquified, so a name can stand for more than
   * one column.
   */
  @Test
  void aProjectionSelectsByIndexWhereTwoColumnsShareAName() {
    NamedScan sharedNames =
        (NamedScan)
            sb.namedScan(List.of("t"), List.of("c", "c", "d"), List.of(R.I64, R.STRING, R.FP64));
    NamedScan masked = NamedScan.builder().from(sharedNames).projection(columns(1, 2)).build();

    RelNode relNode = substraitToCalcite.convert(masked);

    assertEquals(
        masked.getRecordType(),
        SubstraitRelVisitor.convert(relNode, converterProvider).getRecordType());
  }

  @Test
  void aProjectionMasksTheColumnsAVirtualTableReads() {
    NamedStruct schema =
        NamedStruct.of(List.of("col1", "col2", "col3"), R.struct(R.I32, R.STRING, R.BOOLEAN));
    VirtualTableScan table =
        VirtualTableScan.builder()
            .initialSchema(schema)
            .addRows(
                Expression.NestedStruct.builder()
                    .addFields(sb.i32(2), sb.str("a"), sb.bool(true))
                    .build())
            .projection(columns(1, 2))
            .build();

    RelNode relNode = substraitToCalcite.convert(table);

    assertEquals(List.of("col2", "col3"), relNode.getRowType().getFieldNames());
    assertEquals(
        table.getRecordType(),
        SubstraitRelVisitor.convert(relNode, converterProvider).getRecordType());
  }

  /** A row that no {@code LogicalValues} tuple holds is computed, and masked the same way. */
  @Test
  void aProjectionMasksTheColumnsAComputedVirtualTableReads() {
    NamedStruct schema =
        NamedStruct.of(List.of("col1", "col2", "col3"), R.struct(R.I32, R.FP64, R.STRING));
    VirtualTableScan table =
        VirtualTableScan.builder()
            .initialSchema(schema)
            .addRows(
                Expression.NestedStruct.builder()
                    .addFields(sb.i32(2), sb.add(sb.fp64(4.4), sb.fp64(4.5)), sb.str("a"))
                    .build())
            .projection(columns(1, 2))
            .build();

    RelNode relNode = substraitToCalcite.convert(table);

    assertEquals(List.of("col2", "col3"), relNode.getRowType().getFieldNames());
    assertEquals(
        table.getRecordType(),
        SubstraitRelVisitor.convert(relNode, converterProvider).getRecordType());
  }

  /**
   * A mask can select inside a column as well, keeping some of a struct's fields or some of a
   * list's elements. Calcite reads a column as it stands, so such a mask is reported rather than
   * applied to the column it selects from.
   */
  @Test
  void aProjectionThatSelectsInsideAColumnIsRefused() {
    NamedScan structScan =
        (NamedScan)
            sb.namedScan(List.of("t"), List.of("s", "x", "y"), List.of(R.struct(R.I64, R.STRING)));
    MaskExpression insideAColumn =
        MaskExpression.builder()
            .select(
                MaskExpression.StructSelect.builder()
                    .addStructItems(
                        MaskExpression.StructItem.of(
                            0,
                            MaskExpression.StructSelect.builder()
                                .addStructItems(MaskExpression.StructItem.of(1))
                                .build()))
                    .build())
            .build();
    NamedScan masked = NamedScan.builder().from(structScan).projection(insideAColumn).build();

    assertTrue(
        assertThrows(UnsupportedOperationException.class, () -> substraitToCalcite.convert(masked))
            .getMessage()
            .contains("selects inside a column"));
  }
}
