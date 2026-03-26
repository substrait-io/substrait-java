package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.MaskExpression;
import io.substrait.relation.LocalFiles;
import io.substrait.relation.NamedScan;
import io.substrait.relation.VirtualTableScan;
import io.substrait.relation.files.FileFormat;
import io.substrait.relation.files.FileOrFiles;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class ReadRelRoundtripTest extends TestBase {

  @Test
  void namedScan() {
    List<String> tableName = Stream.of("a_table").collect(Collectors.toList());
    List<String> columnNames = Stream.of("column1", "column2").collect(Collectors.toList());
    List<Type> columnTypes = Stream.of(R.I64, R.I64).collect(Collectors.toList());

    NamedScan namedScan = sb.namedScan(tableName, columnNames, columnTypes);
    namedScan =
        NamedScan.builder()
            .from(namedScan)
            .bestEffortFilter(
                sb.equal(sb.fieldReference(namedScan, 0), sb.fieldReference(namedScan, 1)))
            .filter(sb.equal(sb.fieldReference(namedScan, 0), sb.fieldReference(namedScan, 1)))
            .build();

    verifyRoundTrip(namedScan);
  }

  @Test
  void emptyVirtualTableScan() {
    io.substrait.relation.VirtualTableScan emptyVirtualTableScan = sb.emptyVirtualTableScan();
    verifyRoundTrip(emptyVirtualTableScan);
  }

  @Test
  void virtualTable() {
    io.substrait.relation.ImmutableVirtualTableScan virtTable =
        VirtualTableScan.builder()
            .initialSchema(
                NamedStruct.of(
                    Stream.of("column1", "column2").collect(Collectors.toList()),
                    R.struct(R.I64, R.I64)))
            .addRows(
                Expression.NestedStruct.builder()
                    .addFields(ExpressionCreator.i64(false, 1))
                    .addFields(ExpressionCreator.i64(false, 2))
                    .build())
            .build();
    virtTable =
        VirtualTableScan.builder()
            .from(virtTable)
            .bestEffortFilter(
                sb.equal(sb.fieldReference(virtTable, 0), sb.fieldReference(virtTable, 1)))
            .filter(sb.equal(sb.fieldReference(virtTable, 0), sb.fieldReference(virtTable, 1)))
            .build();
    verifyRoundTrip(virtTable);
  }

  @Test
  void virtualTableWithNullable() {
    io.substrait.relation.ImmutableVirtualTableScan virtTable =
        VirtualTableScan.builder()
            .initialSchema(
                NamedStruct.of(
                    Stream.of("nullable_col", "non_nullable_col").collect(Collectors.toList()),
                    R.struct(N.I64, R.I64)))
            .addRows(
                Expression.NestedStruct.builder()
                    .addFields(ExpressionCreator.typedNull(N.I64))
                    .addFields(ExpressionCreator.i64(false, 1))
                    .build())
            .addRows(
                Expression.NestedStruct.builder()
                    .addFields(ExpressionCreator.i64(true, 2))
                    .addFields(ExpressionCreator.i64(false, 3))
                    .build())
            .build();
    verifyRoundTrip(virtTable);
  }

  @Test
  void namedScanWithSimpleProjection() {
    List<String> tableName = Stream.of("my_table").collect(Collectors.toList());
    List<String> columnNames = Stream.of("col_a", "col_b", "col_c").collect(Collectors.toList());
    List<Type> columnTypes = Stream.of(R.I32, R.STRING, R.I64).collect(Collectors.toList());

    // Select columns 0 and 2
    MaskExpression.MaskExpr projection =
        MaskExpression.MaskExpr.builder()
            .select(
                MaskExpression.StructSelect.builder()
                    .addStructItems(MaskExpression.StructItem.of(0))
                    .addStructItems(MaskExpression.StructItem.of(2))
                    .build())
            .build();

    NamedScan namedScan =
        NamedScan.builder()
            .from(sb.namedScan(tableName, columnNames, columnTypes))
            .projection(projection)
            .build();

    verifyRoundTrip(namedScan);
  }

  @Test
  void namedScanWithNestedProjection() {
    List<String> tableName = Stream.of("nested_table").collect(Collectors.toList());
    List<String> columnNames = Stream.of("outer_struct", "simple_col").collect(Collectors.toList());
    List<Type> columnTypes =
        Stream.of(R.struct(R.I32, R.STRING, R.I64), R.I32).collect(Collectors.toList());

    // Select field 0, but within it only subfields 0 and 2
    MaskExpression.MaskExpr projection =
        MaskExpression.MaskExpr.builder()
            .select(
                MaskExpression.StructSelect.builder()
                    .addStructItems(
                        MaskExpression.StructItem.of(
                            0,
                            MaskExpression.Select.ofStruct(
                                MaskExpression.StructSelect.builder()
                                    .addStructItems(MaskExpression.StructItem.of(0))
                                    .addStructItems(MaskExpression.StructItem.of(2))
                                    .build())))
                    .addStructItems(MaskExpression.StructItem.of(1))
                    .build())
            .maintainSingularStruct(true)
            .build();

    NamedScan namedScan =
        NamedScan.builder()
            .from(sb.namedScan(tableName, columnNames, columnTypes))
            .projection(projection)
            .build();

    verifyRoundTrip(namedScan);
  }

  @Test
  void namedScanWithListProjection() {
    List<String> tableName = Stream.of("list_table").collect(Collectors.toList());
    List<String> columnNames = Stream.of("list_col", "id").collect(Collectors.toList());
    List<Type> columnTypes = Stream.of(R.list(R.I32), R.I64).collect(Collectors.toList());

    // Select field 0 with list element and slice selection, and field 1
    MaskExpression.MaskExpr projection =
        MaskExpression.MaskExpr.builder()
            .select(
                MaskExpression.StructSelect.builder()
                    .addStructItems(
                        MaskExpression.StructItem.of(
                            0,
                            MaskExpression.Select.ofList(
                                MaskExpression.ListSelect.builder()
                                    .addSelection(
                                        MaskExpression.ListSelectItem.ofItem(
                                            MaskExpression.ListElement.of(0)))
                                    .addSelection(
                                        MaskExpression.ListSelectItem.ofSlice(
                                            MaskExpression.ListSlice.of(2, 5)))
                                    .build())))
                    .addStructItems(MaskExpression.StructItem.of(1))
                    .build())
            .build();

    NamedScan namedScan =
        NamedScan.builder()
            .from(sb.namedScan(tableName, columnNames, columnTypes))
            .projection(projection)
            .build();

    verifyRoundTrip(namedScan);
  }

  @Test
  void namedScanWithMapProjection() {
    List<String> tableName = Stream.of("map_table").collect(Collectors.toList());
    List<String> columnNames = Stream.of("map_col", "id").collect(Collectors.toList());
    List<Type> columnTypes = Stream.of(R.map(R.STRING, R.I32), R.I64).collect(Collectors.toList());

    // Select field 0 with map key selection, and field 1
    MaskExpression.MaskExpr projection =
        MaskExpression.MaskExpr.builder()
            .select(
                MaskExpression.StructSelect.builder()
                    .addStructItems(
                        MaskExpression.StructItem.of(
                            0,
                            MaskExpression.Select.ofMap(
                                MaskExpression.MapSelect.ofKey(
                                    MaskExpression.MapKey.of("my_key")))))
                    .addStructItems(MaskExpression.StructItem.of(1))
                    .build())
            .build();

    NamedScan namedScan =
        NamedScan.builder()
            .from(sb.namedScan(tableName, columnNames, columnTypes))
            .projection(projection)
            .build();

    verifyRoundTrip(namedScan);
  }

  @Test
  void namedScanWithMapKeyExpressionProjection() {
    List<String> tableName = Stream.of("map_table").collect(Collectors.toList());
    List<String> columnNames = Stream.of("map_col").collect(Collectors.toList());
    List<Type> columnTypes = Stream.of(R.map(R.STRING, R.I32)).collect(Collectors.toList());

    MaskExpression.MaskExpr projection =
        MaskExpression.MaskExpr.builder()
            .select(
                MaskExpression.StructSelect.builder()
                    .addStructItems(
                        MaskExpression.StructItem.of(
                            0,
                            MaskExpression.Select.ofMap(
                                MaskExpression.MapSelect.ofExpression(
                                    MaskExpression.MapKeyExpression.of("prefix_*")))))
                    .build())
            .build();

    NamedScan namedScan =
        NamedScan.builder()
            .from(sb.namedScan(tableName, columnNames, columnTypes))
            .projection(projection)
            .build();

    verifyRoundTrip(namedScan);
  }

  @Test
  void virtualTableWithProjection() {
    MaskExpression.MaskExpr projection =
        MaskExpression.MaskExpr.builder()
            .select(
                MaskExpression.StructSelect.builder()
                    .addStructItems(MaskExpression.StructItem.of(0))
                    .build())
            .build();

    io.substrait.relation.ImmutableVirtualTableScan virtTable =
        VirtualTableScan.builder()
            .initialSchema(
                NamedStruct.of(
                    Stream.of("column1", "column2").collect(Collectors.toList()),
                    R.struct(R.I64, R.I64)))
            .addRows(
                Expression.NestedStruct.builder()
                    .addFields(ExpressionCreator.i64(false, 1))
                    .addFields(ExpressionCreator.i64(false, 2))
                    .build())
            .projection(projection)
            .build();

    verifyRoundTrip(virtTable);
  }

  @Test
  void localFilesWithProjection() {
    MaskExpression.MaskExpr projection =
        MaskExpression.MaskExpr.builder()
            .select(
                MaskExpression.StructSelect.builder()
                    .addStructItems(MaskExpression.StructItem.of(1))
                    .build())
            .build();

    LocalFiles localFiles =
        LocalFiles.builder()
            .initialSchema(
                NamedStruct.of(
                    Stream.of("col_a", "col_b").collect(Collectors.toList()),
                    R.struct(R.I32, R.STRING)))
            .addItems(
                FileOrFiles.builder()
                    .pathType(FileOrFiles.PathType.URI_PATH)
                    .path("/data/file.parquet")
                    .partitionIndex(0)
                    .start(0)
                    .length(1024)
                    .fileFormat(FileFormat.ParquetReadOptions.builder().build())
                    .build())
            .projection(projection)
            .build();

    verifyRoundTrip(localFiles);
  }

  @Test
  void namedScanWithProjectionAndFilter() {
    List<String> tableName = Stream.of("filtered_table").collect(Collectors.toList());
    List<String> columnNames = Stream.of("col_a", "col_b").collect(Collectors.toList());
    List<Type> columnTypes = Stream.of(R.I64, R.I64).collect(Collectors.toList());

    MaskExpression.MaskExpr projection =
        MaskExpression.MaskExpr.builder()
            .select(
                MaskExpression.StructSelect.builder()
                    .addStructItems(MaskExpression.StructItem.of(0))
                    .addStructItems(MaskExpression.StructItem.of(1))
                    .build())
            .maintainSingularStruct(true)
            .build();

    NamedScan namedScan = sb.namedScan(tableName, columnNames, columnTypes);
    namedScan =
        NamedScan.builder()
            .from(namedScan)
            .filter(sb.equal(sb.fieldReference(namedScan, 0), sb.fieldReference(namedScan, 1)))
            .projection(projection)
            .build();

    verifyRoundTrip(namedScan);
  }

  @Test
  void namedScanWithListSelectWithChild() {
    // list<struct<i32, string, i64>> - select list elements, then pick struct subfields
    List<String> tableName = Stream.of("nested_list_table").collect(Collectors.toList());
    List<String> columnNames = Stream.of("items", "id").collect(Collectors.toList());
    List<Type> columnTypes =
        Stream.of(R.list(R.struct(R.I32, R.STRING, R.I64)), R.I64).collect(Collectors.toList());

    MaskExpression.MaskExpr projection =
        MaskExpression.MaskExpr.builder()
            .select(
                MaskExpression.StructSelect.builder()
                    .addStructItems(
                        MaskExpression.StructItem.of(
                            0,
                            MaskExpression.Select.ofList(
                                MaskExpression.ListSelect.builder()
                                    .addSelection(
                                        MaskExpression.ListSelectItem.ofSlice(
                                            MaskExpression.ListSlice.of(0, 5)))
                                    .child(
                                        MaskExpression.Select.ofStruct(
                                            MaskExpression.StructSelect.builder()
                                                .addStructItems(MaskExpression.StructItem.of(0))
                                                .addStructItems(MaskExpression.StructItem.of(2))
                                                .build()))
                                    .build())))
                    .addStructItems(MaskExpression.StructItem.of(1))
                    .build())
            .build();

    NamedScan namedScan =
        NamedScan.builder()
            .from(sb.namedScan(tableName, columnNames, columnTypes))
            .projection(projection)
            .build();

    verifyRoundTrip(namedScan);
  }

  @Test
  void namedScanWithMapSelectWithChild() {
    // map<string, struct<i32, string>> - select by key, then pick struct subfields
    List<String> tableName = Stream.of("nested_map_table").collect(Collectors.toList());
    List<String> columnNames = Stream.of("entries", "id").collect(Collectors.toList());
    List<Type> columnTypes =
        Stream.of(R.map(R.STRING, R.struct(R.I32, R.STRING)), R.I64).collect(Collectors.toList());

    MaskExpression.MaskExpr projection =
        MaskExpression.MaskExpr.builder()
            .select(
                MaskExpression.StructSelect.builder()
                    .addStructItems(
                        MaskExpression.StructItem.of(
                            0,
                            MaskExpression.Select.ofMap(
                                MaskExpression.MapSelect.builder()
                                    .key(MaskExpression.MapKey.of("user_info"))
                                    .child(
                                        MaskExpression.Select.ofStruct(
                                            MaskExpression.StructSelect.builder()
                                                .addStructItems(MaskExpression.StructItem.of(1))
                                                .build()))
                                    .build())))
                    .addStructItems(MaskExpression.StructItem.of(1))
                    .build())
            .build();

    NamedScan namedScan =
        NamedScan.builder()
            .from(sb.namedScan(tableName, columnNames, columnTypes))
            .projection(projection)
            .build();

    verifyRoundTrip(namedScan);
  }

  @Test
  void namedScanWithMapKeyExpressionAndChild() {
    // map<string, struct<i32, string>> - select by key expression, then pick struct subfield
    List<String> tableName = Stream.of("map_expr_table").collect(Collectors.toList());
    List<String> columnNames = Stream.of("data").collect(Collectors.toList());
    List<Type> columnTypes =
        Stream.of(R.map(R.STRING, R.struct(R.I32, R.STRING))).collect(Collectors.toList());

    MaskExpression.MaskExpr projection =
        MaskExpression.MaskExpr.builder()
            .select(
                MaskExpression.StructSelect.builder()
                    .addStructItems(
                        MaskExpression.StructItem.of(
                            0,
                            MaskExpression.Select.ofMap(
                                MaskExpression.MapSelect.builder()
                                    .expression(MaskExpression.MapKeyExpression.of("user_*"))
                                    .child(
                                        MaskExpression.Select.ofStruct(
                                            MaskExpression.StructSelect.builder()
                                                .addStructItems(MaskExpression.StructItem.of(0))
                                                .build()))
                                    .build())))
                    .build())
            .build();

    NamedScan namedScan =
        NamedScan.builder()
            .from(sb.namedScan(tableName, columnNames, columnTypes))
            .projection(projection)
            .build();

    verifyRoundTrip(namedScan);
  }
}
