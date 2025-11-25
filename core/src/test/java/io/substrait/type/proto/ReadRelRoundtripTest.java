package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.ExpressionCreator;
import io.substrait.relation.NamedScan;
import io.substrait.relation.VirtualTableScan;
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

    NamedScan namedScan = b.namedScan(tableName, columnNames, columnTypes);
    namedScan =
        NamedScan.builder()
            .from(namedScan)
            .bestEffortFilter(
                b.equal(b.fieldReference(namedScan, 0), b.fieldReference(namedScan, 1)))
            .filter(b.equal(b.fieldReference(namedScan, 0), b.fieldReference(namedScan, 1)))
            .build();

    verifyRoundTrip(namedScan);
  }

  @Test
  void emptyScan() {
    io.substrait.relation.EmptyScan emptyScan = b.emptyScan();
    verifyRoundTrip(emptyScan);
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
                ExpressionCreator.struct(
                    false, ExpressionCreator.i64(false, 1), ExpressionCreator.i64(false, 2)))
            .build();
    virtTable =
        VirtualTableScan.builder()
            .from(virtTable)
            .bestEffortFilter(
                b.equal(b.fieldReference(virtTable, 0), b.fieldReference(virtTable, 1)))
            .filter(b.equal(b.fieldReference(virtTable, 0), b.fieldReference(virtTable, 1)))
            .build();
    verifyRoundTrip(virtTable);
  }
}
