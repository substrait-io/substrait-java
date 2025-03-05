package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.ExpressionCreator;
import io.substrait.relation.EmptyScan;
import io.substrait.relation.NamedScan;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class ReadRelRoundtripTest extends TestBase {

  @Test
  void namedScan() {
    var tableName = Stream.of("a_table").collect(Collectors.toList());
    var columnNames = Stream.of("column1", "column2").collect(Collectors.toList());
    List<Type> columnTypes = Stream.of(R.I64, R.I64).collect(Collectors.toList());

    var namedScan = b.namedScan(tableName, columnNames, columnTypes);
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
    var emptyScan =
        EmptyScan.builder()
            .initialSchema(NamedStruct.of(Collections.emptyList(), R.struct()))
            .build();
    verifyRoundTrip(emptyScan);
  }

  @Test
  void virtualTable() {
    var virtTable =
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
