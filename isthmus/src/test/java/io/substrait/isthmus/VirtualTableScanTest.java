package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.expression.Expression;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
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
