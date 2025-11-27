package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.relation.Rel;
import io.substrait.relation.VirtualTableScan;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.util.Assert;

public class VirtualTableScanTest extends PlanTestBase {

  final SubstraitBuilder b = new SubstraitBuilder(extensions);
  final SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);

  @Test
  void literalOnlyVirtualTable() {
    NamedStruct schema =
        NamedStruct.of(List.of("col1", "col2", "col3"), R.struct(R.I32, N.FP64, R.STRING));
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema,
            List.of(b.i32(2), b.fp64(4), b.str("a")),
            List.of(b.i32(6), b.fp64(8.8), b.str("b")));

    // Check the specific Calcite encoding
    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalValues(type=[RecordType(INTEGER col1, DOUBLE col2, VARCHAR col3)], tuples=[[{ 2, 4.0E0, 'a' }, { 6, 8.8E0, 'b' }]])\n",
        explain(relNode));

    // Check full roundtrip conversion
    assertFullRoundTrip(virtualTableScan);
  }

  @Test
  void expressionContainingVirtualTableNullable() {
      //      set a field to nullable
    NamedStruct schema = NamedStruct.of(List.of("col1", "col2"), R.struct(N.I32, R.FP64));
      var a = Expression.I32Literal.builder().nullable(true).value(2).build();
      var x = b.multiply(b.i32(6), b.i32(2));
      var c = Expression.ScalarFunctionInvocation.builder().from(x).outputType(Type.I32.builder().nullable(true).build()).build();

      //    calcite is not allowing nullable types in fields
    VirtualTableScan virtualTableScan =
        createVirtualTableScan(
            schema,
                List.of(a, b.add(b.fp64(4.4), b.fp64(4.5))),
                List.of(c, b.fp64(8.8)));


    // Check the specific Calcite encoding
    RelNode relNode = substraitToCalcite.convert(virtualTableScan);
    assertEquals(
        "LogicalExpressions(type=[RecordType(INTEGER col1, DOUBLE col2)], tuples=[[{ 2, +(4.4E0:DOUBLE, 4.5E0:DOUBLE) }, { *(6, 2), 8.8E0:DOUBLE }]])\n",
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
                        List.of(b.i32(2), b.add(b.fp64(4.4), b.fp64(4.5))),
                        List.of(b.multiply(b.i32(6), b.i32(2)), b.fp64(8.8)));

        // Check the specific Calcite encoding
        RelNode relNode = substraitToCalcite.convert(virtualTableScan);
        assertEquals(
                "LogicalProject(inputs=[0..1])\n" +
                        "  LogicalUnion(all=[true])\n" +
                        "    LogicalProject(exprs=[[2, +(4.4E0:DOUBLE, 4.5E0:DOUBLE)]])\n" +
                        "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n" +
                        "    LogicalProject(exprs=[[*(6, 2), 8.8E0:DOUBLE]])\n" +
                        "      LogicalValues(type=[RecordType()], tuples=[[{  }]])\n",
                explain(relNode));

        // Check full roundtrip conversion
        assertFullRoundTrip(virtualTableScan);
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
