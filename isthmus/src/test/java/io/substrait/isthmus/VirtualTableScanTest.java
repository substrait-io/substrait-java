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

//    @Test
//    void testNestedStruct() {
//        VirtualTableScan virtualTableScan = createVirtualTableScan();
//        RelNode relNode = substraitToCalcite.convert(virtualTableScan); //    substrait rel to calcite
//
//        Rel virtualTableScan2 =
//                SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
////        io.substrait.proto.Rel proto = relProtoConverter.toProto(virtualTableScan2); // pojo to proto
////        ReadRel.VirtualTable protoVirtualTable = proto.getRead().getVirtualTable();
////        Assert.equals(1, protoVirtualTable.getExpressionsList().size());
////        Assert.equals(2, protoVirtualTable.getExpressionsList().get(0).getFieldsList().size());
//
//        Assert.equals(virtualTableScan, virtualTableScan2); // pojo -> calcite -> pojo
//    }
////
//    @Test
//    void testNestedStructMultiRow() {
//        VirtualTableScan virtualTableScan = createMultiExpressionVirtualTableScan();
//        RelNode relNode = substraitToCalcite.convert(virtualTableScan); //    substrait rel to calcite
//
//        LogicalValues logicalValues = (LogicalValues) relNode.getInput(0);
//        Assert.equals(2, logicalValues.tuples.size()); // one row
//        Assert.equals(2, logicalValues.tuples.get(0).size()); // 2 literal expressions
//        LogicalProject logicalProject = (LogicalProject) relNode;
//        Assert.equals(2, logicalProject.getProjects().size()); // two non-literal expression
//
//        Rel virtualTableScan2 =
//                SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
//        io.substrait.proto.Rel proto = relProtoConverter.toProto(virtualTableScan2); // pojo to proto
//
//        ReadRel.VirtualTable protoVirtualTable = proto.getRead().getVirtualTable();
//        Assert.equals(2, protoVirtualTable.getExpressionsList().size());
//        Assert.equals(2, protoVirtualTable.getExpressionsList().get(0).getFieldsList().size());
//
//        Assert.equals(virtualTableScan, virtualTableScan2); // pojo -> calcite -> pojo
//    }

//    VirtualTableScan createVirtualTableScan() {
//        Expression.ScalarFunctionInvocation scalarExpr = b.add(b.i32(7), b.i32(42));
//        Expression literalExpr = b.i32(100);
//
//        Expression.NestedStruct structRow =
//                Expression.NestedStruct.builder().addFields(scalarExpr).addFields(literalExpr).build();
//
//        return VirtualTableScan.builder()
//                .initialSchema(
//                        io.substrait.type.NamedStruct.of(
//                                Stream.of("col1", "col2").collect(Collectors.toList()), R.struct(R.I32, R.I32)))
//                .addRows(structRow)
//                .build();
//    }
//
//    VirtualTableScan createMultiExpressionVirtualTableScan() {
//        Expression.ScalarFunctionInvocation scalarExpr = b.add(b.i32(1), b.i32(1));
//        Expression.ScalarFunctionInvocation scalarExpr2 = b.add(b.i32(2), b.i32(2));
//        Expression literalExpr = b.i32(6);
//        Expression literalExpr2 = b.i32(7);
//
//        Expression.NestedStruct structRow =
//                Expression.NestedStruct.builder().addFields(scalarExpr).addFields(literalExpr).build();
//        Expression.NestedStruct structRow2 =
//                Expression.NestedStruct.builder().addFields(literalExpr2).addFields(scalarExpr2).build();
//
//        return VirtualTableScan.builder()
//                .initialSchema(
//                        io.substrait.type.NamedStruct.of(
//                                Stream.of("col1", "col2").collect(Collectors.toList()), R.struct(R.I32, R.I32)))
//                .addRows(structRow)
//                .addRows(structRow2)
//                .build();
//    }

}

/*
// The top level logicalProject defines to number of columns needed in the row
A row: [1,2] , [1, 2+3] , [3+7,4+4], is union of a logicalproject with a logicalValues(mix),
or just straight logicalValues, or union of many logicalValues?

can logicalProject be unioned with logicalValues

Calcite seeminlgy, for each row just has an empty logicalValues then has all the columns in the logicalProject
then unnion all logicalProject rows together
- for each row just add all expressions into logicalproject,
- create empty logicalValues to pass as input to logicalProject
- add new logicalProject to list or rows to later pass as input to logicalUnion

[2+5] => LogicalProject[2+5]
            LogicalValues[0]

VirtualTable:
    [1,2,3, 1+1]

    ->

 */


/*
VirtualTable:
    [1,2,3, 1+1]
    [1,2+2,3, 1+1]
 */

/*
Just use logical Values
VirtualTable:
    [1,2,3,4]
    [1,2,3,6]
 */



