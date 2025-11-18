package io.substrait.isthmus;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.extension.ExtensionCollector;
import io.substrait.proto.ReadRel;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.relation.VirtualTableScan;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.util.Assert;

public class LogicalValuesTest extends PlanTestBase {

  final SubstraitBuilder b = new SubstraitBuilder(extensions);
  SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);
  ExtensionCollector functionCollector = new ExtensionCollector();
  RelProtoConverter relProtoConverter = new RelProtoConverter(functionCollector);

  @Test
  void testNestedStruct() {
    VirtualTableScan virtualTableScan = createVirtualTableScan();
    RelNode relNode = substraitToCalcite.convert(virtualTableScan); //    substrait rel to calcite

    LogicalValues logicalValues = (LogicalValues) relNode.getInput(0);
    Assert.equals(1, logicalValues.tuples.size()); // one row
    Assert.equals(2, logicalValues.tuples.get(0).size()); // 2 literal expressions
    LogicalProject logicalProject = (LogicalProject) relNode;
    Assert.equals(1, logicalProject.getProjects().size()); // one non-literal expression

    Rel virtualTableScan2 =
        SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
    io.substrait.proto.Rel proto = relProtoConverter.toProto(virtualTableScan2); // pojo to proto

    ReadRel.VirtualTable protoVirtualTable = proto.getRead().getVirtualTable();
    Assert.equals(1, protoVirtualTable.getExpressionsList().size());
    Assert.equals(2, protoVirtualTable.getExpressionsList().get(0).getFieldsList().size());

    Assert.equals(virtualTableScan, virtualTableScan2); // pojo -> calcite -> pojo
  }

  @Test
  void testNestedStructMultiRow() {
    VirtualTableScan virtualTableScan = createMultiExpressionVirtualTableScan();
    RelNode relNode = substraitToCalcite.convert(virtualTableScan); //    substrait rel to calcite

    LogicalValues logicalValues = (LogicalValues) relNode.getInput(0);
    Assert.equals(2, logicalValues.tuples.size()); // one row
    Assert.equals(2, logicalValues.tuples.get(0).size()); // 2 literal expressions
    LogicalProject logicalProject = (LogicalProject) relNode;
    Assert.equals(2, logicalProject.getProjects().size()); // two non-literal expression

    Rel virtualTableScan2 =
        SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
    io.substrait.proto.Rel proto = relProtoConverter.toProto(virtualTableScan2); // pojo to proto

    ReadRel.VirtualTable protoVirtualTable = proto.getRead().getVirtualTable();
    Assert.equals(2, protoVirtualTable.getExpressionsList().size());
    Assert.equals(2, protoVirtualTable.getExpressionsList().get(0).getFieldsList().size());

    Assert.equals(virtualTableScan, virtualTableScan2); // pojo -> calcite -> pojo
  }

  VirtualTableScan createVirtualTableScan() {
    Expression.ScalarFunctionInvocation scalarExpr = b.add(b.i32(7), b.i32(42));
    Expression literalExpr = b.i32(100);

    Expression.NestedStruct structRow =
        Expression.NestedStruct.builder().addFields(scalarExpr).addFields(literalExpr).build();

    return VirtualTableScan.builder()
        .initialSchema(
            io.substrait.type.NamedStruct.of(
                Stream.of("col1", "col2").collect(Collectors.toList()), R.struct(R.I32, R.I32)))
        .addRows(structRow)
        .build();
  }

  VirtualTableScan createMultiExpressionVirtualTableScan() {
    Expression.ScalarFunctionInvocation scalarExpr = b.add(b.i32(1), b.i32(1));
    Expression.ScalarFunctionInvocation scalarExpr2 = b.add(b.i32(2), b.i32(2));
    Expression literalExpr = b.i32(6);
    Expression literalExpr2 = b.i32(7);

    Expression.NestedStruct structRow =
        Expression.NestedStruct.builder().addFields(scalarExpr).addFields(literalExpr).build();
    Expression.NestedStruct structRow2 =
        Expression.NestedStruct.builder().addFields(literalExpr2).addFields(scalarExpr2).build();

    return VirtualTableScan.builder()
        .initialSchema(
            io.substrait.type.NamedStruct.of(
                Stream.of("col1", "col2").collect(Collectors.toList()), R.struct(R.I32, R.I32)))
        .addRows(structRow)
        .addRows(structRow2)
        .build();
  }
}
