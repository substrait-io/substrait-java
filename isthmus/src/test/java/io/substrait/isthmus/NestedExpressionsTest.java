package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import io.substrait.expression.Expression;
import io.substrait.expression.ImmutableExpression;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;

class NestedExpressionsTest extends PlanTestBase {

  Expression literalExpression = Expression.BoolLiteral.builder().value(true).build();
  Expression.ScalarFunctionInvocation nonLiteralExpression = sb.add(sb.i32(7), sb.i32(42));
  Expression.ScalarFunctionInvocation nonLiteralExpression2 = sb.add(sb.i32(3), sb.i32(4));

  final List<Type> tableType = List.of(R.I32, R.FP32, N.STRING, N.BOOLEAN, N.STRING);
  final Rel commonTable =
      sb.namedScan(List.of("example"), List.of("a", "b", "c", "d", "e"), tableType);
  final Rel emptyTable = sb.emptyScan();

  Expression fieldRef1 = sb.fieldReference(commonTable, 2);
  Expression fieldRef2 = sb.fieldReference(commonTable, 4);

  @Test
  void nestedListWithLiteralsTest() {
    List<Expression> expressionList = new ArrayList<>();
    Expression.NestedList literalNestedList =
        Expression.NestedList.builder()
            .addValues(literalExpression)
            .addValues(literalExpression)
            .build();
    expressionList.add(literalNestedList);

    Project project = Project.builder().expressions(expressionList).input(emptyTable).build();

    RelNode relNode = substraitToCalcite.convert(project); //    substrait rel to calcite
    Rel substraitRel = SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
    Expression project2 = ((Project) substraitRel).getExpressions().get(0);
    assertEquals(ImmutableExpression.ListLiteral.class, project2.getClass());
    Expression.ListLiteral listLiteral = (Expression.ListLiteral) project2;
    assertEquals(literalNestedList.values(), listLiteral.values());
  }

  @Test
  void nestedListWithNonLiteralsTest() {
    List<Expression> expressionList = new ArrayList<>();

    Expression.NestedList nonLiteralNestedList =
        Expression.NestedList.builder()
            .addValues(nonLiteralExpression)
            .addValues(nonLiteralExpression2)
            .build();
    expressionList.add(nonLiteralNestedList);

    Project project =
        Project.builder()
            .expressions(expressionList)
            .input(commonTable)
            // project only the nestedList expression and exclude the 5 input columns
            .remap(Rel.Remap.of(Collections.singleton(5)))
            .build();

    assertFullRoundTrip(project);
  }

  @Test
  void nestedListWithFieldReferenceTest() {
    Expression.NestedList nestedListWithField =
        Expression.NestedList.builder().addValues(fieldRef1).addValues(fieldRef2).build();

    List<Expression> expressionList = new ArrayList<>();
    expressionList.add(nestedListWithField);

    Project project =
        Project.builder()
            .expressions(expressionList)
            .input(commonTable)
            .remap(Rel.Remap.of(Collections.singleton(5)))
            .build();

    assertFullRoundTrip(project);
  }

  @Test
  void nestedListWithStringLiteralsTest() {
    Expression.NestedList nestedList =
        Expression.NestedList.builder().addValues(sb.str("xzy")).addValues(sb.str("abc")).build();

    Rel project = Project.builder().expressions(List.of(nestedList)).input(emptyTable).build();

    RelNode relNode = substraitToCalcite.convert(project); //    substrait rel to calcite
    Rel substraitRel = SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
    Expression project2 = ((Project) substraitRel).getExpressions().get(0);
    assertEquals(ImmutableExpression.ListLiteral.class, project2.getClass());
    Expression.ListLiteral listLiteral = (Expression.ListLiteral) project2;
    assertEquals(nestedList.values(), listLiteral.values());
  }

  @Test
  void nestedListWithBinaryLiteralTest() {
    Expression binaryLiteral =
        Expression.BinaryLiteral.builder()
            .value(ByteString.copyFrom(new byte[] {0x01, 0x02}))
            .build();

    Expression.NestedList nestedList =
        Expression.NestedList.builder().addValues(binaryLiteral).addValues(binaryLiteral).build();

    Rel project = Project.builder().expressions(List.of(nestedList)).input(emptyTable).build();

    RelNode relNode = substraitToCalcite.convert(project); //    substrait rel to calcite
    Rel substraitRel = SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
    Expression project2 = ((Project) substraitRel).getExpressions().get(0);
    assertEquals(ImmutableExpression.ListLiteral.class, project2.getClass());
    Expression.ListLiteral listLiteral = (Expression.ListLiteral) project2;
    assertEquals(nestedList.values(), listLiteral.values());
  }

  @Test
  void nestedListWithSingleLiteralTest() {
    List<Expression> expressionList = new ArrayList<>();
    Expression.NestedList literalNestedList =
        Expression.NestedList.builder().addValues(literalExpression).build();
    expressionList.add(literalNestedList);

    Project project = Project.builder().expressions(expressionList).input(emptyTable).build();

    RelNode relNode = substraitToCalcite.convert(project); //    substrait rel to calcite
    Rel substraitRel = SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
    Expression project2 = ((Project) substraitRel).getExpressions().get(0);
    assertEquals(ImmutableExpression.ListLiteral.class, project2.getClass());
    Expression.ListLiteral listLiteral = (Expression.ListLiteral) project2;
    assertEquals(literalNestedList.values(), listLiteral.values());
  }

  @Test
  void nullableNestedListTest() {
    List<Expression> expressionList = new ArrayList<>();
    Expression.NestedList literalNestedList =
        Expression.NestedList.builder()
            .addValues(nonLiteralExpression)
            .addValues(nonLiteralExpression2)
            .nullable(true)
            .build();
    expressionList.add(literalNestedList);

    Project project = Project.builder().expressions(expressionList).input(emptyTable).build();

    assertFullRoundTrip(project);
  }
}
