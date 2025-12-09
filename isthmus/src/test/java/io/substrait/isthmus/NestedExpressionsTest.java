package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;

class NestedExpressionsTest extends PlanTestBase {

  protected static final SimpleExtension.ExtensionCollection defaultExtensionCollection =
      DefaultExtensionCatalog.DEFAULT_COLLECTION;
  protected SubstraitBuilder b = new SubstraitBuilder(defaultExtensionCollection);
  SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(extensions, typeFactory);

  io.substrait.expression.Expression literalExpression =
      Expression.BoolLiteral.builder().value(true).build();
  Expression.ScalarFunctionInvocation nonLiteralExpression = b.add(b.i32(7), b.i32(42));
  Expression.ScalarFunctionInvocation nonLiteralExpression2 = b.add(b.i32(3), b.i32(4));

  final List<Type> tableType = List.of(R.I32, R.FP32, N.STRING, N.BOOLEAN);
  final Rel commonTable = b.namedScan(List.of("example"), List.of("a", "b", "c", "d"), tableType);
  final Rel emptyTable = b.emptyScan();

  @Test
  void NestedListWithJustLiteralsTest() {
    List<Expression> expressionList = new ArrayList<>();
    Expression.NestedList literalNestedList =
        Expression.NestedList.builder()
            .addValues(literalExpression)
            .addValues(literalExpression)
            .build();
    expressionList.add(literalNestedList);

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .expressions(expressionList)
            .input(emptyTable)
            .build();

    RelNode relNode = substraitToCalcite.convert(project); //    substrait rel to calcite
    Rel project2 = SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
    assertEquals(project, project2); // pojo -> calcite -> pojo
  }

  @Test
  void NestedListWithNonLiteralsTest() {
    List<Expression> expressionList = new ArrayList<>();

    Expression.NestedList nonLiteralNestedList =
        Expression.NestedList.builder()
            .addValues(nonLiteralExpression)
            .addValues(nonLiteralExpression2)
            .build();
    expressionList.add(nonLiteralNestedList);

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .expressions(expressionList)
            .input(commonTable)
            .remap(Rel.Remap.of(Collections.singleton(4)))
            .build();

    RelNode relNode = substraitToCalcite.convert(project); //    substrait rel to calcite
    Rel project2 = SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
    assertEquals(project, project2); // pojo -> calcite -> pojo
  }
}
