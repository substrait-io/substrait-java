package io.substrait.isthmus;

import com.google.protobuf.ByteString;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class NestedExpressionsTest extends PlanTestBase {

  protected static final SimpleExtension.ExtensionCollection defaultExtensionCollection =
      DefaultExtensionCatalog.DEFAULT_COLLECTION;
  protected SubstraitBuilder b = new SubstraitBuilder(defaultExtensionCollection);

  io.substrait.expression.Expression literalExpression =
      Expression.BoolLiteral.builder().value(true).build();
  Expression.ScalarFunctionInvocation nonLiteralExpression = b.add(b.i32(7), b.i32(42));
  Expression.ScalarFunctionInvocation nonLiteralExpression2 = b.add(b.i32(3), b.i32(4));

  final List<Type> tableType = List.of(R.I32, R.FP32, N.STRING, N.BOOLEAN, N.STRING);
  final Rel commonTable =
      b.namedScan(List.of("example"), List.of("a", "b", "c", "d", "e"), tableType);
  final Rel emptyTable = b.emptyScan();

  Expression fieldRef1 = b.fieldReference(commonTable, 2);
  Expression fieldRef2 = b.fieldReference(commonTable, 4);

  @Test
  void nestedListWithLiteralsTest() {
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

    assertFullRoundTrip(project);
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

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
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

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .expressions(expressionList)
            .input(commonTable)
            .remap(Rel.Remap.of(Collections.singleton(5)))
            .build();

    assertFullRoundTrip(project);
  }

  @Test
  void nestedListWithStringLiteralsTest() {
    Expression.NestedList nestedList =
        Expression.NestedList.builder().addValues(b.str("xzy")).addValues(b.str("abc")).build();

    Rel project =
        io.substrait.relation.Project.builder()
            .expressions(List.of(nestedList))
            .input(emptyTable)
            .build();

    assertFullRoundTrip(project);
  }

  @Test
  void nestedListWithBinaryLiteralTest() {
    Expression binaryLiteral =
        Expression.BinaryLiteral.builder()
            .value(ByteString.copyFrom(new byte[] {0x01, 0x02}))
            .build();

    Expression.NestedList nestedList =
        Expression.NestedList.builder().addValues(binaryLiteral).addValues(binaryLiteral).build();

    Rel project =
        io.substrait.relation.Project.builder()
            .expressions(List.of(nestedList))
            .input(emptyTable)
            .build();

    assertFullRoundTrip(project);
  }

  @Test
  void nestedListWithSingleLiteralTest() {
    List<Expression> expressionList = new ArrayList<>();
    Expression.NestedList literalNestedList =
        Expression.NestedList.builder().addValues(literalExpression).build();
    expressionList.add(literalNestedList);

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .expressions(expressionList)
            .input(emptyTable)
            .build();

    assertFullRoundTrip(project);
  }

  @Test
  void nullableNestedListTest() {
    List<Expression> expressionList = new ArrayList<>();
    Expression.NestedList literalNestedList =
        Expression.NestedList.builder()
            .addValues(literalExpression)
            .addValues(literalExpression)
            .nullable(true)
            .build();
    expressionList.add(literalNestedList);

    io.substrait.relation.Project project =
        io.substrait.relation.Project.builder()
            .expressions(expressionList)
            .input(emptyTable)
            .build();

    assertFullRoundTrip(project);
  }
}
