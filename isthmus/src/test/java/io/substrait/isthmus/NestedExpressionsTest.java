package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class NestedExpressionsTest extends PlanTestBase {

  Expression literalExpression = Expression.BoolLiteral.builder().value(true).build();
  Expression.ScalarFunctionInvocation nonLiteralExpression = sb.add(sb.i32(7), sb.i32(42));
  Expression.ScalarFunctionInvocation nonLiteralExpression2 = sb.add(sb.i32(3), sb.i32(4));

  final List<Type> tableType = List.of(R.I32, R.FP32, N.STRING, N.BOOLEAN, N.STRING);
  final Rel commonTable =
      sb.namedScan(List.of("example"), List.of("a", "b", "c", "d", "e"), tableType);
  final Rel emptyTable = sb.emptyVirtualTableScan();

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

  @Test
  void nestedStructWithLiteralsTest() {
    Expression.NestedStruct literalNestedStruct =
        Expression.NestedStruct.builder()
            .addFields(literalExpression)
            .addFields(sb.i32(12))
            .build();

    Project project =
        Project.builder().expressions(List.of(literalNestedStruct)).input(emptyTable).build();

    RelNode relNode = substraitToCalcite.convert(project); // substrait rel to calcite
    Rel substraitRel = SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
    Expression roundTripped = ((Project) substraitRel).getExpressions().get(0);
    assertEquals(ImmutableExpression.StructLiteral.class, roundTripped.getClass());
    Expression.StructLiteral structLiteral = (Expression.StructLiteral) roundTripped;
    assertEquals(literalNestedStruct.fields(), structLiteral.fields());
  }

  @Test
  void nullableNestedStructWithLiteralsTest() {
    // An all-literal struct collapses to a StructLiteral, but its nullability has to survive the
    // collapse: on a Substrait literal, nullable describes the type, not a null value.
    Expression.NestedStruct literalNestedStruct =
        Expression.NestedStruct.builder()
            .addFields(literalExpression)
            .addFields(sb.i32(12))
            .nullable(true)
            .build();

    Project project =
        Project.builder().expressions(List.of(literalNestedStruct)).input(emptyTable).build();

    RelNode relNode = substraitToCalcite.convert(project); // substrait rel to calcite
    Rel substraitRel = SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
    Expression roundTripped = ((Project) substraitRel).getExpressions().get(0);
    assertEquals(ImmutableExpression.StructLiteral.class, roundTripped.getClass());
    assertTrue(((Expression.StructLiteral) roundTripped).nullable());
  }

  @Test
  void nullableStructLiteralTest() {
    // The same nullability, on a value that is a StructLiteral to begin with. Its fields are
    // nullable because Calcite makes every field of a nullable record type nullable.
    Expression.StructLiteral structLiteral =
        ExpressionCreator.struct(true, ExpressionCreator.i32(true, 7));

    Project project =
        Project.builder().expressions(List.of(structLiteral)).input(emptyTable).build();

    assertFullRoundTrip(project);
  }

  @Test
  void nestedStructWithNonLiteralsTest() {
    Expression.NestedStruct nonLiteralNestedStruct =
        Expression.NestedStruct.builder()
            .addFields(nonLiteralExpression)
            .addFields(nonLiteralExpression2)
            .build();

    Project project =
        Project.builder()
            .expressions(List.of(nonLiteralNestedStruct))
            .input(commonTable)
            // project only the nestedStruct expression and exclude the 5 input columns
            .remap(Rel.Remap.of(Collections.singleton(5)))
            .build();

    assertFullRoundTrip(project);
  }

  @Test
  void heterogeneouslyTypedNestedStructTest() {
    Expression.NestedStruct nestedStruct =
        Expression.NestedStruct.builder()
            .addFields(nonLiteralExpression)
            .addFields(fieldRef1)
            .addFields(literalExpression)
            .build();

    Project project =
        Project.builder()
            .expressions(List.of(nestedStruct))
            .input(commonTable)
            .remap(Rel.Remap.of(Collections.singleton(5)))
            .build();

    assertFullRoundTrip(project);
  }

  @Test
  void nullableNestedStructTest() {
    Expression.NestedStruct nestedStruct =
        Expression.NestedStruct.builder()
            .addFields(nonLiteralExpression)
            .addFields(nonLiteralExpression2)
            .nullable(true)
            .build();

    Project project =
        Project.builder().expressions(List.of(nestedStruct)).input(emptyTable).build();

    assertFullRoundTrip(project);
  }

  @Test
  void nestedMapWithLiteralsTest() {
    // keys deliberately out of natural order, so that the assertion on key order below would catch
    // a map that no longer preserves the order the pairs were written in
    Expression.NestedMap literalNestedMap =
        Expression.NestedMap.builder()
            .putValues(sb.str("zzz"), literalExpression)
            .putValues(sb.str("aaa"), literalExpression)
            .putValues(sb.str("mmm"), literalExpression)
            .build();

    Project project =
        Project.builder().expressions(List.of(literalNestedMap)).input(emptyTable).build();

    RelNode relNode = substraitToCalcite.convert(project); // substrait rel to calcite
    Rel substraitRel = SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
    Expression roundTripped = ((Project) substraitRel).getExpressions().get(0);
    assertEquals(ImmutableExpression.MapLiteral.class, roundTripped.getClass());
    Expression.MapLiteral mapLiteral = (Expression.MapLiteral) roundTripped;
    assertEquals(literalNestedMap.values(), mapLiteral.values());
    // Map.equals ignores order, so compare the key sequences directly
    assertEquals(
        new ArrayList<>(literalNestedMap.values().keySet()),
        new ArrayList<>(mapLiteral.values().keySet()));
  }

  @Test
  void nullableNestedMapWithLiteralsTest() {
    // An all-literal map collapses to a MapLiteral, but its nullability has to survive the
    // collapse: on a Substrait literal, nullable describes the type, not a null value.
    Expression.NestedMap literalNestedMap =
        Expression.NestedMap.builder()
            .putValues(sb.str("a"), literalExpression)
            .putValues(sb.str("b"), literalExpression)
            .nullable(true)
            .build();

    Project project =
        Project.builder().expressions(List.of(literalNestedMap)).input(emptyTable).build();

    RelNode relNode = substraitToCalcite.convert(project); // substrait rel to calcite
    Rel substraitRel = SubstraitRelVisitor.convert(relNode, extensions); // calcite to substrait
    Expression roundTripped = ((Project) substraitRel).getExpressions().get(0);
    assertEquals(ImmutableExpression.MapLiteral.class, roundTripped.getClass());
    assertTrue(((Expression.MapLiteral) roundTripped).nullable());
  }

  @Test
  void nullableMapLiteralTest() {
    // The same nullability, on a value that is a MapLiteral to begin with.
    Expression.MapLiteral mapLiteral =
        ExpressionCreator.map(
            true, Map.of(ExpressionCreator.string(false, "a"), ExpressionCreator.i32(false, 1)));

    Project project = Project.builder().expressions(List.of(mapLiteral)).input(emptyTable).build();

    assertFullRoundTrip(project);
  }

  @Test
  void nullableListLiteralTest() {
    // And on a ListLiteral, the third of the three literal containers.
    Expression.ListLiteral listLiteral =
        ExpressionCreator.list(true, ExpressionCreator.i32(false, 1));

    Project project = Project.builder().expressions(List.of(listLiteral)).input(emptyTable).build();

    assertFullRoundTrip(project);
  }

  @Test
  void nestedMapWithNonLiteralsTest() {
    Expression.NestedMap nonLiteralNestedMap =
        Expression.NestedMap.builder()
            .putValues(sb.str("a"), nonLiteralExpression)
            .putValues(sb.str("b"), nonLiteralExpression2)
            .build();

    Project project =
        Project.builder()
            .expressions(List.of(nonLiteralNestedMap))
            .input(commonTable)
            // project only the nestedMap expression and exclude the 5 input columns
            .remap(Rel.Remap.of(Collections.singleton(5)))
            .build();

    assertFullRoundTrip(project);
  }

  @Test
  void nestedMapWithFieldReferenceTest() {
    Expression.NestedMap nestedMapWithField =
        Expression.NestedMap.builder().putValues(fieldRef1, fieldRef2).build();

    Project project =
        Project.builder()
            .expressions(List.of(nestedMapWithField))
            .input(commonTable)
            .remap(Rel.Remap.of(Collections.singleton(5)))
            .build();

    assertFullRoundTrip(project);
  }

  @Test
  void nullableNestedMapTest() {
    Expression.NestedMap nestedMap =
        Expression.NestedMap.builder()
            .putValues(sb.str("a"), nonLiteralExpression)
            .nullable(true)
            .build();

    Project project = Project.builder().expressions(List.of(nestedMap)).input(emptyTable).build();

    assertFullRoundTrip(project);
  }

  @Test
  void nestedStructOfNestedTypesTest() {
    Expression.NestedList list =
        Expression.NestedList.builder()
            .addValues(nonLiteralExpression)
            .addValues(nonLiteralExpression2)
            .build();
    Expression.NestedMap map =
        Expression.NestedMap.builder().putValues(sb.str("a"), nonLiteralExpression).build();

    Expression.NestedStruct nestedStruct =
        Expression.NestedStruct.builder().addFields(list).addFields(map).build();

    Project project =
        Project.builder().expressions(List.of(nestedStruct)).input(emptyTable).build();

    assertFullRoundTrip(project);
  }

  @Test
  void rowConstructorFromSqlTest() throws SqlParseException {
    assertFullRoundTrip("SELECT ROW(a + 1, b) FROM t", "CREATE TABLE t (a INT, b INT)");
  }

  @Test
  void mapConstructorFromSqlTest() throws SqlParseException {
    assertFullRoundTrip("SELECT MAP['key', a + 1] FROM t", "CREATE TABLE t (a INT)");
  }
}
