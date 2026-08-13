package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import java.util.List;
import org.junit.jupiter.api.Test;

class NestedMapExpressionTest extends TestBase {
  Expression literalExpression = Expression.BoolLiteral.builder().value(true).build();
  Expression.ScalarFunctionInvocation nonLiteralExpression = sb.add(sb.i32(7), sb.i32(42));

  @Test
  void rejectEmptyNestedMap() {
    ImmutableExpression.NestedMap.Builder builder = Expression.NestedMap.builder();
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void rejectNestedMapWithKeysOfDifferentTypes() {
    ImmutableExpression.NestedMap.Builder builder =
        Expression.NestedMap.builder()
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("a"), literalExpression))
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.i32(1), literalExpression));
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void rejectNestedMapWithValuesOfDifferentTypes() {
    ImmutableExpression.NestedMap.Builder builder =
        Expression.NestedMap.builder()
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("a"), literalExpression))
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("b"), sb.i32(1)));
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void rejectNestedMapWithValuesOfDifferentDecimalTypes() {
    // Values of the same kind but with different parameters are not the same type.
    ImmutableExpression.NestedMap.Builder builder =
        Expression.NestedMap.builder()
            .addKeyValues(
                Expression.NestedMap.KeyValue.of(
                    sb.str("a"), ExpressionCreator.typedNull(N.decimal(10, 2))))
            .addKeyValues(
                Expression.NestedMap.KeyValue.of(
                    sb.str("b"), ExpressionCreator.typedNull(N.decimal(12, 2))));
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void acceptNestedMapWithKeysAndValuesOfMixedNullability() {
    // Keys or values that differ only in nullability are valid; SQL builds such maps from
    // MAP['a', not_null_column, 'b', nullable_column].
    Expression.NestedMap.KeyValue notNull =
        Expression.NestedMap.KeyValue.of(sb.str("a"), sb.i32(1));
    Expression.NestedMap.KeyValue nullable =
        Expression.NestedMap.KeyValue.of(
            ExpressionCreator.typedNull(N.STRING), ExpressionCreator.typedNull(N.I32));

    Expression.NestedMap mixedNullability =
        Expression.NestedMap.builder().addKeyValues(notNull).addKeyValues(nullable).build();
    // The key and value types are nullable, because the map holds a null key and a null value.
    assertEquals(R.map(N.STRING, N.I32), mixedNullability.getType());
    // The pair order does not change the type.
    assertEquals(
        mixedNullability.getType(),
        Expression.NestedMap.builder()
            .addKeyValues(nullable)
            .addKeyValues(notNull)
            .build()
            .getType());

    verifyRoundTrip(projectOf(mixedNullability));
  }

  @Test
  void acceptNestedMapWithKeysAndValuesOfSameType() {
    ImmutableExpression.NestedMap.Builder builder =
        Expression.NestedMap.builder()
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("a"), nonLiteralExpression))
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("b"), sb.i32(12)));
    assertDoesNotThrow(builder::build);

    verifyRoundTrip(projectOf(builder.build()));
  }

  @Test
  void literalNestedMapTest() {
    Expression.NestedMap literalNestedMap =
        Expression.NestedMap.builder()
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("a"), literalExpression))
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("b"), literalExpression))
            .build();

    verifyRoundTrip(projectOf(literalNestedMap));
  }

  @Test
  void literalNullableNestedMapTest() {
    Expression.NestedMap literalNestedMap =
        Expression.NestedMap.builder()
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("a"), literalExpression))
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("b"), literalExpression))
            .nullable(true)
            .build();

    verifyRoundTrip(projectOf(literalNestedMap));
  }

  @Test
  void nonLiteralNestedMapTest() {
    Expression.NestedMap nonLiteralNestedMap =
        Expression.NestedMap.builder()
            .addKeyValues(
                Expression.NestedMap.KeyValue.of(nonLiteralExpression, nonLiteralExpression))
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.i32(12), sb.i32(13)))
            .build();

    verifyRoundTrip(projectOf(nonLiteralNestedMap));
  }

  @Test
  void nestedMapOfNestedMapsTest() {
    Expression.NestedMap inner =
        Expression.NestedMap.builder()
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("a"), sb.i32(1)))
            .build();

    Expression.NestedMap outer =
        Expression.NestedMap.builder()
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.str("outer"), inner))
            .build();

    verifyRoundTrip(projectOf(outer));
  }

  @Test
  void repeatedKeysNestedMapTest() {
    // A Substrait map expression is a repeated list of key-value pairs, so the same key may appear
    // more than once. Both pairs have to survive a round trip.
    Expression.NestedMap repeatedKeys =
        Expression.NestedMap.builder()
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.i32(1), sb.i32(10)))
            .addKeyValues(Expression.NestedMap.KeyValue.of(sb.i32(1), sb.i32(20)))
            .build();

    assertEquals(2, repeatedKeys.keyValues().size());
    verifyRoundTrip(projectOf(repeatedKeys));
  }

  @Test
  void keyValueOrderIsPreservedTest() {
    // Keys deliberately out of natural order, so that a representation which reorders the pairs
    // would fail here.
    List<Expression.NestedMap.KeyValue> keyValues =
        List.of(
            Expression.NestedMap.KeyValue.of(sb.str("zzz"), sb.i32(1)),
            Expression.NestedMap.KeyValue.of(sb.str("aaa"), sb.i32(2)),
            Expression.NestedMap.KeyValue.of(sb.str("mmm"), sb.i32(3)));

    Expression.NestedMap nestedMap =
        Expression.NestedMap.builder().addAllKeyValues(keyValues).build();

    assertEquals(keyValues, nestedMap.keyValues());
    verifyRoundTrip(projectOf(nestedMap));
  }

  @Test
  void rejectMapPairWithoutKeyOnImport() {
    io.substrait.proto.Expression.Nested.Map.KeyValue keyValue =
        io.substrait.proto.Expression.Nested.Map.KeyValue.newBuilder()
            .setValue(expressionProtoConverter.toProto(sb.i32(1)))
            .build();

    assertThrows(
        IllegalArgumentException.class, () -> protoExpressionConverter.from(protoMapOf(keyValue)));
  }

  @Test
  void rejectMapPairWithoutValueOnImport() {
    io.substrait.proto.Expression.Nested.Map.KeyValue keyValue =
        io.substrait.proto.Expression.Nested.Map.KeyValue.newBuilder()
            .setKey(expressionProtoConverter.toProto(sb.str("a")))
            .build();

    assertThrows(
        IllegalArgumentException.class, () -> protoExpressionConverter.from(protoMapOf(keyValue)));
  }

  @Test
  void rejectNestedExpressionWithoutKindOnImport() {
    io.substrait.proto.Expression nested =
        io.substrait.proto.Expression.newBuilder()
            .setNested(io.substrait.proto.Expression.Nested.newBuilder())
            .build();

    assertThrows(IllegalArgumentException.class, () -> protoExpressionConverter.from(nested));
  }

  /** Builds a proto map expression directly, so that pairs the POJO model rejects can be tested. */
  private io.substrait.proto.Expression protoMapOf(
      io.substrait.proto.Expression.Nested.Map.KeyValue... keyValues) {
    return io.substrait.proto.Expression.newBuilder()
        .setNested(
            io.substrait.proto.Expression.Nested.newBuilder()
                .setMap(
                    io.substrait.proto.Expression.Nested.Map.newBuilder()
                        .addAllKeyValues(List.of(keyValues))))
        .build();
  }
}
