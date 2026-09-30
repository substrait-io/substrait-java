package io.substrait.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import org.junit.jupiter.api.Test;

class SortFieldTest extends TestBase {

  @Test
  void neitherDirectionNorComparisonFunctionIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Expression.SortField.builder().expr(sb.i64(1)).build());
  }

  @Test
  void bothDirectionAndComparisonFunctionIsRejected() {
    SimpleExtension.ScalarFunctionVariant comparisonFunction =
        extensions.getScalarFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "nullif:any_any"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            Expression.SortField.builder()
                .expr(sb.i64(1))
                .direction(Expression.SortDirection.ASC_NULLS_FIRST)
                .comparisonFunction(comparisonFunction)
                .build());
  }

  @Test
  void protoWithNoSortKindSetIsRejected() {
    io.substrait.proto.SortField protoSortField =
        io.substrait.proto.SortField.newBuilder()
            .setExpr(expressionProtoConverter.toProto(sb.i64(1)))
            .build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> protoExpressionConverter.fromSortField(protoSortField));
    assertEquals("SortField has no sort_kind set", e.getMessage());
  }
}
