package io.substrait.type.proto;

import com.google.protobuf.Any;
import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

public class LiteralRoundtripTest extends TestBase {

  @Test
  void decimal() {
    io.substrait.expression.Expression.DecimalLiteral val =
        ExpressionCreator.decimal(false, BigDecimal.TEN, 10, 2);
    verifyRoundTrip(val);
  }

  @Test
  void userDefinedLiteralWithAnyRepresentation() {
    // Create a struct literal inline representing a point with latitude=42, longitude=100
    io.substrait.proto.Expression.Literal.Struct pointStruct =
        io.substrait.proto.Expression.Literal.Struct.newBuilder()
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(42))
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(100))
            .build();
    io.substrait.proto.Expression.Literal innerLiteral =
        io.substrait.proto.Expression.Literal.newBuilder().setStruct(pointStruct).build();
    Any anyValue = Any.pack(innerLiteral);

    Expression.UserDefinedLiteral val =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "point",
            java.util.Collections.emptyList(),
            anyValue);

    verifyRoundTrip(val);
  }

  @Test
  void userDefinedLiteralWithStructRepresentation() {
    java.util.List<Expression.Literal> fields =
        java.util.Arrays.asList(
            ExpressionCreator.i32(false, 42), ExpressionCreator.i32(false, 100));
    Expression.UserDefinedLiteral val =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "point",
            java.util.Collections.emptyList(),
            fields);

    verifyRoundTrip(val);
  }
}
