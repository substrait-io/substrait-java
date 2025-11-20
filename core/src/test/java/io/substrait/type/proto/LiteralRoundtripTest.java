package io.substrait.type.proto;

import com.google.protobuf.Any;
import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.SimpleExtension;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

public class LiteralRoundtripTest extends TestBase {

  // Load custom extensions for UserDefined literal tests
  private static final SimpleExtension.ExtensionCollection testExtensions =
      SimpleExtension.load(java.util.Arrays.asList("/extensions/custom_extensions.yaml"));

  @Test
  void decimal() {
    io.substrait.expression.Expression.DecimalLiteral val =
        ExpressionCreator.decimal(false, BigDecimal.TEN, 10, 2);
    verifyRoundTrip(val, null);
  }

  @Test
  void userDefinedLiteralWithAnyRepresentation() {
    io.substrait.proto.Expression.Literal innerLiteral =
        io.substrait.proto.Expression.Literal.newBuilder().setI32(42).build();
    Any anyValue = Any.pack(innerLiteral);

    String urn = "extension:test:custom_extensions";
    String typeName = "customType1";

    Expression.UserDefinedLiteral val =
        ExpressionCreator.userDefinedLiteralAny(false, urn, typeName, anyValue);

    verifyRoundTrip(val, testExtensions);
  }

  @Test
  void userDefinedLiteralWithStructRepresentation() {
    String urn = "extension:test:custom_extensions";
    String typeName = "customType2";

    java.util.List<Expression.Literal> fields =
        java.util.Arrays.asList(
            ExpressionCreator.i32(false, 42), ExpressionCreator.string(false, "test"));
    Expression.UserDefinedLiteral val =
        ExpressionCreator.userDefinedLiteralStruct(false, urn, typeName, fields);

    verifyRoundTrip(val, testExtensions);
  }

  @Test
  void userDefinedLiteralWithAnyRepresentationAndTypeParameters() {
    io.substrait.proto.Expression.Literal innerLiteral =
        io.substrait.proto.Expression.Literal.newBuilder().setI32(42).build();
    Any anyValue = Any.pack(innerLiteral);

    String urn = "extension:test:custom_extensions";
    String typeName = "customType1";

    java.util.List<io.substrait.proto.Type.Parameter> typeParams =
        java.util.Arrays.asList(
            io.substrait.proto.Type.Parameter.newBuilder()
                .setDataType(
                    io.substrait.proto.Type.newBuilder()
                        .setI32(
                            io.substrait.proto.Type.I32
                                .newBuilder()
                                .setNullability(
                                    io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED)))
                .build());

    Expression.UserDefinedLiteral val =
        ExpressionCreator.userDefinedLiteralAny(false, urn, typeName, typeParams, anyValue);

    verifyRoundTrip(val, testExtensions);
  }

  @Test
  void userDefinedLiteralWithStructRepresentationAndTypeParameters() {
    String urn = "extension:test:custom_extensions";
    String typeName = "customType2";

    java.util.List<Expression.Literal> fields =
        java.util.Arrays.asList(
            ExpressionCreator.i32(false, 42), ExpressionCreator.string(false, "test"));

    java.util.List<io.substrait.proto.Type.Parameter> typeParams =
        java.util.Arrays.asList(
            io.substrait.proto.Type.Parameter.newBuilder()
                .setDataType(
                    io.substrait.proto.Type.newBuilder()
                        .setString(
                            io.substrait.proto.Type.String.newBuilder()
                                .setNullability(
                                    io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE)))
                .build());

    Expression.UserDefinedLiteral val =
        ExpressionCreator.userDefinedLiteralStruct(false, urn, typeName, typeParams, fields);

    verifyRoundTrip(val, testExtensions);
  }
}
