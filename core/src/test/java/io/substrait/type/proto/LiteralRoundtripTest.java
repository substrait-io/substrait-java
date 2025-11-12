package io.substrait.type.proto;

import static io.substrait.expression.proto.ProtoExpressionConverter.EMPTY_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.Any;
import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.SimpleExtension;
import io.substrait.util.EmptyVisitationContext;
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
    ExpressionProtoConverter to = new ExpressionProtoConverter(null, null);
    ProtoExpressionConverter from =
        new ProtoExpressionConverter(null, null, EMPTY_TYPE, protoRelConverter);
    assertEquals(val, from.from(val.accept(to, EmptyVisitationContext.INSTANCE)));
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

    ExpressionProtoConverter exprProtoConv =
        new ExpressionProtoConverter(functionCollector, relProtoConverter);
    ProtoExpressionConverter protoExprConv =
        new ProtoExpressionConverter(
            functionCollector, testExtensions, EMPTY_TYPE, protoRelConverter);
    assertEquals(val, protoExprConv.from(exprProtoConv.toProto(val)));
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

    ExpressionProtoConverter exprProtoConv =
        new ExpressionProtoConverter(functionCollector, relProtoConverter);
    ProtoExpressionConverter protoExprConv =
        new ProtoExpressionConverter(
            functionCollector, testExtensions, EMPTY_TYPE, protoRelConverter);
    assertEquals(val, protoExprConv.from(exprProtoConv.toProto(val)));
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

    ExpressionProtoConverter exprProtoConv =
        new ExpressionProtoConverter(functionCollector, relProtoConverter);
    ProtoExpressionConverter protoExprConv =
        new ProtoExpressionConverter(
            functionCollector, testExtensions, EMPTY_TYPE, protoRelConverter);

    Expression.UserDefinedLiteral roundtripped =
        (Expression.UserDefinedLiteral) protoExprConv.from(exprProtoConv.toProto(val));

    assertEquals(val, roundtripped);
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

    ExpressionProtoConverter exprProtoConv =
        new ExpressionProtoConverter(functionCollector, relProtoConverter);
    ProtoExpressionConverter protoExprConv =
        new ProtoExpressionConverter(
            functionCollector, testExtensions, EMPTY_TYPE, protoRelConverter);

    Expression.UserDefinedLiteral roundtripped =
        (Expression.UserDefinedLiteral) protoExprConv.from(exprProtoConv.toProto(val));

    assertEquals(val, roundtripped);
  }
}
