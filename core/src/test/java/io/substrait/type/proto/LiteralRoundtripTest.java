package io.substrait.type.proto;

import static io.substrait.expression.proto.ProtoExpressionConverter.EMPTY_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.Any;
import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.RelProtoConverter;
import java.math.BigDecimal;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class LiteralRoundtripTest extends TestBase {

  private static final String NESTED_TYPES_URN = "extension:io.substrait:test_nested_types";

  private static final String NESTED_TYPES_YAML =
      "---\n"
          + "urn: "
          + NESTED_TYPES_URN
          + "\n"
          + "types:\n"
          + "  - name: point\n"
          + "    structure:\n"
          + "      latitude: i32\n"
          + "      longitude: i32\n"
          + "  - name: triangle\n"
          + "    structure:\n"
          + "      p1: point\n"
          + "      p2: point\n"
          + "      p3: point\n"
          + "  - name: vector\n"
          + "    parameters:\n"
          + "      - name: T\n"
          + "        type: dataType\n"
          + "    structure:\n"
          + "      x: T\n"
          + "      y: T\n"
          + "      z: T\n";

  private static final SimpleExtension.ExtensionCollection NESTED_TYPES_EXTENSIONS =
      SimpleExtension.load("nested_types.yaml", NESTED_TYPES_YAML);

  private static final ExtensionCollector NESTED_TYPES_FUNCTION_COLLECTOR =
      new ExtensionCollector();
  private static final RelProtoConverter NESTED_TYPES_REL_PROTO_CONVERTER =
      new RelProtoConverter(NESTED_TYPES_FUNCTION_COLLECTOR);
  private static final ProtoRelConverter NESTED_TYPES_PROTO_REL_CONVERTER =
      new ProtoRelConverter(NESTED_TYPES_FUNCTION_COLLECTOR, NESTED_TYPES_EXTENSIONS);
  private static final ExpressionProtoConverter NESTED_TYPES_EXPRESSION_TO_PROTO =
      new ExpressionProtoConverter(
          NESTED_TYPES_FUNCTION_COLLECTOR, NESTED_TYPES_REL_PROTO_CONVERTER);
  private static final ProtoExpressionConverter NESTED_TYPES_PROTO_TO_EXPRESSION =
      new ProtoExpressionConverter(
          NESTED_TYPES_FUNCTION_COLLECTOR,
          NESTED_TYPES_EXTENSIONS,
          EMPTY_TYPE,
          NESTED_TYPES_PROTO_REL_CONVERTER);

  @Test
  void decimal() {
    io.substrait.expression.Expression.DecimalLiteral val =
        ExpressionCreator.decimal(false, BigDecimal.TEN, 10, 2);
    verifyRoundTrip(val);
  }

  /** Verifies round-trip conversion of a simple user-defined type using Any representation. */
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

  /** Verifies round-trip conversion of a simple user-defined type using Struct representation. */
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

  /**
   * Verifies round-trip conversion of nested user-defined types where a triangle UDT contains three
   * point UDTs. Both outer and nested types use Struct representation.
   */
  @Test
  void nestedUserDefinedLiteralWithStructRepresentation() {
    Expression.UserDefinedStruct p1 =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "point",
            Collections.emptyList(),
            java.util.Arrays.asList(
                ExpressionCreator.i32(false, 0), ExpressionCreator.i32(false, 0)));

    Expression.UserDefinedStruct p2 =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "point",
            Collections.emptyList(),
            java.util.Arrays.asList(
                ExpressionCreator.i32(false, 10), ExpressionCreator.i32(false, 0)));

    Expression.UserDefinedStruct p3 =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "point",
            Collections.emptyList(),
            java.util.Arrays.asList(
                ExpressionCreator.i32(false, 5), ExpressionCreator.i32(false, 10)));

    Expression.UserDefinedStruct triangle =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "triangle",
            Collections.emptyList(),
            java.util.Arrays.asList(p1, p2, p3));

    io.substrait.proto.Expression protoExpression =
        NESTED_TYPES_EXPRESSION_TO_PROTO.toProto(triangle);
    Expression result = NESTED_TYPES_PROTO_TO_EXPRESSION.from(protoExpression);
    assertEquals(triangle, result);
  }

  /**
   * Verifies round-trip conversion of nested user-defined types where a triangle UDT contains three
   * point UDTs. Both outer and nested types use Any representation.
   */
  @Test
  void nestedUserDefinedLiteralWithAnyRepresentation() {

    // Create three point UDTs using Any representation
    io.substrait.proto.Expression.Literal.Struct p1Struct =
        io.substrait.proto.Expression.Literal.Struct.newBuilder()
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(0))
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(0))
            .build();
    Any p1Any =
        Any.pack(io.substrait.proto.Expression.Literal.newBuilder().setStruct(p1Struct).build());
    Expression.UserDefinedAny p1 =
        ExpressionCreator.userDefinedLiteralAny(
            false, NESTED_TYPES_URN, "point", Collections.emptyList(), p1Any);

    io.substrait.proto.Expression.Literal.Struct p2Struct =
        io.substrait.proto.Expression.Literal.Struct.newBuilder()
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(10))
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(0))
            .build();
    Any p2Any =
        Any.pack(io.substrait.proto.Expression.Literal.newBuilder().setStruct(p2Struct).build());
    Expression.UserDefinedAny p2 =
        ExpressionCreator.userDefinedLiteralAny(
            false, NESTED_TYPES_URN, "point", Collections.emptyList(), p2Any);

    io.substrait.proto.Expression.Literal.Struct p3Struct =
        io.substrait.proto.Expression.Literal.Struct.newBuilder()
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(5))
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(10))
            .build();
    Any p3Any =
        Any.pack(io.substrait.proto.Expression.Literal.newBuilder().setStruct(p3Struct).build());
    Expression.UserDefinedAny p3 =
        ExpressionCreator.userDefinedLiteralAny(
            false, NESTED_TYPES_URN, "point", Collections.emptyList(), p3Any);

    // Create a "triangle" struct containing three point UDTs
    io.substrait.proto.Expression.Literal.Struct triangleStruct =
        io.substrait.proto.Expression.Literal.Struct.newBuilder()
            .addFields(NESTED_TYPES_EXPRESSION_TO_PROTO.toProto(p1).getLiteral())
            .addFields(NESTED_TYPES_EXPRESSION_TO_PROTO.toProto(p2).getLiteral())
            .addFields(NESTED_TYPES_EXPRESSION_TO_PROTO.toProto(p3).getLiteral())
            .build();
    Any triangleAny =
        Any.pack(
            io.substrait.proto.Expression.Literal.newBuilder().setStruct(triangleStruct).build());

    Expression.UserDefinedAny triangle =
        ExpressionCreator.userDefinedLiteralAny(
            false, NESTED_TYPES_URN, "triangle", Collections.emptyList(), triangleAny);

    io.substrait.proto.Expression protoExpression =
        NESTED_TYPES_EXPRESSION_TO_PROTO.toProto(triangle);
    Expression result = NESTED_TYPES_PROTO_TO_EXPRESSION.from(protoExpression);
    assertEquals(triangle, result);
  }

  /**
   * Verifies round-trip conversion of nested user-defined types with mixed representations. The
   * triangle UDT uses Struct representation while the nested point UDTs use Any representation.
   */
  @Test
  void mixedRepresentationNestedUserDefinedLiteral() {
    io.substrait.proto.Expression.Literal.Struct p1Struct =
        io.substrait.proto.Expression.Literal.Struct.newBuilder()
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(0))
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(0))
            .build();
    Any p1Any =
        Any.pack(io.substrait.proto.Expression.Literal.newBuilder().setStruct(p1Struct).build());
    Expression.UserDefinedAny p1 =
        ExpressionCreator.userDefinedLiteralAny(
            false, NESTED_TYPES_URN, "point", Collections.emptyList(), p1Any);

    io.substrait.proto.Expression.Literal.Struct p2Struct =
        io.substrait.proto.Expression.Literal.Struct.newBuilder()
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(10))
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(0))
            .build();
    Any p2Any =
        Any.pack(io.substrait.proto.Expression.Literal.newBuilder().setStruct(p2Struct).build());
    Expression.UserDefinedAny p2 =
        ExpressionCreator.userDefinedLiteralAny(
            false, NESTED_TYPES_URN, "point", Collections.emptyList(), p2Any);

    io.substrait.proto.Expression.Literal.Struct p3Struct =
        io.substrait.proto.Expression.Literal.Struct.newBuilder()
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(5))
            .addFields(io.substrait.proto.Expression.Literal.newBuilder().setI32(10))
            .build();
    Any p3Any =
        Any.pack(io.substrait.proto.Expression.Literal.newBuilder().setStruct(p3Struct).build());
    Expression.UserDefinedAny p3 =
        ExpressionCreator.userDefinedLiteralAny(
            false, NESTED_TYPES_URN, "point", Collections.emptyList(), p3Any);

    // Create a "triangle" UDT using Struct representation, but with Any-encoded point fields
    Expression.UserDefinedStruct triangle =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "triangle",
            Collections.emptyList(),
            java.util.Arrays.asList(p1, p2, p3));

    io.substrait.proto.Expression protoExpression =
        NESTED_TYPES_EXPRESSION_TO_PROTO.toProto(triangle);
    Expression result = NESTED_TYPES_PROTO_TO_EXPRESSION.from(protoExpression);
    assertEquals(triangle, result);
  }

  /**
   * Verifies round-trip conversion of a parameterized user-defined type. Tests that type parameters
   * are correctly preserved during serialization and deserialization.
   */
  @Test
  void userDefinedLiteralWithTypeParameters() {
    // Create a type parameter for i32
    io.substrait.proto.Type i32Type =
        io.substrait.proto.Type.newBuilder()
            .setI32(
                io.substrait.proto.Type.I32
                    .newBuilder()
                    .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED))
            .build();
    io.substrait.proto.Type.Parameter typeParam =
        io.substrait.proto.Type.Parameter.newBuilder().setDataType(i32Type).build();

    // Create a vector<i32> instance with fields (x: 1, y: 2, z: 3)
    Expression.UserDefinedStruct vectorI32 =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "vector",
            java.util.Arrays.asList(typeParam),
            java.util.Arrays.asList(
                ExpressionCreator.i32(false, 1),
                ExpressionCreator.i32(false, 2),
                ExpressionCreator.i32(false, 3)));

    io.substrait.proto.Expression protoExpression =
        NESTED_TYPES_EXPRESSION_TO_PROTO.toProto(vectorI32);
    Expression result = NESTED_TYPES_PROTO_TO_EXPRESSION.from(protoExpression);
    assertEquals(vectorI32, result);

    Expression.UserDefinedStruct resultStruct = (Expression.UserDefinedStruct) result;
    assertEquals(1, resultStruct.typeParameters().size());
    assertEquals(typeParam, resultStruct.typeParameters().get(0));
  }
}
