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

class LiteralRoundtripTest extends TestBase {

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
          + "      z: T\n"
          + "  - name: multi_param\n"
          + "    parameters:\n"
          + "      - name: T\n"
          + "        type: dataType\n"
          + "      - name: size\n"
          + "        type: integer\n"
          + "      - name: nullable\n"
          + "        type: boolean\n"
          + "      - name: encoding\n"
          + "        type: string\n"
          + "      - name: precision\n"
          + "        type: dataType\n"
          + "      - name: mode\n"
          + "        type: enum\n"
          + "    structure:\n"
          + "      value: T\n";

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

  private void verifyNestedTypesRoundTrip(Expression expression) {
    io.substrait.proto.Expression protoExpression =
        NESTED_TYPES_EXPRESSION_TO_PROTO.toProto(expression);
    Expression result = NESTED_TYPES_PROTO_TO_EXPRESSION.from(protoExpression);
    assertEquals(expression, result);
  }

  @Test
  void decimal() {
    io.substrait.expression.Expression.DecimalLiteral val =
        ExpressionCreator.decimal(false, BigDecimal.TEN, 10, 2);
    verifyRoundTrip(val);
  }

  /** Verifies round-trip conversion of a simple user-defined type using Any representation. */
  @Test
  void userDefinedLiteralWithAnyRepresentation() {
    Any anyValue =
        Any.pack(com.google.protobuf.StringValue.of("<Some User-Defined Representation>"));

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
    Expression.UserDefinedStructLiteral p1 =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "point",
            Collections.emptyList(),
            java.util.Arrays.asList(
                ExpressionCreator.i32(false, 0), ExpressionCreator.i32(false, 0)));

    Expression.UserDefinedStructLiteral p2 =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "point",
            Collections.emptyList(),
            java.util.Arrays.asList(
                ExpressionCreator.i32(false, 10), ExpressionCreator.i32(false, 0)));

    Expression.UserDefinedStructLiteral p3 =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "point",
            Collections.emptyList(),
            java.util.Arrays.asList(
                ExpressionCreator.i32(false, 5), ExpressionCreator.i32(false, 10)));

    Expression.UserDefinedStructLiteral triangle =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "triangle",
            Collections.emptyList(),
            java.util.Arrays.asList(p1, p2, p3));

    verifyNestedTypesRoundTrip(triangle);
  }

  /**
   * Verifies round-trip conversion of nested user-defined types where a triangle UDT contains three
   * point UDTs. Both outer and nested types use Any representation.
   */
  @Test
  void nestedUserDefinedLiteralWithAnyRepresentation() {
    Any triangleAny =
        Any.pack(com.google.protobuf.StringValue.of("<Some User-Defined Representation>"));

    Expression.UserDefinedAnyLiteral triangle =
        ExpressionCreator.userDefinedLiteralAny(
            false, NESTED_TYPES_URN, "triangle", Collections.emptyList(), triangleAny);

    verifyNestedTypesRoundTrip(triangle);
  }

  /**
   * Verifies round-trip conversion of nested user-defined types with mixed representations. The
   * triangle UDT uses Struct representation while the nested point UDTs use Any representation.
   */
  @Test
  void mixedRepresentationNestedUserDefinedLiteral() {
    Any anyValue =
        Any.pack(com.google.protobuf.StringValue.of("<Some User-Defined Representation>"));

    // Create point UDTs using Any representation
    Expression.UserDefinedAnyLiteral p1 =
        ExpressionCreator.userDefinedLiteralAny(
            false, NESTED_TYPES_URN, "point", Collections.emptyList(), anyValue);

    Expression.UserDefinedAnyLiteral p2 =
        ExpressionCreator.userDefinedLiteralAny(
            false, NESTED_TYPES_URN, "point", Collections.emptyList(), anyValue);

    Expression.UserDefinedAnyLiteral p3 =
        ExpressionCreator.userDefinedLiteralAny(
            false, NESTED_TYPES_URN, "point", Collections.emptyList(), anyValue);

    // Create a "triangle" UDT using Struct representation, but with Any-encoded point fields
    Expression.UserDefinedStructLiteral triangle =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "triangle",
            Collections.emptyList(),
            java.util.Arrays.asList(p1, p2, p3));

    verifyNestedTypesRoundTrip(triangle);
  }

  /**
   * Verifies round-trip conversion of a parameterized user-defined type. Tests that type parameters
   * are correctly preserved during serialization and deserialization.
   */
  @Test
  void userDefinedLiteralWithTypeParameters() {
    // Create a type parameter for i32
    io.substrait.type.Type.Parameter typeParam =
        io.substrait.type.ImmutableType.ParameterDataType.builder()
            .type(io.substrait.type.Type.I32.builder().nullable(false).build())
            .build();

    // Create a vector<i32> instance with fields (x: 1, y: 2, z: 3)
    Expression.UserDefinedStructLiteral vectorI32 =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "vector",
            java.util.Arrays.asList(typeParam),
            java.util.Arrays.asList(
                ExpressionCreator.i32(false, 1),
                ExpressionCreator.i32(false, 2),
                ExpressionCreator.i32(false, 3)));

    verifyNestedTypesRoundTrip(vectorI32);
  }

  /**
   * Verifies round-trip conversion of a user-defined type with all parameter types. Tests that all
   * parameter kinds (type, integer, boolean, string, null, enum) are correctly preserved during
   * serialization and deserialization.
   */
  @Test
  void userDefinedLiteralWithAllParameterTypes() {
    io.substrait.type.Type.Parameter typeParam =
        io.substrait.type.ImmutableType.ParameterDataType.builder()
            .type(io.substrait.type.Type.I32.builder().nullable(false).build())
            .build();

    io.substrait.type.Type.Parameter intParam =
        io.substrait.type.ImmutableType.ParameterIntegerValue.builder().value(100L).build();

    io.substrait.type.Type.Parameter boolParam =
        io.substrait.type.ImmutableType.ParameterBooleanValue.builder().value(true).build();

    io.substrait.type.Type.Parameter stringParam =
        io.substrait.type.ImmutableType.ParameterStringValue.builder().value("utf8").build();

    io.substrait.type.Type.Parameter nullParam = io.substrait.type.Type.ParameterNull.INSTANCE;

    io.substrait.type.Type.Parameter enumParam =
        io.substrait.type.ImmutableType.ParameterEnumValue.builder().value("FAST").build();

    Expression.UserDefinedStructLiteral multiParam =
        ExpressionCreator.userDefinedLiteralStruct(
            false,
            NESTED_TYPES_URN,
            "multi_param",
            java.util.Arrays.asList(
                typeParam, intParam, boolParam, stringParam, nullParam, enumParam),
            java.util.Arrays.asList(ExpressionCreator.i32(false, 42)));

    verifyNestedTypesRoundTrip(multiParam);
  }
}
