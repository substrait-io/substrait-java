package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.Any;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression.UserDefinedLiteral;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.isthmus.utils.UserTypeFactory;
import io.substrait.proto.Expression;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.RelBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests for User-Defined Type literals, including both UserDefinedAny (protobuf Any-based) and
 * UserDefinedStruct (struct-based) encoding strategies.
 *
 * <p>These tests verify proto serialization/deserialization of UDT literals (core's
 * responsibility), using custom extensions defined in isthmus test resources.
 */
public class UserDefinedTypeLiteralTest extends PlanTestBase {

  // Define custom types in a "functions_custom.yaml" extension
  static final String URN = "extension:substrait:functions_custom";
  static final String FUNCTIONS_CUSTOM;

  static {
    try {
      FUNCTIONS_CUSTOM = asString("extensions/functions_custom.yaml");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // Load custom extension into an ExtensionCollection
  static final SimpleExtension.ExtensionCollection testExtensions =
      SimpleExtension.load("custom.yaml", FUNCTIONS_CUSTOM);

  final SubstraitBuilder b = new SubstraitBuilder(testExtensions);

  // Create user-defined types
  static final String aTypeName = "a_type";
  static final String bTypeName = "b_type";
  static final UserTypeFactory aTypeFactory = new UserTypeFactory(URN, aTypeName);
  static final UserTypeFactory bTypeFactory = new UserTypeFactory(URN, bTypeName);

  // Mapper for user-defined types
  static final UserTypeMapper userTypeMapper =
      new UserTypeMapper() {
        @Nullable
        @Override
        public Type toSubstrait(RelDataType relDataType) {
          if (aTypeFactory.isTypeFromFactory(relDataType)) {
            return TypeCreator.of(relDataType.isNullable()).userDefined(URN, aTypeName);
          }
          if (bTypeFactory.isTypeFromFactory(relDataType)) {
            return TypeCreator.of(relDataType.isNullable()).userDefined(URN, bTypeName);
          }
          return null;
        }

        @Nullable
        @Override
        public RelDataType toCalcite(Type.UserDefined type) {
          if (type.urn().equals(URN)) {
            if (type.name().equals(aTypeName)) {
              return aTypeFactory.createCalcite(type.nullable());
            }
            if (type.name().equals(bTypeName)) {
              return bTypeFactory.createCalcite(type.nullable());
            }
          }
          return null;
        }
      };

  TypeConverter typeConverter = new TypeConverter(userTypeMapper);

  // Create Function Converters that can handle the custom types
  ScalarFunctionConverter scalarFunctionConverter =
      new ScalarFunctionConverter(
          testExtensions.scalarFunctions(), List.of(), typeFactory, typeConverter);
  AggregateFunctionConverter aggregateFunctionConverter =
      new AggregateFunctionConverter(
          testExtensions.aggregateFunctions(), List.of(), typeFactory, typeConverter);
  WindowFunctionConverter windowFunctionConverter =
      new WindowFunctionConverter(testExtensions.windowFunctions(), typeFactory);

  final SubstraitToCalcite substraitToCalcite =
      new CustomSubstraitToCalcite(testExtensions, typeFactory, typeConverter);

  // Create a SubstraitRelVisitor that uses the custom Function Converters
  final SubstraitRelVisitor calciteToSubstrait =
      new SubstraitRelVisitor(
          typeFactory,
          scalarFunctionConverter,
          aggregateFunctionConverter,
          windowFunctionConverter,
          typeConverter,
          ImmutableFeatureBoard.builder().build());

  // Create a SubstraitToCalcite converter that has access to the custom Function Converters
  class CustomSubstraitToCalcite extends SubstraitToCalcite {

    public CustomSubstraitToCalcite(
        SimpleExtension.ExtensionCollection extensions,
        RelDataTypeFactory typeFactory,
        TypeConverter typeConverter) {
      super(extensions, typeFactory, typeConverter);
    }

    @Override
    protected SubstraitRelNodeConverter createSubstraitRelNodeConverter(RelBuilder relBuilder) {
      return new SubstraitRelNodeConverter(
          typeFactory,
          relBuilder,
          scalarFunctionConverter,
          aggregateFunctionConverter,
          windowFunctionConverter,
          typeConverter);
    }
  }

  /**
   * Verifies proto roundtrip for a relation. This test class needs this method locally since it's
   * testing proto serialization (core's responsibility) but must reside in isthmus to access custom
   * test extensions.
   */
  private void verifyProtoRoundTrip(Rel rel) {
    ExtensionCollector functionCollector = new ExtensionCollector();
    RelProtoConverter relProtoConverter = new RelProtoConverter(functionCollector);
    ProtoRelConverter protoRelConverter = new ProtoRelConverter(functionCollector, testExtensions);

    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
    Rel relReturned = protoRelConverter.from(protoRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void multipleDifferentUserDefinedAnyTypesProtoRoundtrip() {
    // Test that UserDefinedAny literals with different payload types have different type names
    // a_type wraps int, b_type wraps string - proto only
    Expression.Literal.Builder bldr1 = Expression.Literal.newBuilder();
    Any anyValue1 = Any.pack(bldr1.setI32(100).build());
    UserDefinedLiteral aTypeLit =
        ExpressionCreator.userDefinedLiteralAny(
            false, URN, "a_type", java.util.Collections.emptyList(), anyValue1);

    Expression.Literal.Builder bldr2 = Expression.Literal.newBuilder();
    Any anyValue2 = Any.pack(bldr2.setString("b_value").build());
    UserDefinedLiteral bTypeLit =
        ExpressionCreator.userDefinedLiteralAny(
            false, URN, "b_type", java.util.Collections.emptyList(), anyValue2);

    Rel originalRel =
        b.project(
            input -> List.of(aTypeLit, bTypeLit),
            b.remap(1, 2), // Select both expressions
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    verifyProtoRoundTrip(originalRel);
  }

  @Test
  void singleUserDefinedAnyCalciteRoundtrip() {
    // Test that a single UserDefinedAny literal can roundtrip through Calcite
    Expression.Literal.Builder bldr1 = Expression.Literal.newBuilder();
    Any anyValue1 = Any.pack(bldr1.setI32(100).build());
    UserDefinedLiteral aTypeLit =
        ExpressionCreator.userDefinedLiteralAny(
            false, URN, "a_type", java.util.Collections.emptyList(), anyValue1);

    Rel originalRel =
        b.project(
            input -> List.of(aTypeLit),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    assertCalciteRoundtrip(originalRel, substraitToCalcite, calciteToSubstrait, testExtensions);
  }

  @Test
  void singleUserDefinedStructCalciteRoundtrip() {
    // Test that a single UserDefinedStruct literal can roundtrip through Calcite
    io.substrait.expression.Expression.UserDefinedStruct val =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.i32(false, 42))
            .addFields(ExpressionCreator.string(false, "hello"))
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(val),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    assertCalciteRoundtrip(originalRel, substraitToCalcite, calciteToSubstrait, testExtensions);
  }

  @Test
  void multipleDifferentUserDefinedAnyTypesCalciteRoundtrip() {
    // Test that multiple UserDefinedAny literals with different types can roundtrip through Calcite
    Expression.Literal.Builder bldr1 = Expression.Literal.newBuilder();
    Any anyValue1 = Any.pack(bldr1.setI32(100).build());
    UserDefinedLiteral aTypeLit =
        ExpressionCreator.userDefinedLiteralAny(
            false, URN, "a_type", java.util.Collections.emptyList(), anyValue1);

    Expression.Literal.Builder bldr2 = Expression.Literal.newBuilder();
    Any anyValue2 = Any.pack(bldr2.setString("b_value").build());
    UserDefinedLiteral bTypeLit =
        ExpressionCreator.userDefinedLiteralAny(
            false, URN, "b_type", java.util.Collections.emptyList(), anyValue2);

    Rel originalRel =
        b.project(
            input -> List.of(aTypeLit, bTypeLit),
            b.remap(1, 2), // Select both expressions
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    assertCalciteRoundtrip(originalRel, substraitToCalcite, calciteToSubstrait, testExtensions);
  }

  @Test
  void userDefinedStructWithPrimitivesProtoRoundtrip() {
    // Test UserDefinedStruct with various primitive field types - proto roundtrip only
    io.substrait.expression.Expression.UserDefinedStruct val =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.i32(false, 42))
            .addFields(ExpressionCreator.string(false, "hello"))
            .addFields(ExpressionCreator.bool(false, true))
            .addFields(ExpressionCreator.fp64(false, 2.718))
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(val),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    verifyProtoRoundTrip(originalRel);
  }

  @Test
  void userDefinedStructWithNestedStructProtoRoundtrip() {
    // Test UserDefinedStruct with nested struct fields - proto roundtrip only
    io.substrait.expression.Expression.StructLiteral innerStruct =
        ExpressionCreator.struct(
            false, ExpressionCreator.i32(false, 10), ExpressionCreator.string(false, "nested"));

    io.substrait.expression.Expression.UserDefinedStruct val =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.i32(false, 100))
            .addFields(innerStruct)
            .addFields(ExpressionCreator.bool(false, false))
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(val),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    verifyProtoRoundTrip(originalRel);
  }

  @Test
  void multipleUserDefinedStructDifferentStructuresProtoRoundtrip() {
    // Test multiple UserDefinedStruct types with different struct schemas
    // a_type: {content: string}
    // b_type: {content_int: i32, content_fp: fp64}
    io.substrait.expression.Expression.UserDefinedStruct aTypeStruct =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.string(false, "hello"))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct bTypeStruct =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("b_type")
            .addFields(ExpressionCreator.i32(false, 42))
            .addFields(ExpressionCreator.fp64(false, 3.14159))
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(aTypeStruct, bTypeStruct),
            b.remap(2),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    verifyProtoRoundTrip(originalRel);
  }

  @Test
  void intermixedUserDefinedAnyAndStructProtoRoundtrip() {
    // Test intermixing UserDefinedAny and UserDefinedStruct in the same query
    Expression.Literal.Builder bldr1 = Expression.Literal.newBuilder();
    Any anyValue1 = Any.pack(bldr1.setI64(999L).build());
    UserDefinedLiteral anyLit1 =
        ExpressionCreator.userDefinedLiteralAny(
            false, URN, "a_type", java.util.Collections.emptyList(), anyValue1);

    io.substrait.expression.Expression.UserDefinedStruct structLit1 =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.i32(false, 123))
            .addFields(ExpressionCreator.bool(false, false))
            .build();

    Expression.Literal.Builder bldr2 = Expression.Literal.newBuilder();
    Any anyValue2 = Any.pack(bldr2.setString("mixed").build());
    UserDefinedLiteral anyLit2 =
        ExpressionCreator.userDefinedLiteralAny(
            false, URN, "a_type", java.util.Collections.emptyList(), anyValue2);

    io.substrait.expression.Expression.UserDefinedStruct structLit2 =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.fp64(false, 1.414))
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(anyLit1, structLit1, anyLit2, structLit2),
            b.remap(4),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    verifyProtoRoundTrip(originalRel);
  }

  @Test
  void multipleDifferentUDTTypesWithAnyAndStructProtoRoundtrip() {
    // Test multiple different UDT type names (a_type, b_type) with both Any and Struct
    Expression.Literal.Builder aTypeBldr = Expression.Literal.newBuilder();
    Any aTypeAny = Any.pack(aTypeBldr.setI32(42).build());
    UserDefinedLiteral aTypeAny1 =
        ExpressionCreator.userDefinedLiteralAny(
            false, URN, "a_type", java.util.Collections.emptyList(), aTypeAny);

    io.substrait.expression.Expression.UserDefinedStruct aTypeStruct =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("a_type")
            .addFields(ExpressionCreator.i32(false, 100))
            .build();

    Expression.Literal.Builder bTypeBldr = Expression.Literal.newBuilder();
    Any bTypeAny = Any.pack(bTypeBldr.setString("b_val").build());
    UserDefinedLiteral bTypeAny1 =
        ExpressionCreator.userDefinedLiteralAny(
            false, URN, "b_type", java.util.Collections.emptyList(), bTypeAny);

    io.substrait.expression.Expression.UserDefinedStruct bTypeStruct =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(URN)
            .name("b_type")
            .addFields(ExpressionCreator.string(false, "struct_b"))
            .addFields(ExpressionCreator.bool(false, true))
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(aTypeAny1, aTypeStruct, bTypeAny1, bTypeStruct),
            b.remap(4),
            b.namedScan(List.of("example"), List.of("a"), List.of(N.userDefined(URN, "a_type"))));

    verifyProtoRoundTrip(originalRel);
  }
}
