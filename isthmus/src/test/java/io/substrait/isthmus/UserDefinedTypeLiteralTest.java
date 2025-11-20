package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.google.protobuf.Any;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression.UserDefinedLiteral;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
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
 * <p>These tests verify proto serialization/deserialization and Calcite roundtrips of UDT literals,
 * using standard types from extension_types.yaml (point and line).
 */
public class UserDefinedTypeLiteralTest extends PlanTestBase {

  final SubstraitBuilder b = new SubstraitBuilder(DefaultExtensionCatalog.DEFAULT_COLLECTION);

  // Create user-defined types using standard types from extension_types.yaml
  static final UserTypeFactory pointTypeFactory =
      new UserTypeFactory(DefaultExtensionCatalog.EXTENSION_TYPES, "point");
  static final UserTypeFactory lineTypeFactory =
      new UserTypeFactory(DefaultExtensionCatalog.EXTENSION_TYPES, "line");

  // Mapper for user-defined types
  static final UserTypeMapper userTypeMapper =
      new UserTypeMapper() {
        @Nullable
        @Override
        public Type toSubstrait(RelDataType relDataType) {
          if (pointTypeFactory.isTypeFromFactory(relDataType)) {
            return TypeCreator.of(relDataType.isNullable())
                .userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "point");
          }
          if (lineTypeFactory.isTypeFromFactory(relDataType)) {
            return TypeCreator.of(relDataType.isNullable())
                .userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "line");
          }
          return null;
        }

        @Nullable
        @Override
        public RelDataType toCalcite(Type.UserDefined type) {
          if (type.urn().equals(DefaultExtensionCatalog.EXTENSION_TYPES)) {
            if (type.name().equals("point")) {
              return pointTypeFactory.createCalcite(type.nullable());
            }
            if (type.name().equals("line")) {
              return lineTypeFactory.createCalcite(type.nullable());
            }
          }
          return null;
        }
      };

  TypeConverter typeConverter = new TypeConverter(userTypeMapper);

  // Create Function Converters that can handle the user-defined types
  ScalarFunctionConverter scalarFunctionConverter =
      new ScalarFunctionConverter(
          DefaultExtensionCatalog.DEFAULT_COLLECTION.scalarFunctions(),
          List.of(),
          typeFactory,
          typeConverter);
  AggregateFunctionConverter aggregateFunctionConverter =
      new AggregateFunctionConverter(
          DefaultExtensionCatalog.DEFAULT_COLLECTION.aggregateFunctions(),
          List.of(),
          typeFactory,
          typeConverter);
  WindowFunctionConverter windowFunctionConverter =
      new WindowFunctionConverter(
          DefaultExtensionCatalog.DEFAULT_COLLECTION.windowFunctions(), typeFactory);

  final SubstraitToCalcite substraitToCalcite =
      new CustomSubstraitToCalcite(
          DefaultExtensionCatalog.DEFAULT_COLLECTION, typeFactory, typeConverter);

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
   * testing proto serialization (core's responsibility) but must reside in isthmus to access
   * Calcite integration components.
   */
  private void verifyProtoRoundTrip(Rel rel) {
    ExtensionCollector functionCollector = new ExtensionCollector();
    RelProtoConverter relProtoConverter = new RelProtoConverter(functionCollector);
    ProtoRelConverter protoRelConverter =
        new ProtoRelConverter(functionCollector, DefaultExtensionCatalog.DEFAULT_COLLECTION);

    io.substrait.proto.Rel protoRel = relProtoConverter.toProto(rel);
    Rel relReturned = protoRelConverter.from(protoRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void multipleDifferentUserDefinedAnyTypesProtoRoundtrip() {
    // Test that UserDefinedAny literals with different type names - proto only
    // point wraps struct with two i32 fields, line wraps struct with two point fields
    Expression.Literal.Builder bldr1 = Expression.Literal.newBuilder();
    Expression.Literal.Struct pointStruct =
        Expression.Literal.Struct.newBuilder()
            .addFields(Expression.Literal.newBuilder().setI32(42))
            .addFields(Expression.Literal.newBuilder().setI32(100))
            .build();
    Any anyValue1 = Any.pack(bldr1.setStruct(pointStruct).build());
    UserDefinedLiteral pointLit =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "point",
            java.util.Collections.emptyList(),
            anyValue1);

    Expression.Literal.Builder bldr2 = Expression.Literal.newBuilder();
    Expression.Literal.Struct lineStruct =
        Expression.Literal.Struct.newBuilder()
            .addFields(bldr1.build()) // reuse point struct as start
            .addFields(bldr1.build()) // reuse point struct as end
            .build();
    Any anyValue2 = Any.pack(bldr2.setStruct(lineStruct).build());
    UserDefinedLiteral lineLit =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "line",
            java.util.Collections.emptyList(),
            anyValue2);

    Rel originalRel =
        b.project(
            input -> List.of(pointLit, lineLit),
            b.remap(1, 2), // Select both expressions
            b.namedScan(
                List.of("example"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "point"))));

    verifyProtoRoundTrip(originalRel);
  }

  @Test
  void singleUserDefinedAnyCalciteRoundtrip() {
    // Test that a single UserDefinedAny literal can roundtrip through Calcite
    Expression.Literal.Builder bldr1 = Expression.Literal.newBuilder();
    Expression.Literal.Struct pointStruct =
        Expression.Literal.Struct.newBuilder()
            .addFields(Expression.Literal.newBuilder().setI32(42))
            .addFields(Expression.Literal.newBuilder().setI32(100))
            .build();
    Any anyValue1 = Any.pack(bldr1.setStruct(pointStruct).build());
    UserDefinedLiteral pointLit =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "point",
            java.util.Collections.emptyList(),
            anyValue1);

    Rel originalRel =
        b.project(
            input -> List.of(pointLit),
            b.remap(1),
            b.namedScan(
                List.of("example"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "point"))));

    assertCalciteRoundtrip(
        originalRel,
        substraitToCalcite,
        calciteToSubstrait,
        DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  @Test
  void singleUserDefinedStructCalciteRoundtrip() {
    // Test that a single UserDefinedStruct literal can roundtrip through Calcite
    io.substrait.expression.Expression.UserDefinedStruct val =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 42))
            .addFields(ExpressionCreator.i32(false, 100))
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(val),
            b.remap(1),
            b.namedScan(
                List.of("example"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "point"))));

    assertCalciteRoundtrip(
        originalRel,
        substraitToCalcite,
        calciteToSubstrait,
        DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  @Test
  void nestedUserDefinedStructCalciteRoundtrip() {
    io.substrait.expression.Expression.UserDefinedStruct startPoint =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 5))
            .addFields(ExpressionCreator.i32(false, 15))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct endPoint =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 25))
            .addFields(ExpressionCreator.i32(false, 35))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct lineStructLit =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("line")
            .addFields(startPoint)
            .addFields(endPoint)
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(lineStructLit),
            b.remap(1),
            b.namedScan(
                List.of("example"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "line"))));

    assertCalciteRoundtrip(
        originalRel,
        substraitToCalcite,
        calciteToSubstrait,
        DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  void multipleDifferentUserDefinedAnyTypesCalciteRoundtrip() {
    // Test that multiple UserDefinedAny literals with different types can roundtrip through Calcite
    Expression.Literal.Builder bldr1 = Expression.Literal.newBuilder();
    Expression.Literal.Struct pointStruct =
        Expression.Literal.Struct.newBuilder()
            .addFields(Expression.Literal.newBuilder().setI32(42))
            .addFields(Expression.Literal.newBuilder().setI32(100))
            .build();
    Any anyValue1 = Any.pack(bldr1.setStruct(pointStruct).build());
    UserDefinedLiteral pointLit =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "point",
            java.util.Collections.emptyList(),
            anyValue1);

    Expression.Literal.Builder bldr2 = Expression.Literal.newBuilder();
    Expression.Literal.Struct lineStruct =
        Expression.Literal.Struct.newBuilder()
            .addFields(bldr1.build())
            .addFields(bldr1.build())
            .build();
    Any anyValue2 = Any.pack(bldr2.setStruct(lineStruct).build());
    UserDefinedLiteral lineLit =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "line",
            java.util.Collections.emptyList(),
            anyValue2);

    Rel originalRel =
        b.project(
            input -> List.of(pointLit, lineLit),
            b.remap(1, 2), // Select both expressions
            b.namedScan(
                List.of("example"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "point"))));

    assertCalciteRoundtrip(
        originalRel,
        substraitToCalcite,
        calciteToSubstrait,
        DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  @Test
  void userDefinedStructWithPrimitivesProtoRoundtrip() {
    // Test UserDefinedStruct with primitive field types - proto roundtrip only
    io.substrait.expression.Expression.UserDefinedStruct val =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 42))
            .addFields(ExpressionCreator.i32(false, 100))
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(val),
            b.remap(1),
            b.namedScan(
                List.of("example"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "point"))));

    verifyProtoRoundTrip(originalRel);
  }

  @Test
  void userDefinedStructWithNestedStructProtoRoundtrip() {
    // Test UserDefinedStruct with nested UDT fields - proto roundtrip only
    // line contains nested point UDT fields
    io.substrait.expression.Expression.UserDefinedStruct startPoint =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 10))
            .addFields(ExpressionCreator.i32(false, 20))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct endPoint =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 30))
            .addFields(ExpressionCreator.i32(false, 40))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct line =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("line")
            .addFields(startPoint)
            .addFields(endPoint)
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(line),
            b.remap(1),
            b.namedScan(
                List.of("example"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "point"))));

    verifyProtoRoundTrip(originalRel);
  }

  @Test
  void userDefinedStructWithNestedAnyCalciteRoundtrip() {
    // Mix struct-encoded and Any-encoded fields inside a single UserDefinedStruct literal
    Expression.Literal.Builder pointBuilder = Expression.Literal.newBuilder();
    Expression.Literal.Struct pointStructProto =
        Expression.Literal.Struct.newBuilder()
            .addFields(Expression.Literal.newBuilder().setI32(5))
            .addFields(Expression.Literal.newBuilder().setI32(10))
            .build();
    Any pointAny = Any.pack(pointBuilder.setStruct(pointStructProto).build());
    UserDefinedLiteral pointAnyLiteral =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "point",
            java.util.Collections.emptyList(),
            pointAny);

    io.substrait.expression.Expression.UserDefinedStruct pointStructLiteral =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 20))
            .addFields(ExpressionCreator.i32(false, 30))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct line =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("line")
            .addFields(pointAnyLiteral)
            .addFields(pointStructLiteral)
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(line),
            b.remap(1),
            b.namedScan(
                List.of("example"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "point"))));

    assertCalciteRoundtrip(
        originalRel,
        substraitToCalcite,
        calciteToSubstrait,
        DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }

  @Test
  void multipleUserDefinedStructDifferentStructuresProtoRoundtrip() {
    // Test multiple UserDefinedStruct types with different struct schemas
    // point: {latitude: i32, longitude: i32}
    // line: {start: point, end: point}
    io.substrait.expression.Expression.UserDefinedStruct pointStruct =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 42))
            .addFields(ExpressionCreator.i32(false, 100))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct startPoint =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 10))
            .addFields(ExpressionCreator.i32(false, 20))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct endPoint =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 30))
            .addFields(ExpressionCreator.i32(false, 40))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct lineStruct =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("line")
            .addFields(startPoint)
            .addFields(endPoint)
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(pointStruct, lineStruct),
            b.remap(2),
            b.namedScan(
                List.of("example"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "point"))));

    verifyProtoRoundTrip(originalRel);
  }

  @Test
  void sameUdTypeDifferentEncodingsCalciteRoundtrip() {
    // Validate that "line" UDT survives Calcite roundtrip in both Any and Struct encodings
    Expression.Literal.Builder lineBuilder = Expression.Literal.newBuilder();
    Expression.Literal.Builder pointBuilder = Expression.Literal.newBuilder();
    Expression.Literal.Struct pointStructProto =
        Expression.Literal.Struct.newBuilder()
            .addFields(pointBuilder.clear().setI32(5).build())
            .addFields(pointBuilder.clear().setI32(15).build())
            .build();
    Expression.Literal.Struct lineStructProto =
        Expression.Literal.Struct.newBuilder()
            .addFields(Expression.Literal.newBuilder().setStruct(pointStructProto).build())
            .addFields(Expression.Literal.newBuilder().setStruct(pointStructProto).build())
            .build();
    Any lineAnyValue = Any.pack(lineBuilder.setStruct(lineStructProto).build());
    UserDefinedLiteral lineAny =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "line",
            java.util.Collections.emptyList(),
            lineAnyValue);

    io.substrait.expression.Expression.UserDefinedStruct startPoint =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 1))
            .addFields(ExpressionCreator.i32(false, 2))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct endPoint =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 3))
            .addFields(ExpressionCreator.i32(false, 4))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct lineStruct =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("line")
            .addFields(startPoint)
            .addFields(endPoint)
            .build();

    Rel relWithAny =
        b.project(
            input -> List.of(lineAny),
            b.remap(1),
            b.namedScan(
                List.of("example_any"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "line"))));
    Rel relWithStruct =
        b.project(
            input -> List.of(lineStruct),
            b.remap(1),
            b.namedScan(
                List.of("example_struct"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "line"))));

    Rel roundtrippedAny = calciteToSubstrait.apply(substraitToCalcite.convert(relWithAny));
    assertInstanceOf(io.substrait.relation.Project.class, roundtrippedAny);
    io.substrait.relation.Project anyProject = (io.substrait.relation.Project) roundtrippedAny;
    assertEquals(1, anyProject.getExpressions().size());
    assertInstanceOf(
        io.substrait.expression.Expression.UserDefinedAny.class,
        anyProject.getExpressions().get(0));

    Rel roundtrippedStruct = calciteToSubstrait.apply(substraitToCalcite.convert(relWithStruct));
    assertInstanceOf(io.substrait.relation.Project.class, roundtrippedStruct);
    io.substrait.relation.Project structProject =
        (io.substrait.relation.Project) roundtrippedStruct;
    assertEquals(1, structProject.getExpressions().size());
    assertInstanceOf(
        io.substrait.expression.Expression.UserDefinedStruct.class,
        structProject.getExpressions().get(0));
  }

  @Test
  void intermixedUserDefinedAnyAndStructProtoRoundtrip() {
    // Test intermixing UserDefinedAny and UserDefinedStruct in the same query
    Expression.Literal.Builder bldr1 = Expression.Literal.newBuilder();
    Expression.Literal.Struct pointStruct1 =
        Expression.Literal.Struct.newBuilder()
            .addFields(Expression.Literal.newBuilder().setI32(10))
            .addFields(Expression.Literal.newBuilder().setI32(20))
            .build();
    Any anyValue1 = Any.pack(bldr1.setStruct(pointStruct1).build());
    UserDefinedLiteral anyLit1 =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "point",
            java.util.Collections.emptyList(),
            anyValue1);

    io.substrait.expression.Expression.UserDefinedStruct structLit1 =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 123))
            .addFields(ExpressionCreator.i32(false, 456))
            .build();

    Expression.Literal.Builder bldr2 = Expression.Literal.newBuilder();
    Expression.Literal.Struct pointStruct2 =
        Expression.Literal.Struct.newBuilder()
            .addFields(Expression.Literal.newBuilder().setI32(30))
            .addFields(Expression.Literal.newBuilder().setI32(40))
            .build();
    Any anyValue2 = Any.pack(bldr2.setStruct(pointStruct2).build());
    UserDefinedLiteral anyLit2 =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "point",
            java.util.Collections.emptyList(),
            anyValue2);

    io.substrait.expression.Expression.UserDefinedStruct structLit2 =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 789))
            .addFields(ExpressionCreator.i32(false, 101))
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(anyLit1, structLit1, anyLit2, structLit2),
            b.remap(4),
            b.namedScan(
                List.of("example"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "point"))));

    verifyProtoRoundTrip(originalRel);
  }

  @Test
  void multipleDifferentUDTTypesWithAnyAndStructProtoRoundtrip() {
    // Test multiple different UDT type names (point, line) with both Any and Struct
    Expression.Literal.Builder pointBldr = Expression.Literal.newBuilder();
    Expression.Literal.Struct pointStruct =
        Expression.Literal.Struct.newBuilder()
            .addFields(Expression.Literal.newBuilder().setI32(42))
            .addFields(Expression.Literal.newBuilder().setI32(100))
            .build();
    Any pointAny = Any.pack(pointBldr.setStruct(pointStruct).build());
    UserDefinedLiteral pointAny1 =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "point",
            java.util.Collections.emptyList(),
            pointAny);

    io.substrait.expression.Expression.UserDefinedStruct pointStructLit =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 10))
            .addFields(ExpressionCreator.i32(false, 20))
            .build();

    Expression.Literal.Builder lineBldr = Expression.Literal.newBuilder();
    Expression.Literal.Struct lineStruct =
        Expression.Literal.Struct.newBuilder()
            .addFields(pointBldr.build())
            .addFields(pointBldr.build())
            .build();
    Any lineAny = Any.pack(lineBldr.setStruct(lineStruct).build());
    UserDefinedLiteral lineAny1 =
        ExpressionCreator.userDefinedLiteralAny(
            false,
            DefaultExtensionCatalog.EXTENSION_TYPES,
            "line",
            java.util.Collections.emptyList(),
            lineAny);

    io.substrait.expression.Expression.UserDefinedStruct startPoint =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 50))
            .addFields(ExpressionCreator.i32(false, 60))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct endPoint =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("point")
            .addFields(ExpressionCreator.i32(false, 70))
            .addFields(ExpressionCreator.i32(false, 80))
            .build();

    io.substrait.expression.Expression.UserDefinedStruct lineStructLit =
        io.substrait.expression.Expression.UserDefinedStruct.builder()
            .nullable(false)
            .urn(DefaultExtensionCatalog.EXTENSION_TYPES)
            .name("line")
            .addFields(startPoint)
            .addFields(endPoint)
            .build();

    Rel originalRel =
        b.project(
            input -> List.of(pointAny1, pointStructLit, lineAny1, lineStructLit),
            b.remap(4),
            b.namedScan(
                List.of("example"),
                List.of("a"),
                List.of(N.userDefined(DefaultExtensionCatalog.EXTENSION_TYPES, "point"))));

    verifyProtoRoundTrip(originalRel);
  }
}
