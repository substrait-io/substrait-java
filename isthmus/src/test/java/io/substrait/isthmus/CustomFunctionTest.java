package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.isthmus.expression.WindowRelFunctionConverter;
import io.substrait.isthmus.utils.UserTypeFactory;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

/** Verify that custom functions can convert from Substrait to Calcite and back. */
public class CustomFunctionTest extends PlanTestBase {
  static final TypeCreator R = TypeCreator.of(false);
  static final TypeCreator N = TypeCreator.of(true);

  // Define custom functions in a "functions_custom.yaml" extension
  static final String NAMESPACE = "/functions_custom";
  static final String FUNCTIONS_CUSTOM;

  static {
    try {
      FUNCTIONS_CUSTOM = asString("extensions/functions_custom.yaml");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Load custom extension into an ExtensionCollection
  static final SimpleExtension.ExtensionCollection extensionCollection =
      SimpleExtension.load("/functions_custom", FUNCTIONS_CUSTOM);

  final SubstraitBuilder b = new SubstraitBuilder(extensionCollection);

  // Create user-defined types
  static final String aTypeName = "a_type";
  static final String bTypeName = "b_type";
  static final UserTypeFactory aTypeFactory = new UserTypeFactory(NAMESPACE, aTypeName);
  static final UserTypeFactory bTypeFactory = new UserTypeFactory(NAMESPACE, bTypeName);

  // Mapper for user-defined types
  static final UserTypeMapper userTypeMapper =
      new UserTypeMapper() {
        @Nullable
        @Override
        public Type toSubstrait(RelDataType relDataType) {
          if (aTypeFactory.isTypeFromFactory(relDataType)) {
            return TypeCreator.of(relDataType.isNullable()).userDefined(NAMESPACE, aTypeName);
          }
          if (bTypeFactory.isTypeFromFactory(relDataType)) {
            return TypeCreator.of(relDataType.isNullable()).userDefined(NAMESPACE, bTypeName);
          }
          return null;
        }

        @Nullable
        @Override
        public RelDataType toCalcite(Type.UserDefined type) {
          if (type.uri().equals(NAMESPACE)) {
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

  // Define additional mapping signatures for the custom scalar functions
  final List<FunctionMappings.Sig> additionalScalarSignatures =
      List.of(FunctionMappings.s(customScalarFn), FunctionMappings.s(toBType));

  static final SqlFunction customScalarFn =
      new SqlFunction(
          "custom_scalar",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARCHAR),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  static final SqlFunction toBType =
      new SqlFunction(
          "to_b_type",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(bTypeFactory.createCalcite(false)),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  // Define additional mapping signatures for the custom aggregate functions
  final List<FunctionMappings.Sig> additionalAggregateSignatures =
      List.of(FunctionMappings.s(customAggregateFn));

  static final SqlAggFunction customAggregateFn =
      new SqlAggFunction(
          "custom_aggregate",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_FUNCTION) {};

  TypeConverter typeConverter = new TypeConverter(userTypeMapper);

  // Create Function Converters that can handle the custom functions
  ScalarFunctionConverter scalarFunctionConverter =
      new ScalarFunctionConverter(
          extensionCollection.scalarFunctions(),
          additionalScalarSignatures,
          typeFactory,
          typeConverter);
  AggregateFunctionConverter aggregateFunctionConverter =
      new AggregateFunctionConverter(
          extensionCollection.aggregateFunctions(),
          additionalAggregateSignatures,
          typeFactory,
          typeConverter);
  WindowFunctionConverter windowFunctionConverter =
      new WindowFunctionConverter(extensionCollection.windowFunctions(), typeFactory);

  WindowRelFunctionConverter windowRelFunctionConverter =
      new WindowRelFunctionConverter(extensionCollection.windowFunctions(), typeFactory);

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
          windowRelFunctionConverter,
          typeConverter);
    }
  }

  final SubstraitToCalcite substraitToCalcite =
      new CustomSubstraitToCalcite(extensionCollection, typeFactory, typeConverter);

  // Create a SubstraitRelVisitor that uses the custom Function Converters
  final SubstraitRelVisitor calciteToSubstrait =
      new SubstraitRelVisitor(
          typeFactory,
          scalarFunctionConverter,
          aggregateFunctionConverter,
          windowFunctionConverter,
          windowRelFunctionConverter,
          typeConverter,
          ImmutableFeatureBoard.builder().build());

  @Test
  void customScalarFunctionRoundtrip() {
    // CREATE TABLE example(a TEXT)
    // SELECT custom_scalar(a) FROM example
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        NAMESPACE, "custom_scalar:str", R.STRING, b.fieldReference(input, 0))),
            b.remap(1),
            b.namedScan(List.of("example"), List.of("a"), List.of(R.STRING)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    var relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customAggregateFunctionRoundtrip() {
    // CREATE TABLE example (a BIGINT)
    // SELECT custom_aggregate(a) FROM example GROUP BY a
    Rel rel =
        b.aggregate(
            input -> b.grouping(input, 0),
            input ->
                List.of(
                    b.measure(
                        b.aggregateFn(
                            NAMESPACE, "custom_aggregate:i64", R.I64, b.fieldReference(input, 0)))),
            b.namedScan(List.of("example"), List.of("a"), List.of(R.I64)));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    var relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }

  @Test
  void customTypesInFunctionsRoundtrip() {
    // CREATE TABLE example(a a_type)
    // SELECT to_b_type(a) FROM example
    Rel rel =
        b.project(
            input ->
                List.of(
                    b.scalarFn(
                        NAMESPACE,
                        "to_b_type:u!a_type",
                        R.userDefined(NAMESPACE, "b_type"),
                        b.fieldReference(input, 0))),
            b.remap(1),
            b.namedScan(
                List.of("example"), List.of("a"), List.of(N.userDefined(NAMESPACE, "a_type"))));

    RelNode calciteRel = substraitToCalcite.convert(rel);
    var relReturned = calciteToSubstrait.apply(calciteRel);
    assertEquals(rel, relReturned);
  }
}
