package io.substrait.isthmus.expression;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for variadic parameter consistency validation in FunctionConverter. */
class VariadicParameterConsistencyTest extends PlanTestBase {

  private static final TypeCreator R = TypeCreator.of(false);
  private static final TypeCreator N = TypeCreator.of(true);

  /**
   * Helper method to test if input types match defined arguments with variadic behavior. Creates a
   * minimal FunctionConverter and FunctionFinder to access the package-private method.
   */
  private boolean testInputTypesMatch(
      List<Type> inputTypes,
      List<SimpleExtension.Argument> args,
      SimpleExtension.VariadicBehavior variadic) {
    // Create a minimal test function to use with FunctionConverter
    SimpleExtension.ScalarFunctionVariant testFunction =
        ImmutableSimpleExtension.ScalarFunctionVariant.builder()
            .urn("extension:test:variadic")
            .name("test_func")
            .args(args)
            .variadic(
                variadic != null ? java.util.Optional.of(variadic) : java.util.Optional.empty())
            .returnType(R.I64)
            .options(java.util.Collections.emptyMap())
            .build();

    // Create a ScalarFunctionConverter with our test function
    ScalarFunctionConverter converter =
        new ScalarFunctionConverter(
            List.of(testFunction),
            java.util.Collections.emptyList(),
            typeFactory,
            TypeConverter.DEFAULT);

    // Create a test helper that can access the package-private method
    TestHelper testHelper = new TestHelper(converter, testFunction);
    return testHelper.testInputTypesMatch(inputTypes, args, variadic);
  }

  /**
   * Test helper class that extends ScalarFunctionConverter to access package-private
   * inputTypesMatchDefinedArguments method.
   */
  private static class TestHelper extends ScalarFunctionConverter {
    private final SimpleExtension.ScalarFunctionVariant testFunction;

    TestHelper(ScalarFunctionConverter parent, SimpleExtension.ScalarFunctionVariant testFunction) {
      super(
          java.util.Collections.emptyList(),
          java.util.Collections.emptyList(),
          parent.typeFactory,
          parent.typeConverter);
      this.testFunction = testFunction;
    }

    boolean testInputTypesMatch(
        List<Type> inputTypes,
        List<SimpleExtension.Argument> args,
        SimpleExtension.VariadicBehavior variadic) {
      // Create a minimal FunctionFinder to access the package-private method
      // We need a SqlOperator, so create a dummy one
      org.apache.calcite.sql.SqlFunction dummyOperator =
          new org.apache.calcite.sql.SqlFunction(
              "test",
              org.apache.calcite.sql.SqlKind.OTHER_FUNCTION,
              org.apache.calcite.sql.type.ReturnTypes.explicit(
                  org.apache.calcite.sql.type.SqlTypeName.BIGINT),
              null,
              null,
              org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION);

      // Create a FunctionFinder with the test function so it can compute argRange properly
      // FunctionFinder is a protected inner class, so we can access it from a subclass
      FunctionFinder finder = new FunctionFinder("test_func", dummyOperator, List.of(testFunction));

      // Access the package-private method
      return finder.inputTypesMatchDefinedArguments(inputTypes, args, variadic);
    }
  }

  @Test
  void testConsistentVariadicWithSameTypes() {
    // Function: test_func(i64, i64...) with CONSISTENT parameterConsistency
    List<SimpleExtension.Argument> args =
        List.of(
            SimpleExtension.ValueArgument.builder()
                .value(R.I64)
                .name("arg1")
                .build());

    SimpleExtension.VariadicBehavior variadic =
        ImmutableSimpleExtension.VariadicBehavior.builder()
            .min(1)
            .parameterConsistency(SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT)
            .build();

    // All variadic arguments are i64 - should pass
    assertTrue(
        testInputTypesMatch(
            List.of(R.I64, R.I64, R.I64), args, variadic),
        "Consistent variadic with same types should match");

    // All variadic arguments are i64 (with different nullability) - should pass
    assertTrue(
        testInputTypesMatch(
            List.of(R.I64, R.I64, N.I64, R.I64), args, variadic),
        "Consistent variadic with same types but different nullability should match");

  }

  @Test
  void testConsistentVariadicWithDifferentTypes() {
    // Function: test_func(i64, any...) with CONSISTENT parameterConsistency
    List<SimpleExtension.Argument> args =
        List.of(
            SimpleExtension.ValueArgument.builder()
                .value(R.I64)
                .name("arg1")
                .build());

    SimpleExtension.VariadicBehavior variadic =
        ImmutableSimpleExtension.VariadicBehavior.builder()
            .min(1)
            .parameterConsistency(SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT)
            .build();

    // Variadic arguments have different types - should fail
    assertFalse(
        testInputTypesMatch(
            List.of(R.I64, R.I64, R.FP64), args, variadic),
        "Consistent variadic with different types should not match");

    assertFalse(
        testInputTypesMatch(
            List.of(R.I64, R.I32, R.I64), args, variadic),
        "Consistent variadic with different types should not match");
  }

  @Test
  void testInconsistentVariadicWithDifferentTypes() {
    // Function: test_func(i64, any...) with INCONSISTENT parameterConsistency
    // When INCONSISTENT, each variadic argument can be a different type, but they all need to
    // match the parameterized type constraint (any in this case)
    ParameterizedType anyType =
        ParameterizedType.StringLiteral.builder().value("any").nullable(false).build();

    List<SimpleExtension.Argument> args =
        List.of(
            SimpleExtension.ValueArgument.builder()
                .value(R.I64)
                .name("arg1")
                .build(),
            SimpleExtension.ValueArgument.builder()
                .value(anyType)
                .name("variadic_arg")
                .build());

    SimpleExtension.VariadicBehavior variadic =
        ImmutableSimpleExtension.VariadicBehavior.builder()
            .min(1)
            .parameterConsistency(
                SimpleExtension.VariadicBehavior.ParameterConsistency.INCONSISTENT)
            .build();

    // Variadic arguments have different types - should pass with INCONSISTENT and wildcard type
    assertTrue(
        testInputTypesMatch(
            List.of(R.I64, R.I64, R.FP64), args, variadic),
        "Inconsistent variadic with different types should match when using wildcard type");

    assertTrue(
        testInputTypesMatch(
            List.of(R.I64, R.I32, R.FP64, R.STRING), args, variadic),
        "Inconsistent variadic with different types should match when using wildcard type");
  }

  @Test
  void testConsistentVariadicWithWildcardType() {
    // Function: test_func(any, any...) with CONSISTENT parameterConsistency
    // The variadic arguments should all have the same concrete type
    ParameterizedType anyType =
        ParameterizedType.StringLiteral.builder().value("any").nullable(false).build();

    List<SimpleExtension.Argument> args =
        List.of(
            SimpleExtension.ValueArgument.builder()
                .value(anyType)
                .name("arg1")
                .build());

    SimpleExtension.VariadicBehavior variadic =
        ImmutableSimpleExtension.VariadicBehavior.builder()
            .min(1)
            .parameterConsistency(SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT)
            .build();

    // All variadic arguments are i64 - should pass
    assertTrue(
        testInputTypesMatch(
            List.of(R.I64, R.I64, R.I64), args, variadic),
        "Consistent variadic with wildcard type and same concrete types should match");

    // Variadic arguments have different types - should fail
    assertFalse(
        testInputTypesMatch(
            List.of(R.I64, R.I64, R.FP64), args, variadic),
        "Consistent variadic with wildcard type and different concrete types should not match");
  }

  @Test
  void testConsistentVariadicWithMinGreaterThanOne() {
    // Function: test_func(i64, i64...) with CONSISTENT and min=2
    List<SimpleExtension.Argument> args =
        List.of(
            SimpleExtension.ValueArgument.builder()
                .value(R.I64)
                .name("arg1")
                .build());

    SimpleExtension.VariadicBehavior variadic =
        ImmutableSimpleExtension.VariadicBehavior.builder()
            .min(2)
            .parameterConsistency(SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT)
            .build();

    // All variadic arguments are i64 - should pass
    assertTrue(
        testInputTypesMatch(
            List.of(R.I64, R.I64, R.I64, R.I64), args, variadic),
        "Consistent variadic with min=2 and same types should match");

    // Variadic arguments have different types - should fail
    assertFalse(
        testInputTypesMatch(
            List.of(R.I64, R.I64, R.I64, R.FP64), args, variadic),
        "Consistent variadic with min=2 and different types should not match");
  }

  @Test
  void testNoVariadicBehavior() {
    // Function: test_func(i64) with no variadic behavior
    List<SimpleExtension.Argument> args =
        List.of(
            SimpleExtension.ValueArgument.builder()
                .value(R.I64)
                .name("arg1")
                .build());

    // No variadic behavior - should pass regardless of consistency
    assertTrue(
        testInputTypesMatch(List.of(R.I64), args, null),
        "No variadic behavior should always match");
  }

  @Test
  void testConsistentVariadicWithNullableTypes() {
    // Function: test_func(i64, i64...) with CONSISTENT parameterConsistency
    List<SimpleExtension.Argument> args =
        List.of(
            SimpleExtension.ValueArgument.builder()
                .value(R.I64)
                .name("arg1")
                .build());

    SimpleExtension.VariadicBehavior variadic =
        ImmutableSimpleExtension.VariadicBehavior.builder()
            .min(1)
            .parameterConsistency(SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT)
            .build();

    // Mix of nullable and non-nullable i64 - should pass (nullability is ignored)
    assertTrue(
        testInputTypesMatch(
            List.of(R.I64, N.I64, R.I64, N.I64), args, variadic),
        "Consistent variadic with same types but different nullability should match");
  }
}

