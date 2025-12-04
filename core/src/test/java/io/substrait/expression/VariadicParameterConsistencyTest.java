package io.substrait.expression;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for variadic parameter consistency validation in Expression. */
class VariadicParameterConsistencyTest {

  private static final TypeCreator R = TypeCreator.of(false);
  private static final TypeCreator N = TypeCreator.of(true);

  /**
   * Helper method to create a ScalarFunctionInvocation and test if it validates correctly. The
   * validation happens in the @Value.Check method when the Expression is built.
   */
  private Expression.ScalarFunctionInvocation createScalarFunctionInvocation(
      List<SimpleExtension.Argument> args,
      SimpleExtension.VariadicBehavior variadic,
      List<FunctionArg> arguments) {
    SimpleExtension.ScalarFunctionVariant declaration =
        ImmutableSimpleExtension.ScalarFunctionVariant.builder()
            .urn("extension:test:variadic")
            .name("test_func")
            .args(args)
            .variadic(
                variadic != null ? java.util.Optional.of(variadic) : java.util.Optional.empty())
            .returnType(R.I64)
            .options(java.util.Collections.emptyMap())
            .build();

    return Expression.ScalarFunctionInvocation.builder()
        .declaration(declaration)
        .arguments(arguments)
        .outputType(R.I64)
        .options(java.util.Collections.emptyList())
        .build();
  }

  @Test
  void testConsistentVariadicWithSameTypes() {
    // Function: test_func(i64, i64...) with CONSISTENT parameterConsistency
    List<SimpleExtension.Argument> args =
        List.of(SimpleExtension.ValueArgument.builder().value(R.I64).name("arg1").build());

    SimpleExtension.VariadicBehavior variadic =
        ImmutableSimpleExtension.VariadicBehavior.builder()
            .min(1)
            .parameterConsistency(SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT)
            .build();

    // All variadic arguments are i64 - should pass
    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    Expression.I64Literal.builder().value(1).build(),
                    Expression.I64Literal.builder().value(2).build(),
                    Expression.I64Literal.builder().value(3).build())),
        "Consistent variadic with same types should pass");

    // All variadic arguments are i64 (with different nullability) - should pass
    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    Expression.I64Literal.builder().value(1).build(),
                    Expression.I64Literal.builder().value(2).nullable(true).build(),
                    Expression.I64Literal.builder().value(3).nullable(true).build(),
                    Expression.I64Literal.builder().value(4).build())),
        "Consistent variadic with same types but different nullability should pass");
  }

  @Test
  void testConsistentVariadicWithDifferentTypes() {
    // Function: test_func(i64, i64...) with CONSISTENT parameterConsistency
    List<SimpleExtension.Argument> args =
        List.of(SimpleExtension.ValueArgument.builder().value(R.I64).name("arg1").build());

    SimpleExtension.VariadicBehavior variadic =
        ImmutableSimpleExtension.VariadicBehavior.builder()
            .min(1)
            .parameterConsistency(SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT)
            .build();

    // Variadic arguments have different types - should fail
    assertThrows(
        AssertionError.class,
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    Expression.I64Literal.builder().value(1).build(),
                    Expression.I64Literal.builder().value(2).build(),
                    Expression.FP64Literal.builder().value(3.0).build())),
        "Consistent variadic with different types should fail");

    assertThrows(
        AssertionError.class,
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    Expression.I64Literal.builder().value(1).build(),
                    Expression.I32Literal.builder().value(2).build(),
                    Expression.I64Literal.builder().value(3).build())),
        "Consistent variadic with different types should fail");
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
            SimpleExtension.ValueArgument.builder().value(R.I64).name("arg1").build(),
            SimpleExtension.ValueArgument.builder().value(anyType).name("variadic_arg").build());

    SimpleExtension.VariadicBehavior variadic =
        ImmutableSimpleExtension.VariadicBehavior.builder()
            .min(1)
            .parameterConsistency(
                SimpleExtension.VariadicBehavior.ParameterConsistency.INCONSISTENT)
            .build();

    // Variadic arguments have different types - should pass with INCONSISTENT
    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    Expression.I64Literal.builder().value(1).build(),
                    Expression.I64Literal.builder().value(2).build(),
                    Expression.FP64Literal.builder().value(3.0).build())),
        "Inconsistent variadic with different types should pass");

    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    Expression.I64Literal.builder().value(1).build(),
                    Expression.I32Literal.builder().value(2).build(),
                    Expression.FP64Literal.builder().value(3.0).build(),
                    Expression.StrLiteral.builder().value("test").build())),
        "Inconsistent variadic with different types should pass");
  }

  @Test
  void testConsistentVariadicWithWildcardType() {
    // Function: test_func(any, any...) with CONSISTENT parameterConsistency
    // The variadic arguments should all have the same concrete type
    ParameterizedType anyType =
        ParameterizedType.StringLiteral.builder().value("any").nullable(false).build();

    List<SimpleExtension.Argument> args =
        List.of(SimpleExtension.ValueArgument.builder().value(anyType).name("arg1").build());

    SimpleExtension.VariadicBehavior variadic =
        ImmutableSimpleExtension.VariadicBehavior.builder()
            .min(1)
            .parameterConsistency(SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT)
            .build();

    // All variadic arguments are i64 - should pass
    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    Expression.I64Literal.builder().value(1).build(),
                    Expression.I64Literal.builder().value(2).build(),
                    Expression.I64Literal.builder().value(3).build())),
        "Consistent variadic with wildcard type and same concrete types should pass");

    // Variadic arguments have different types - should fail
    assertThrows(
        AssertionError.class,
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    Expression.I64Literal.builder().value(1).build(),
                    Expression.I64Literal.builder().value(2).build(),
                    Expression.FP64Literal.builder().value(3.0).build())),
        "Consistent variadic with wildcard type and different concrete types should fail");
  }

  @Test
  void testConsistentVariadicWithMinGreaterThanOne() {
    // Function: test_func(i64, i64...) with CONSISTENT and min=2
    List<SimpleExtension.Argument> args =
        List.of(SimpleExtension.ValueArgument.builder().value(R.I64).name("arg1").build());

    SimpleExtension.VariadicBehavior variadic =
        ImmutableSimpleExtension.VariadicBehavior.builder()
            .min(2)
            .parameterConsistency(SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT)
            .build();

    // All variadic arguments are i64 - should pass
    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    Expression.I64Literal.builder().value(1).build(),
                    Expression.I64Literal.builder().value(2).build(),
                    Expression.I64Literal.builder().value(3).build(),
                    Expression.I64Literal.builder().value(4).build())),
        "Consistent variadic with min=2 and same types should pass");

    // Variadic arguments have different types - should fail
    assertThrows(
        AssertionError.class,
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    Expression.I64Literal.builder().value(1).build(),
                    Expression.I64Literal.builder().value(2).build(),
                    Expression.I64Literal.builder().value(3).build(),
                    Expression.FP64Literal.builder().value(4.0).build())),
        "Consistent variadic with min=2 and different types should fail");
  }

  @Test
  void testNoVariadicBehavior() {
    // Function: test_func(i64) with no variadic behavior
    List<SimpleExtension.Argument> args =
        List.of(SimpleExtension.ValueArgument.builder().value(R.I64).name("arg1").build());

    // No variadic behavior - should pass regardless of consistency
    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args, null, List.of(Expression.I64Literal.builder().value(1).build())),
        "No variadic behavior should always pass");
  }

  @Test
  void testConsistentVariadicWithNullableTypes() {
    // Function: test_func(i64, i64...) with CONSISTENT parameterConsistency
    List<SimpleExtension.Argument> args =
        List.of(SimpleExtension.ValueArgument.builder().value(R.I64).name("arg1").build());

    SimpleExtension.VariadicBehavior variadic =
        ImmutableSimpleExtension.VariadicBehavior.builder()
            .min(1)
            .parameterConsistency(SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT)
            .build();

    // Mix of nullable and non-nullable i64 - should pass (nullability is ignored)
    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    Expression.I64Literal.builder().value(1).build(),
                    Expression.I64Literal.builder().value(2).nullable(true).build(),
                    Expression.I64Literal.builder().value(3).build(),
                    Expression.I64Literal.builder().value(4).nullable(true).build())),
        "Consistent variadic with same types but different nullability should pass");
  }
}

