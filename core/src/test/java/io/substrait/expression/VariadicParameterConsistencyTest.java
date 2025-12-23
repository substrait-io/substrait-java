package io.substrait.expression;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.TestBase;
import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Tests for variadic parameter consistency validation in Expression. */
class VariadicParameterConsistencyTest extends TestBase {

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
            .variadic(variadic != null ? Optional.of(variadic) : Optional.empty())
            .returnType(R.I64)
            .options(Collections.emptyMap())
            .build();

    return ExpressionCreator.scalarFunction(declaration, R.I64, arguments);
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

    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    ExpressionCreator.i64(false, 1),
                    ExpressionCreator.i64(false, 2),
                    ExpressionCreator.i64(false, 3))),
        "Consistent variadic with same types should pass");

    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    ExpressionCreator.i64(false, 1),
                    ExpressionCreator.i64(true, 2),
                    ExpressionCreator.i64(true, 3),
                    ExpressionCreator.i64(false, 4))),
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

    assertThrows(
        AssertionError.class,
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    ExpressionCreator.i64(false, 1),
                    ExpressionCreator.i64(false, 2),
                    ExpressionCreator.fp64(false, 3.0))),
        "Consistent variadic with different types should fail");

    assertThrows(
        AssertionError.class,
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    ExpressionCreator.i64(false, 1),
                    ExpressionCreator.i32(false, 2),
                    ExpressionCreator.i64(false, 3))),
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

    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    ExpressionCreator.i64(false, 1),
                    ExpressionCreator.i64(false, 2),
                    ExpressionCreator.fp64(false, 3.0))),
        "Inconsistent variadic with different types should pass");

    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    ExpressionCreator.i64(false, 1),
                    ExpressionCreator.i32(false, 2),
                    ExpressionCreator.fp64(false, 3.0),
                    ExpressionCreator.string(false, "test"))),
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

    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    ExpressionCreator.i64(false, 1),
                    ExpressionCreator.i64(false, 2),
                    ExpressionCreator.i64(false, 3))),
        "Consistent variadic with wildcard type and same concrete types should pass");

    assertThrows(
        AssertionError.class,
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    ExpressionCreator.i64(false, 1),
                    ExpressionCreator.i64(false, 2),
                    ExpressionCreator.fp64(false, 3.0))),
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

    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    ExpressionCreator.i64(false, 1),
                    ExpressionCreator.i64(false, 2),
                    ExpressionCreator.i64(false, 3),
                    ExpressionCreator.i64(false, 4))),
        "Consistent variadic with min=2 and same types should pass");

    assertThrows(
        AssertionError.class,
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    ExpressionCreator.i64(false, 1),
                    ExpressionCreator.i64(false, 2),
                    ExpressionCreator.i64(false, 3),
                    ExpressionCreator.fp64(false, 4.0))),
        "Consistent variadic with min=2 and different types should fail");
  }

  @Test
  void testNoVariadicBehavior() {
    // Function: test_func(i64) with no variadic behavior
    List<SimpleExtension.Argument> args =
        List.of(SimpleExtension.ValueArgument.builder().value(R.I64).name("arg1").build());

    assertDoesNotThrow(
        () -> createScalarFunctionInvocation(args, null, List.of(ExpressionCreator.i64(false, 1))),
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

    assertDoesNotThrow(
        () ->
            createScalarFunctionInvocation(
                args,
                variadic,
                List.of(
                    ExpressionCreator.i64(false, 1),
                    ExpressionCreator.i64(true, 2),
                    ExpressionCreator.i64(false, 3),
                    ExpressionCreator.i64(true, 4))),
        "Consistent variadic with same types but different nullability should pass");
  }
}
