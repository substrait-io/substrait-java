package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ResolvedAggregateBindingTest extends TestBase {

  /** The intermediate state {@code avg} accumulates: a sum and a count. */
  static final Type AVG_INTERMEDIATE = R.struct(R.I64, R.I64);

  @Test
  void finalPhaseProducesTheDeclaredReturnType() {
    ResolvedAggregateBinding binding =
        ResolvedAggregateBinding.resolve(avg(Expression.AggregationPhase.INITIAL_TO_RESULT));
    assertEquals(N.I32, binding.outputType());
  }

  @Test
  void partialPhaseProducesTheDeclaredIntermediateType() {
    // A phase that stops at the intermediate state produces avg's accumulator, not its average.
    for (Expression.AggregationPhase phase :
        new Expression.AggregationPhase[] {
          Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE,
          Expression.AggregationPhase.INTERMEDIATE_TO_INTERMEDIATE
        }) {
      assertEquals(AVG_INTERMEDIATE, ResolvedAggregateBinding.resolve(avg(phase)).outputType());
    }
  }

  @Test
  void validationComparesAgainstThePhaseOutputType() {
    ResolvedAggregateBinding partial =
        ResolvedAggregateBinding.resolve(avg(Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE));
    // A partial aggregate that declares the intermediate type is valid ...
    FunctionBindingResolver.validate(partial, AVG_INTERMEDIATE);
    // ... while one declaring the final return type is not.
    assertThrows(
        InvalidFunctionBindingException.class,
        () -> FunctionBindingResolver.validate(partial, N.I32));
  }

  @Test
  void explicitIntermediateTypeWins() {
    ResolvedAggregateBinding resolved =
        ResolvedAggregateBinding.resolve(avg(Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE));
    ResolvedAggregateBinding overridden =
        ResolvedAggregateBinding.builder().from(resolved).intermediateType(R.struct(R.I64)).build();
    assertEquals(R.struct(R.I64), overridden.outputType());
  }

  @Test
  void phaseIsPartOfTheIdentity() {
    // Identity has to tell a partial aggregate apart from a full one: they produce different types
    // and cannot be substituted for one another.
    ResolvedAggregateBinding partial =
        ResolvedAggregateBinding.resolve(avg(Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE));
    ResolvedAggregateBinding full =
        ResolvedAggregateBinding.resolve(avg(Expression.AggregationPhase.INITIAL_TO_RESULT));
    assertNotEquals(partial, full);
  }

  @Test
  void intermediateConsumingPhaseTakesTheIntermediateState() {
    // sum:i32 accumulates into i64?, so its final phase consumes an i64? — validating that against
    // the declaration's i32 argument would reject a perfectly valid partial-aggregation plan.
    ResolvedAggregateBinding finalPhase =
        sum(Expression.AggregationPhase.INTERMEDIATE_TO_RESULT, N.I64);
    FunctionBindingResolver.validate(finalPhase, N.I64);
    assertTrue(FunctionBindingResolver.matchesDeclaration(finalPhase));

    // The same argument in an initial phase is not valid: there it is the declared i32 that is
    // expected.
    assertFalse(
        FunctionBindingResolver.matchesDeclaration(
            sum(Expression.AggregationPhase.INITIAL_TO_RESULT, N.I64)));
  }

  @Test
  void intermediateConsumingPhaseRejectsAnotherState() {
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.validate(
                sum(Expression.AggregationPhase.INTERMEDIATE_TO_RESULT, R.I32), N.I64));
  }

  @Test
  void bindingStopsMatchingWhenTheArgumentsChange() {
    // What the reverse conversion relies on: a binding that no longer describes the call it is
    // attached to (a rule replaced the i32 operand with an i64 one) must not be restored.
    assertFalse(
        FunctionBindingResolver.matchesDeclaration(
            sum(Expression.AggregationPhase.INITIAL_TO_RESULT, R.I64)));
    assertTrue(
        FunctionBindingResolver.matchesDeclaration(
            sum(Expression.AggregationPhase.INITIAL_TO_RESULT, R.I32)));
  }

  @Test
  void intermediateStateMustArriveAsAValueArgument() {
    // A type argument carries no operand: conversion drops it and leaves the aggregate with no
    // arguments at all, so accepting one here would validate a plan that cannot be converted.
    ResolvedAggregateBinding binding =
        ResolvedAggregateBinding.builder()
            .function(
                FunctionBindingResolver.resolve(
                    sumDeclaration(), List.of(ResolvedArgument.type(N.I64)), List.of()))
            .phase(Expression.AggregationPhase.INTERMEDIATE_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .intermediateType(Optional.empty())
            .build();
    assertFalse(FunctionBindingResolver.matchesDeclaration(binding));
  }

  @Test
  void nonDecomposableDeclarationHasNoIntermediateState() {
    // mode declares no decomposability, so there is no intermediate state for a partial phase to
    // produce — deriving one fails closed rather than inventing it.
    SimpleExtension.AggregateFunctionVariant mode =
        extensions.getAggregateFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "mode:i32"));
    ResolvedAggregateBinding binding =
        ResolvedAggregateBinding.resolve(
            AggregateFunctionInvocation.builder()
                .declaration(mode)
                .outputType(N.I32)
                .aggregationPhase(Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE)
                .invocation(Expression.AggregationInvocation.ALL)
                .addArguments(sb.i32(1))
                .build());
    assertThrows(InvalidFunctionBindingException.class, binding::outputType);
  }

  private SimpleExtension.AggregateFunctionVariant sumDeclaration() {
    return extensions.getAggregateFunction(
        SimpleExtension.FunctionAnchor.of(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "sum:i32"));
  }

  private ResolvedAggregateBinding sum(Expression.AggregationPhase phase, Type argumentType) {
    SimpleExtension.AggregateFunctionVariant declaration = sumDeclaration();
    Rel input = sb.namedScan(List.of("t"), List.of("x"), List.of(argumentType));
    return ResolvedAggregateBinding.resolve(
        AggregateFunctionInvocation.builder()
            .declaration(declaration)
            .outputType(N.I64)
            .aggregationPhase(phase)
            .invocation(Expression.AggregationInvocation.ALL)
            .addArguments(sb.fieldReference(input, 0))
            .build());
  }

  private AggregateFunctionInvocation avg(Expression.AggregationPhase phase) {
    SimpleExtension.AggregateFunctionVariant declaration =
        extensions.getAggregateFunction(
            SimpleExtension.FunctionAnchor.of(
                DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "avg:i32"));
    return AggregateFunctionInvocation.builder()
        .declaration(declaration)
        .outputType(N.I32)
        .aggregationPhase(phase)
        .invocation(Expression.AggregationInvocation.ALL)
        .addArguments(sb.i32(1))
        .build();
  }
}
