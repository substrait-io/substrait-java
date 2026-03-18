package io.substrait.expression;

import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Builds lambda expressions with build-time validation of parameter references.
 *
 * <p>Maintains a stack of lambda parameter scopes. Each call to {@link #lambda} pushes parameters
 * onto the stack, builds the body via a callback, and pops. Nested lambdas simply call {@code
 * lambda()} again on the same builder.
 *
 * <p>The callback receives a {@link Scope} handle for creating validated parameter references. The
 * correct {@code stepsOut} value is computed automatically from the stack.
 *
 * <pre>{@code
 * LambdaBuilder lb = new LambdaBuilder();
 *
 * // Simple: (x: i32) -> x
 * Expression.Lambda simple = lb.lambda(List.of(R.I32), params -> params.ref(0));
 *
 * // Nested: (x: i32) -> (y: i64) -> add(x, y)
 * Expression.Lambda nested = lb.lambda(List.of(R.I32), outer ->
 *     lb.lambda(List.of(R.I64), inner ->
 *         add(outer.ref(0), inner.ref(0))
 *     )
 * );
 * }</pre>
 */
public class LambdaBuilder {
  private final List<Type.Struct> lambdaContext = new ArrayList<>();

  /**
   * Builds a lambda expression. The body function receives a {@link Scope} for creating validated
   * parameter references. Nested lambdas are built by calling this method again inside the
   * callback.
   *
   * @param paramTypes the lambda's parameter types
   * @param bodyFn function that builds the lambda body given a scope handle
   * @return the constructed lambda expression
   */
  public Expression.Lambda lambda(List<Type> paramTypes, Function<Scope, Expression> bodyFn) {
    Type.Struct params = Type.Struct.builder().nullable(false).addAllFields(paramTypes).build();
    pushLambdaContext(params);
    try {
      Scope scope = new Scope(params);
      Expression body = bodyFn.apply(scope);
      return ImmutableExpression.Lambda.builder().parameters(params).body(body).build();
    } finally {
      popLambdaContext();
    }
  }

  /**
   * Builds a lambda expression from a pre-built parameter struct. Used by internal converters that
   * already have a Type.Struct (e.g., during protobuf deserialization).
   *
   * @param params the lambda's parameter struct
   * @param bodyFn function that builds the lambda body
   * @return the constructed lambda expression
   */
  public Expression.Lambda lambdaFromStruct(
      Type.Struct params, java.util.function.Supplier<Expression> bodyFn) {
    pushLambdaContext(params);
    try {
      Expression body = bodyFn.get();
      return ImmutableExpression.Lambda.builder().parameters(params).body(body).build();
    } finally {
      popLambdaContext();
    }
  }

  /**
   * Resolves the parameter struct for a lambda at the given stepsOut from the current innermost
   * scope. Used by internal converters to validate lambda parameter references during
   * deserialization.
   *
   * @param stepsOut number of lambda scopes to traverse outward (0 = current/innermost)
   * @return the parameter struct at the target scope level
   * @throws IllegalArgumentException if stepsOut exceeds the current nesting depth
   */
  public Type.Struct resolveParams(int stepsOut) {
    int targetDepth = lambdaContext.size() - stepsOut;
    if (targetDepth <= 0 || targetDepth > lambdaContext.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Lambda parameter reference with stepsOut=%d is invalid (current depth: %d)",
              stepsOut, lambdaContext.size()));
    }
    return lambdaContext.get(targetDepth - 1);
  }

  /**
   * Creates a validated field reference to a lambda parameter. Validates that stepsOut is valid for
   * the current lambda nesting context.
   *
   * @param stepsOut number of lambda scopes to traverse outward (0 = current/innermost)
   * @param paramIndex index of the parameter within the target lambda's parameter struct
   * @return a field reference to the specified lambda parameter
   * @throws IllegalArgumentException if stepsOut exceeds the current nesting depth
   * @throws IndexOutOfBoundsException if paramIndex is out of bounds for the target lambda
   */
  public FieldReference newParameterReference(int stepsOut, int paramIndex) {
    Type.Struct params = resolveParams(stepsOut);
    Type type = params.fields().get(paramIndex);
    return FieldReference.newLambdaParameterReference(stepsOut, paramIndex, type);
  }

  /**
   * Pushes a lambda's parameters onto the context stack. This makes the parameters available for
   * validation when building the lambda's body, and allows nested lambda parameter references to
   * correctly compute their stepsOut values.
   */
  private void pushLambdaContext(Type.Struct params) {
    lambdaContext.add(params);
  }

  /**
   * Pops the most recently pushed lambda parameters from the context stack. Called after a lambda's
   * body has been built, restoring the context to the enclosing lambda's scope.
   */
  private void popLambdaContext() {
    lambdaContext.remove(lambdaContext.size() - 1);
  }

  /**
   * A handle to a particular lambda's parameter scope. Use {@link #ref} to create validated
   * parameter references.
   *
   * <p>Each Scope captures the depth of the lambdaContext stack at the time it was created. When
   * {@link #ref} is called, the Substrait {@code stepsOut} value is computed as the difference
   * between the current stack depth and the captured depth. This means the same Scope produces
   * different stepsOut values depending on the nesting level at the time of the call, which is what
   * allows outer.ref(0) to produce stepsOut=1 when called inside a nested lambda.
   */
  public class Scope {
    private final Type.Struct params;
    private final int depth;

    private Scope(Type.Struct params) {
      this.params = params;
      this.depth = lambdaContext.size();
    }

    /**
     * Computes the number of lambda boundaries between this scope and the current innermost scope.
     * This value changes dynamically as nested lambdas are built.
     */
    private int stepsOut() {
      return lambdaContext.size() - depth;
    }

    /**
     * Creates a validated reference to a parameter of this lambda.
     *
     * @param paramIndex index of the parameter within this lambda's parameter struct
     * @return a {@link FieldReference} pointing to the specified parameter
     * @throws IndexOutOfBoundsException if paramIndex is out of bounds
     */
    public FieldReference ref(int paramIndex) throws IndexOutOfBoundsException {
      Type type = params.fields().get(paramIndex);
      return FieldReference.newLambdaParameterReference(stepsOut(), paramIndex, type);
    }
  }
}
