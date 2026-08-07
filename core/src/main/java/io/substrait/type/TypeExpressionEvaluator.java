package io.substrait.type;

import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.function.TypeExpressionVisitor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Evaluates a {@link TypeExpression} to a concrete {@link Type} given a set of actual arguments.
 *
 * <p>A declaration's {@code return} expression is either already concrete (e.g. {@code i64?}) or
 * parameterized in terms of its argument types (e.g. {@code DECIMAL?<38,S>}, where {@code S} is the
 * scale of the argument). This evaluator resolves the latter by binding the integer type parameters
 * ({@code P}, {@code S}, ...) from the actual argument types and substituting them.
 *
 * <p>Expression shapes that are not yet supported (arithmetic derivations, {@code if/then}, return
 * programs, ...) fail closed with an {@link UnsupportedOperationException}. The evaluator never
 * falls back to a caller-supplied type: an unresolved expression is an error, not a default.
 */
public class TypeExpressionEvaluator {

  /**
   * Evaluates a return-type expression to a concrete {@link Type}.
   *
   * @param returnExpression the type expression to evaluate
   * @param declaredArguments the declared arguments of the function (used to bind type parameters)
   * @param actualTypes the actual argument types supplied at the call site
   * @return the resolved concrete type
   * @throws UnsupportedOperationException if the expression cannot be evaluated
   */
  public static Type evaluateExpression(
      TypeExpression returnExpression,
      List<SimpleExtension.Argument> declaredArguments,
      List<Type> actualTypes) {
    return evaluateExpression(returnExpression, declaredArguments, Optional.empty(), actualTypes);
  }

  /**
   * Evaluates a return-type expression to a concrete {@link Type}, taking the declaration's
   * variadic behavior into account.
   *
   * @param returnExpression the type expression to evaluate
   * @param declaredArguments the declared arguments of the function (used to bind type parameters)
   * @param variadic the declaration's variadic behavior, if it is variadic
   * @param actualTypes the actual argument types supplied at the call site
   * @return the resolved concrete type
   * @throws UnsupportedOperationException if the expression cannot be evaluated
   */
  public static Type evaluateExpression(
      TypeExpression returnExpression,
      List<SimpleExtension.Argument> declaredArguments,
      Optional<SimpleExtension.VariadicBehavior> variadic,
      List<Type> actualTypes) {
    // Bind the parameters even when the return type is concrete: binding is what catches a
    // signature that uses one parameter name inconsistently — f(DECIMAL<P,S>, DECIMAL<P,S>) called
    // with two different scales — and that is an error whatever the function returns.
    ParameterBindings bindings = bindParameters(declaredArguments, variadic, actualTypes);
    if (returnExpression instanceof Type) {
      // The declared return type is already concrete; nothing to derive.
      return (Type) returnExpression;
    }
    return returnExpression.accept(new ReturnTypeEvaluator(returnExpression, bindings));
  }

  /**
   * Binds the declaration's type parameters — both numbered wildcards (e.g. the {@code any1} of
   * {@code min(any1) -> any1}) and integer parameters (e.g. the {@code P} and {@code S} of {@code
   * DECIMAL<P,S>}) — by matching each declared value argument against the corresponding actual
   * argument type in a single pass.
   *
   * <p>Binding the same parameter name to two different values is a signature error and is rejected
   * rather than silently overwritten, for wildcards and integer parameters alike. A variadic
   * declaration states its trailing argument once but accepts it repeatedly, so every actual
   * argument is bound against that trailing declaration — unless the declaration marks its
   * parameters {@code INCONSISTENT}, in which case each repetition is independent and only the
   * first one binds.
   */
  private static ParameterBindings bindParameters(
      List<SimpleExtension.Argument> declaredArguments,
      Optional<SimpleExtension.VariadicBehavior> variadic,
      List<Type> actualTypes) {
    // Enum arguments select an overload and carry no value type, so they do not consume an actual
    // type; dropping them aligns the declared and actual arguments positionally. A null entry is a
    // type argument: it consumes an actual type but binds no parameter.
    List<ParameterizedType> declaredTypes = new ArrayList<>();
    for (SimpleExtension.Argument declared : declaredArguments) {
      if (declared instanceof SimpleExtension.EnumArgument) {
        continue;
      }
      declaredTypes.add(
          declared instanceof SimpleExtension.ValueArgument
              ? ((SimpleExtension.ValueArgument) declared).value()
              : null);
    }

    ParameterBindings bindings = new ParameterBindings();
    if (declaredTypes.isEmpty()) {
      return bindings;
    }
    boolean bindRepeats =
        variadic
            .map(
                behavior ->
                    behavior.parameterConsistency()
                        == SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT)
            .orElse(false);
    for (int index = 0; index < actualTypes.size(); index++) {
      boolean repeated = index >= declaredTypes.size();
      if (repeated && !bindRepeats) {
        // Either the arity is wrong (validated by the resolver, not here) or the repetitions are
        // declared independent; in both cases there is nothing further to bind.
        break;
      }
      ParameterizedType declared =
          repeated ? declaredTypes.get(declaredTypes.size() - 1) : declaredTypes.get(index);
      if (declared != null) {
        bindings.bind(declared, actualTypes.get(index));
      }
    }
    return bindings;
  }

  /** The wildcard and integer type parameters bound from a call site's actual argument types. */
  private static final class ParameterBindings {

    private final Map<String, Type> types = new HashMap<>();
    private final Map<String, Integer> integers = new HashMap<>();

    private Type boundType(String name) {
      return types.get(name);
    }

    private Integer boundInteger(String token) {
      return integers.get(token);
    }

    private void bind(ParameterizedType declared, Type actual) {
      if (declared instanceof ParameterizedType.StringLiteral) {
        ParameterizedType.StringLiteral literal = (ParameterizedType.StringLiteral) declared;
        // Only a numbered wildcard names a parameter that a return expression can refer to and that
        // has to stay consistent across the call; a plain "any" binds independently each time.
        if (literal.isNumberedWildcard()) {
          bindType(literal.value(), actual);
        }
      } else if (declared instanceof ParameterizedType.Decimal && actual instanceof Type.Decimal) {
        ParameterizedType.Decimal declaredDecimal = (ParameterizedType.Decimal) declared;
        Type.Decimal actualDecimal = (Type.Decimal) actual;
        bindInteger(declaredDecimal.precision().value(), actualDecimal.precision());
        bindInteger(declaredDecimal.scale().value(), actualDecimal.scale());
      }
    }

    private void bindType(String name, Type actual) {
      // Nullability is not part of a wildcard's identity: any1 binds to i32 and i32? alike, and the
      // return expression's own nullability (or the MIRROR policy) decides the result's.
      Type existing = types.putIfAbsent(name, actual);
      if (existing != null && !existing.equalsIgnoringNullability(actual)) {
        throw new UnsupportedOperationException(
            String.format(
                "Inconsistent binding for type parameter '%s': %s vs %s", name, existing, actual));
      }
    }

    private void bindInteger(String token, int value) {
      if (isInteger(token)) {
        // A numeric token is a literal, not a parameter name; nothing to bind.
        return;
      }
      Integer existing = integers.putIfAbsent(token, value);
      if (existing != null && existing != value) {
        throw new UnsupportedOperationException(
            String.format(
                "Inconsistent binding for type parameter '%s': %d vs %d", token, existing, value));
      }
    }

    private static boolean isInteger(String token) {
      try {
        Integer.parseInt(token.trim());
        return true;
      } catch (NumberFormatException e) {
        return false;
      }
    }
  }

  /**
   * Evaluates the supported return-type expression shapes. Everything else falls through to the
   * throwing base, keeping unsupported derivations fail-closed.
   */
  private static final class ReturnTypeEvaluator
      extends TypeExpressionVisitor.TypeExpressionThrowsVisitor<Type, RuntimeException> {

    private final ParameterBindings bindings;

    private ReturnTypeEvaluator(TypeExpression returnExpression, ParameterBindings bindings) {
      super("Cannot evaluate return-type expression: " + returnExpression);
      this.bindings = bindings;
    }

    @Override
    public Type visit(ParameterizedType.Decimal decimal) {
      int precision = resolveInteger(decimal.precision().value());
      int scale = resolveInteger(decimal.scale().value());
      return TypeCreator.of(decimal.nullable()).decimal(precision, scale);
    }

    @Override
    public Type visit(ParameterizedType.StringLiteral stringLiteral) {
      // A wildcard return (e.g. min(any1) -> any1) resolves to the bound argument type, taking the
      // nullability declared on the return expression in both directions (a required return forces
      // the type non-null, a nullable one forces it nullable). MIRROR policy, if any, is applied
      // afterwards by the caller.
      Type bound = bindings.boundType(stringLiteral.value());
      if (bound == null) {
        throw new UnsupportedOperationException(
            "Unbound type parameter '" + stringLiteral.value() + "' in return-type expression");
      }
      return bound.withNullable(stringLiteral.nullable());
    }

    private int resolveInteger(String token) {
      Integer bound = bindings.boundInteger(token);
      if (bound != null) {
        return bound;
      }
      try {
        return Integer.parseInt(token.trim());
      } catch (NumberFormatException e) {
        throw new UnsupportedOperationException(
            "Unbound type parameter '" + token + "' in return-type expression");
      }
    }
  }
}
