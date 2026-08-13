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
import java.util.OptionalInt;

/**
 * Evaluates a {@link TypeExpression} to a concrete {@link Type} given a set of actual arguments.
 *
 * <p>A declaration's {@code return} expression is either already concrete (e.g. {@code i64?}) or
 * parameterized in terms of its argument types (e.g. {@code DECIMAL?<38,S>}, where {@code S} is the
 * scale of the argument). This evaluator resolves the latter by binding the integer type parameters
 * ({@code P}, {@code S}, ...) from the actual argument types and substituting them.
 *
 * <p>Expression shapes that are not yet supported fail closed with an {@link
 * UnsupportedOperationException}. The evaluator never falls back to a caller-supplied type: an
 * unresolved expression is an error, not a default.
 *
 * <p>Supported shapes are concrete types, numbered wildcards ({@code any1}) and parameterized
 * decimals ({@code DECIMAL<P,S>}). What actually fails on the standard extension catalog is the
 * other parameterized type classes — {@code varchar<L1>}, {@code fixedchar<L1>}, {@code
 * precision_time<P>}, {@code precision_timestamp<P>}, {@code precision_timestamp_tz<P>}, {@code
 * interval_day<P>}, {@code list<anyN>}, parameterized structs — and multi-line return programs;
 * {@code concat}, {@code concat_ws}, {@code assume_timezone} and the {@code strptime_*} family are
 * all rejected today. Among the standard aggregates, {@code quantile} is the one whose output type
 * cannot be derived at all: its declared return {@code LIST?<any>} uses a plain {@code any}, which
 * carries no identity to bind (spec v0.99.0).
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
    // type; dropping them aligns the declared and actual arguments positionally. A type argument
    // consumes an actual type and binds its declared pattern too, so that a declaration like
    // (<type> DECIMAL<P,S>) -> DECIMAL<P,S> can derive its return from the supplied type.
    List<ParameterizedType> declaredTypes = new ArrayList<>();
    for (SimpleExtension.Argument declared : declaredArguments) {
      if (declared instanceof SimpleExtension.EnumArgument) {
        continue;
      }
      declaredTypes.add(
          declared instanceof SimpleExtension.ValueArgument
              ? ((SimpleExtension.ValueArgument) declared).value()
              : declared instanceof SimpleExtension.TypeArgument
                  ? ((SimpleExtension.TypeArgument) declared).type()
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
      if (repeated && !variadic.isPresent()) {
        // The arity is wrong; that is validated by the resolver, not here.
        break;
      }
      ParameterizedType declared =
          repeated ? declaredTypes.get(declaredTypes.size() - 1) : declaredTypes.get(index);
      if (declared == null) {
        continue;
      }
      // An INCONSISTENT variadic repetition binds no named parameters — each repetition is
      // independent — but a literal constraint (the 0 of DECIMAL<P,0>) still applies to it.
      bindings.bind(declared, actualTypes.get(index), !repeated || bindRepeats);
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

    /**
     * Binds the declared pattern's parameters from the actual type. With {@code bindNames} false —
     * an INCONSISTENT variadic repetition — named parameters are left unbound (each repetition is
     * independent) while literal constraints are still enforced.
     */
    private void bind(ParameterizedType declared, Type actual, boolean bindNames) {
      if (declared instanceof ParameterizedType.StringLiteral) {
        ParameterizedType.StringLiteral literal = (ParameterizedType.StringLiteral) declared;
        // Only a numbered wildcard names a parameter that a return expression can refer to and that
        // has to stay consistent across the call; a plain "any" binds independently each time.
        if (bindNames && literal.isNumberedWildcard()) {
          bindType(literal.value(), actual);
        }
      } else if (declared instanceof ParameterizedType.Decimal && actual instanceof Type.Decimal) {
        ParameterizedType.Decimal declaredDecimal = (ParameterizedType.Decimal) declared;
        Type.Decimal actualDecimal = (Type.Decimal) actual;
        bindInteger(declaredDecimal.precision().value(), actualDecimal.precision(), bindNames);
        bindInteger(declaredDecimal.scale().value(), actualDecimal.scale(), bindNames);
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

    private void bindInteger(String token, int value, boolean bindNames) {
      OptionalInt literal = parseIntegerLiteral(token);
      if (literal.isPresent()) {
        // A numeric token is a literal constraint, not a parameter name: the actual value has to
        // equal it — a declared DECIMAL<P,0> only accepts a scale of exactly 0.
        if (literal.getAsInt() != value) {
          throw new UnsupportedOperationException(
              String.format(
                  "Declared literal %s does not match the actual value %d", token, value));
        }
        return;
      }
      if (!bindNames) {
        return;
      }
      Integer existing = integers.putIfAbsent(token, value);
      if (existing != null && existing != value) {
        throw new UnsupportedOperationException(
            String.format(
                "Inconsistent binding for type parameter '%s': %d vs %d", token, existing, value));
      }
    }
  }

  /**
   * Parses a type-parameter token as an integer literal, or returns empty when the token is a
   * parameter name. The type grammar sends whitespace to a hidden channel, so the token is exact
   * and needs no trimming.
   */
  private static OptionalInt parseIntegerLiteral(String token) {
    try {
      return OptionalInt.of(Integer.parseInt(token));
    } catch (NumberFormatException e) {
      return OptionalInt.empty();
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
      return parseIntegerLiteral(token)
          .orElseThrow(
              () ->
                  new UnsupportedOperationException(
                      "Unbound type parameter '" + token + "' in return-type expression"));
    }
  }
}
