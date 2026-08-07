package io.substrait.extension;

import io.substrait.expression.FunctionOption;
import io.substrait.function.NullableType;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.type.Type;
import io.substrait.type.TypeExpressionEvaluator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves an extension function invocation to a {@link ResolvedFunctionBinding} and, on demand,
 * validates and derives its output type.
 *
 * <p>{@link #resolve} only captures identity (anchor, arguments, options); it performs no
 * validation, so a plan that merely differs from the declaration still resolves. {@link #validate}
 * (and {@link #resolveAndValidate}) opt into checking the signature (arity, argument kinds and
 * types), the options and the plan-declared output type against the declaration. {@link
 * #deriveOutputType} derives the return type; unsupported derivations fail closed (see {@link
 * TypeExpressionEvaluator}), never falling back to a plan-supplied type.
 *
 * <p>Signature type matching covers value-argument kinds, wildcards and the common
 * concrete/decimal/char/binary type classes; other parameterized argument shapes are accepted
 * without deep structural checking. Occurrences of one numbered wildcard ({@code any1}) must agree
 * on a single type, while each plain {@code any} matches independently; a variadic declaration
 * repeats its trailing argument, and only requires the repetitions to agree when its parameters are
 * {@code CONSISTENT}. Enum options and option preferences are matched case-insensitively; an
 * unspecified enum option is rejected only where the declaration requires one.
 */
public final class FunctionBindingResolver {

  private FunctionBindingResolver() {}

  /**
   * Resolves a function declaration and arguments into a binding, capturing its identity. This
   * performs no validation against the declaration; see {@link #validate}.
   *
   * @param declaration the function declaration
   * @param arguments the ordered resolved arguments
   * @param options the function options
   * @return the resolved binding
   */
  public static ResolvedFunctionBinding resolve(
      SimpleExtension.Function declaration,
      List<ResolvedArgument> arguments,
      List<FunctionOption> options) {
    // Resolving only captures identity; it does not validate. Signature/options/output validation
    // is a separate, opt-in concern (see #validate), so a plan that merely differs from the
    // declaration still converts under the default policy.
    // Options are kept exactly as the plan spelled them, so a binding always reproduces the
    // invocation it was resolved from.
    return ResolvedFunctionBinding.builder()
        .anchor(declaration.getAnchor())
        .declaration(declaration)
        .arguments(arguments)
        .options(options)
        .build();
  }

  /**
   * Resolves a function declaration and validates that the plan-declared output type matches the
   * derived one.
   *
   * @param declaration the function declaration
   * @param arguments the ordered resolved arguments
   * @param options the function options
   * @param declaredOutputType the output type declared by the plan
   * @return the resolved binding
   * @throws InvalidFunctionBindingException if the signature, options or output type are invalid
   */
  public static ResolvedFunctionBinding resolveAndValidate(
      SimpleExtension.Function declaration,
      List<ResolvedArgument> arguments,
      List<FunctionOption> options,
      Type declaredOutputType) {
    ResolvedFunctionBinding binding = resolve(declaration, arguments, options);
    validate(binding, declaredOutputType);
    return binding;
  }

  /**
   * Validates a resolved binding against its extension declaration: the signature (arity, argument
   * kinds and types), the options, and — against the given plan-declared type — the output type.
   * This is the opt-in extension-declaration check, separate from resolving the binding.
   *
   * @param binding the resolved binding
   * @param declaredOutputType the output type declared by the plan
   * @throws InvalidFunctionBindingException if the binding is inconsistent with the declaration
   */
  public static void validate(ResolvedFunctionBinding binding, Type declaredOutputType) {
    validateSignature(binding.declaration(), binding.arguments());
    validateOptions(binding.declaration(), binding.options());
    validateOutputType(binding, declaredOutputType);
  }

  /**
   * Validates a resolved aggregate binding against its extension declaration. Identical to {@link
   * #validate(ResolvedFunctionBinding, Type)} except that the type a partial aggregate produces is
   * its declaration's intermediate type rather than its return type, which {@link
   * ResolvedAggregateBinding#outputType()} selects by phase.
   *
   * @param binding the resolved aggregate binding
   * @param declaredOutputType the output type declared by the plan
   * @throws InvalidFunctionBindingException if the binding is inconsistent with the declaration
   */
  public static void validate(ResolvedAggregateBinding binding, Type declaredOutputType) {
    validateAggregate(binding);
    if (!binding.outputType().equals(declaredOutputType)) {
      throw InvalidFunctionBindingException.outputTypeMismatch(
          binding.function().anchor(), declaredOutputType, binding.outputType());
    }
  }

  /**
   * Reports whether a resolved aggregate binding is consistent with its declaration — its signature
   * (as its phase defines it) and its options — without comparing any plan-declared output type and
   * without throwing. Note that consistency is not the same question as "does this binding still
   * describe that invocation": a phase whose state is parameterized by the initial arguments cannot
   * be re-validated from the state alone, so comparing the recorded arguments answers that better.
   *
   * @param binding the resolved aggregate binding
   * @return {@code true} if the binding satisfies its declaration
   */
  public static boolean matchesDeclaration(ResolvedAggregateBinding binding) {
    try {
      validateAggregate(binding);
      return true;
    } catch (InvalidFunctionBindingException e) {
      return false;
    }
  }

  private static void validateAggregate(ResolvedAggregateBinding binding) {
    ResolvedFunctionBinding function = binding.function();
    if (binding.consumesIntermediateState()) {
      validateIntermediateSignature(binding);
    } else {
      validateSignature(function.declaration(), function.arguments());
    }
    validateOptions(function.declaration(), function.options());
  }

  /**
   * Validates the signature of a phase that consumes an intermediate state: by contract its single
   * argument is the declaration's intermediate value, not one of its declared arguments, and only a
   * decomposable declaration has such a state at all.
   */
  private static void validateIntermediateSignature(ResolvedAggregateBinding binding) {
    ResolvedFunctionBinding function = binding.function();
    SimpleExtension.Function declaration = function.declaration();
    if (!(declaration instanceof SimpleExtension.AggregateFunctionVariant)) {
      throw new InvalidFunctionBindingException(
          String.format("%s is not an aggregate declaration", function.anchor()));
    }
    List<ResolvedArgument> arguments = function.arguments();
    if (arguments.size() != 1) {
      throw new InvalidFunctionBindingException(
          String.format(
              "%s in phase %s takes a single intermediate-state argument but got %d",
              function.anchor(), binding.phase(), arguments.size()));
    }
    ResolvedArgument state = arguments.get(0);
    if (state.kind() != ResolvedArgument.Kind.VALUE) {
      // The state is a value the previous phase produced. A type or enum argument in its place
      // would carry no operand at all, and conversion drops it silently.
      throw new InvalidFunctionBindingException(
          String.format(
              "%s in phase %s takes its intermediate state as a value argument but got %s",
              function.anchor(), binding.phase(), state.kind()));
    }
    Type intermediate =
        deriveIntermediateType(
            (SimpleExtension.AggregateFunctionVariant) declaration, function.arguments());
    Type stateType = state.type().orElseThrow(IllegalStateException::new);
    if (!stateType.equals(intermediate)) {
      throw new InvalidFunctionBindingException(
          String.format(
              "%s in phase %s takes its intermediate state %s but got %s",
              function.anchor(), binding.phase(), intermediate, stateType));
    }
  }

  /**
   * Validates that a plan-declared output type exactly matches the type derived from the binding's
   * declaration.
   *
   * @param binding the resolved binding
   * @param declaredOutputType the output type declared by the plan
   * @throws InvalidFunctionBindingException if the declared type differs from the derived type
   */
  public static void validateOutputType(ResolvedFunctionBinding binding, Type declaredOutputType) {
    if (!binding.outputType().equals(declaredOutputType)) {
      throw InvalidFunctionBindingException.outputTypeMismatch(
          binding.anchor(), declaredOutputType, binding.outputType());
    }
  }

  /**
   * Derives the output type of a function from its declaration and arguments, applying the
   * declaration's nullability policy.
   *
   * @param declaration the function declaration
   * @param arguments the ordered resolved arguments
   * @return the derived output type
   */
  public static Type deriveOutputType(
      SimpleExtension.Function declaration, List<ResolvedArgument> arguments) {
    return derive(declaration.returnType(), declaration, arguments);
  }

  /**
   * Derives the intermediate type of a decomposable aggregate — the type its partial phases produce
   * — from its declaration and arguments, applying the declaration's nullability policy.
   *
   * <p>The type parameters are bound from the given arguments, so an intermediate expression that
   * refers to a parameter of the <em>initial</em> arguments cannot be derived from a phase that
   * only sees the intermediate value; that fails closed rather than guessing.
   *
   * @param declaration the aggregate function declaration
   * @param arguments the ordered resolved arguments
   * @return the derived intermediate type
   * @throws InvalidFunctionBindingException if the declaration is not decomposable or its
   *     intermediate expression cannot be derived
   */
  public static Type deriveIntermediateType(
      SimpleExtension.AggregateFunctionVariant declaration, List<ResolvedArgument> arguments) {
    TypeExpression intermediate = declaration.intermediate();
    if (declaration.decomposability() == SimpleExtension.Decomposability.NONE
        || intermediate == null) {
      throw new InvalidFunctionBindingException(
          String.format(
              "%s is not decomposable and has no intermediate state", declaration.getAnchor()));
    }
    return derive(intermediate, declaration, arguments);
  }

  private static Type derive(
      TypeExpression expression,
      SimpleExtension.Function declaration,
      List<ResolvedArgument> arguments) {
    List<Type> valueTypes = valueAndTypeArgumentTypes(arguments);
    Type base;
    try {
      base =
          TypeExpressionEvaluator.evaluateExpression(
              expression, declaration.args(), declaration.variadic(), valueTypes);
    } catch (UnsupportedOperationException e) {
      // Surface unresolved/inconsistent type expressions as a binding error rather than an
      // unchecked "not implemented" exception, so callers can handle them uniformly.
      throw new InvalidFunctionBindingException(
          String.format("Cannot derive type for %s: %s", declaration.getAnchor(), e.getMessage()));
    }
    if (declaration.nullability() == SimpleExtension.Nullability.MIRROR) {
      // MIRROR: the output is nullable iff any value argument is nullable.
      return base.withNullable(anyValueArgumentNullable(arguments));
    }
    // DECLARED_OUTPUT / DISCRETE: the declared type's nullability is authoritative.
    return base;
  }

  private static List<Type> valueAndTypeArgumentTypes(List<ResolvedArgument> arguments) {
    List<Type> types = new ArrayList<>();
    for (ResolvedArgument argument : arguments) {
      if (argument.kind() != ResolvedArgument.Kind.ENUM) {
        argument.type().ifPresent(types::add);
      }
    }
    return types;
  }

  private static boolean anyValueArgumentNullable(List<ResolvedArgument> arguments) {
    for (ResolvedArgument argument : arguments) {
      if (argument.kind() == ResolvedArgument.Kind.VALUE
          && argument.type().map(Type::nullable).orElse(false)) {
        return true;
      }
    }
    return false;
  }

  private static void validateSignature(
      SimpleExtension.Function declaration, List<ResolvedArgument> arguments) {
    List<SimpleExtension.Argument> declared = declaration.args();
    // getRange() already accounts for variadic min/max and required arguments.
    if (!declaration.getRange().within(arguments.size())) {
      throw new InvalidFunctionBindingException(
          String.format(
              "%s does not accept %d argument(s)", declaration.getAnchor(), arguments.size()));
    }
    if (declared.isEmpty()) {
      return;
    }
    boolean discrete = declaration.nullability() == SimpleExtension.Nullability.DISCRETE;
    // A variadic declaration whose parameters are INCONSISTENT repeats its trailing argument
    // without requiring the repetitions to agree with each other.
    boolean bindRepeats =
        declaration
            .variadic()
            .map(
                behavior ->
                    behavior.parameterConsistency()
                        == SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT)
            .orElse(false);
    Map<String, Type> wildcardBindings = new HashMap<>();
    for (int i = 0; i < arguments.size(); i++) {
      // For variadic functions the trailing declared argument repeats.
      SimpleExtension.Argument declaredArgument = declared.get(Math.min(i, declared.size() - 1));
      boolean repeated = i >= declared.size();
      checkArgument(
          declaration,
          i,
          declaredArgument,
          arguments.get(i),
          discrete,
          !repeated || bindRepeats,
          wildcardBindings);
    }
  }

  private static void checkArgument(
      SimpleExtension.Function declaration,
      int index,
      SimpleExtension.Argument declared,
      ResolvedArgument actual,
      boolean discrete,
      boolean bindWildcards,
      Map<String, Type> wildcardBindings) {
    if (declared instanceof SimpleExtension.ValueArgument) {
      requireKind(declaration, index, ResolvedArgument.Kind.VALUE, actual);
      Type actualType = actual.type().orElseThrow(IllegalStateException::new);
      ParameterizedType declaredType = ((SimpleExtension.ValueArgument) declared).value();
      if (declaredType instanceof ParameterizedType.StringLiteral
          && ((ParameterizedType.StringLiteral) declaredType).isWildcard()) {
        checkWildcardArgument(
            declaration,
            index,
            (ParameterizedType.StringLiteral) declaredType,
            actualType,
            discrete,
            bindWildcards,
            wildcardBindings);
      } else if (!typeMatches(declaredType, actualType, discrete)) {
        throw new InvalidFunctionBindingException(
            String.format(
                "%s argument %d: type %s is not compatible with declared %s",
                declaration.getAnchor(), index, actualType, declared.toTypeString()));
      }
    } else if (declared instanceof SimpleExtension.TypeArgument) {
      requireKind(declaration, index, ResolvedArgument.Kind.TYPE, actual);
    } else if (declared instanceof SimpleExtension.EnumArgument) {
      requireKind(declaration, index, ResolvedArgument.Kind.ENUM, actual);
      checkEnumOption(declaration, index, (SimpleExtension.EnumArgument) declared, actual);
    }
  }

  private static void checkWildcardArgument(
      SimpleExtension.Function declaration,
      int index,
      ParameterizedType.StringLiteral declared,
      Type actualType,
      boolean discrete,
      boolean bindWildcards,
      Map<String, Type> wildcardBindings) {
    if (discrete && declared.nullable() != actualType.nullable()) {
      // Under DISCRETE the declared nullability is part of the signature, wildcards included.
      throw new InvalidFunctionBindingException(
          String.format(
              "%s argument %d: type %s is not compatible with declared %s",
              declaration.getAnchor(), index, actualType, declared.value()));
    }
    if (!declared.isNumberedWildcard() || !bindWildcards) {
      // A plain "any" matches each argument independently; only a numbered wildcard (any1) has to
      // bind to a single type across the invocation.
      return;
    }
    String name = declared.value();
    Type existing = wildcardBindings.putIfAbsent(name, actualType);
    if (existing != null && !existing.equalsIgnoringNullability(actualType)) {
      throw new InvalidFunctionBindingException(
          String.format(
              "%s argument %d: wildcard '%s' is bound to both %s and %s",
              declaration.getAnchor(), index, name, existing, actualType));
    }
  }

  private static void checkEnumOption(
      SimpleExtension.Function declaration,
      int index,
      SimpleExtension.EnumArgument declared,
      ResolvedArgument actual) {
    Optional<String> option = actual.enumValue();
    if (!option.isPresent()) {
      // A plan may leave an enum argument unspecified; that is only valid where the declaration
      // does not require an option.
      if (declared.required()) {
        throw new InvalidFunctionBindingException(
            String.format(
                "%s argument %d: enum option is required but was left unspecified (expected one of"
                    + " %s)",
                declaration.getAnchor(), index, declared.options()));
      }
      return;
    }
    // Substrait matches enum option symbols case-insensitively.
    for (String allowed : declared.options()) {
      if (allowed.equalsIgnoreCase(option.get())) {
        return;
      }
    }
    throw new InvalidFunctionBindingException(
        String.format(
            "%s argument %d: enum option '%s' is not one of %s",
            declaration.getAnchor(), index, option.get(), declared.options()));
  }

  private static void requireKind(
      SimpleExtension.Function declaration,
      int index,
      ResolvedArgument.Kind expected,
      ResolvedArgument actual) {
    if (actual.kind() != expected) {
      throw new InvalidFunctionBindingException(
          String.format(
              "%s argument %d: expected %s argument but got %s",
              declaration.getAnchor(), index, expected, actual.kind()));
    }
  }

  private static boolean typeMatches(
      ParameterizedType declared, Type actual, boolean exactNullability) {
    if (declared instanceof ParameterizedType.StringLiteral) {
      // Non-wildcard extension parameter names at the top level are accepted; numbered wildcards
      // are handled by the caller for cross-argument consistency.
      return true;
    }
    if (declared instanceof Type) {
      // A concrete declared argument type (e.g. i32) matches ignoring nullability, except under a
      // DISCRETE declaration where argument nullability is part of the signature.
      return exactNullability
          ? actual.equals(declared)
          : actual.equalsIgnoringNullability((Type) declared);
    }
    if (declared instanceof ParameterizedType.Decimal) {
      return actual instanceof Type.Decimal
          && nullabilityMatches(declared, actual, exactNullability);
    }
    if (declared instanceof ParameterizedType.FixedChar) {
      return actual instanceof Type.FixedChar
          && nullabilityMatches(declared, actual, exactNullability);
    }
    if (declared instanceof ParameterizedType.VarChar) {
      return actual instanceof Type.VarChar
          && nullabilityMatches(declared, actual, exactNullability);
    }
    if (declared instanceof ParameterizedType.FixedBinary) {
      return actual instanceof Type.FixedBinary
          && nullabilityMatches(declared, actual, exactNullability);
    }
    if (declared instanceof ParameterizedType.PrecisionTimestamp) {
      return actual instanceof Type.PrecisionTimestamp
          && nullabilityMatches(declared, actual, exactNullability);
    }
    if (declared instanceof ParameterizedType.PrecisionTimestampTZ) {
      return actual instanceof Type.PrecisionTimestampTZ
          && nullabilityMatches(declared, actual, exactNullability);
    }
    // Other parameterized shapes (structs, lists, maps, ...) are accepted without deep checking.
    return true;
  }

  /**
   * Under a DISCRETE declaration the declared nullability is part of the signature, for a
   * parameterized argument as much as for a concrete one. A declared shape that carries no
   * nullability of its own has nothing to compare, so it is accepted.
   */
  private static boolean nullabilityMatches(
      ParameterizedType declared, Type actual, boolean exactNullability) {
    if (!exactNullability || !(declared instanceof NullableType)) {
      return true;
    }
    return ((NullableType) declared).nullable() == actual.nullable();
  }

  private static void validateOptions(
      SimpleExtension.Function declaration, List<FunctionOption> options) {
    // A binding keeps option names and values exactly as the plan spelled them, so lower-case both
    // sides here: the Substrait spec matches options case-insensitively.
    Map<String, List<String>> declaredOptions = new LinkedHashMap<>();
    for (Map.Entry<String, SimpleExtension.Option> entry : declaration.options().entrySet()) {
      List<String> values = new ArrayList<>();
      for (String value : entry.getValue().getValues()) {
        values.add(value.toLowerCase(Locale.ROOT));
      }
      declaredOptions.put(entry.getKey().toLowerCase(Locale.ROOT), values);
    }
    for (FunctionOption option : options) {
      String name = option.getName().toLowerCase(Locale.ROOT);
      List<String> allowed = declaredOptions.get(name);
      if (allowed == null) {
        throw new InvalidFunctionBindingException(
            String.format("%s has no option named '%s'", declaration.getAnchor(), name));
      }
      if (option.values().isEmpty()) {
        throw new InvalidFunctionBindingException(
            String.format(
                "%s option '%s' has an empty preference list", declaration.getAnchor(), name));
      }
      for (String value : option.values()) {
        if (!allowed.contains(value.toLowerCase(Locale.ROOT))) {
          throw new InvalidFunctionBindingException(
              String.format(
                  "%s option '%s' value '%s' is not one of %s",
                  declaration.getAnchor(), name, value, allowed));
        }
      }
    }
  }
}
