package io.substrait.type;

import io.substrait.extension.SimpleExtension;
import io.substrait.function.NullableType;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.function.TypeExpressionVisitor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

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
 * <p>Supported shapes are concrete types, numbered wildcards ({@code any1}), and the parameterized
 * type classes whose parameter is an integer to substitute: {@code DECIMAL<P,S>}, {@code
 * varchar<L1>}, {@code fixedchar<L1>}, {@code fixedbinary<L1>}, {@code precision_time<P>}, {@code
 * precision_timestamp<P>}, {@code precision_timestamp_tz<P>}, {@code interval_day<P>} and {@code
 * interval_compound<P>}. Integer arithmetic, comparisons, boolean operations and conditionals can
 * appear in those parameters or in the assignments of a multi-line return program. Intermediate
 * results are signed 64-bit values; overflow and narrowing to a type parameter's 32-bit
 * representation are checked rather than wrapped. An integer literal itself must fit in 32 bits,
 * because the parser reads it as an {@code int}. The spec settles neither of two choices made here:
 * integer division truncates toward zero, and {@code AND} and {@code OR} evaluate both operands.
 * Both follow substrait-go. The two branches of a conditional must agree in kind (type, integer or
 * boolean), checked from their shape without evaluating the branch not taken. Neither the spec nor
 * substrait-go asks for this; it makes a declaration whose branches disagree fail for every
 * argument, not only for those that choose the bad branch. No standard extension declares a
 * parameterized {@code fixedbinary} or {@code interval_compound} at all, as an argument or as a
 * return -- those two are supported for symmetry, and pinned against hand-written declarations
 * rather than the catalog.
 *
 * <p>List, map, struct and function declarations bind their element, field, parameter and return
 * types recursively. Container returns also evaluate their children, including integer parameters
 * such as {@code List<varchar<L1>>} and type parameters such as {@code list<any1>}. Nested
 * nullability is preserved; only the outermost argument nullability is excluded from wildcard
 * identity. A program referring to an argument's value rather than a parameter of its type still
 * fails: this API receives only argument types. A plain {@code any} has no identity to bind, so it
 * cannot be derived as a return type.
 *
 * <p>Which shipped variants those cover is pinned by {@code ParameterizedReturnTypeTest} against
 * the declarations the catalog ships, and deliberately not repeated here -- the catalog is owned
 * upstream, so a list of names in this Javadoc would go stale on a {@code substrait-packaging} bump
 * with nothing to catch it.
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
    try {
      return new ReturnTypeEvaluator(returnExpression, bindings)
          .evaluate(returnExpression, Type.class);
    } catch (ArithmeticException e) {
      throw new UnsupportedOperationException(
          "Cannot evaluate return-type arithmetic: " + e.getMessage(), e);
    }
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
      bindings.bind(declared, actualTypes.get(index), !repeated || bindRepeats, false);
    }
    return bindings;
  }

  /** The wildcard and integer type parameters bound from a call site's actual argument types. */
  private static final class ParameterBindings {

    private final Map<String, Type> types = new HashMap<>();
    private final Set<String> exactTypeNullabilities = new HashSet<>();
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
    private void bind(ParameterizedType declared, Type actual, boolean bindNames, boolean nested) {
      if (nested && !(declared instanceof ParameterizedType.StringLiteral)) {
        if ((declared instanceof NullableType
                && ((NullableType) declared).nullable() != actual.nullable())
            || (declared instanceof Type && !declared.equals(actual))) {
          throw cannotBind(declared, actual);
        }
      }
      if (declared instanceof ParameterizedType.StringLiteral) {
        ParameterizedType.StringLiteral literal = (ParameterizedType.StringLiteral) declared;
        // Only a numbered wildcard names a parameter that a return expression can refer to and that
        // has to stay consistent across the call; a plain "any" binds independently each time.
        if (nested && literal.nullable() && !actual.nullable()) {
          throw cannotBind(declared, actual);
        }
        if (bindNames && literal.isNumberedWildcard()) {
          // An unmarked nested wildcard binds the complete type, including nullability. A '?'
          // marker requires a nullable actual, but does not constrain the variable's own
          // nullability: both i32 and i32? become i32? after substitution.
          boolean exactNullability = !nested || !literal.nullable();
          Type binding = nested && !literal.nullable() ? actual : actual.withNullable(false);
          bindType(literal.value(), binding, exactNullability);
        }
      } else if (declared instanceof ParameterizedType.Decimal && actual instanceof Type.Decimal) {
        ParameterizedType.Decimal declaredDecimal = (ParameterizedType.Decimal) declared;
        Type.Decimal actualDecimal = (Type.Decimal) actual;
        bindInteger(declaredDecimal.precision().value(), actualDecimal.precision(), bindNames);
        bindInteger(declaredDecimal.scale().value(), actualDecimal.scale(), bindNames);
      } else if (declared instanceof ParameterizedType.FixedChar
          && actual instanceof Type.FixedChar) {
        bindInteger(
            ((ParameterizedType.FixedChar) declared).length().value(),
            ((Type.FixedChar) actual).length(),
            bindNames);
      } else if (declared instanceof ParameterizedType.VarChar && actual instanceof Type.VarChar) {
        bindInteger(
            ((ParameterizedType.VarChar) declared).length().value(),
            ((Type.VarChar) actual).length(),
            bindNames);
      } else if (declared instanceof ParameterizedType.FixedBinary
          && actual instanceof Type.FixedBinary) {
        bindInteger(
            ((ParameterizedType.FixedBinary) declared).length().value(),
            ((Type.FixedBinary) actual).length(),
            bindNames);
      } else if (declared instanceof ParameterizedType.PrecisionTime
          && actual instanceof Type.PrecisionTime) {
        bindInteger(
            ((ParameterizedType.PrecisionTime) declared).precision().value(),
            ((Type.PrecisionTime) actual).precision(),
            bindNames);
      } else if (declared instanceof ParameterizedType.PrecisionTimestamp
          && actual instanceof Type.PrecisionTimestamp) {
        bindInteger(
            ((ParameterizedType.PrecisionTimestamp) declared).precision().value(),
            ((Type.PrecisionTimestamp) actual).precision(),
            bindNames);
      } else if (declared instanceof ParameterizedType.PrecisionTimestampTZ
          && actual instanceof Type.PrecisionTimestampTZ) {
        bindInteger(
            ((ParameterizedType.PrecisionTimestampTZ) declared).precision().value(),
            ((Type.PrecisionTimestampTZ) actual).precision(),
            bindNames);
      } else if (declared instanceof ParameterizedType.IntervalDay
          && actual instanceof Type.IntervalDay) {
        bindInteger(
            ((ParameterizedType.IntervalDay) declared).precision().value(),
            ((Type.IntervalDay) actual).precision(),
            bindNames);
      } else if (declared instanceof ParameterizedType.IntervalCompound
          && actual instanceof Type.IntervalCompound) {
        bindInteger(
            ((ParameterizedType.IntervalCompound) declared).precision().value(),
            ((Type.IntervalCompound) actual).precision(),
            bindNames);
      } else if (declared instanceof ParameterizedType.ListType
          && actual instanceof Type.ListType) {
        bind(
            ((ParameterizedType.ListType) declared).name(),
            ((Type.ListType) actual).elementType(),
            bindNames,
            true);
      } else if (declared instanceof ParameterizedType.Map && actual instanceof Type.Map) {
        ParameterizedType.Map pattern = (ParameterizedType.Map) declared;
        Type.Map map = (Type.Map) actual;
        bind(pattern.key(), map.key(), bindNames, true);
        bind(pattern.value(), map.value(), bindNames, true);
      } else if (declared instanceof ParameterizedType.Struct && actual instanceof Type.Struct) {
        bindFields(
            ((ParameterizedType.Struct) declared).fields(),
            ((Type.Struct) actual).fields(),
            bindNames);
      } else if (declared instanceof ParameterizedType.Func && actual instanceof Type.Func) {
        ParameterizedType.Func pattern = (ParameterizedType.Func) declared;
        Type.Func function = (Type.Func) actual;
        bindFields(pattern.parameterTypes(), function.parameterTypes(), bindNames);
        bind(pattern.returnType(), function.returnType(), bindNames, true);
      } else if (!(declared instanceof Type)) {
        throw cannotBind(declared, actual);
      }
    }

    private void bindFields(
        List<ParameterizedType> declared, List<Type> actual, boolean bindNames) {
      if (declared.size() != actual.size()) {
        throw new UnsupportedOperationException(
            "Cannot bind container fields: expected "
                + declared.size()
                + " types but got "
                + actual.size());
      }
      for (int index = 0; index < declared.size(); index++) {
        bind(declared.get(index), actual.get(index), bindNames, true);
      }
    }

    private static UnsupportedOperationException cannotBind(
        ParameterizedType declared, Type actual) {
      return new UnsupportedOperationException(
          String.format(
              "Cannot bind parameters from declared argument type %s to actual type %s",
              declared, actual));
    }

    private void bindType(String name, Type actual, boolean exactNullability) {
      Type existing = types.putIfAbsent(name, actual);
      boolean existingExact = exactTypeNullabilities.contains(name);
      if (existing != null
          && (!existing.equalsIgnoringNullability(actual)
              || (existingExact && exactNullability && !existing.equals(actual)))) {
        throw new UnsupportedOperationException(
            String.format(
                "Inconsistent binding for type parameter '%s': %s vs %s", name, existing, actual));
      }
      if (exactNullability) {
        exactTypeNullabilities.add(name);
        if (!existingExact) {
          types.put(name, actual);
        }
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
      extends TypeExpressionVisitor.TypeExpressionThrowsVisitor<Object, RuntimeException> {

    private final ParameterBindings bindings;
    // The derivation language has three value kinds: integer, boolean and type. Local assignments
    // can hold any of them; each operation checks the kind it consumes.
    private final Map<String, Object> locals = new HashMap<>();
    private boolean inProgram;

    private ReturnTypeEvaluator(TypeExpression returnExpression, ParameterBindings bindings) {
      // Rendered only on failure: the expression can be a whole return program, and most
      // derivations succeed.
      super(() -> "Cannot evaluate return-type expression: " + returnExpression);
      this.bindings = bindings;
    }

    @Override
    public Type visit(ParameterizedType.Decimal decimal) {
      return decimal(decimal.nullable(), decimal.precision(), decimal.scale());
    }

    @Override
    public Type visit(ParameterizedType.FixedChar fixedChar) {
      return fixedChar(fixedChar.nullable(), fixedChar.length());
    }

    @Override
    public Type visit(ParameterizedType.VarChar varChar) {
      return varChar(varChar.nullable(), varChar.length());
    }

    @Override
    public Type visit(ParameterizedType.FixedBinary fixedBinary) {
      return fixedBinary(fixedBinary.nullable(), fixedBinary.length());
    }

    @Override
    public Type visit(ParameterizedType.PrecisionTime precisionTime) {
      return precisionTime(precisionTime.nullable(), precisionTime.precision());
    }

    @Override
    public Type visit(ParameterizedType.PrecisionTimestamp precisionTimestamp) {
      return precisionTimestamp(precisionTimestamp.nullable(), precisionTimestamp.precision());
    }

    @Override
    public Type visit(ParameterizedType.PrecisionTimestampTZ precisionTimestampTZ) {
      return precisionTimestampTZ(
          precisionTimestampTZ.nullable(), precisionTimestampTZ.precision());
    }

    @Override
    public Type visit(ParameterizedType.IntervalDay intervalDay) {
      return intervalDay(intervalDay.nullable(), intervalDay.precision());
    }

    @Override
    public Type visit(ParameterizedType.IntervalCompound intervalCompound) {
      return intervalCompound(intervalCompound.nullable(), intervalCompound.precision());
    }

    @Override
    public Type visit(ParameterizedType.ListType list) {
      return TypeCreator.of(list.nullable()).list(evaluateNested(list.name()));
    }

    @Override
    public Type visit(ParameterizedType.Map map) {
      return TypeCreator.of(map.nullable())
          .map(evaluateNested(map.key()), evaluateNested(map.value()));
    }

    @Override
    public Type visit(ParameterizedType.Struct struct) {
      return TypeCreator.of(struct.nullable())
          .struct(struct.fields().stream().map(this::evaluateNested).collect(Collectors.toList()));
    }

    @Override
    public Type visit(ParameterizedType.Func function) {
      return TypeCreator.of(function.nullable())
          .func(
              function.parameterTypes().stream()
                  .map(this::evaluateNested)
                  .collect(Collectors.toList()),
              evaluateNested(function.returnType()));
    }

    /**
     * Evaluates a type nested in a container. Unlike a top-level name, a nested name keeps the
     * nullability it was bound with, and a {@code ?} marker only widens it.
     */
    private Type evaluateNested(ParameterizedType expression) {
      if (expression instanceof ParameterizedType.StringLiteral) {
        ParameterizedType.StringLiteral variable = (ParameterizedType.StringLiteral) expression;
        Object local = locals.get(variable.value());
        Type bound = local instanceof Type ? (Type) local : bindings.boundType(variable.value());
        if (bound != null) {
          if (local == null
              && !variable.nullable()
              && !bindings.exactTypeNullabilities.contains(variable.value())) {
            throw new UnsupportedOperationException(
                "Cannot derive nullability of type parameter '" + variable.value() + "'");
          }
          return bound.withNullable(bound.nullable() || variable.nullable());
        }
      }
      return evaluate(expression, Type.class);
    }

    @Override
    public Object visit(ParameterizedType.StringLiteral stringLiteral) {
      Object local = locals.get(stringLiteral.value());
      if (local != null) {
        return local instanceof Type
            ? ((Type) local).withNullable(stringLiteral.nullable())
            : local;
      }
      Integer integer = bindings.boundInteger(stringLiteral.value());
      if (integer != null) {
        return integer.longValue();
      }
      // A wildcard return (e.g. min(any1) -> any1) resolves to the bound argument type, taking the
      // nullability declared on the return expression in both directions (a required return forces
      // the type non-null, a nullable one forces it nullable). MIRROR policy, if any, is applied
      // afterwards by the caller. This is checked before parsing the token as an integer literal
      // because parseIntegerLiteral reports "not a number" by throwing, and a bound type name is
      // never a numeral.
      Type bound = bindings.boundType(stringLiteral.value());
      if (bound != null) {
        return bound.withNullable(stringLiteral.nullable());
      }
      OptionalInt literal = parseIntegerLiteral(stringLiteral.value());
      if (literal.isPresent()) {
        return (long) literal.getAsInt();
      }
      throw new UnsupportedOperationException(
          "Unbound type parameter '" + stringLiteral.value() + "' in return-type expression");
    }

    @Override
    public Type visit(TypeExpression.Decimal decimal) {
      return decimal(decimal.nullable(), decimal.precision(), decimal.scale());
    }

    @Override
    public Type visit(TypeExpression.FixedChar fixedChar) {
      return fixedChar(fixedChar.nullable(), fixedChar.length());
    }

    @Override
    public Type visit(TypeExpression.VarChar varChar) {
      return varChar(varChar.nullable(), varChar.length());
    }

    @Override
    public Type visit(TypeExpression.FixedBinary fixedBinary) {
      return fixedBinary(fixedBinary.nullable(), fixedBinary.length());
    }

    @Override
    public Type visit(TypeExpression.PrecisionTime precisionTime) {
      return precisionTime(precisionTime.nullable(), precisionTime.precision());
    }

    @Override
    public Type visit(TypeExpression.PrecisionTimestamp precisionTimestamp) {
      return precisionTimestamp(precisionTimestamp.nullable(), precisionTimestamp.precision());
    }

    @Override
    public Type visit(TypeExpression.PrecisionTimestampTZ precisionTimestampTZ) {
      return precisionTimestampTZ(
          precisionTimestampTZ.nullable(), precisionTimestampTZ.precision());
    }

    @Override
    public Type visit(TypeExpression.IntervalDay intervalDay) {
      return intervalDay(intervalDay.nullable(), intervalDay.precision());
    }

    @Override
    public Type visit(TypeExpression.IntervalCompound intervalCompound) {
      return intervalCompound(intervalCompound.nullable(), intervalCompound.precision());
    }

    @Override
    public Object visit(TypeExpression.ReturnProgram program) {
      // The grammar lets an assignment's right-hand side be a program of its own, and nothing
      // defines its scope. The locals here are one flat map, so an inner program's assignments
      // would leak into the enclosing one; refuse it instead.
      if (inProgram) {
        throw new UnsupportedOperationException(
            "Cannot evaluate a return program nested in another: " + program);
      }
      inProgram = true;
      for (TypeExpression.ReturnProgram.Assignment assignment : program.assignments()) {
        locals.put(assignment.name(), evaluate(assignment.expr(), Object.class));
      }
      return evaluate(program.finalExpression(), Type.class);
    }

    @Override
    public Long visit(TypeExpression.IntegerLiteral literal) {
      return (long) literal.value();
    }

    @Override
    public Object visit(TypeExpression.IfOperation conditional) {
      branchKind(conditional);
      return evaluate(
          evaluate(conditional.ifCondition(), Boolean.class)
              ? conditional.thenExpr()
              : conditional.elseExpr(),
          Object.class);
    }

    @Override
    public Boolean visit(TypeExpression.NotOperation operation) {
      return !evaluate(operation.inner(), Boolean.class);
    }

    @Override
    public Object visit(TypeExpression.BinaryOperation operation) {
      switch (operation.opType()) {
        case AND:
        case OR:
          boolean left = evaluate(operation.left(), Boolean.class);
          boolean right = evaluate(operation.right(), Boolean.class);
          return operation.opType() == TypeExpression.BinaryOperation.OpType.AND
              ? left && right
              : left || right;
        default:
          break;
      }
      long left = evaluate(operation.left(), Long.class);
      long right = evaluate(operation.right(), Long.class);
      switch (operation.opType()) {
        case ADD:
          return Math.addExact(left, right);
        case SUBTRACT:
          return Math.subtractExact(left, right);
        case MULTIPLY:
          return Math.multiplyExact(left, right);
        case DIVIDE:
          if (left == Long.MIN_VALUE && right == -1) {
            throw new ArithmeticException("long overflow");
          }
          return left / right;
        case MIN:
          return Math.min(left, right);
        case MAX:
          return Math.max(left, right);
        case LT:
          return left < right;
        case GT:
          return left > right;
        case LTE:
          return left <= right;
        case GTE:
          return left >= right;
        case EQ:
          return left == right;
        case NOT_EQ:
          return left != right;
        default:
          throw new UnsupportedOperationException(
              "Cannot evaluate operation " + operation.opType());
      }
    }

    // The ParameterizedType and TypeExpression forms of each parameterized type class evaluate
    // alike, so both visits above delegate to one helper per class.

    private Type decimal(boolean nullable, TypeExpression precision, TypeExpression scale) {
      return TypeCreator.of(nullable).decimal(resolveInteger(precision), resolveInteger(scale));
    }

    private Type fixedChar(boolean nullable, TypeExpression length) {
      return TypeCreator.of(nullable).fixedChar(resolveInteger(length));
    }

    private Type varChar(boolean nullable, TypeExpression length) {
      return TypeCreator.of(nullable).varChar(resolveInteger(length));
    }

    private Type fixedBinary(boolean nullable, TypeExpression length) {
      return TypeCreator.of(nullable).fixedBinary(resolveInteger(length));
    }

    private Type precisionTime(boolean nullable, TypeExpression precision) {
      return TypeCreator.of(nullable).precisionTime(resolveInteger(precision));
    }

    private Type precisionTimestamp(boolean nullable, TypeExpression precision) {
      return TypeCreator.of(nullable).precisionTimestamp(resolveInteger(precision));
    }

    private Type precisionTimestampTZ(boolean nullable, TypeExpression precision) {
      return TypeCreator.of(nullable).precisionTimestampTZ(resolveInteger(precision));
    }

    private Type intervalDay(boolean nullable, TypeExpression precision) {
      return TypeCreator.of(nullable).intervalDay(resolveInteger(precision));
    }

    private Type intervalCompound(boolean nullable, TypeExpression precision) {
      return TypeCreator.of(nullable).intervalCompound(resolveInteger(precision));
    }

    /**
     * Checks that both branches of a conditional agree in kind and returns that kind, or null when
     * neither branch's shape says. Only the chosen branch is evaluated, so without this a
     * declaration whose branches disagree fails only for the arguments that choose the bad one.
     */
    private Class<?> branchKind(TypeExpression.IfOperation conditional) {
      Class<?> thenKind = kindOf(conditional.thenExpr());
      Class<?> elseKind = kindOf(conditional.elseExpr());
      if (thenKind != null && elseKind != null && thenKind != elseKind) {
        throw new UnsupportedOperationException(
            String.format(
                "The branches of %s differ in kind: %s and %s",
                conditional, thenKind.getSimpleName(), elseKind.getSimpleName()));
      }
      return thenKind != null ? thenKind : elseKind;
    }

    /**
     * The kind an expression evaluates to, read from its shape without evaluating it: {@link Type},
     * {@link Long} or {@link Boolean}, the kinds {@link #evaluate} checks. A name is looked up in
     * the same order {@link #visit(ParameterizedType.StringLiteral)} resolves it; a name bound to
     * nothing yields null, since only evaluating it would fail.
     */
    private Class<?> kindOf(TypeExpression expression) {
      if (expression instanceof ParameterizedType.StringLiteral) {
        String name = ((ParameterizedType.StringLiteral) expression).value();
        Object local = locals.get(name);
        if (local != null) {
          return local instanceof Type ? Type.class : local.getClass();
        }
        if (bindings.boundInteger(name) != null) {
          return Long.class;
        }
        if (bindings.boundType(name) != null) {
          return Type.class;
        }
        return parseIntegerLiteral(name).isPresent() ? Long.class : null;
      }
      if (expression instanceof TypeExpression.IntegerLiteral) {
        return Long.class;
      }
      if (expression instanceof TypeExpression.NotOperation) {
        return Boolean.class;
      }
      if (expression instanceof TypeExpression.BinaryOperation) {
        switch (((TypeExpression.BinaryOperation) expression).opType()) {
          case ADD:
          case SUBTRACT:
          case MULTIPLY:
          case DIVIDE:
          case MIN:
          case MAX:
            return Long.class;
          default:
            return Boolean.class;
        }
      }
      if (expression instanceof TypeExpression.IfOperation) {
        return branchKind((TypeExpression.IfOperation) expression);
      }
      // Everything else is a type: concrete, parameterized, or a program whose last line is one.
      return Type.class;
    }

    private int resolveInteger(TypeExpression expression) {
      return Math.toIntExact(evaluate(expression, Long.class));
    }

    private <T> T evaluate(TypeExpression expression, Class<T> expectedKind) {
      Object result = expression instanceof Type ? expression : expression.accept(this);
      if (!expectedKind.isInstance(result)) {
        throw new UnsupportedOperationException(
            String.format(
                "Expected %s in return-type expression, got %s",
                expectedKind.getSimpleName(), result));
      }
      return expectedKind.cast(result);
    }
  }
}
