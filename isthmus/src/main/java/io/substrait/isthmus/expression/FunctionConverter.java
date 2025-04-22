package io.substrait.isthmus.expression;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Streams;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ToTypeString;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.Utils;
import io.substrait.type.Type;
import io.substrait.util.Util;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

public abstract class FunctionConverter<
    F extends SimpleExtension.Function, T, C extends FunctionConverter.GenericCall> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionConverter.class);

  protected final Map<SqlOperator, FunctionFinder> signatures;
  protected final RelDataTypeFactory typeFactory;
  protected final TypeConverter typeConverter;
  protected final RexBuilder rexBuilder;

  protected final Multimap<String, SqlOperator> substraitFuncKeyToSqlOperatorMap;

  public FunctionConverter(List<F> functions, RelDataTypeFactory typeFactory) {
    this(functions, Collections.EMPTY_LIST, typeFactory, TypeConverter.DEFAULT);
  }

  public FunctionConverter(
      List<F> functions,
      List<FunctionMappings.Sig> additionalSignatures,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    this.rexBuilder = new RexBuilder(typeFactory);
    this.typeConverter = typeConverter;
    var signatures =
        new ArrayList<FunctionMappings.Sig>(getSigs().size() + additionalSignatures.size());
    signatures.addAll(additionalSignatures);
    signatures.addAll(getSigs());
    this.typeFactory = typeFactory;
    this.substraitFuncKeyToSqlOperatorMap = ArrayListMultimap.<String, SqlOperator>create();

    var alm = ArrayListMultimap.<String, F>create();
    for (var f : functions) {
      alm.put(f.name().toLowerCase(Locale.ROOT), f);
    }

    ListMultimap<String, FunctionMappings.Sig> calciteOperators =
        signatures.stream()
            .collect(
                Multimaps.toMultimap(
                    FunctionMappings.Sig::name, f -> f, () -> ArrayListMultimap.create()));
    var matcherMap = new IdentityHashMap<SqlOperator, FunctionFinder>();
    for (String key : alm.keySet()) {
      var sigs = calciteOperators.get(key);
      if (sigs == null) {
        logger.atInfo().log("Dropping function due to no binding: {}", key);
        continue;
      }

      for (var sig : sigs) {
        var implList = alm.get(key);
        if (implList == null || implList.isEmpty()) {
          continue;
        }

        matcherMap.put(sig.operator(), new FunctionFinder(key, sig.operator(), implList));
      }
    }

    for (var entry : alm.entries()) {
      String key = entry.getKey();
      var func = entry.getValue();
      for (FunctionMappings.Sig sig : calciteOperators.get(key)) {
        substraitFuncKeyToSqlOperatorMap.put(func.key(), sig.operator());
      }
    }

    this.signatures = matcherMap;
  }

  public Optional<SqlOperator> getSqlOperatorFromSubstraitFunc(String key, Type outputType) {
    var resolver = getTypeBasedResolver();
    if (!substraitFuncKeyToSqlOperatorMap.containsKey(key)) {
      return Optional.empty();
    }
    var operators = substraitFuncKeyToSqlOperatorMap.get(key);
    // only one SqlOperator is possible
    if (operators.size() == 1) {
      return Optional.of(operators.iterator().next());
    }
    // at least 2 operators. Use output type to resolve SqlOperator.
    String outputTypeStr = outputType.accept(ToTypeString.INSTANCE);
    var resolvedOperators =
        operators.stream()
            .filter(
                operator ->
                    resolver.containsKey(operator)
                        && resolver.get(operator).types().contains(outputTypeStr))
            .collect(Collectors.toList());
    // only one SqlOperator is possible
    if (resolvedOperators.size() == 1) {
      return Optional.of(resolvedOperators.get(0));
    } else if (resolvedOperators.size() > 1) {
      throw new RuntimeException(
          String.format(
              "Found %d SqlOperators: %s for ScalarFunction %s: ",
              resolvedOperators.size(), resolvedOperators, key));
    }
    return Optional.empty();
  }

  private Map<SqlOperator, FunctionMappings.TypeBasedResolver> getTypeBasedResolver() {
    return FunctionMappings.OPERATOR_RESOLVER;
  }

  protected abstract ImmutableList<FunctionMappings.Sig> getSigs();

  protected class FunctionFinder {
    private final String name;
    private final SqlOperator operator;
    private final List<F> functions;
    private final Map<String, F> directMap;
    private final Optional<SingularArgumentMatcher<F>> singularInputType;
    private final Util.IntRange argRange;

    public FunctionFinder(String name, SqlOperator operator, List<F> functions) {
      this.name = name;
      this.operator = operator;
      this.functions = functions;
      this.argRange =
          Util.IntRange.of(
              functions.stream().mapToInt(t -> t.getRange().getStartInclusive()).min().getAsInt(),
              functions.stream().mapToInt(t -> t.getRange().getEndExclusive()).max().getAsInt());
      this.singularInputType = getSingularInputType(functions);
      var directMap = ImmutableMap.<String, F>builder();
      for (var func : functions) {
        String key = func.key();
        directMap.put(key, func);
        if (func.requiredArguments().size() != func.args().size()) {
          directMap.put(F.constructKey(name, func.requiredArguments()), func);
        }
      }
      this.directMap = directMap.build();
    }

    public boolean allowedArgCount(int count) {
      return argRange.within(count);
    }

    private Optional<F> signatureMatch(List<Type> inputTypes, Type outputType) {
      for (F function : functions) {
        List<SimpleExtension.Argument> args = function.requiredArguments();
        // Make sure that arguments & return are within bounds and match the types
        if (function.returnType() instanceof ParameterizedType
            && isMatch(outputType, (ParameterizedType) function.returnType())
            && inputTypesMatchDefinedArguments(inputTypes, args)) {
          return Optional.of(function);
        }
      }

      return Optional.empty();
    }

    /**
     * Checks to see if the given input types satisfy the function arguments given. Checks that
     *
     * <ul>
     *   <li>Variadic arguments all have the same input type
     *   <li>Matched wildcard arguments (i.e.`any`, `any1`, `any2`, etc) all have the same input
     *       type
     * </ul>
     *
     * @param inputTypes input types to check against arguments
     * @param args expected arguments as defined in a {@link SimpleExtension.Function}
     * @return true if the {@code inputTypes} satisfy the {@code args}, false otherwise
     */
    private static boolean inputTypesMatchDefinedArguments(
        List<Type> inputTypes, List<SimpleExtension.Argument> args) {

      Map<String, Set<Type>> wildcardToType = new HashMap<>();
      for (int i = 0; i < inputTypes.size(); i++) {
        Type givenType = inputTypes.get(i);
        SimpleExtension.ValueArgument wantType =
            (SimpleExtension.ValueArgument)
                args.get(
                    // Variadic arguments should match the last argument's type
                    Integer.min(i, args.size() - 1));

        if (!isMatch(givenType, wantType.value())) {
          return false;
        }

        // Register the wildcard to type
        if (wantType.value().isWildcard()) {
          wildcardToType
              .computeIfAbsent(
                  wantType.value().accept(ToTypeString.ToTypeLiteralStringLossless.INSTANCE),
                  k -> new HashSet<>())
              .add(givenType);
        }
      }

      // If all the types match, check if the wildcard types are compatible.
      // TODO: Determine if non-enumerated wildcard types (i.e. `any` as opposed to `any1`) need to
      //   have the same type.
      return wildcardToType.values().stream().allMatch(s -> s.size() == 1);
    }

    /**
     * If some of the function variants for this function name have single, repeated argument type,
     * we will attempt to find matches using these patterns and least-restrictive casting.
     *
     * <p>If this exists, the function finder will attempt to find a least-restrictive match using
     * these.
     */
    private static <F extends SimpleExtension.Function>
        Optional<SingularArgumentMatcher<F>> getSingularInputType(List<F> functions) {
      List<SingularArgumentMatcher> matchers = new ArrayList<>();
      for (var f : functions) {

        ParameterizedType firstType = null;

        // determine if all the required arguments are the of the same type. If so,
        for (var a : f.requiredArguments()) {
          if (!(a instanceof SimpleExtension.ValueArgument)) {
            firstType = null;
            break;
          }

          var pt = ((SimpleExtension.ValueArgument) a).value();

          if (firstType == null) {
            firstType = pt;
          } else {
            // TODO: decide if this is too lenient.
            if (!isMatch(firstType, pt)) {
              firstType = null;
              break;
            }
          }
        }

        if (firstType != null) {
          matchers.add(singular(f, firstType));
        }
      }

      return switch (matchers.size()) {
        case 0 -> Optional.empty();
        case 1 -> Optional.of(matchers.get(0));
        default -> Optional.of(chained(matchers));
      };
    }

    public static <F extends SimpleExtension.Function> SingularArgumentMatcher<F> singular(
        F function, ParameterizedType type) {
      return (inputType, outputType) -> {
        var check = isMatch(inputType, type);
        if (check) {
          return Optional.of(function);
        }
        return Optional.empty();
      };
    }

    public static SingularArgumentMatcher chained(List<SingularArgumentMatcher> matchers) {
      return (inputType, outputType) -> {
        for (var s : matchers) {
          var outcome = s.tryMatch(inputType, outputType);
          if (outcome.isPresent()) {
            return outcome;
          }
        }

        return Optional.empty();
      };
    }

    /*
     * In case of a `RexLiteral` of an Enum value try both `req` and `op` signatures
     * for that argument position.
     */
    private Stream<String> matchKeys(List<RexNode> rexOperands, List<String> opTypes) {

      assert (rexOperands.size() == opTypes.size());

      if (rexOperands.isEmpty()) {
        return Stream.of("");
      } else {
        List<List<String>> argTypeLists =
            Streams.zip(
                    rexOperands.stream(),
                    opTypes.stream(),
                    (rexArg, opType) -> {
                      boolean isOption = false;
                      if (rexArg instanceof RexLiteral) {
                        isOption = ((RexLiteral) rexArg).getValue() instanceof Enum;
                      }
                      return isOption ? List.of("req", "opt") : List.of(opType);
                    })
                .collect(Collectors.toList());

        return Utils.crossProduct(argTypeLists)
            .map(typList -> typList.stream().collect(Collectors.joining("_")));
      }
    }

    public Optional<T> attemptMatch(C call, Function<RexNode, Expression> topLevelConverter) {

      /*
       * Here the RexLiteral with an Enum value is mapped to String Literal.
       * Not enough context here to construct a substrait EnumArg.
       * Once a FunctionVariant is resolved we can map the String Literal
       * to a EnumArg.
       */
      List<RexNode> operandsList = call.getOperands().collect(Collectors.toList());
      List<Expression> operands =
          call.getOperands().map(topLevelConverter).collect(Collectors.toList());
      List<Type> opTypes = operands.stream().map(Expression::getType).collect(Collectors.toList());

      Type outputType = typeConverter.toSubstrait(call.getType());

      // try to do a direct match
      List<String> typeStrings =
          opTypes.stream().map(t -> t.accept(ToTypeString.INSTANCE)).collect(Collectors.toList());
      Stream<String> possibleKeys = matchKeys(operandsList, typeStrings);

      Optional<String> directMatchKey =
          possibleKeys
              .map(argList -> name + ":" + argList)
              .filter(directMap::containsKey)
              .findFirst();

      if (directMatchKey.isPresent()) {
        F variant = directMap.get(directMatchKey.get());
        variant.validateOutputType(operands, outputType);
        List<FunctionArg> funcArgs =
            IntStream.range(0, operandsList.size())
                .mapToObj(
                    i -> {
                      RexNode r = operandsList.get(i);
                      Expression o = operands.get(i);
                      if (EnumConverter.isEnumValue(r)) {
                        return EnumConverter.fromRex(variant, (RexLiteral) r, i).orElse(null);
                      } else {
                        return o;
                      }
                    })
                .collect(Collectors.toList());
        boolean allArgsMapped = funcArgs.stream().filter(Objects::isNull).findFirst().isEmpty();
        if (allArgsMapped) {
          return Optional.of(generateBinding(call, variant, funcArgs, outputType));
        } else {
          return Optional.empty();
        }
      }

      if (singularInputType.isPresent()) {
        Optional<T> coerced = matchCoerced(call, outputType, operands);
        if (coerced.isPresent()) {
          return coerced;
        }
        Optional<T> leastRestrictive = matchByLeastRestrictive(call, outputType, operands);
        if (leastRestrictive.isPresent()) {
          return leastRestrictive;
        }
      }
      return Optional.empty();
    }

    private Optional<T> matchByLeastRestrictive(
        C call, Type outputType, List<Expression> operands) {
      RelDataType leastRestrictive =
          typeFactory.leastRestrictive(
              call.getOperands().map(RexNode::getType).collect(Collectors.toList()));
      if (leastRestrictive == null) {
        return Optional.empty();
      }
      Type type = typeConverter.toSubstrait(leastRestrictive);
      var out = singularInputType.orElseThrow().tryMatch(type, outputType);

      return out.map(
          declaration -> {
            var coercedArgs = coerceArguments(operands, type);
            declaration.validateOutputType(coercedArgs, outputType);
            return generateBinding(call, out.get(), coercedArgs, outputType);
          });
    }

    private Optional<T> matchCoerced(C call, Type outputType, List<Expression> expressions) {
      // Convert the operands to the proper Substrait type
      List<Type> operandTypes =
          call.getOperands()
              .map(RexNode::getType)
              .map(typeConverter::toSubstrait)
              .collect(Collectors.toList());

      // See if all the input types can be made to match the function
      Optional<F> matchFunction = signatureMatch(operandTypes, outputType);
      if (matchFunction.isEmpty()) {
        return Optional.empty();
      }

      var coercedArgs =
          Streams.zip(
                  expressions.stream(), operandTypes.stream(), FunctionConverter::coerceArgument)
              .collect(Collectors.toList());
      return Optional.of(generateBinding(call, matchFunction.get(), coercedArgs, outputType));
    }

    protected String getName() {
      return name;
    }

    public SqlOperator getOperator() {
      return operator;
    }
  }

  public interface GenericCall {
    Stream<RexNode> getOperands();

    RelDataType getType();
  }

  /**
   * Coerced types according to an expected output type. Coercion is only done for type mismatches,
   * not for nullability or parameter mismatches.
   */
  private static List<Expression> coerceArguments(List<Expression> arguments, Type targetType) {
    return arguments.stream().map(a -> coerceArgument(a, targetType)).collect(Collectors.toList());
  }

  private static Expression coerceArgument(Expression argument, Type type) {
    if (isMatch(type, argument.getType())) {
      return argument;
    }

    return ExpressionCreator.cast(type, argument, Expression.FailureBehavior.THROW_EXCEPTION);
  }

  protected abstract T generateBinding(
      C call, F function, List<? extends FunctionArg> arguments, Type outputType);

  @FunctionalInterface
  private interface SingularArgumentMatcher<F> {
    Optional<F> tryMatch(Type type, Type outputType);
  }

  private static boolean isMatch(ParameterizedType actualType, ParameterizedType targetType) {
    if (targetType.isWildcard()) {
      return true;
    }
    return actualType.accept(new IgnoreNullableAndParameters(targetType));
  }
}
