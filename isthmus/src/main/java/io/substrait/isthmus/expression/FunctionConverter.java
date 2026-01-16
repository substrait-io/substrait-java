package io.substrait.isthmus.expression;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Streams;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.extension.SimpleExtension.Argument;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ToTypeString;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.Utils;
import io.substrait.isthmus.expression.FunctionMappings.Sig;
import io.substrait.type.Type;
import io.substrait.util.Util;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for converting between Calcite {@link SqlOperator}s and Substrait function
 * invocations.
 *
 * <p>Supports Calcite → Substrait conversion via signature matching/coercion and Substrait →
 * Calcite lookup via function keys.
 *
 * @param <F> function variant type (e.g., ScalarFunctionVariant, AggregateFunctionVariant)
 * @param <T> return type produced when binding Substrait invocations
 * @param <C> generic call wrapper exposing operands and type
 */
public abstract class FunctionConverter<
    F extends SimpleExtension.Function, T, C extends FunctionConverter.GenericCall> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionConverter.class);

  /**
   * Maps Calcite {@link SqlOperator}s to {@link FunctionFinder}s for signature-based matching. Used
   * to locate Substrait functions based on Calcite calls and operand shapes.
   */
  protected final Map<SqlOperator, FunctionFinder> signatures;

  /** Calcite {@link RelDataTypeFactory} used for creating and inspecting relational types. */
  protected final RelDataTypeFactory typeFactory;

  /** Converter handling Substrait ↔ Calcite type mappings and nullability rules. */
  protected final TypeConverter typeConverter;

  /**
   * Calcite {@link org.apache.calcite.rex.RexBuilder} for constructing {@link
   * org.apache.calcite.rex.RexNode}s.
   */
  protected final RexBuilder rexBuilder;

  /**
   * Multimap from Substrait function key (e.g., canonical name) to Calcite {@link SqlOperator}s.
   * Enables reverse lookup when converting Substrait function invocations to Calcite operators.
   */
  protected final Multimap<String, SqlOperator> substraitFuncKeyToSqlOperatorMap;

  /**
   * Creates a converter with the given functions.
   *
   * <p>Last-wins precedence applies when multiple variants share the same name/signature.
   *
   * @param functions function variants to register
   * @param typeFactory Calcite type factory
   */
  public FunctionConverter(List<F> functions, RelDataTypeFactory typeFactory) {
    this(functions, Collections.EMPTY_LIST, typeFactory, TypeConverter.DEFAULT);
  }

  /**
   * Creates a converter with functions and additional operator signatures.
   *
   * <p>Last-wins precedence applies when multiple variants share the same name/signature.
   *
   * @param functions function variants to register
   * @param additionalSignatures extra Calcite operator signatures to map
   * @param typeFactory Calcite type factory
   * @param typeConverter type converter to Substrait
   */
  public FunctionConverter(
      List<F> functions,
      List<FunctionMappings.Sig> additionalSignatures,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    this.rexBuilder = new RexBuilder(typeFactory);
    this.typeConverter = typeConverter;
    List<FunctionMappings.Sig> signatures =
        new ArrayList<>(getSigs().size() + additionalSignatures.size());
    signatures.addAll(additionalSignatures);
    signatures.addAll(getSigs());
    this.typeFactory = typeFactory;
    this.substraitFuncKeyToSqlOperatorMap = ArrayListMultimap.create();

    ArrayListMultimap<String, F> nameToFn = ArrayListMultimap.<String, F>create();
    for (F f : functions) {
      nameToFn.put(f.name().toLowerCase(Locale.ROOT), f);
    }

    Multimap<String, FunctionMappings.Sig> calciteOperators =
        signatures.stream()
            .collect(
                Multimaps.toMultimap(
                    FunctionMappings.Sig::name, Function.identity(), ArrayListMultimap::create));
    Map<SqlOperator, FunctionFinder> matcherMap = new HashMap<>();
    for (String key : nameToFn.keySet()) {
      Collection<Sig> sigs = calciteOperators.get(key);
      if (sigs.isEmpty()) {
        LOGGER.atDebug().log("No binding for function: {}", key);
      }

      for (Sig sig : sigs) {
        List<F> implList = nameToFn.get(key);
        if (!implList.isEmpty()) {
          matcherMap.put(sig.operator(), new FunctionFinder(key, sig.operator(), implList));
        }
      }
    }

    for (Entry<String, F> entry : nameToFn.entries()) {
      String key = entry.getKey();
      F func = entry.getValue();
      for (FunctionMappings.Sig sig : calciteOperators.get(key)) {
        substraitFuncKeyToSqlOperatorMap.put(func.key(), sig.operator());
      }
    }

    this.signatures = matcherMap;
  }

  /**
   * Resolves a Calcite {@link SqlOperator} from a Substrait function key (Substrait → Calcite).
   * <p>
   * Given a Substrait function key (e.g., "concat:str_str") and output type, this method finds
   * the corresponding Calcite {@link SqlOperator}. When multiple operators match, the output type
   * is used to disambiguate.
   *
   * @param key Substrait function key (e.g., {@code concat:str_str})
   * @param outputType expected Substrait output type used for disambiguation
   * @return matching {@link SqlOperator}, or empty if none
   */
  public Optional<SqlOperator> getSqlOperatorFromSubstraitFunc(String key, Type outputType) {
    Map<SqlOperator, FunctionMappings.TypeBasedResolver> resolver = getTypeBasedResolver();
    Collection<SqlOperator> operators = substraitFuncKeyToSqlOperatorMap.get(key);
    if (operators.isEmpty()) {
      return Optional.empty();
    }

    // only one SqlOperator is possible
    if (operators.size() == 1) {
      return Optional.of(operators.iterator().next());
    }

    // at least 2 operators. Use output type to resolve SqlOperator.
    String outputTypeStr = outputType.accept(ToTypeString.INSTANCE);
    List<SqlOperator> resolvedOperators =
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
      throw new IllegalStateException(
          String.format(
              "Found %d SqlOperators: %s for ScalarFunction %s: ",
              resolvedOperators.size(), resolvedOperators, key));
    }
    return Optional.empty();
  }

  /**
   * Returns the resolver used to disambiguate Calcite operators by output type.
   *
   * @return map from {@link SqlOperator} to type-based resolver
   */
  private Map<SqlOperator, FunctionMappings.TypeBasedResolver> getTypeBasedResolver() {
    return FunctionMappings.OPERATOR_RESOLVER;
  }

  /**
   * Provides the set of Calcite operator signatures supported by this converter.
   *
   * @return immutable list of supported signatures
   */
  protected abstract ImmutableList<FunctionMappings.Sig> getSigs();

  /**
   * Helper class for locating and matching Calcite {@link org.apache.calcite.sql.SqlOperator}
   * signatures to Substrait functions.
   *
   * <p>Used during expression conversion to determine if a given {@link
   * org.apache.calcite.rex.RexCall} corresponds to a known Substrait function and to validate
   * argument counts.
   */
  protected class FunctionFinder {
    private final String substraitName;
    private final SqlOperator operator;
    private final List<F> functions;
    private final ListMultimap<String, F> directMap;
    private final Optional<SingularArgumentMatcher<F>> singularInputType;
    private final Util.IntRange argRange;

    /**
     * Creates a function finder for a Substrait name/operator over given variants.
     *
     * @param substraitName canonical Substrait function name
     * @param operator Calcite operator being matched
     * @param functions registered function variants for this name
     */
    public FunctionFinder(String substraitName, SqlOperator operator, List<F> functions) {
      this.substraitName = substraitName;
      this.operator = operator;
      this.functions = functions;
      this.argRange =
          Util.IntRange.of(
              functions.stream().mapToInt(t -> t.getRange().getStartInclusive()).min().getAsInt(),
              functions.stream().mapToInt(t -> t.getRange().getEndExclusive()).max().getAsInt());
      this.singularInputType = getSingularInputType(functions);
      ImmutableListMultimap.Builder<String, F> directMap = ImmutableListMultimap.builder();
      for (F func : functions) {
        String key = func.key();
        directMap.put(key, func);
        if (func.requiredArguments().size() != func.args().size()) {
          directMap.put(F.constructKey(substraitName, func.requiredArguments()), func);
        }
      }
      this.directMap = directMap.build();
    }

    /**
     * Returns whether the given argument count is within this operator's allowed range.
     *
     * @param count number of operands
     * @return {@code true} if allowed; otherwise {@code false}
     */
    public boolean allowedArgCount(int count) {
      return argRange.within(count);
    }

    /**
     * Attempts an exact signature match against required arguments and return type.
     *
     * @param inputTypes operand types (Substrait)
     * @param outputType expected output type (Substrait)
     * @return matching function variant if found; otherwise empty
     */
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
     * Checks that input types satisfy the function's required arguments.
     *
     * <p>Ensures variadic arguments share a type and matched wildcards (anyN) are consistent.
     *
     * @param inputTypes operand types to verify
     * @param args expected arguments as defined in a {@link SimpleExtension.Function}
     * @return {@code true} if compatible; otherwise {@code false}
     */
    private boolean inputTypesMatchDefinedArguments(
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
     * Derives singular-argument matchers for variants whose required arguments share one type.
     *
     * @param functions variants to inspect
     * @return optional matcher chain; empty if none
     */
    private Optional<SingularArgumentMatcher<F>> getSingularInputType(List<F> functions) {
      List<SingularArgumentMatcher<F>> matchers = new ArrayList<>();
      for (F f : functions) {

        ParameterizedType firstType = null;

        // determine if all the required arguments are the of the same type. If so,
        for (Argument a : f.requiredArguments()) {
          if (!(a instanceof SimpleExtension.ValueArgument)) {
            firstType = null;
            break;
          }

          ParameterizedType pt = ((SimpleExtension.ValueArgument) a).value();

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

      switch (matchers.size()) {
        case 0:
          return Optional.empty();
        case 1:
          return Optional.of(matchers.get(0));
        default:
          return Optional.of(chained(matchers));
      }
    }

    /**
     * Creates a matcher for a single repeated parameter type.
     *
     * @param function function variant
     * @param type repeated parameter type
     * @return matcher accepting input/output types
     */
    private SingularArgumentMatcher<F> singular(F function, ParameterizedType type) {
      return (inputType, outputType) -> {
        boolean check = isMatch(inputType, type);
        if (check) {
          return Optional.of(function);
        }
        return Optional.empty();
      };
    }

    /**
     * Chains multiple singular matchers, returning the first successful match.
     *
     * @param matchers matchers to try in order
     * @return composite matcher
     */
    private SingularArgumentMatcher<F> chained(List<SingularArgumentMatcher<F>> matchers) {
      return (inputType, outputType) -> {
        for (SingularArgumentMatcher<F> s : matchers) {
          Optional<F> outcome = s.tryMatch(inputType, outputType);
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
    /**
     * Produces candidate signature keys considering enum literals as required/optional.
     *
     * @param rexOperands operand RexNodes
     * @param opTypes operand type strings (Substrait)
     * @return stream of candidate key suffixes to test
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

    /**
     * Converts a Calcite call to a Substrait function invocation (Calcite → Substrait).
     *
     * <p>Tries direct signature match, then coercion, then least-restrictive type resolution.
     *
     * @param call generic call wrapper (operands and type)
     * @param topLevelConverter converter from {@link RexNode} to Substrait {@link Expression}
     * @return matched binding, or empty if none
     */
    public Optional<T> attemptMatch(C call, Function<RexNode, Expression> topLevelConverter) {

      /*
       * Here the RexLiteral with an Enum value is mapped to String Literal.
       * Not enough context here to construct a substrait EnumArg.
       * Once a FunctionVariant is resolved we can map the String Literal
       * to a EnumArg.
       *
       * Note that if there are multiple registered function extensions which can match a particular Call,
       * the last one added to the extension collection will be matched.
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
              .map(argList -> substraitName + ":" + argList)
              .filter(directMap::containsKey)
              .findFirst();

      if (directMatchKey.isPresent()) {
        List<F> variants = directMap.get(directMatchKey.get());
        if (variants.isEmpty()) {

          return Optional.empty();
        }

        F variant = variants.get(variants.size() - 1);
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

    /**
     * Tries matching using Calcite's least-restrictive type for operands.
     *
     * @param call generic call wrapper
     * @param outputType expected output type (Substrait)
     * @param operands converted operand expressions
     * @return binding if a singular-type variant matches; otherwise empty
     */
    private Optional<T> matchByLeastRestrictive(
        C call, Type outputType, List<Expression> operands) {
      RelDataType leastRestrictive =
          typeFactory.leastRestrictive(
              call.getOperands().map(RexNode::getType).collect(Collectors.toList()));
      if (leastRestrictive == null) {
        return Optional.empty();
      }
      Type type = typeConverter.toSubstrait(leastRestrictive);
      Optional<F> out = singularInputType.orElseThrow().tryMatch(type, outputType);

      return out.map(
          declaration -> {
            List<Expression> coercedArgs = coerceArguments(operands, type);
            declaration.validateOutputType(coercedArgs, outputType);
            return generateBinding(call, out.get(), coercedArgs, outputType);
          });
    }

    /**
     * Tries matching by coercing each operand to its Substrait type and checking signatures.
     *
     * @param call generic call wrapper
     * @param outputType expected output type (Substrait)
     * @param expressions operand expressions
     * @return binding if a signature match is found; otherwise empty
     */
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

      List<Expression> coercedArgs =
          Streams.zip(
                  expressions.stream(), operandTypes.stream(), FunctionConverter::coerceArgument)
              .collect(Collectors.toList());
      return Optional.of(generateBinding(call, matchFunction.get(), coercedArgs, outputType));
    }

    /**
     * Returns the canonical Substrait name this finder resolves.
     *
     * @return Substrait function name
     */
    protected String getSubstraitName() {
      return substraitName;
    }

    /**
     * Returns the Calcite operator associated with this finder.
     *
     * @return Calcite operator
     */
    public SqlOperator getOperator() {
      return operator;
    }
  }

  /**
   * Represents a generic function or operator call abstraction used during expression conversion.
   *
   * <p>Provides access to the operands and the resulting Calcite type of the call.
   */
  public interface GenericCall {
    /**
     * Returns the operand stream for this call.
     *
     * @return stream of {@link RexNode} operands
     */
    Stream<RexNode> getOperands();

    /**
     * Returns the Calcite result type of the call.
     *
     * @return {@link RelDataType} for the call
     */
    RelDataType getType();
  }

  /**
   * Coerces arguments to the target type when mismatched (ignores nullability/parameters).
   *
   * @param arguments input expressions
   * @param targetType target Substrait type
   * @return list of coerced expressions (casts applied as needed)
   */
  private static List<Expression> coerceArguments(List<Expression> arguments, Type targetType) {
    return arguments.stream().map(a -> coerceArgument(a, targetType)).collect(Collectors.toList());
  }

  /**
   * Coerces a single expression to the target type, if needed.
   *
   * @param argument expression to coerce
   * @param type target Substrait type
   * @return original expression or casted expression
   */
  private static Expression coerceArgument(Expression argument, Type type) {
    if (isMatch(type, argument.getType())) {
      return argument;
    }

    return ExpressionCreator.cast(type, argument, Expression.FailureBehavior.THROW_EXCEPTION);
  }

  /**
   * Creates the Substrait binding for a matched function variant.
   *
   * @param call generic call wrapper (operands and type)
   * @param function matched extension function variant
   * @param arguments converted function arguments
   * @param outputType expected Substrait output type
   * @return binding to return to the caller
   */
  protected abstract T generateBinding(
      C call, F function, List<? extends FunctionArg> arguments, Type outputType);

  /**
   * Matcher for functions whose required arguments share a single repeated type.
   *
   * @param <F> function variant type
   */
  @FunctionalInterface
  private interface SingularArgumentMatcher<F> {
    /**
     * Attempts a match for the provided input/output types.
     *
     * @param type singular input type
     * @param outputType expected output type
     * @return matching function if successful; otherwise empty
     */
    Optional<F> tryMatch(Type type, Type outputType);
  }

  /**
   * Compares parameterized types, allowing wildcards and ignoring nullability/parameters when
   * appropriate.
   *
   * @param actualType actual parameterized type
   * @param targetType target parameterized type
   * @return {@code true} if compatible; otherwise {@code false}
   */
  private static boolean isMatch(ParameterizedType actualType, ParameterizedType targetType) {
    if (targetType.isWildcard()) {
      return true;
    }
    return actualType.accept(new IgnoreNullableAndParameters(targetType));
  }
}
