package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import com.google.common.math.LongMath;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.FunctionOption;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.SimpleExtensionToSqlOperator;
import io.substrait.isthmus.SubstraitTypeSystem;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Converts Calcite {@link RexCall} scalar functions to Substrait {@link Expression} using known
 * Substrait {@link SimpleExtension.ScalarFunctionVariant} declarations.
 *
 * <p>Supports custom function mappers for special cases (e.g., TRIM, SQRT), and falls back to
 * default signature-based matching. Produces {@link Expression.ScalarFunctionInvocation},
 * optionally wrapped in a cast to preserve Calcite's inferred result type for datetime calls.
 */
public class ScalarFunctionConverter
    extends FunctionConverter<
        SimpleExtension.ScalarFunctionVariant,
        Expression,
        ScalarFunctionConverter.WrappedScalarCall>
    implements CallConverter {
  /**
   * Function mappers provide a hook point for any custom mapping to Substrait functions and
   * arguments.
   */
  private final List<ScalarFunctionMapper> mappers;

  private final List<ScalarFunctionOptionPolicy> optionPolicies =
      List.of(
          new StringFunctionOptions(),
          new IntegerFunctionOptions(),
          new DecimalFunctionOptions(),
          new FloatingPointFunctionOptions());

  /**
   * Creates a converter with the given functions and type factory.
   *
   * @param functions available Substrait scalar function variants
   * @param typeFactory Calcite type factory for type conversions
   */
  public ScalarFunctionConverter(
      List<SimpleExtension.ScalarFunctionVariant> functions, RelDataTypeFactory typeFactory) {
    this(functions, Collections.emptyList(), typeFactory, TypeConverter.DEFAULT);
  }

  /**
   * Creates a converter with additional signatures and a custom type converter.
   *
   * @param functions available Substrait scalar function variants
   * @param additionalSignatures extra Calcite-to-Substrait signature mappings
   * @param typeFactory Calcite type factory for type conversions
   * @param typeConverter converter for Calcite {@link RelDataType} to Substrait {@link Type}
   */
  public ScalarFunctionConverter(
      List<SimpleExtension.ScalarFunctionVariant> functions,
      List<FunctionMappings.Sig> additionalSignatures,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    super(functions, additionalSignatures, typeFactory, typeConverter);

    mappers =
        List.of(
            new DatetimeSubtractionFunctionMapper(functions),
            new ConcatFunctionMapper(functions),
            new TrimFunctionMapper(functions),
            new SqrtFunctionMapper(functions, typeFactory),
            new ExtractDateFunctionMapper(functions),
            new PositionFunctionMapper(functions),
            new StrptimeDateFunctionMapper(functions),
            new StrptimeTimeFunctionMapper(functions),
            new StrptimeTimestampFunctionMapper(functions));
  }

  /**
   * Returns the set of known scalar function signatures.
   *
   * @return immutable list of scalar signatures
   */
  @Override
  protected ImmutableList<FunctionMappings.Sig> getSigs() {
    return FunctionMappings.SCALAR_SIGS;
  }

  /**
   * Converts a {@link RexCall} into a Substrait {@link Expression}, applying any registered custom
   * mapping first, then default matching if needed.
   *
   * @param call the Calcite function call to convert
   * @param topLevelConverter converter for nested operands
   * @return the converted expression if a match is found; otherwise {@link Optional#empty()}
   */
  @Override
  public Optional<Expression> convert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    // If a mapping applies to this call, use it; otherwise default behavior.
    return getMappingForCall(call)
        .map(mapping -> mappedConvert(mapping, call, topLevelConverter))
        .orElseGet(() -> defaultConvert(call, topLevelConverter));
  }

  private Optional<SubstraitFunctionMapping> getMappingForCall(final RexCall call) {
    return mappers.stream()
        .map(mapper -> mapper.toSubstrait(call))
        .filter(Optional::isPresent)
        .findFirst()
        .orElse(Optional.empty());
  }

  /** Application of the more complex mappings. */
  private Optional<Expression> mappedConvert(
      SubstraitFunctionMapping mapping,
      RexCall call,
      Function<RexNode, Expression> topLevelConverter) {
    FunctionFinder finder =
        new FunctionFinder(mapping.substraitName(), call.op, mapping.functions());
    WrappedScalarCall wrapped =
        new WrappedScalarCall(call) {
          @Override
          public Stream<RexNode> getOperands() {
            return mapping.operands().stream();
          }
        };

    return attemptMatch(finder, wrapped, topLevelConverter);
  }

  /** Default conversion for functions that have simple 1:1 mappings. */
  private Optional<Expression> defaultConvert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    FunctionFinder finder = signatures.get(call.op);
    if (finder == null) {
      for (ScalarFunctionOptionPolicy policy : optionPolicies) {
        finder = signatures.get(policy.signatureOperator(call));
        if (finder != null) {
          break;
        }
      }
    }
    WrappedScalarCall wrapped = new WrappedScalarCall(call);

    return attemptMatch(finder, wrapped, topLevelConverter);
  }

  private Optional<Expression> attemptMatch(
      FunctionFinder finder,
      WrappedScalarCall call,
      Function<RexNode, Expression> topLevelConverter) {
    if (!isPotentialFunctionMatch(finder, call)) {
      return Optional.empty();
    }

    return finder.attemptMatch(call, topLevelConverter);
  }

  private boolean isPotentialFunctionMatch(FunctionFinder finder, WrappedScalarCall call) {
    return Objects.nonNull(finder) && finder.allowedArgCount((int) call.getOperands().count());
  }

  /**
   * Binds a matched function. Datetime calls carry the declaration's resolved type, with a cast
   * back when Calcite inferred a different result type. Dynamically mapped placeholder types do not
   * require a cast.
   *
   * @param call the wrapped Calcite call providing operands and type
   * @param function the Substrait scalar function declaration to invoke
   * @param arguments converted argument list for the invocation
   * @param outputType the Calcite call's result type converted to Substrait
   * @return a scalar invocation, optionally wrapped in a cast to the inferred result type
   * @throws UnsupportedOperationException for DATE arithmetic with a non-literal sub-day interval
   * @throws IllegalArgumentException if the resolved timestamp precision is unsupported or a
   *     widened datetime literal overflows
   */
  @Override
  protected Expression generateBinding(
      WrappedScalarCall call,
      SimpleExtension.ScalarFunctionVariant function,
      List<? extends FunctionArg> arguments,
      Type outputType) {
    if (!DefaultExtensionCatalog.FUNCTIONS_DATETIME.equals(function.getAnchor().urn())) {
      return Expression.ScalarFunctionInvocation.builder()
          .declaration(function)
          .outputType(outputType)
          .addAllArguments(arguments)
          .options(options(call.delegate, function))
          .build();
    }
    // The datetime extension declares its results by parameter, where Calcite keeps an operand's
    // own type: add(date, interval_day<P>) is a precision_timestamp<P> there and a DATE here. The
    // call carries the declared type, and a cast back to Calcite's keeps the column the type the
    // query gives it.
    List<? extends FunctionArg> bound =
        dateArithmeticArguments(call, function, arguments, outputType);
    if (!(function.returnType() instanceof Type)) {
      // Precision-carrying arguments share one parameter in datetime declarations. Comparisons
      // return bool and keep their operand precisions; widening also narrows a timestamp's range.
      bound = widenedToOnePrecision(bound);
    }
    Optional<Type> declared = declaredType(function, bound);
    if (declared.isEmpty()) {
      return ExpressionCreator.scalarFunction(function, outputType, arguments);
    }
    requireSupportedTimestampPrecision(declared.get());
    // Keep the call's nullability when its declared type otherwise matches.
    boolean sameType = declared.get().equalsIgnoringNullability(outputType);
    Expression invocation =
        ExpressionCreator.scalarFunction(function, sameType ? outputType : declared.get(), bound);
    return sameType
            || SimpleExtensionToSqlOperator.hasPlaceholderReturnType(call.delegate.getOperator())
        ? invocation
        : ExpressionCreator.cast(
            outputType, invocation, Expression.FailureBehavior.THROW_EXCEPTION);
  }

  private static List<? extends FunctionArg> dateArithmeticArguments(
      WrappedScalarCall call,
      SimpleExtension.ScalarFunctionVariant function,
      List<? extends FunctionArg> arguments,
      Type outputType) {
    // A Substrait-origin call carries a timestamp result and must keep its sub-day interval.
    if (!(outputType instanceof Type.Date)
        || !("add:date_iday".equals(function.key())
            || "subtract:date_iday".equals(function.key()))) {
      return arguments;
    }
    RexNode intervalOperand = call.getOperands().skip(1).findFirst().orElseThrow();
    if (intervalOperand.getType().getSqlTypeName() == SqlTypeName.INTERVAL_DAY) {
      return arguments;
    }
    FunctionArg interval = arguments.get(1);
    if (interval instanceof Expression.NullLiteral) {
      return arguments;
    }
    if (!(interval instanceof Expression.IntervalDayLiteral)) {
      throw new UnsupportedOperationException(
          "DATE arithmetic with a non-literal sub-day interval is not supported");
    }
    // LiteralConverter decomposes the total duration towards zero. Keeping only its days makes
    // the timestamp call followed by a DATE cast agree with Calcite's DATE arithmetic.
    Expression.IntervalDayLiteral literal = (Expression.IntervalDayLiteral) interval;
    return List.of(
        arguments.get(0),
        ExpressionCreator.intervalDay(
            literal.nullable(), literal.days(), 0, 0, literal.precision()));
  }

  private static Optional<Type> declaredType(
      SimpleExtension.ScalarFunctionVariant function, List<? extends FunctionArg> arguments) {
    List<Type> types =
        arguments.stream()
            .filter(Expression.class::isInstance)
            .map(argument -> ((Expression) argument).getType())
            .collect(Collectors.toList());
    try {
      return Optional.of(function.resolveType(types));
    } catch (UnsupportedOperationException e) {
      return Optional.empty();
    }
  }

  private List<FunctionArg> widenedToOnePrecision(List<? extends FunctionArg> arguments) {
    int precision =
        arguments.stream()
            .filter(Expression.class::isInstance)
            .map(argument -> precisionOf(((Expression) argument).getType()))
            .flatMap(Optional::stream)
            .max(Integer::compare)
            .orElse(0);
    return arguments.stream()
        .map(
            argument -> {
              if (!(argument instanceof Expression)) {
                return argument;
              }
              Expression expression = (Expression) argument;
              Type type = expression.getType();
              Optional<Integer> own = precisionOf(type);
              if (own.isEmpty() || own.get() == precision) {
                return argument;
              }
              Type widened = withPrecision(type, precision);
              requireSupportedTimestampPrecision(widened);
              if (expression instanceof Expression.NullLiteral) {
                return ExpressionCreator.typedNull(widened);
              }
              try {
                long factor = LongMath.checkedPow(10, precision - own.get());
                if (expression instanceof Expression.IntervalDayLiteral) {
                  Expression.IntervalDayLiteral literal =
                      (Expression.IntervalDayLiteral) expression;
                  return ExpressionCreator.intervalDay(
                      literal.nullable(),
                      literal.days(),
                      literal.seconds(),
                      Math.multiplyExact(literal.subseconds(), factor),
                      precision);
                }
                if (expression instanceof Expression.PrecisionTimestampLiteral) {
                  Expression.PrecisionTimestampLiteral literal =
                      (Expression.PrecisionTimestampLiteral) expression;
                  return ExpressionCreator.precisionTimestamp(
                      literal.nullable(), Math.multiplyExact(literal.value(), factor), precision);
                }
                if (expression instanceof Expression.PrecisionTimestampTZLiteral) {
                  Expression.PrecisionTimestampTZLiteral literal =
                      (Expression.PrecisionTimestampTZLiteral) expression;
                  return ExpressionCreator.precisionTimestampTZ(
                      literal.nullable(), Math.multiplyExact(literal.value(), factor), precision);
                }
              } catch (ArithmeticException e) {
                throw new IllegalArgumentException(
                    "Datetime literal cannot be represented at precision "
                        + precision
                        + " in a signed 64-bit value",
                    e);
              }
              return ExpressionCreator.cast(
                  widened, expression, Expression.FailureBehavior.THROW_EXCEPTION);
            })
        .collect(Collectors.toList());
  }

  private void requireSupportedTimestampPrecision(Type type) {
    if (type instanceof Type.PrecisionTimestamp) {
      SubstraitTypeSystem.requireSupportedPrecision(
          typeFactory.getTypeSystem(),
          SqlTypeName.TIMESTAMP,
          "precision_timestamp",
          ((Type.PrecisionTimestamp) type).precision());
    } else if (type instanceof Type.PrecisionTimestampTZ) {
      SubstraitTypeSystem.requireSupportedPrecision(
          typeFactory.getTypeSystem(),
          SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
          "precision_timestamp_tz",
          ((Type.PrecisionTimestampTZ) type).precision());
    }
  }

  private static Optional<Integer> precisionOf(Type type) {
    if (type instanceof Type.PrecisionTimestamp) {
      return Optional.of(((Type.PrecisionTimestamp) type).precision());
    }
    if (type instanceof Type.PrecisionTimestampTZ) {
      return Optional.of(((Type.PrecisionTimestampTZ) type).precision());
    }
    if (type instanceof Type.IntervalDay) {
      return Optional.of(((Type.IntervalDay) type).precision());
    }
    return Optional.empty();
  }

  private static Type withPrecision(Type type, int precision) {
    TypeCreator creator = TypeCreator.of(type.nullable());
    if (type instanceof Type.PrecisionTimestamp) {
      return creator.precisionTimestamp(precision);
    }
    if (type instanceof Type.PrecisionTimestampTZ) {
      return creator.precisionTimestampTZ(precision);
    }
    if (type instanceof Type.IntervalDay) {
      return creator.intervalDay(precision);
    }
    throw new IllegalArgumentException("Unsupported datetime type: " + type);
  }

  /**
   * Returns the Substrait arguments for a given scalar invocation, applying any custom mapping if
   * present; otherwise returns the invocation's own arguments.
   *
   * @param expression the scalar function invocation
   * @return the argument list, possibly remapped; never {@code null}
   */
  public List<FunctionArg> getExpressionArguments(Expression.ScalarFunctionInvocation expression) {
    // If a mapping applies to this expression, use it to get the arguments; otherwise default
    // behavior.
    return getMappedExpressionArguments(expression).orElseGet(expression::arguments);
  }

  /**
   * Resolves an invocation through the existing operator mapping and its option policy.
   *
   * @param expression the Substrait scalar invocation
   * @return the selected operator, or empty when no mapping exists
   */
  public Optional<SqlOperator> getSqlOperatorFromSubstraitFunc(
      Expression.ScalarFunctionInvocation expression) {
    return getSqlOperatorFromSubstraitFunc(expression.declaration().key(), expression.outputType())
        .map(operator -> resolveOptions(expression, operator));
  }

  /**
   * Selects an operator that honors the requested options. Custom converters can override this
   * together with {@link #options} to supply their own semantics in both directions.
   *
   * @param expression the Substrait invocation
   * @param operator the selected Calcite operator
   * @return the operator implementing a supported preference
   * @throws UnsupportedOperationException if no requested preference is supported
   */
  protected SqlOperator resolveOptions(
      Expression.ScalarFunctionInvocation expression, SqlOperator operator) {
    for (ScalarFunctionOptionPolicy policy : optionPolicies) {
      operator = policy.resolve(expression, operator);
    }
    return operator;
  }

  /**
   * Returns the option preferences implemented by the matched Calcite call.
   *
   * @param call the Calcite call
   * @param function the bound Substrait variant
   * @return the preferences to export
   */
  protected List<FunctionOption> options(
      RexCall call, SimpleExtension.ScalarFunctionVariant function) {
    return optionPolicies.stream()
        .flatMap(policy -> policy.forCall(call, function).stream())
        .collect(Collectors.toList());
  }

  /**
   * Builds a Calcite call, applying a reverse mapping when the native operator is not executable.
   *
   * @param expression the Substrait invocation
   * @param operator the selected Calcite operator
   * @param arguments converted arguments
   * @param returnType the declared result type
   * @param rexBuilder builder for the target Calcite plan
   * @return the converted call, including any cast needed to preserve its declared type
   */
  RexNode createCall(
      Expression.ScalarFunctionInvocation expression,
      SqlOperator operator,
      List<RexNode> arguments,
      RelDataType returnType,
      RexBuilder rexBuilder) {
    return mappers.stream()
        .map(mapper -> mapper.toCalcite(expression, operator, arguments, returnType, rexBuilder))
        .flatMap(Optional::stream)
        .findFirst()
        .orElseGet(() -> rexBuilder.makeCall(returnType, operator, arguments));
  }

  private Optional<List<FunctionArg>> getMappedExpressionArguments(
      Expression.ScalarFunctionInvocation expression) {
    return mappers.stream()
        .map(mapper -> mapper.getExpressionArguments(expression))
        .filter(Optional::isPresent)
        .findFirst()
        .orElse(Optional.empty());
  }

  /**
   * Wrapped view of a {@link RexCall} for signature matching.
   *
   * <p>Provides operand stream and type info used by {@link FunctionFinder}.
   */
  protected static class WrappedScalarCall implements FunctionConverter.GenericCall {

    private final RexCall delegate;

    private WrappedScalarCall(RexCall delegate) {
      this.delegate = delegate;
    }

    /**
     * Returns the operand stream of the underlying {@link RexCall}.
     *
     * @return stream of operands
     */
    @Override
    public Stream<RexNode> getOperands() {
      return delegate.getOperands().stream();
    }

    /**
     * Returns the Calcite type of the underlying {@link RexCall}.
     *
     * @return call type
     */
    @Override
    public RelDataType getType() {
      return delegate.getType();
    }
  }
}
