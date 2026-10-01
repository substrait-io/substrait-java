package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.CallConverter;
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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

/**
 * Converts Calcite {@link RexCall} scalar functions to Substrait {@link Expression} using known
 * Substrait {@link SimpleExtension.ScalarFunctionVariant} declarations.
 *
 * <p>Supports custom function mappers for special cases (e.g., TRIM, SQRT), and falls back to
 * default signature-based matching. Produces {@link Expression.ScalarFunctionInvocation}.
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
            new SqrtFunctionMapper(functions),
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
   * Builds an {@link Expression.ScalarFunctionInvocation} for a matched function.
   *
   * @param call the wrapped Calcite call providing operands and type
   * @param function the Substrait scalar function declaration to invoke
   * @param arguments converted argument list for the invocation
   * @param outputType the Substrait output type for the invocation
   * @return a scalar function invocation expression
   */
  @Override
  protected Expression generateBinding(
      WrappedScalarCall call,
      SimpleExtension.ScalarFunctionVariant function,
      List<? extends FunctionArg> arguments,
      Type outputType) {
    if (!DefaultExtensionCatalog.FUNCTIONS_DATETIME.equals(function.getAnchor().urn())) {
      return invocation(function, arguments, outputType);
    }
    // The datetime extension declares its results by parameter, where Calcite keeps an operand's
    // own type: add(date, interval_day<P>) is a precision_timestamp<P> there and a DATE here. The
    // call carries the declared type, and a cast back to Calcite's keeps the column the type the
    // query gives it.
    List<? extends FunctionArg> bound = arguments;
    Optional<Type> declared = declaredType(function, bound);
    if (declared.isEmpty() && !(function.returnType() instanceof Type)) {
      // One parameter bound to two precisions, precision_timestamp<0> and interval_day<6> say.
      // Only parameterized results need this retry. Comparisons return bool and keep their
      // operand precisions; widening a timestamp also narrows its representable date range.
      bound = widenedToOnePrecision(arguments);
      declared = declaredType(function, bound);
    }
    if (declared.isEmpty()) {
      return invocation(function, arguments, outputType);
    }
    Expression invocation = invocation(function, bound, declared.get());
    return declared.get().equals(outputType)
        ? invocation
        : ExpressionCreator.cast(
            outputType, invocation, Expression.FailureBehavior.THROW_EXCEPTION);
  }

  private static Expression invocation(
      SimpleExtension.ScalarFunctionVariant function,
      List<? extends FunctionArg> arguments,
      Type outputType) {
    return Expression.ScalarFunctionInvocation.builder()
        .outputType(outputType)
        .declaration(function)
        .addAllArguments(arguments)
        .build();
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

  private static List<FunctionArg> widenedToOnePrecision(List<? extends FunctionArg> arguments) {
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
              return ExpressionCreator.cast(
                  withPrecision(type, precision),
                  expression,
                  Expression.FailureBehavior.THROW_EXCEPTION);
            })
        .collect(Collectors.toList());
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
    return creator.intervalDay(precision);
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
