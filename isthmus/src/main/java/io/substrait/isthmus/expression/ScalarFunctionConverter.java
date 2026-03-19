package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
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
    return Expression.ScalarFunctionInvocation.builder()
        .outputType(outputType)
        .declaration(function)
        .addAllArguments(arguments)
        .build();
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
