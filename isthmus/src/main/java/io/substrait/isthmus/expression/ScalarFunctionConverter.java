package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

public class ScalarFunctionConverter
    extends FunctionConverter<
        SimpleExtension.ScalarFunctionVariant,
        Expression,
        ScalarFunctionConverter.WrappedScalarCall>
    implements CallConverter {

  private final List<ScalarFunctionMapper> mappers;

  public ScalarFunctionConverter(
      List<SimpleExtension.ScalarFunctionVariant> functions, RelDataTypeFactory typeFactory) {
    this(functions, Collections.emptyList(), typeFactory, TypeConverter.DEFAULT);
  }

  public ScalarFunctionConverter(
      List<SimpleExtension.ScalarFunctionVariant> functions,
      List<FunctionMappings.Sig> additionalSignatures,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    super(functions, additionalSignatures, typeFactory, typeConverter);

    mappers = List.of(new TrimFunctionMapper(functions));
  }

  private List<ScalarFunctionVariant> findFunction(
      String name, Collection<ScalarFunctionVariant> functions) {
    return functions.stream()
        .filter(f -> name.equals(f.name()))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  protected ImmutableList<FunctionMappings.Sig> getSigs() {
    return FunctionMappings.SCALAR_SIGS;
  }

  @Override
  public Optional<Expression> convert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    return getConvertMapper(call)
        .map(mapping -> mappedConvert(mapping, call, topLevelConverter))
        .orElseGet(() -> defaultConvert(call, topLevelConverter));
  }

  private Optional<SubstraitFunctionMapping> getConvertMapper(final RexCall call) {
    return mappers.stream()
        .map(mapper -> mapper.toSubstrait(call))
        .filter(Optional::isPresent)
        .findFirst()
        .orElse(Optional.empty());
  }

  private Optional<Expression> mappedConvert(
      SubstraitFunctionMapping mapping,
      RexCall call,
      Function<RexNode, Expression> topLevelConverter) {
    var finder = new FunctionFinder(mapping.name(), call.op, mapping.functions());
    if (!finder.allowedArgCount(mapping.operands().size())) {
      return null;
    }

    var wrapped =
        new WrappedScalarCall(call) {
          @Override
          public Stream<RexNode> getOperands() {
            return mapping.operands().stream();
          }
        };

    return finder.attemptMatch(wrapped, topLevelConverter);
  }

  private Optional<Expression> defaultConvert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    FunctionFinder m = signatures.get(call.op);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(call.operands.size())) {
      return Optional.empty();
    }

    var wrapped = new WrappedScalarCall(call);
    return m.attemptMatch(wrapped, topLevelConverter);
  }

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

  public List<FunctionArg> getRexArguments(Expression.ScalarFunctionInvocation expression) {
    return getMappedRexArguments(expression).orElseGet(expression::arguments);
  }

  private Optional<List<FunctionArg>> getMappedRexArguments(
      Expression.ScalarFunctionInvocation expression) {
    return mappers.stream()
        .map(mapper -> mapper.getRexArguments(expression))
        .filter(Optional::isPresent)
        .findFirst()
        .orElse(Optional.empty());
  }

  static class WrappedScalarCall implements GenericCall {

    private final RexCall delegate;

    private WrappedScalarCall(RexCall delegate) {
      this.delegate = delegate;
    }

    @Override
    public Stream<RexNode> getOperands() {
      return delegate.getOperands().stream();
    }

    @Override
    public RelDataType getType() {
      return delegate.getType();
    }
  }
}
