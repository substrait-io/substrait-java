package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
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

  public ScalarFunctionConverter(
      List<SimpleExtension.ScalarFunctionVariant> functions, RelDataTypeFactory typeFactory) {
    super(functions, typeFactory);
  }

  public ScalarFunctionConverter(
      List<SimpleExtension.ScalarFunctionVariant> functions,
      List<FunctionMappings.Sig> additionalSignatures,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    super(functions, additionalSignatures, typeFactory, typeConverter);
  }

  @Override
  protected ImmutableList<FunctionMappings.Sig> getSigs() {
    return FunctionMappings.SCALAR_SIGS;
  }

  @Override
  public Optional<Expression> convert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    FunctionFinder m = signatures.get(call.op);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(call.getOperands().size())) {
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
