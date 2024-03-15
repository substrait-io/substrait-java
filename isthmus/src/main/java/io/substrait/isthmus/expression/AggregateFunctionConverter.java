package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.AggregateFunctions;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.FunctionConverter.GenericCall;
import io.substrait.type.Type;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class AggregateFunctionConverter
    extends FunctionConverter<
        SimpleExtension.AggregateFunctionVariant,
        AggregateFunctionInvocation,
        AggregateFunctionConverter.WrappedAggregateCall> {

  @Override
  protected ImmutableList<FunctionMappings.Sig> getSigs() {
    return FunctionMappings.AGGREGATE_SIGS;
  }

  public AggregateFunctionConverter(
      List<SimpleExtension.AggregateFunctionVariant> functions, RelDataTypeFactory typeFactory) {
    super(functions, typeFactory);
  }

  public AggregateFunctionConverter(
      List<SimpleExtension.AggregateFunctionVariant> functions,
      List<FunctionMappings.Sig> additionalSignatures,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    super(functions, additionalSignatures, typeFactory, typeConverter);
  }

  @Override
  protected AggregateFunctionInvocation generateBinding(
      WrappedAggregateCall call,
      SimpleExtension.AggregateFunctionVariant function,
      List<FunctionArg> arguments,
      Type outputType) {
    AggregateCall agg = call.getUnderlying();

    List<Expression.SortField> sorts =
        agg.getCollation() != null
            ? agg.getCollation().getFieldCollations().stream()
                .map(r -> SubstraitRelVisitor.toSortField(r, call.inputType))
                .collect(java.util.stream.Collectors.toList())
            : Collections.emptyList();
    Expression.AggregationInvocation invocation =
        agg.isDistinct()
            ? Expression.AggregationInvocation.DISTINCT
            : Expression.AggregationInvocation.ALL;
    return ExpressionCreator.aggregateFunction(
        function,
        outputType,
        Expression.AggregationPhase.INITIAL_TO_RESULT,
        sorts,
        invocation,
        arguments);
  }

  public Optional<AggregateFunctionInvocation> convert(
      RelNode input,
      Type.Struct inputType,
      AggregateCall call,
      Function<RexNode, Expression> topLevelConverter) {

    var m = getFunctionFinder(call);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(call.getArgList().size())) {
      return Optional.empty();
    }

    var wrapped = new WrappedAggregateCall(call, input, rexBuilder, inputType);
    return m.attemptMatch(wrapped, topLevelConverter);
  }

  protected FunctionFinder getFunctionFinder(AggregateCall call) {
    // replace COUNT() + distinct == true and approximate == true with APPROX_COUNT_DISTINCT
    // before converting into substrait function
    SqlAggFunction aggFunction = call.getAggregation();
    if (aggFunction == SqlStdOperatorTable.COUNT && call.isDistinct() && call.isApproximate()) {
      aggFunction = SqlStdOperatorTable.APPROX_COUNT_DISTINCT;
    }

    SqlAggFunction lookupFunction =
        // Replace default Calcite aggregate calls with Substrait specific variants.
        // See toSubstraitAggVariant for more details.
        AggregateFunctions.toSubstraitAggVariant(aggFunction).orElse(aggFunction);
    return signatures.get(lookupFunction);
  }

  static class WrappedAggregateCall implements GenericCall {
    private final AggregateCall call;
    private final RelNode input;
    private final RexBuilder rexBuilder;
    private final Type.Struct inputType;

    private WrappedAggregateCall(
        AggregateCall call, RelNode input, RexBuilder rexBuilder, Type.Struct inputType) {
      this.call = call;
      this.input = input;
      this.rexBuilder = rexBuilder;
      this.inputType = inputType;
    }

    @Override
    public Stream<RexNode> getOperands() {
      return call.getArgList().stream().map(r -> rexBuilder.makeInputRef(input, r));
    }

    public AggregateCall getUnderlying() {
      return call;
    }

    @Override
    public RelDataType getType() {
      return call.getType();
    }
  }
}
