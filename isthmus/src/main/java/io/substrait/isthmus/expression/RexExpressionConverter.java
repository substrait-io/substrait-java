package io.substrait.isthmus.expression;

import io.substrait.expression.AbstractFunctionInvocation;
import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableExpression;
import io.substrait.expression.ImmutableWindowBound;
import io.substrait.expression.WindowBound;
import io.substrait.expression.WindowFunctionInvocation;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Rel;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class RexExpressionConverter implements RexVisitor<Expression> {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RexExpressionConverter.class);

  private final List<CallConverter> callConverters;
  private final SubstraitRelVisitor relVisitor;
  private List<NonScalarFuncConverter> functionConverters;

  private RelNode inputRel;
  private Type.Struct inputType;

  public RexExpressionConverter(SubstraitRelVisitor relVisitor, CallConverter... callConverters) {
    this(relVisitor, Arrays.asList(callConverters), null);
  }

  public RexExpressionConverter(CallConverter... callConverters) {
    this(null, Arrays.asList(callConverters), null);
  }

  public RexExpressionConverter(
      SubstraitRelVisitor relVisitor,
      List<CallConverter> callConverters,
      List<NonScalarFuncConverter> functionConverters) {
    this.callConverters = callConverters;
    this.relVisitor = relVisitor;
    this.functionConverters = functionConverters;
  }

  public RexExpressionConverter() {
    this.callConverters = CallConverters.DEFAULTS;
    this.relVisitor =
        null; // this RexExpressionConverter is only for Rex expression without any subquery.
  }

  @Override
  public Expression visitInputRef(RexInputRef inputRef) {
    return FieldReference.newRootStructReference(
        inputRef.getIndex(), TypeConverter.convert(inputRef.getType()));
  }

  @Override
  public Expression visitCall(RexCall call) {
    for (var c : callConverters) {
      var out = c.convert(call, r -> r.accept(this));
      if (out.isPresent()) {
        return out.get();
      }
    }

    String msg =
        String.format(
            "Unable to convert call %s(%s).",
            call.getOperator().getName(),
            call.getOperands().stream()
                .map(t -> t.accept(this).getType().accept(new StringTypeVisitor()))
                .collect(Collectors.joining(", ")));
    throw new IllegalArgumentException(msg);
  }

  @Override
  public Expression visitLiteral(RexLiteral literal) {
    return LiteralConverter.convert(literal);
  }

  @Override
  public Expression visitOver(RexOver over) {
    // maybe a aggregate function or a window function
    var lowerBound = toWindowBound(over.getWindow().getLowerBound());
    var upperBound = toWindowBound(over.getWindow().getUpperBound());
    var sqlAggFunction = over.getAggOperator();
    var argList = over.getOperands().stream().map(r -> ((RexSlot) r).getIndex()).toList();
    boolean approximate = false;
    int filterArg = -1;
    var aggCall =
        AggregateCall.create(
            sqlAggFunction,
            over.isDistinct(),
            approximate,
            over.ignoreNulls(),
            argList,
            filterArg,
            null,
            RelCollations.EMPTY,
            over.getType(),
            sqlAggFunction.getName());
    var builder = Expression.Window.builder();
    AbstractFunctionInvocation functionInvocation = null;
    for (var converter : functionConverters) {
      var funcInvocationOpt =
          converter.convert(
              inputRel,
              inputType,
              aggCall,
              t -> {
                RexNode r = (RexNode) t;
                return r.accept(this);
              });
      if (funcInvocationOpt.isPresent()) {
        functionInvocation = (AbstractFunctionInvocation) funcInvocationOpt.get();
        if (functionInvocation instanceof AggregateFunctionInvocation) {
          var aggFuncInvocation = (AggregateFunctionInvocation) functionInvocation;
          var aggMeasure = Aggregate.Measure.builder().function(aggFuncInvocation).build();
          builder.aggregateFunction(aggMeasure).hasNormalAggregateFunction(true);
        } else if (functionInvocation instanceof WindowFunctionInvocation) {
          var windowFuncInvocation = (WindowFunctionInvocation) functionInvocation;
          var windowFunc =
              ImmutableExpression.WindowFunction.builder().function(windowFuncInvocation).build();
          builder.windowFunction(windowFunc).hasNormalAggregateFunction(false);
        } else {
          throw new RuntimeException(
              String.format("Unexpected AbstractFunctionInvocation :%s", functionInvocation));
        }
      }
    }
    if (functionInvocation == null) {
      throw new RuntimeException(
          String.format(
              "Not found the corresponding window aggregate function:%s", sqlAggFunction));
    }
    var window = over.getWindow();
    var partitionExps = window.partitionKeys.stream().map(r -> r.accept(this)).toList();
    var sortFields = window.orderKeys.stream().map(r -> toSortField(r)).toList();

    return builder
        .addAllOrderBy(sortFields)
        .addAllPartitionBy(partitionExps)
        .lowerBound(lowerBound)
        .upperBound(upperBound)
        .type(TypeConverter.convert(over.getType()))
        .build();
  }

  private WindowBound toWindowBound(RexWindowBound rexWindowBound) {
    if (rexWindowBound.isCurrentRow()) {
      return ImmutableWindowBound.CurrentRowWindowBound.builder().build();
    }
    if (rexWindowBound.isUnbounded()) {
      var direction =
          rexWindowBound.isFollowing()
              ? WindowBound.Direction.FOLLOWING
              : WindowBound.Direction.PRECEDING;
      return ImmutableWindowBound.UnboundedWindowBound.builder().direction(direction).build();
    } else {
      var direction =
          rexWindowBound.isFollowing()
              ? WindowBound.Direction.FOLLOWING
              : WindowBound.Direction.PRECEDING;
      var offset = rexWindowBound.getOffset().accept(this);
      return ImmutableWindowBound.BoundedWindowBound.builder()
          .direction(direction)
          .offset(offset)
          .build();
    }
  }

  private Expression.SortField toSortField(RexFieldCollation rexFieldCollation) {
    var expr = rexFieldCollation.left.accept(this);
    var rexDirection = rexFieldCollation.getDirection();
    Expression.SortDirection direction =
        switch (rexDirection) {
          case ASCENDING -> rexFieldCollation.getNullDirection()
                  == RelFieldCollation.NullDirection.LAST
              ? Expression.SortDirection.ASC_NULLS_LAST
              : Expression.SortDirection.ASC_NULLS_FIRST;
          case DESCENDING -> rexFieldCollation.getNullDirection()
                  == RelFieldCollation.NullDirection.LAST
              ? Expression.SortDirection.DESC_NULLS_LAST
              : Expression.SortDirection.DESC_NULLS_FIRST;
          default -> throw new RuntimeException(
              String.format(
                  "Unexpected RelFieldCollation.Direction:%s enum at the RexFieldCollation!",
                  rexDirection));
        };

    return Expression.SortField.builder().expr(expr).direction(direction).build();
  }

  @Override
  public Expression visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new UnsupportedOperationException("RexCorrelVariable not supported");
  }

  @Override
  public Expression visitDynamicParam(RexDynamicParam dynamicParam) {
    throw new UnsupportedOperationException("RexDynamicParam not supported");
  }

  @Override
  public Expression visitRangeRef(RexRangeRef rangeRef) {
    throw new UnsupportedOperationException("RexRangeRef not supported");
  }

  @Override
  public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
    if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
      int stepsOut = relVisitor.getFieldAccessDepth(fieldAccess);

      return FieldReference.newRootStructOuterReference(
          fieldAccess.getField().getIndex(),
          TypeConverter.convert(fieldAccess.getType()),
          stepsOut);
    }
    throw new UnsupportedOperationException(
        "RexFieldAccess for other than RexCorrelVariable not supported");
  }

  @Override
  public Expression visitSubQuery(RexSubQuery subQuery) {
    Rel rel = relVisitor.apply(subQuery.rel);

    if (subQuery.getOperator() == SqlStdOperatorTable.EXISTS) {
      return Expression.SetPredicate.builder()
          .predicateOp(Expression.PredicateOp.PREDICATE_OP_EXISTS)
          .tuples(rel)
          .build();
    } else if (subQuery.getOperator() == SqlStdOperatorTable.UNIQUE) {
      return Expression.SetPredicate.builder()
          .predicateOp(Expression.PredicateOp.PREDICATE_OP_UNIQUE)
          .tuples(rel)
          .build();
    } else if (subQuery.getOperator() == SqlStdOperatorTable.SCALAR_QUERY) {
      return Expression.ScalarSubquery.builder()
          .input(rel)
          .type(TypeConverter.convert(subQuery.getType()))
          .build();
    } else if (subQuery.getOperator() == SqlStdOperatorTable.IN) {
      List<Expression> needles = new ArrayList<>();
      for (RexNode inOperand : subQuery.getOperands()) {
        needles.add(inOperand.accept(this));
      }
      return Expression.InPredicate.builder()
          .needles(needles)
          .haystack(rel)
          .type(TypeConverter.convert(subQuery.rel.getRowType()))
          .build();
    }

    throw new UnsupportedOperationException("RexSubQuery not supported");
  }

  @Override
  public Expression visitTableInputRef(RexTableInputRef fieldRef) {
    throw new UnsupportedOperationException("RexTableInputRef not supported");
  }

  @Override
  public Expression visitLocalRef(RexLocalRef localRef) {
    throw new UnsupportedOperationException("RexLocalRef not supported");
  }

  @Override
  public Expression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw new UnsupportedOperationException("RexPatternFieldRef not supported");
  }

  public RelNode getInputRel() {
    return inputRel;
  }

  public void setInputRel(RelNode inputRel) {
    this.inputRel = inputRel;
  }

  public Type.Struct getInputType() {
    return inputType;
  }

  public void setInputType(Type.Struct inputType) {
    this.inputType = inputType;
  }
}
