package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.relation.Rel;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class RexExpressionConverter implements RexVisitor<Expression> {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RexExpressionConverter.class);

  private final List<CallConverter> callConverters;
  private final SubstraitRelVisitor relVisitor;
  private WindowFunctionConverter windowFunctionConverter;

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
      WindowFunctionConverter windowFunctionConverter) {
    this.callConverters = callConverters;
    this.relVisitor = relVisitor;
    this.windowFunctionConverter = windowFunctionConverter;
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
    var exp =
        windowFunctionConverter.convert(
            inputRel,
            inputType,
            over,
            t -> {
              RexNode r = (RexNode) t;
              return r.accept(this);
            },
            this);

    return exp;
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

  public void setInputRel(RelNode inputRel) {
    this.inputRel = inputRel;
  }

  public void setInputType(Type.Struct inputType) {
    this.inputType = inputType;
  }
}
