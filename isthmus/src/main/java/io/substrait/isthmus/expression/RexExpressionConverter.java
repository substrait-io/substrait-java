package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.relation.Rel;
import io.substrait.type.StringTypeVisitor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class RexExpressionConverter implements RexVisitor<Expression> {

  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RexExpressionConverter.class);

  private final List<CallConverter> callConverters;
  private final SubstraitRelVisitor relVisitor;
  private final TypeConverter typeConverter;
  private WindowFunctionConverter windowFunctionConverter;

  public RexExpressionConverter(SubstraitRelVisitor relVisitor, CallConverter... callConverters) {
    this(relVisitor, Arrays.asList(callConverters), null, TypeConverter.DEFAULT);
  }

  public RexExpressionConverter(CallConverter... callConverters) {
    this(null, Arrays.asList(callConverters), null, TypeConverter.DEFAULT);
  }

  public RexExpressionConverter(
      SubstraitRelVisitor relVisitor,
      List<CallConverter> callConverters,
      WindowFunctionConverter windowFunctionConverter,
      TypeConverter typeConverter) {
    this.callConverters = callConverters;
    this.relVisitor = relVisitor;
    this.windowFunctionConverter = windowFunctionConverter;
    this.typeConverter = typeConverter;
  }

  /**
   * Only used for testing. Missing `ScalarFunctionConverter`, `CallConverters.CREATE_SEARCH_CONV`
   */
  public RexExpressionConverter() {
    this(null, CallConverters.defaults(TypeConverter.DEFAULT), null, TypeConverter.DEFAULT);
    // TODO: Hide this AND/OR UPDATE tests
  }

  @Override
  public Expression visitInputRef(RexInputRef inputRef) {
    return FieldReference.newRootStructReference(
        inputRef.getIndex(), typeConverter.toSubstrait(inputRef.getType()));
  }

  @Override
  public Expression visitCall(RexCall call) {
    for (var c : callConverters) {
      var out = c.convert(call, rexNode -> rexNode.accept(this));
      if (out.isPresent()) {
        return out.get();
      }
    }

    throw new IllegalArgumentException(callConversionFailureMessage(call));
  }

  private String callConversionFailureMessage(RexCall call) {
    return String.format(
        "Unable to convert call %s(%s).",
        call.getOperator().getName(),
        call.getOperands().stream()
            .map(t -> t.accept(this).getType().accept(new StringTypeVisitor()))
            .collect(Collectors.joining(", ")));
  }

  @Override
  public Expression visitLiteral(RexLiteral literal) {
    return (new LiteralConverter(typeConverter)).convert(literal);
  }

  @Override
  public Expression visitOver(RexOver over) {
    if (over.ignoreNulls()) {
      throw new IllegalArgumentException("IGNORE NULLS cannot be expressed in Substrait");
    }

    return windowFunctionConverter
        .convert(over, rexNode -> rexNode.accept(this), this)
        .orElseThrow(() -> new IllegalArgumentException(callConversionFailureMessage(over)));
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
    SqlKind kind = fieldAccess.getReferenceExpr().getKind();
    switch (kind) {
      case CORREL_VARIABLE:
        {
          int stepsOut = relVisitor.getFieldAccessDepth(fieldAccess);

          return FieldReference.newRootStructOuterReference(
              fieldAccess.getField().getIndex(),
              typeConverter.toSubstrait(fieldAccess.getType()),
              stepsOut);
        }
      case ITEM:
      case INPUT_REF:
      case FIELD_ACCESS:
        {
          Expression expression = fieldAccess.getReferenceExpr().accept(this);
          if (expression instanceof FieldReference) {
            FieldReference nestedReference = (FieldReference) expression;
            return nestedReference.dereferenceStruct(fieldAccess.getField().getIndex());
          } else {
            return FieldReference.newStructReference(fieldAccess.getField().getIndex(), expression);
          }
        }
      default:
        throw new UnsupportedOperationException(
            String.format("RexFieldAccess for SqlKind %s not supported", kind));
    }
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
          .type(typeConverter.toSubstrait(subQuery.getType()))
          .build();
    } else if (subQuery.getOperator() == SqlStdOperatorTable.IN) {
      List<Expression> needles = new ArrayList<>();
      for (RexNode inOperand : subQuery.getOperands()) {
        needles.add(inOperand.accept(this));
      }
      return Expression.InPredicate.builder().needles(needles).haystack(rel).build();
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

  @Override
  public Expression visitLambda(RexLambda rexLambda) {
    throw new UnsupportedOperationException("RexLambda not supported");
  }

  @Override
  public Expression visitLambdaRef(RexLambdaRef rexLambdaRef) {
    throw new UnsupportedOperationException("RexLambdaRef not supported");
  }
}
