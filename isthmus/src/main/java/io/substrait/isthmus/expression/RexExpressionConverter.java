package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.function.ImmutableSimpleExtension;
import io.substrait.function.SimpleExtension;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.relation.Rel;
import io.substrait.type.StringTypeVisitor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class RexExpressionConverter implements RexVisitor<Expression> {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RexExpressionConverter.class);

  private final List<CallConverter> callConverters;
  private final SubstraitRelVisitor relVisitor;

  public RexExpressionConverter(SubstraitRelVisitor relVisitor, CallConverter... callConverters) {
    this(relVisitor, Arrays.asList(callConverters));
  }

  public RexExpressionConverter(CallConverter... callConverters) {
    this(null, Arrays.asList(callConverters));
  }

  public RexExpressionConverter(
      SubstraitRelVisitor relVisitor, List<CallConverter> callConverters) {
    this.callConverters = callConverters;
    this.relVisitor = relVisitor;
  }

  public RexExpressionConverter() {
    this.callConverters = CallConverters.DEFAULTS;
    this.relVisitor = null; // TODO
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
    throw new UnsupportedOperationException("RexOver not supported");
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
    // Rel rel = SubstraitRelVisitor.convert(subQuery.rel, EXTENSION_COLLECTION);
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

  private static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION;

  static {
    SimpleExtension.ExtensionCollection defaults =
        ImmutableSimpleExtension.ExtensionCollection.builder().build();
    try {
      defaults = SimpleExtension.loadDefaults();
    } catch (IOException e) {
      throw new RuntimeException("Failure while loading defaults.", e);
    }

    EXTENSION_COLLECTION = defaults;
  }
}
