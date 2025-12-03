package io.substrait.examples.util;

import io.substrait.expression.Expression.BinaryLiteral;
import io.substrait.expression.Expression.BoolLiteral;
import io.substrait.expression.Expression.Cast;
import io.substrait.expression.Expression.DateLiteral;
import io.substrait.expression.Expression.DecimalLiteral;
import io.substrait.expression.Expression.EmptyListLiteral;
import io.substrait.expression.Expression.EmptyMapLiteral;
import io.substrait.expression.Expression.FP32Literal;
import io.substrait.expression.Expression.FP64Literal;
import io.substrait.expression.Expression.FixedBinaryLiteral;
import io.substrait.expression.Expression.FixedCharLiteral;
import io.substrait.expression.Expression.I16Literal;
import io.substrait.expression.Expression.I32Literal;
import io.substrait.expression.Expression.I64Literal;
import io.substrait.expression.Expression.I8Literal;
import io.substrait.expression.Expression.IfThen;
import io.substrait.expression.Expression.InPredicate;
import io.substrait.expression.Expression.IntervalCompoundLiteral;
import io.substrait.expression.Expression.IntervalDayLiteral;
import io.substrait.expression.Expression.IntervalYearLiteral;
import io.substrait.expression.Expression.ListLiteral;
import io.substrait.expression.Expression.MapLiteral;
import io.substrait.expression.Expression.MultiOrList;
import io.substrait.expression.Expression.NullLiteral;
import io.substrait.expression.Expression.PrecisionTimestampLiteral;
import io.substrait.expression.Expression.PrecisionTimestampTZLiteral;
import io.substrait.expression.Expression.ScalarFunctionInvocation;
import io.substrait.expression.Expression.ScalarSubquery;
import io.substrait.expression.Expression.SetPredicate;
import io.substrait.expression.Expression.SingleOrList;
import io.substrait.expression.Expression.StrLiteral;
import io.substrait.expression.Expression.StructLiteral;
import io.substrait.expression.Expression.Switch;
import io.substrait.expression.Expression.TimeLiteral;
import io.substrait.expression.Expression.TimestampLiteral;
import io.substrait.expression.Expression.TimestampTZLiteral;
import io.substrait.expression.Expression.UUIDLiteral;
import io.substrait.expression.Expression.UserDefinedLiteral;
import io.substrait.expression.Expression.VarCharLiteral;
import io.substrait.expression.Expression.WindowFunctionInvocation;
import io.substrait.expression.ExpressionVisitor;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.type.Type;
import io.substrait.util.EmptyVisitationContext;
import java.util.List;

/** ExpressionStringify gives a simple debug text output for Expressions */
public class ExpressionStringify extends ParentStringify
    implements ExpressionVisitor<String, EmptyVisitationContext, RuntimeException> {

  public ExpressionStringify(final int indent) {
    super(indent);
  }

  @Override
  public String visit(final NullLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<NullLiteral>";
  }

  @Override
  public String visit(final BoolLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<BoolLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final I8Literal expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<I8Literal " + expr.value() + ">";
  }

  @Override
  public String visit(final I16Literal expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<I16Literal " + expr.value() + ">";
  }

  @Override
  public String visit(final I32Literal expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<I32Literal " + expr.value() + ">";
  }

  @Override
  public String visit(final I64Literal expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<I64Literal " + expr.value() + ">";
  }

  @Override
  public String visit(final FP32Literal expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<FP32Literal " + expr.value() + ">";
  }

  @Override
  public String visit(final FP64Literal expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<FP64Literal " + expr.value() + ">";
  }

  @Override
  public String visit(final StrLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<StrLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final BinaryLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<BinaryLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final TimeLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<TimeLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final DateLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<DateLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final TimestampLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<TimestampLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final TimestampTZLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<TimestampTZLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final IntervalYearLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<IntervalYearLiteral " + expr.months() + " " + expr.years() + ">";
  }

  @Override
  public String visit(final IntervalDayLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<IntervalYearLiteral " + expr.seconds() + " " + expr.days() + ">";
  }

  @Override
  public String visit(final UUIDLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<UUIDLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final FixedCharLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<FixedCharLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final VarCharLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<VarCharLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final FixedBinaryLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<FixedBinaryLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final DecimalLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<FixedBinaryLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final MapLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<MapLiteral >";
  }

  @Override
  public String visit(final ListLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<ListLiteral >";
  }

  @Override
  public String visit(final EmptyListLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<EmptyListLiteral >";
  }

  @Override
  public String visit(final StructLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<StructLiteral >";
  }

  @Override
  public String visit(final UserDefinedLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<UserDefinedLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final Switch expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<Switch " + expr.switchClauses() + ">";
  }

  @Override
  public String visit(final IfThen expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<IfThen " + expr.ifClauses() + ">";
  }

  @Override
  public String visit(final ScalarFunctionInvocation expr, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = new StringBuilder("<ScalarFn>");

    sb.append(expr.declaration());
    // sb.append(" (");
    final List<FunctionArg> args = expr.arguments();
    for (int i = 0; i < args.size(); i++) {
      final FunctionArg arg = args.get(i);
      sb.append(getContinuationIndentString());
      sb.append("arg" + i + " = ");
      final FunctionArgStringify funcArgVisitor = new FunctionArgStringify(indent);

      sb.append(arg.accept(expr.declaration(), i, funcArgVisitor, context));
      sb.append(" ");
    }
    return sb.toString();
  }

  @Override
  public String visit(final WindowFunctionInvocation expr, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = new StringBuilder("WindowFunctionInvocation#");

    return sb.toString();
  }

  @Override
  public String visit(final Cast expr, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = new StringBuilder("<Cast#");

    sb.append(expr.getType().accept(new TypeStringify(this.indent))).append(" ");
    sb.append(expr.input().accept(this, context)).append(" ");
    sb.append(expr.failureBehavior().toString());
    sb.append(">");
    return sb.toString();
  }

  @Override
  public String visit(final SingleOrList expr, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = new StringBuilder("SingleOrList#");

    return sb.toString();
  }

  @Override
  public String visit(final MultiOrList expr, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = new StringBuilder("Cast#");

    return sb.toString();
  }

  @Override
  public String visit(final FieldReference expr, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = new StringBuilder("FieldRef#");
    final Type type = expr.getType();
    // sb.append(expr.inputExpression());
    sb.append("/").append(type.accept(new TypeStringify(indent))).append("/");
    expr.segments()
        .forEach(
            s -> {
              sb.append(s).append(" ");
            });

    return sb.toString();
  }

  @Override
  public String visit(final SetPredicate expr, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = new StringBuilder("SetPredicate#");

    return sb.toString();
  }

  @Override
  public String visit(final ScalarSubquery expr, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = new StringBuilder("ScalarSubquery#");

    return sb.toString();
  }

  @Override
  public String visit(final InPredicate expr, final EmptyVisitationContext context)
      throws RuntimeException {
    final StringBuilder sb = new StringBuilder("InPredicate#");

    return sb.toString();
  }

  @Override
  public String visit(final PrecisionTimestampLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<PrecisionTimestampLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final PrecisionTimestampTZLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<PrecisionTimestampTZLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(final IntervalCompoundLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<IntervalCompoundLiteral "
        + expr.subseconds()
        + " "
        + expr.seconds()
        + " "
        + expr.days()
        + " "
        + expr.months()
        + " "
        + expr.years()
        + ">";
  }

  @Override
  public String visit(final EmptyMapLiteral expr, final EmptyVisitationContext context)
      throws RuntimeException {
    return "<EmptyMapLiteral>";
  }
}
