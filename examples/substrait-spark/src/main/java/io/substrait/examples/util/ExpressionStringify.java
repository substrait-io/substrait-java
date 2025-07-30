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

  public ExpressionStringify(int indent) {
    super(indent);
  }

  @Override
  public String visit(NullLiteral expr, EmptyVisitationContext context) throws RuntimeException {
    return "<NullLiteral>";
  }

  @Override
  public String visit(BoolLiteral expr, EmptyVisitationContext context) throws RuntimeException {
    return "<BoolLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(I8Literal expr, EmptyVisitationContext context) throws RuntimeException {
    return "<I8Literal " + expr.value() + ">";
  }

  @Override
  public String visit(I16Literal expr, EmptyVisitationContext context) throws RuntimeException {
    return "<I16Literal " + expr.value() + ">";
  }

  @Override
  public String visit(I32Literal expr, EmptyVisitationContext context) throws RuntimeException {
    return "<I32Literal " + expr.value() + ">";
  }

  @Override
  public String visit(I64Literal expr, EmptyVisitationContext context) throws RuntimeException {
    return "<I64Literal " + expr.value() + ">";
  }

  @Override
  public String visit(FP32Literal expr, EmptyVisitationContext context) throws RuntimeException {
    return "<FP32Literal " + expr.value() + ">";
  }

  @Override
  public String visit(FP64Literal expr, EmptyVisitationContext context) throws RuntimeException {
    return "<FP64Literal " + expr.value() + ">";
  }

  @Override
  public String visit(StrLiteral expr, EmptyVisitationContext context) throws RuntimeException {
    return "<StrLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(BinaryLiteral expr, EmptyVisitationContext context) throws RuntimeException {
    return "<BinaryLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(TimeLiteral expr, EmptyVisitationContext context) throws RuntimeException {
    return "<TimeLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(DateLiteral expr, EmptyVisitationContext context) throws RuntimeException {
    return "<DateLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(TimestampLiteral expr, EmptyVisitationContext context)
      throws RuntimeException {
    return "<TimestampLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(TimestampTZLiteral expr, EmptyVisitationContext context)
      throws RuntimeException {
    return "<TimestampTZLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(IntervalYearLiteral expr, EmptyVisitationContext context)
      throws RuntimeException {
    return "<IntervalYearLiteral " + expr.months() + " " + expr.years() + ">";
  }

  @Override
  public String visit(IntervalDayLiteral expr, EmptyVisitationContext context)
      throws RuntimeException {
    return "<IntervalYearLiteral " + expr.seconds() + " " + expr.days() + ">";
  }

  @Override
  public String visit(UUIDLiteral expr, EmptyVisitationContext context) throws RuntimeException {
    return "<UUIDLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(FixedCharLiteral expr, EmptyVisitationContext context)
      throws RuntimeException {
    return "<FixedCharLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(VarCharLiteral expr, EmptyVisitationContext context) throws RuntimeException {
    return "<VarCharLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(FixedBinaryLiteral expr, EmptyVisitationContext context)
      throws RuntimeException {
    return "<FixedBinaryLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(DecimalLiteral expr, EmptyVisitationContext context) throws RuntimeException {
    return "<FixedBinaryLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(MapLiteral expr, EmptyVisitationContext context) throws RuntimeException {
    return "<MapLiteral >";
  }

  @Override
  public String visit(ListLiteral expr, EmptyVisitationContext context) throws RuntimeException {
    return "<ListLiteral >";
  }

  @Override
  public String visit(EmptyListLiteral expr, EmptyVisitationContext context)
      throws RuntimeException {
    return "<EmptyListLiteral >";
  }

  @Override
  public String visit(StructLiteral expr, EmptyVisitationContext context) throws RuntimeException {
    return "<StructLiteral >";
  }

  @Override
  public String visit(UserDefinedLiteral expr, EmptyVisitationContext context)
      throws RuntimeException {
    return "<UserDefinedLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(Switch expr, EmptyVisitationContext context) throws RuntimeException {
    return "<Switch " + expr.switchClauses() + ">";
  }

  @Override
  public String visit(IfThen expr, EmptyVisitationContext context) throws RuntimeException {
    return "<IfThen " + expr.ifClauses() + ">";
  }

  @Override
  public String visit(ScalarFunctionInvocation expr, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = new StringBuilder("<ScalarFn>");

    sb.append(expr.declaration());
    // sb.append(" (");
    List<FunctionArg> args = expr.arguments();
    for (int i = 0; i < args.size(); i++) {
      FunctionArg arg = args.get(i);
      sb.append(getContinuationIndentString());
      sb.append("arg" + i + " = ");
      FunctionArgStringify funcArgVisitor = new FunctionArgStringify(indent);

      sb.append(arg.accept(expr.declaration(), i, funcArgVisitor, context));
      sb.append(" ");
    }
    return sb.toString();
  }

  @Override
  public String visit(WindowFunctionInvocation expr, EmptyVisitationContext context)
      throws RuntimeException {
    StringBuilder sb = new StringBuilder("WindowFunctionInvocation#");

    return sb.toString();
  }

  @Override
  public String visit(Cast expr, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = new StringBuilder("<Cast#");

    sb.append(expr.getType().accept(new TypeStringify(this.indent))).append(" ");
    sb.append(expr.input().accept(this, context)).append(" ");
    sb.append(expr.failureBehavior().toString());
    sb.append(">");
    return sb.toString();
  }

  @Override
  public String visit(SingleOrList expr, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = new StringBuilder("SingleOrList#");

    return sb.toString();
  }

  @Override
  public String visit(MultiOrList expr, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = new StringBuilder("Cast#");

    return sb.toString();
  }

  @Override
  public String visit(FieldReference expr, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = new StringBuilder("FieldRef#");
    Type type = expr.getType();
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
  public String visit(SetPredicate expr, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = new StringBuilder("SetPredicate#");

    return sb.toString();
  }

  @Override
  public String visit(ScalarSubquery expr, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = new StringBuilder("ScalarSubquery#");

    return sb.toString();
  }

  @Override
  public String visit(InPredicate expr, EmptyVisitationContext context) throws RuntimeException {
    StringBuilder sb = new StringBuilder("InPredicate#");

    return sb.toString();
  }

  @Override
  public String visit(PrecisionTimestampLiteral expr, EmptyVisitationContext context)
      throws RuntimeException {
    return "<PrecisionTimestampLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(PrecisionTimestampTZLiteral expr, EmptyVisitationContext context)
      throws RuntimeException {
    return "<PrecisionTimestampTZLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(IntervalCompoundLiteral expr, EmptyVisitationContext context)
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
  public String visit(EmptyMapLiteral expr, EmptyVisitationContext context)
      throws RuntimeException {
    return "<EmptyMapLiteral>";
  }
}
