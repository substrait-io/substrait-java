package io.substrait.examples.util;

import io.substrait.expression.Expression.BinaryLiteral;
import io.substrait.expression.Expression.BoolLiteral;
import io.substrait.expression.Expression.Cast;
import io.substrait.expression.Expression.DateLiteral;
import io.substrait.expression.Expression.DecimalLiteral;
import io.substrait.expression.Expression.EmptyListLiteral;
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
import io.substrait.expression.Expression.IntervalDayLiteral;
import io.substrait.expression.Expression.IntervalYearLiteral;
import io.substrait.expression.Expression.ListLiteral;
import io.substrait.expression.Expression.MapLiteral;
import io.substrait.expression.Expression.MultiOrList;
import io.substrait.expression.Expression.NullLiteral;
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

/** ExpressionStringify gives a simple debug text output for Expressions */
public class ExpressionStringify extends ParentStringify
    implements ExpressionVisitor<String, RuntimeException> {

  public ExpressionStringify(int indent) {
    super(indent);
  }

  @Override
  public String visit(NullLiteral expr) throws RuntimeException {
    return "<NullLiteral>";
  }

  @Override
  public String visit(BoolLiteral expr) throws RuntimeException {
    return "<BoolLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(I8Literal expr) throws RuntimeException {
    return "<I8Literal " + expr.value() + ">";
  }

  @Override
  public String visit(I16Literal expr) throws RuntimeException {
    return "<I16Literal " + expr.value() + ">";
  }

  @Override
  public String visit(I32Literal expr) throws RuntimeException {
    return "<I32Literal " + expr.value() + ">";
  }

  @Override
  public String visit(I64Literal expr) throws RuntimeException {
    return "<I64Literal " + expr.value() + ">";
  }

  @Override
  public String visit(FP32Literal expr) throws RuntimeException {
    return "<FP32Literal " + expr.value() + ">";
  }

  @Override
  public String visit(FP64Literal expr) throws RuntimeException {
    return "<FP64Literal " + expr.value() + ">";
  }

  @Override
  public String visit(StrLiteral expr) throws RuntimeException {
    return "<StrLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(BinaryLiteral expr) throws RuntimeException {
    return "<BinaryLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(TimeLiteral expr) throws RuntimeException {
    return "<TimeLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(DateLiteral expr) throws RuntimeException {
    return "<DateLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(TimestampLiteral expr) throws RuntimeException {
    return "<TimestampLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(TimestampTZLiteral expr) throws RuntimeException {
    return "<TimestampeTXLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(IntervalYearLiteral expr) throws RuntimeException {
    return "<IntervalYearLiteral " + expr.months() + " " + expr.years() + ">";
  }

  @Override
  public String visit(IntervalDayLiteral expr) throws RuntimeException {
    return "<IntervalYearLiteral " + expr.seconds() + " " + expr.days() + ">";
  }

  @Override
  public String visit(UUIDLiteral expr) throws RuntimeException {
    return "<UUIDLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(FixedCharLiteral expr) throws RuntimeException {
    return "<FixedCharLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(VarCharLiteral expr) throws RuntimeException {
    return "<VarCharLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(FixedBinaryLiteral expr) throws RuntimeException {
    return "<FixedBinaryLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(DecimalLiteral expr) throws RuntimeException {
    return "<FixedBinaryLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(MapLiteral expr) throws RuntimeException {
    return "<MapLiteral >";
  }

  @Override
  public String visit(ListLiteral expr) throws RuntimeException {
    return "<ListLiteral >";
  }

  @Override
  public String visit(EmptyListLiteral expr) throws RuntimeException {
    return "<EmptyListLiteral >";
  }

  @Override
  public String visit(StructLiteral expr) throws RuntimeException {
    return "<StructLiteral >";
  }

  @Override
  public String visit(UserDefinedLiteral expr) throws RuntimeException {
    return "<UserDefinedLiteral " + expr.value() + ">";
  }

  @Override
  public String visit(Switch expr) throws RuntimeException {
    return "<Switch " + expr.switchClauses() + ">";
  }

  @Override
  public String visit(IfThen expr) throws RuntimeException {
    return "<IfThen " + expr.ifClauses() + ">";
  }

  @Override
  public String visit(ScalarFunctionInvocation expr) throws RuntimeException {
    var sb = new StringBuilder("<ScalarFn>");

    sb.append(expr.declaration());
    // sb.append(" (");
    var args = expr.arguments();
    for (var i = 0; i < args.size(); i++) {
      var arg = args.get(i);
      sb.append(getContinuationIndentString());
      sb.append("arg" + i + " = ");
      var funcArgVisitor = new FunctionArgStringify(indent);

      sb.append(arg.accept(expr.declaration(), i, funcArgVisitor));
      sb.append(" ");
    }
    return sb.toString();
  }

  @Override
  public String visit(WindowFunctionInvocation expr) throws RuntimeException {
    var sb = new StringBuilder("WindowFunctionInvocation#");

    return sb.toString();
  }

  @Override
  public String visit(Cast expr) throws RuntimeException {
    var sb = new StringBuilder("<Cast#");

    sb.append(expr.getType().accept(new TypeStringify(this.indent))).append(" ");
    sb.append(expr.input().accept(this)).append(" ");
    sb.append(expr.failureBehavior().toString());
    sb.append(">");
    return sb.toString();
  }

  @Override
  public String visit(SingleOrList expr) throws RuntimeException {
    var sb = new StringBuilder("SingleOrList#");

    return sb.toString();
  }

  @Override
  public String visit(MultiOrList expr) throws RuntimeException {
    var sb = new StringBuilder("Cast#");

    return sb.toString();
  }

  @Override
  public String visit(FieldReference expr) throws RuntimeException {
    StringBuilder sb = new StringBuilder("FieldRef#");
    var type = expr.getType();
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
  public String visit(SetPredicate expr) throws RuntimeException {
    var sb = new StringBuilder("SetPredicate#");

    return sb.toString();
  }

  @Override
  public String visit(ScalarSubquery expr) throws RuntimeException {
    var sb = new StringBuilder("ScalarSubquery#");

    return sb.toString();
  }

  @Override
  public String visit(InPredicate expr) throws RuntimeException {
    var sb = new StringBuilder("InPredicate#");

    return sb.toString();
  }
}
