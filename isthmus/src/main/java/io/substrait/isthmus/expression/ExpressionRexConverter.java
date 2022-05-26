package io.substrait.isthmus.expression;

import io.substrait.expression.AbstractExpressionVisitor;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.isthmus.TypeConverter;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

public class ExpressionRexConverter extends AbstractExpressionVisitor<RexNode, RuntimeException> {
  private final RelDataTypeFactory typeFactory;
  private final RexBuilder rexBuilder;
  private final ScalarFunctionConverter scalarFunctionConverter;

  public ExpressionRexConverter(
      RelDataTypeFactory typeFactory, ScalarFunctionConverter scalarFunctionConverter) {
    this.typeFactory = typeFactory;
    this.rexBuilder = new RexBuilder(typeFactory);
    this.scalarFunctionConverter = scalarFunctionConverter;
  }

  @Override
  public RexNode visit(Expression.NullLiteral expr) throws RuntimeException {
    return null;
  }

  @Override
  public RexNode visit(Expression.BoolLiteral expr) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value());
  }

  @Override
  public RexNode visit(Expression.I8Literal expr) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value(), TypeConverter.convert(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.I16Literal expr) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value(), TypeConverter.convert(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.I32Literal expr) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value(), TypeConverter.convert(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.I64Literal expr) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value(), TypeConverter.convert(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.FP32Literal expr) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value(), TypeConverter.convert(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.FP64Literal expr) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value(), TypeConverter.convert(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.StrLiteral expr) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value(), TypeConverter.convert(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.BinaryLiteral expr) throws RuntimeException {
    // Calcite RexLiteral only takes ByteString
    return rexBuilder.makeLiteral(
        new ByteString(expr.value().toByteArray()),
        TypeConverter.convert(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.TimeLiteral expr) throws RuntimeException {
    // Expression.TimeLiteral is Micros, while RexLiteral assumes Milliseconds for time type
    int milliS = (int) (TimeUnit.MICROSECONDS.toMillis(expr.value()));
    int nanoS =
        (int) (TimeUnit.MICROSECONDS.toNanos(expr.value()) - TimeUnit.MILLISECONDS.toNanos(milliS));

    TimeString timeString = TimeString.fromMillisOfDay(milliS).withNanos(nanoS);
    return rexBuilder.makeLiteral(timeString, TypeConverter.convert(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.DateLiteral expr) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value(), TypeConverter.convert(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.TimestampLiteral expr) throws RuntimeException {
    // Expression.TimestampLiteral is Micros, while RexLiteral assumes Milliseconds for timestamp
    // type
    long milliS = TimeUnit.MICROSECONDS.toMillis(expr.value());
    int nanoS =
        (int) (TimeUnit.MICROSECONDS.toNanos(expr.value()) - TimeUnit.MILLISECONDS.toNanos(milliS));

    TimestampString tsString = TimestampString.fromMillisSinceEpoch(milliS).withNanos(nanoS);
    return rexBuilder.makeLiteral(tsString, TypeConverter.convert(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.TimestampTZLiteral expr) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value(), TypeConverter.convert(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.ScalarFunctionInvocation expr) throws RuntimeException {
    var args = expr.arguments().stream().map(a -> a.accept(this)).toList();
    Optional<SqlOperator> operator = scalarFunctionConverter.getSqlOperatorFromSubstraitFunc(expr);
    if (operator.isPresent()) {
      return rexBuilder.makeCall(operator.get(), args);
    }

    return visitFallback(expr);
  }

  @Override
  public RexNode visit(Expression.Cast expr) throws RuntimeException {
    return rexBuilder.makeAbstractCast(
        TypeConverter.convert(typeFactory, expr.getType()), expr.input().accept(this));
  }

  @Override
  public RexNode visit(FieldReference expr) throws RuntimeException {
    if (expr.isSimpleRootReference()) {
      var segment = expr.segments().get(0);

      RexInputRef rexInputRef =
          switch (segment) {
            case FieldReference.StructField f -> {
              yield new RexInputRef(f.offset(), TypeConverter.convert(typeFactory, expr.getType()));
            }
            default -> throw new IllegalArgumentException("Unhandled type: " + segment);
          };

      return rexInputRef;
    }

    return visitFallback(expr);
  }

  @Override
  public RexNode visitFallback(Expression expr) {
    throw new UnsupportedOperationException(
        String.format(
            "Rel of type %s not handled by visitor type %s.",
            expr.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }
}
