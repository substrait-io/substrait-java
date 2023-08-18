package io.substrait.isthmus.expression;

import io.substrait.expression.*;
import io.substrait.expression.Expression.SingleOrList;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.Type;
import io.substrait.util.DecimalUtil;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

/**
 * ExpressionVisitor that converts Substrait Expression into Calcite Rex. Unsupported Expression
 * node will call visitFallback and throw UnsupportedOperationException.
 */
public class ExpressionRexConverter extends AbstractExpressionVisitor<RexNode, RuntimeException>
    implements FunctionArg.FuncArgVisitor<RexNode, RuntimeException> {
  private final RelDataTypeFactory typeFactory;
  private final TypeConverter typeConverter;
  private final RexBuilder rexBuilder;
  private final ScalarFunctionConverter scalarFunctionConverter;

  private final AggregateFunctionConverter aggregateFunctionConverter;

  private static final SqlIntervalQualifier YEAR_MONTH_INTERVAL =
      new SqlIntervalQualifier(
          org.apache.calcite.avatica.util.TimeUnit.YEAR,
          -1,
          org.apache.calcite.avatica.util.TimeUnit.MONTH,
          -1,
          SqlParserPos.QUOTED_ZERO);

  private static final SqlIntervalQualifier DAY_SECOND_INTERVAL =
      new SqlIntervalQualifier(
          org.apache.calcite.avatica.util.TimeUnit.DAY,
          -1,
          org.apache.calcite.avatica.util.TimeUnit.SECOND,
          3, // Calcite only supports millisecond at the moment
          SqlParserPos.QUOTED_ZERO);

  public ExpressionRexConverter(
      RelDataTypeFactory typeFactory,
      ScalarFunctionConverter scalarFunctionConverter,
      AggregateFunctionConverter aggregateFunctionConverter,
      TypeConverter typeConverter) {
    this.typeFactory = typeFactory;
    this.typeConverter = typeConverter;
    this.rexBuilder = new RexBuilder(typeFactory);
    this.scalarFunctionConverter = scalarFunctionConverter;
    this.aggregateFunctionConverter = aggregateFunctionConverter;
  }

  @Override
  public RexNode visit(Expression.NullLiteral expr) throws RuntimeException {
    return rexBuilder.makeLiteral(null, typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.BoolLiteral expr) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value());
  }

  @Override
  public RexNode visit(Expression.I8Literal expr) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.I16Literal expr) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.I32Literal expr) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.I64Literal expr) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.FP32Literal expr) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.FP64Literal expr) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.FixedCharLiteral expr) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value());
  }

  @Override
  public RexNode visit(Expression.StrLiteral expr) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.BinaryLiteral expr) throws RuntimeException {
    // Calcite RexLiteral only takes ByteString
    return rexBuilder.makeLiteral(
        new ByteString(expr.value().toByteArray()),
        typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.TimeLiteral expr) throws RuntimeException {
    // Expression.TimeLiteral is Microseconds
    // Construct a TimeString :
    // 1. Truncate microseconds to seconds
    // 2. Get the fraction seconds in precision of nanoseconds.
    // 3. Construct TimeString :  seconds  + fraction_seconds part.
    long microSec = expr.value();
    long seconds = TimeUnit.MICROSECONDS.toSeconds(microSec);
    int fracSecondsInNano =
        (int) (TimeUnit.MICROSECONDS.toNanos(microSec) - TimeUnit.SECONDS.toNanos(seconds));
    TimeString timeString =
        TimeString.fromMillisOfDay((int) TimeUnit.SECONDS.toMillis(seconds))
            .withNanos(fracSecondsInNano);
    return rexBuilder.makeLiteral(timeString, typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(SingleOrList expr) throws RuntimeException {
    var lhs = expr.condition().accept(this);
    return rexBuilder.makeIn(
        lhs, expr.options().stream().map(e -> e.accept(this)).collect(Collectors.toList()));
  }

  @Override
  public RexNode visit(Expression.DateLiteral expr) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.TimestampLiteral expr) throws RuntimeException {
    // Expression.TimestampLiteral is microseconds
    // Construct a TimeStampString :
    // 1. Truncate microseconds to seconds
    // 2. Get the fraction seconds in precision of nanoseconds.
    // 3. Construct TimeStampString :  seconds  + fraction_seconds part.
    long microSec = expr.value();
    long seconds = TimeUnit.MICROSECONDS.toSeconds(microSec);
    int fracSecondsInNano =
        (int) (TimeUnit.MICROSECONDS.toNanos(microSec) - TimeUnit.SECONDS.toNanos(seconds));

    TimestampString tsString =
        TimestampString.fromMillisSinceEpoch(TimeUnit.SECONDS.toMillis(seconds))
            .withNanos(fracSecondsInNano);
    return rexBuilder.makeLiteral(tsString, typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.IntervalYearLiteral expr) throws RuntimeException {
    return rexBuilder.makeIntervalLiteral(
        new BigDecimal(expr.years() * 12 + expr.months()), YEAR_MONTH_INTERVAL);
  }

  private static final long MICROS_IN_DAY = TimeUnit.DAYS.toMicros(1);

  @Override
  public RexNode visit(Expression.IntervalDayLiteral expr) throws RuntimeException {
    return rexBuilder.makeIntervalLiteral(
        // Current Calcite behavior is to store milliseconds since Epoch
        // microseconds version: new BigDecimal(expr.days() * MICROS_IN_DAY + expr.seconds() *
        // 100000L + expr.microseconds()), DAY_SECOND_INTERVAL);
        new BigDecimal(
            (expr.days() * MICROS_IN_DAY + expr.seconds() * 1_000_000L + expr.microseconds())
                / 1000L),
        DAY_SECOND_INTERVAL);
  }

  @Override
  public RexNode visit(Expression.DecimalLiteral expr) throws RuntimeException {
    byte[] value = expr.value().toByteArray();
    BigDecimal decimal = DecimalUtil.getBigDecimalFromBytes(value, expr.scale(), 16);
    return rexBuilder.makeLiteral(decimal, typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.IfThen expr) throws RuntimeException {
    // In Calcite, the arguments to the CASE operator are given as:
    //   <cond1> <value1> <cond2> <value2> ... <condN> <valueN> ... <else>
    Stream<RexNode> ifThenArgs =
        expr.ifClauses().stream()
            .flatMap(
                clause -> Stream.of(clause.condition().accept(this), clause.then().accept(this)));
    Stream<RexNode> elseArg = Stream.of(expr.elseClause().accept(this));
    List<RexNode> args = Stream.concat(ifThenArgs, elseArg).collect(Collectors.toList());
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, args);
  }

  @Override
  public RexNode visit(Expression.ScalarFunctionInvocation expr) throws RuntimeException {
    var eArgs = expr.arguments();
    var args =
        IntStream.range(0, expr.arguments().size())
            .mapToObj(i -> eArgs.get(i).accept(expr.declaration(), i, this))
            .collect(java.util.stream.Collectors.toList());

    Optional<SqlOperator> operator =
        scalarFunctionConverter.getSqlOperatorFromSubstraitFunc(
            expr.declaration().key(), expr.outputType());
    if (operator.isPresent()) {
      return rexBuilder.makeCall(operator.get(), args);
    } else {
      String msg =
          String.format(
              "Unable to convert scalar function %s(%s).",
              expr.declaration().name(),
              expr.arguments().stream().map(this::convert).collect(Collectors.joining(", ")));
      throw new IllegalArgumentException(msg);
    }
  }

  private String convert(FunctionArg a) {
    String v;
    if (a instanceof EnumArg ea) {
      v = ea.value().toString();
    } else if (a instanceof Expression e) {
      v = e.getType().accept(new StringTypeVisitor());
    } else if (a instanceof Type t) {
      v = t.accept(new StringTypeVisitor());
    } else {
      throw new IllegalStateException("Unexpected value: " + a);
    }
    return v;
  }

  @Override
  public RexNode visit(Expression.Cast expr) throws RuntimeException {
    return rexBuilder.makeAbstractCast(
        typeConverter.toCalcite(typeFactory, expr.getType()), expr.input().accept(this));
  }

  @Override
  public RexNode visit(FieldReference expr) throws RuntimeException {
    if (expr.isSimpleRootReference()) {
      var segment = expr.segments().get(0);

      RexInputRef rexInputRef;
      if (segment instanceof FieldReference.StructField f) {
        rexInputRef =
            new RexInputRef(f.offset(), typeConverter.toCalcite(typeFactory, expr.getType()));
      } else {
        throw new IllegalArgumentException("Unhandled type: " + segment);
      }

      return rexInputRef;
    }

    return visitFallback(expr);
  }

  @Override
  public RexNode visit(Expression.Window expr) throws RuntimeException {
    // todo:to construct the RexOver
    return visitFallback(expr);
  }

  @Override
  public RexNode visitFallback(Expression expr) {
    throw new UnsupportedOperationException(
        String.format(
            "Expression %s of type %s not handled by visitor type %s.",
            expr, expr.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }

  @Override
  public RexNode visitExpr(SimpleExtension.Function fnDef, int argIdx, Expression e)
      throws RuntimeException {
    return e.accept(this);
  }

  @Override
  public RexNode visitType(SimpleExtension.Function fnDef, int argIdx, Type t)
      throws RuntimeException {
    throw new UnsupportedOperationException(
        String.format(
            "FunctionArg %s not handled by visitor type %s.",
            t, this.getClass().getCanonicalName()));
  }

  @Override
  public RexNode visitEnumArg(SimpleExtension.Function fnDef, int argIdx, EnumArg e)
      throws RuntimeException {

    return EnumConverter.toRex(rexBuilder, fnDef, argIdx, e)
        .orElseThrow(
            () ->
                new UnsupportedOperationException(
                    String.format(
                        "EnumArg(value=%s) not handled by visitor type %s.",
                        e.value(), this.getClass().getCanonicalName())));
  }
}
