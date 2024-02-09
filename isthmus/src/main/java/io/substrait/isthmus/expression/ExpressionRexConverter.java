package io.substrait.isthmus.expression;

import static io.substrait.expression.Expression.SortDirection.ASC_NULLS_FIRST;
import static io.substrait.expression.Expression.SortDirection.ASC_NULLS_LAST;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.AbstractExpressionVisitor;
import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.SingleOrList;
import io.substrait.expression.Expression.Switch;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.WindowBound;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelNodeConverter;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.Type;
import io.substrait.util.DecimalUtil;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
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
  protected final RelDataTypeFactory typeFactory;
  protected final TypeConverter typeConverter;
  protected final RexBuilder rexBuilder;
  protected final ScalarFunctionConverter scalarFunctionConverter;
  protected final WindowFunctionConverter windowFunctionConverter;
  protected final WindowRelFunctionConverter windowRelFunctionConverter;
  protected SubstraitRelNodeConverter relNodeConverter;

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
      WindowFunctionConverter windowFunctionConverter,
      WindowRelFunctionConverter windowRelFunctionConverter,
      TypeConverter typeConverter) {
    this.typeFactory = typeFactory;
    this.typeConverter = typeConverter;
    this.rexBuilder = new RexBuilder(typeFactory);
    this.scalarFunctionConverter = scalarFunctionConverter;
    this.windowFunctionConverter = windowFunctionConverter;
    this.windowRelFunctionConverter = windowRelFunctionConverter;
  }

  public void setRelNodeConverter(final SubstraitRelNodeConverter substraitRelNodeConverter) {
    this.relNodeConverter = substraitRelNodeConverter;
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
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()), true);
  }

  @Override
  public RexNode visit(Expression.VarCharLiteral expr) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()), true);
  }

  @Override
  public RexNode visit(Expression.FixedBinaryLiteral expr) throws RuntimeException {
    return rexBuilder.makeLiteral(
        new ByteString(expr.value().toByteArray()),
        typeConverter.toCalcite(typeFactory, expr.getType()),
        true);
  }

  @Override
  public RexNode visit(Expression.BinaryLiteral expr) throws RuntimeException {
    return rexBuilder.makeLiteral(
        new ByteString(expr.value().toByteArray()),
        typeConverter.toCalcite(typeFactory, expr.getType()),
        true);
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
  public RexNode visit(Expression.ListLiteral expr) throws RuntimeException {
    List<RexNode> args =
        expr.values().stream().map(l -> l.accept(this)).collect(Collectors.toList());
    return rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, args);
  }

  @Override
  public RexNode visit(Expression.MapLiteral expr) throws RuntimeException {
    var args =
        expr.values().entrySet().stream()
            .flatMap(entry -> Stream.of(entry.getKey().accept(this), entry.getValue().accept(this)))
            .collect(Collectors.toList());
    return rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, args);
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
  public RexNode visit(Switch expr) throws RuntimeException {
    RexNode match = expr.match().accept(this);
    Stream<RexNode> caseThenArgs =
        expr.switchClauses().stream()
            .flatMap(
                clause ->
                    Stream.of(
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.EQUALS, match, clause.condition().accept(this)),
                        clause.then().accept(this)));
    Stream<RexNode> defaultArg = Stream.of(expr.defaultClause().accept(this));
    List<RexNode> args = Stream.concat(caseThenArgs, defaultArg).collect(Collectors.toList());
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, args);
  }

  @Override
  public RexNode visit(Expression.ScalarFunctionInvocation expr) throws RuntimeException {
    SqlOperator operator =
        scalarFunctionConverter
            .getSqlOperatorFromSubstraitFunc(expr.declaration().key(), expr.outputType())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        callConversionFailureMessage(
                            "scalar", expr.declaration().name(), expr.arguments())));

    var eArgs = expr.arguments();
    var args =
        IntStream.range(0, expr.arguments().size())
            .mapToObj(i -> eArgs.get(i).accept(expr.declaration(), i, this))
            .collect(java.util.stream.Collectors.toList());

    return rexBuilder.makeCall(operator, args);
  }

  private String callConversionFailureMessage(
      String functionType, String name, List<FunctionArg> args) {
    return String.format(
        "Unable to convert %s function %s(%s).",
        functionType, name, args.stream().map(this::convert).collect(Collectors.joining(", ")));
  }

  @Override
  public RexNode visit(Expression.WindowFunctionInvocation expr) throws RuntimeException {
    SqlOperator operator =
        windowFunctionConverter
            .getSqlOperatorFromSubstraitFunc(expr.declaration().key(), expr.outputType())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        callConversionFailureMessage(
                            "window", expr.declaration().name(), expr.arguments())));

    RelDataType outputType = typeConverter.toCalcite(typeFactory, expr.outputType());

    List<FunctionArg> eArgs = expr.arguments();
    List<RexNode> args =
        IntStream.range(0, expr.arguments().size())
            .mapToObj(i -> eArgs.get(i).accept(expr.declaration(), i, this))
            .collect(java.util.stream.Collectors.toList());

    List<RexNode> partitionKeys =
        expr.partitionBy().stream().map(e -> e.accept(this)).collect(Collectors.toList());

    ImmutableList<RexFieldCollation> orderKeys =
        expr.sort().stream()
            .map(
                sf -> {
                  Set<SqlKind> direction =
                      switch (sf.direction()) {
                        case ASC_NULLS_FIRST -> Set.of(SqlKind.NULLS_FIRST);
                        case ASC_NULLS_LAST -> Set.of(SqlKind.NULLS_LAST);
                        case DESC_NULLS_FIRST -> Set.of(SqlKind.DESCENDING, SqlKind.NULLS_FIRST);
                        case DESC_NULLS_LAST -> Set.of(SqlKind.DESCENDING, SqlKind.NULLS_LAST);
                        case CLUSTERED -> throw new IllegalArgumentException(
                            "SORT_DIRECTION_CLUSTERED is not supported");
                      };
                  return new RexFieldCollation(sf.expr().accept(this), direction);
                })
            .collect(ImmutableList.toImmutableList());

    RexWindowBound lowerBound = ToRexWindowBound.lowerBound(rexBuilder, expr.lowerBound());
    RexWindowBound upperBound = ToRexWindowBound.upperBound(rexBuilder, expr.upperBound());

    boolean rowMode =
        switch (expr.boundsType()) {
          case ROWS -> true;
          case RANGE -> false;
          case UNSPECIFIED -> throw new IllegalArgumentException(
              "bounds type on window function must be specified");
        };

    boolean distinct =
        switch (expr.invocation()) {
          case UNSPECIFIED, ALL -> false;
          case DISTINCT -> true;
        };

    // For queries like: SELECT last_value() IGNORE NULLS OVER ...
    // Substrait has no mechanism to set this, so by default it is false
    boolean ignoreNulls = false;

    // These both control a rewrite rule within rexBuilder.makeOver that rewrites the given
    // expression into a case expression. These values are set as such to avoid this rewrite.
    boolean nullWhenCountZero = false;
    boolean allowPartial = true;

    return rexBuilder.makeOver(
        outputType,
        (SqlAggFunction) operator,
        args,
        partitionKeys,
        orderKeys,
        lowerBound,
        upperBound,
        rowMode,
        allowPartial,
        nullWhenCountZero,
        distinct,
        ignoreNulls);
  }

  public RexNode visit(Expression.WindowRelFunctionInvocation expr) throws RuntimeException {
    SqlOperator operator =
        windowRelFunctionConverter
            .getSqlOperatorFromSubstraitFunc(expr.declaration().key(), expr.outputType())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        callConversionFailureMessage(
                            "windowRel", expr.declaration().name(), expr.arguments())));

    RelDataType outputType = typeConverter.toCalcite(typeFactory, expr.outputType());

    List<FunctionArg> eArgs = expr.arguments();
    List<RexNode> args =
        IntStream.range(0, expr.arguments().size())
            .mapToObj(i -> eArgs.get(i).accept(expr.declaration(), i, this))
            .collect(java.util.stream.Collectors.toList());

    RexWindowBound lowerBound = ToRexWindowBound.lowerBound(rexBuilder, expr.lowerBound());
    RexWindowBound upperBound = ToRexWindowBound.upperBound(rexBuilder, expr.upperBound());

    boolean rowMode =
        switch (expr.boundsType()) {
          case ROWS -> true;
          case RANGE -> false;
          case UNSPECIFIED -> throw new IllegalArgumentException(
              "bounds type on window function must be specified");
        };

    boolean distinct =
        switch (expr.invocation()) {
          case UNSPECIFIED, ALL -> false;
          case DISTINCT -> true;
        };

    // For queries like: SELECT last_value() IGNORE NULLS OVER ...
    // Substrait has no mechanism to set this, so by default it is false
    boolean ignoreNulls = false;

    // These both control a rewrite rule within rexBuilder.makeOver that rewrites the given
    // expression into a case expression. These values are set as such to avoid this rewrite.
    boolean nullWhenCountZero = false;
    boolean allowPartial = true;

    // TODO: what to pass on partitionKeys / orderKeys? send the ConsistentWindowRel?
    return rexBuilder.makeOver(
        outputType,
        (SqlAggFunction) operator,
        args,
        null,
        null,
        lowerBound,
        upperBound,
        rowMode,
        allowPartial,
        nullWhenCountZero,
        distinct,
        ignoreNulls);
  }

  @Override
  public RexNode visit(Expression.InPredicate expr) throws RuntimeException {
    List<RexNode> needles =
        expr.needles().stream().map(e -> e.accept(this)).collect(Collectors.toList());
    RelNode rel = expr.haystack().accept(relNodeConverter);
    return RexSubQuery.in(rel, ImmutableList.copyOf(needles));
  }

  static class ToRexWindowBound
      implements WindowBound.WindowBoundVisitor<RexWindowBound, RuntimeException> {

    static RexWindowBound lowerBound(RexBuilder rexBuilder, WindowBound bound) {
      // per the spec, unbounded on the lower bound means the start of the partition
      // thus UNBOUNDED_PRECEDING should be used when bound is unbounded
      return bound.accept(new ToRexWindowBound(rexBuilder, RexWindowBounds.UNBOUNDED_PRECEDING));
    }

    static RexWindowBound upperBound(RexBuilder rexBuilder, WindowBound bound) {
      // per the spec, unbounded on the upper bound means the end of the partition
      // thus UNBOUNDED_FOLLOWING should be used when bound is unbounded
      return bound.accept(new ToRexWindowBound(rexBuilder, RexWindowBounds.UNBOUNDED_FOLLOWING));
    }

    private final RexBuilder rexBuilder;
    private final RexWindowBound unboundedVariant;

    private ToRexWindowBound(RexBuilder rexBuilder, RexWindowBound unboundedVariant) {
      this.rexBuilder = rexBuilder;
      this.unboundedVariant = unboundedVariant;
    }

    @Override
    public RexWindowBound visit(WindowBound.Preceding preceding) {
      var offset = BigDecimal.valueOf(preceding.offset());
      return RexWindowBounds.preceding(rexBuilder.makeBigintLiteral(offset));
    }

    @Override
    public RexWindowBound visit(WindowBound.Following following) {
      var offset = BigDecimal.valueOf(following.offset());
      return RexWindowBounds.following(rexBuilder.makeBigintLiteral(offset));
    }

    @Override
    public RexWindowBound visit(WindowBound.CurrentRow currentRow) {
      return RexWindowBounds.CURRENT_ROW;
    }

    @Override
    public RexWindowBound visit(WindowBound.Unbounded unbounded) {
      return unboundedVariant;
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
