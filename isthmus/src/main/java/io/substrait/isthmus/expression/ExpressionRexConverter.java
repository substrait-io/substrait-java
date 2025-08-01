package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import io.substrait.expression.AbstractExpressionVisitor;
import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.Expression.FailureBehavior;
import io.substrait.expression.Expression.PrecisionTimestampLiteral;
import io.substrait.expression.Expression.PrecisionTimestampTZLiteral;
import io.substrait.expression.Expression.ScalarSubquery;
import io.substrait.expression.Expression.SetPredicate;
import io.substrait.expression.Expression.SingleOrList;
import io.substrait.expression.Expression.Switch;
import io.substrait.expression.Expression.TimestampTZLiteral;
import io.substrait.expression.FieldReference;
import io.substrait.expression.FieldReference.ReferenceSegment;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.WindowBound;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelNodeConverter;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.Type;
import io.substrait.util.DecimalUtil;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
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
public class ExpressionRexConverter
    extends AbstractExpressionVisitor<RexNode, Context, RuntimeException>
    implements FunctionArg.FuncArgVisitor<RexNode, Context, RuntimeException> {
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

  private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

  protected final RelDataTypeFactory typeFactory;
  protected final TypeConverter typeConverter;
  protected final RexBuilder rexBuilder;
  protected final ScalarFunctionConverter scalarFunctionConverter;
  protected final WindowFunctionConverter windowFunctionConverter;
  protected SubstraitRelNodeConverter relNodeConverter;

  public ExpressionRexConverter(
      RelDataTypeFactory typeFactory,
      ScalarFunctionConverter scalarFunctionConverter,
      WindowFunctionConverter windowFunctionConverter,
      TypeConverter typeConverter) {
    this.typeFactory = typeFactory;
    this.typeConverter = typeConverter;
    this.rexBuilder = new RexBuilder(typeFactory);
    this.scalarFunctionConverter = scalarFunctionConverter;
    this.windowFunctionConverter = windowFunctionConverter;
  }

  public void setRelNodeConverter(final SubstraitRelNodeConverter substraitRelNodeConverter) {
    this.relNodeConverter = substraitRelNodeConverter;
  }

  @Override
  public RexNode visit(Expression.NullLiteral expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(null, typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.UserDefinedLiteral expr, Context context)
      throws RuntimeException {
    RexLiteral binaryLiteral =
        rexBuilder.makeBinaryLiteral(new ByteString(expr.value().toByteArray()));
    RelDataType type = typeConverter.toCalcite(typeFactory, expr.getType());
    return rexBuilder.makeReinterpretCast(type, binaryLiteral, rexBuilder.makeLiteral(false));
  }

  @Override
  public RexNode visit(Expression.BoolLiteral expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value());
  }

  @Override
  public RexNode visit(Expression.I8Literal expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.I16Literal expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.I32Literal expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.I64Literal expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.FP32Literal expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.FP64Literal expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.FixedCharLiteral expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(expr.value());
  }

  @Override
  public RexNode visit(Expression.StrLiteral expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()), true);
  }

  @Override
  public RexNode visit(Expression.VarCharLiteral expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()), true);
  }

  @Override
  public RexNode visit(Expression.FixedBinaryLiteral expr, Context context)
      throws RuntimeException {
    return rexBuilder.makeLiteral(
        new ByteString(expr.value().toByteArray()),
        typeConverter.toCalcite(typeFactory, expr.getType()),
        true);
  }

  @Override
  public RexNode visit(Expression.BinaryLiteral expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        new ByteString(expr.value().toByteArray()),
        typeConverter.toCalcite(typeFactory, expr.getType()),
        true);
  }

  @Override
  public RexNode visit(Expression.TimeLiteral expr, Context context) throws RuntimeException {
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
  public RexNode visit(SingleOrList expr, Context context) throws RuntimeException {
    RexNode lhs = expr.condition().accept(this, context);
    return rexBuilder.makeIn(
        lhs,
        expr.options().stream().map(e -> e.accept(this, context)).collect(Collectors.toList()));
  }

  @Override
  public RexNode visit(Expression.DateLiteral expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        expr.value(), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.TimestampLiteral expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        getTimestampString(expr.value()), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(TimestampTZLiteral expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        getTimestampString(expr.value()), typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(PrecisionTimestampLiteral expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        getTimestampString(expr.value(), expr.precision()),
        typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(PrecisionTimestampTZLiteral expr, Context context) throws RuntimeException {
    return rexBuilder.makeLiteral(
        getTimestampString(expr.value(), expr.precision()),
        typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  private TimestampString getTimestampString(long microSec) {
    return getTimestampString(microSec, 6);
  }

  private TimestampString getTimestampString(long value, int precision) {
    switch (precision) {
      case 0:
        return TimestampString.fromMillisSinceEpoch(TimeUnit.SECONDS.toMillis(value));
      case 3:
        {
          long seconds = TimeUnit.MILLISECONDS.toSeconds(value);
          int fracSecondsInNano =
              (int) (TimeUnit.MILLISECONDS.toNanos(value) - TimeUnit.SECONDS.toNanos(seconds));
          return TimestampString.fromMillisSinceEpoch(TimeUnit.SECONDS.toMillis(seconds))
              .withNanos(fracSecondsInNano);
        }
      case 6:
        {
          long seconds = TimeUnit.MICROSECONDS.toSeconds(value);
          int fracSecondsInNano =
              (int) (TimeUnit.MICROSECONDS.toNanos(value) - TimeUnit.SECONDS.toNanos(seconds));
          return TimestampString.fromMillisSinceEpoch(TimeUnit.SECONDS.toMillis(seconds))
              .withNanos(fracSecondsInNano);
        }
      case 9:
        {
          long seconds = TimeUnit.NANOSECONDS.toSeconds(value);
          int fracSecondsInNano = (int) (value - TimeUnit.SECONDS.toNanos(seconds));
          return TimestampString.fromMillisSinceEpoch(TimeUnit.SECONDS.toMillis(seconds))
              .withNanos(fracSecondsInNano);
        }
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot handle PrecisionTimestamp with precision %d.", precision));
    }
  }

  @Override
  public RexNode visit(Expression.IntervalYearLiteral expr, Context context)
      throws RuntimeException {
    return rexBuilder.makeIntervalLiteral(
        new BigDecimal(expr.years() * 12 + expr.months()), YEAR_MONTH_INTERVAL);
  }

  @Override
  public RexNode visit(Expression.IntervalDayLiteral expr, Context context)
      throws RuntimeException {
    long milliseconds =
        expr.precision() > 3
            ? (expr.subseconds() / (int) Math.pow(10, expr.precision() - 3))
            : (expr.subseconds() * (int) Math.pow(10, 3 - expr.precision()));
    return rexBuilder.makeIntervalLiteral(
        // Current Calcite behavior is to store milliseconds since Epoch
        new BigDecimal((expr.days() * MILLIS_IN_DAY + expr.seconds() * 1_000L + milliseconds)),
        DAY_SECOND_INTERVAL);
  }

  @Override
  public RexNode visit(Expression.DecimalLiteral expr, Context context) throws RuntimeException {
    byte[] value = expr.value().toByteArray();
    BigDecimal decimal = DecimalUtil.getBigDecimalFromBytes(value, expr.scale(), 16);
    return rexBuilder.makeLiteral(decimal, typeConverter.toCalcite(typeFactory, expr.getType()));
  }

  @Override
  public RexNode visit(Expression.ListLiteral expr, Context context) throws RuntimeException {
    List<RexNode> args =
        expr.values().stream().map(l -> l.accept(this, context)).collect(Collectors.toList());
    return rexBuilder.makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, args);
  }

  @Override
  public RexNode visit(Expression.EmptyListLiteral expr, Context context) throws RuntimeException {
    RelDataType calciteType = typeConverter.toCalcite(typeFactory, expr.getType());
    return rexBuilder.makeCall(
        calciteType, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, Collections.emptyList());
  }

  @Override
  public RexNode visit(Expression.MapLiteral expr, Context context) throws RuntimeException {
    List<RexNode> args =
        expr.values().entrySet().stream()
            .flatMap(
                entry ->
                    Stream.of(
                        entry.getKey().accept(this, context),
                        entry.getValue().accept(this, context)))
            .collect(Collectors.toList());
    return rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, args);
  }

  @Override
  public RexNode visit(Expression.IfThen expr, Context context) throws RuntimeException {
    // In Calcite, the arguments to the CASE operator are given as:
    //   <cond1> <value1> <cond2> <value2> ... <condN> <valueN> ... <else>
    Stream<RexNode> ifThenArgs =
        expr.ifClauses().stream()
            .flatMap(
                clause ->
                    Stream.of(
                        clause.condition().accept(this, context),
                        clause.then().accept(this, context)));
    Stream<RexNode> elseArg = Stream.of(expr.elseClause().accept(this, context));
    List<RexNode> args = Stream.concat(ifThenArgs, elseArg).collect(Collectors.toList());
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, args);
  }

  @Override
  public RexNode visit(Switch expr, Context context) throws RuntimeException {
    RexNode match = expr.match().accept(this, context);
    Stream<RexNode> caseThenArgs =
        expr.switchClauses().stream()
            .flatMap(
                clause ->
                    Stream.of(
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.EQUALS,
                            match,
                            clause.condition().accept(this, context)),
                        clause.then().accept(this, context)));
    Stream<RexNode> defaultArg = Stream.of(expr.defaultClause().accept(this, context));
    List<RexNode> args = Stream.concat(caseThenArgs, defaultArg).collect(Collectors.toList());
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, args);
  }

  @Override
  public RexNode visit(Expression.ScalarFunctionInvocation expr, Context context)
      throws RuntimeException {
    SqlOperator operator =
        scalarFunctionConverter
            .getSqlOperatorFromSubstraitFunc(expr.declaration().key(), expr.outputType())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        callConversionFailureMessage(
                            "scalar", expr.declaration().name(), expr.arguments())));

    List<FunctionArg> eArgs = scalarFunctionConverter.getExpressionArguments(expr);
    List<RexNode> args =
        IntStream.range(0, eArgs.size())
            .mapToObj(i -> eArgs.get(i).accept(expr.declaration(), i, this, context))
            .collect(Collectors.toList());

    RelDataType returnType = typeConverter.toCalcite(typeFactory, expr.outputType());
    return rexBuilder.makeCall(returnType, operator, args);
  }

  private String callConversionFailureMessage(
      String functionType, String name, List<FunctionArg> args) {
    return String.format(
        "Unable to convert %s function %s(%s).",
        functionType, name, args.stream().map(this::convert).collect(Collectors.joining(", ")));
  }

  @Override
  public RexNode visit(Expression.WindowFunctionInvocation expr, Context context)
      throws RuntimeException {
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
        IntStream.range(0, eArgs.size())
            .mapToObj(i -> eArgs.get(i).accept(expr.declaration(), i, this, context))
            .collect(Collectors.toList());

    List<RexNode> partitionKeys =
        expr.partitionBy().stream().map(e -> e.accept(this, context)).collect(Collectors.toList());

    ImmutableList<RexFieldCollation> orderKeys =
        expr.sort().stream()
            .map(
                sf -> {
                  Set<SqlKind> direction = asSqlKind(sf.direction());
                  return new RexFieldCollation(sf.expr().accept(this, context), direction);
                })
            .collect(ImmutableList.toImmutableList());

    RexWindowBound lowerBound = ToRexWindowBound.lowerBound(rexBuilder, expr.lowerBound());
    RexWindowBound upperBound = ToRexWindowBound.upperBound(rexBuilder, expr.upperBound());

    boolean rowMode = isRowMode(expr);
    boolean distinct = isDistinct(expr);

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

  private Set<SqlKind> asSqlKind(Expression.SortDirection direction) {
    switch (direction) {
      case ASC_NULLS_FIRST:
        return Set.of(SqlKind.NULLS_FIRST);
      case ASC_NULLS_LAST:
        return Set.of(SqlKind.NULLS_LAST);
      case DESC_NULLS_FIRST:
        return Set.of(SqlKind.DESCENDING, SqlKind.NULLS_FIRST);
      case DESC_NULLS_LAST:
        return Set.of(SqlKind.DESCENDING, SqlKind.NULLS_LAST);
      case CLUSTERED:
        throw new IllegalArgumentException("SORT_DIRECTION_CLUSTERED is not supported");
      default:
        throw new IllegalArgumentException("Unsupported sort direction: " + direction);
    }
  }

  private boolean isRowMode(Expression.WindowFunctionInvocation expr) {
    Expression.WindowBoundsType boundsType = expr.boundsType();

    switch (boundsType) {
      case ROWS:
        return true;
      case RANGE:
        return false;
      case UNSPECIFIED:
        throw new IllegalArgumentException("bounds type on window function must be specified");
      default:
        throw new IllegalArgumentException(
            "Unsupported window function bounds type: " + boundsType);
    }
  }

  private boolean isDistinct(Expression.WindowFunctionInvocation expr) {
    Expression.AggregationInvocation invocation = expr.invocation();

    switch (invocation) {
      case UNSPECIFIED:
      case ALL:
        return false;
      case DISTINCT:
        return true;
      default:
        throw new IllegalArgumentException("Unsupported window function invocation: " + invocation);
    }
  }

  @Override
  public RexNode visit(Expression.InPredicate expr, Context context) throws RuntimeException {
    List<RexNode> needles =
        expr.needles().stream().map(e -> e.accept(this, context)).collect(Collectors.toList());
    context.incrementSubqueryDepth();
    RelNode rel = expr.haystack().accept(relNodeConverter, context);
    context.decrementSubqueryDepth();
    return RexSubQuery.in(rel, ImmutableList.copyOf(needles));
  }

  static class ToRexWindowBound
      implements WindowBound.WindowBoundVisitor<RexWindowBound, RuntimeException> {

    private final RexBuilder rexBuilder;
    private final RexWindowBound unboundedVariant;

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

    private ToRexWindowBound(RexBuilder rexBuilder, RexWindowBound unboundedVariant) {
      this.rexBuilder = rexBuilder;
      this.unboundedVariant = unboundedVariant;
    }

    @Override
    public RexWindowBound visit(WindowBound.Preceding preceding) {
      BigDecimal offset = BigDecimal.valueOf(preceding.offset());
      return RexWindowBounds.preceding(rexBuilder.makeBigintLiteral(offset));
    }

    @Override
    public RexWindowBound visit(WindowBound.Following following) {
      BigDecimal offset = BigDecimal.valueOf(following.offset());
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
    if (a instanceof EnumArg) {
      v = ((EnumArg) a).value().toString();
    } else if (a instanceof Expression) {
      v = ((Expression) a).getType().accept(new StringTypeVisitor());
    } else if (a instanceof Type) {
      v = ((Type) a).accept(new StringTypeVisitor());
    } else {
      throw new IllegalStateException("Unexpected value: " + a);
    }
    return v;
  }

  @Override
  public RexNode visit(Expression.Cast expr, Context context) throws RuntimeException {
    boolean safeCast = expr.failureBehavior() == FailureBehavior.RETURN_NULL;
    return rexBuilder.makeAbstractCast(
        typeConverter.toCalcite(typeFactory, expr.getType()),
        expr.input().accept(this, context),
        safeCast);
  }

  @Override
  public RexNode visit(FieldReference expr, Context context) throws RuntimeException {
    if (expr.isSimpleRootReference()) {
      final ReferenceSegment segment = expr.segments().get(0);

      final RexInputRef rexInputRef;
      if (segment instanceof FieldReference.StructField) {
        final FieldReference.StructField field = (FieldReference.StructField) segment;
        rexInputRef =
            new RexInputRef(field.offset(), typeConverter.toCalcite(typeFactory, expr.getType()));
      } else {
        throw new IllegalArgumentException("Unhandled type: " + segment);
      }

      return rexInputRef;
    } else if (expr.isOuterReference()) {
      final ReferenceSegment segment = expr.segments().get(0);

      final RexNode rexInputRef;
      if (segment instanceof FieldReference.StructField) {
        final FieldReference.StructField field = (FieldReference.StructField) segment;

        final RangeMap<Integer, RelDataType> fieldRangeMap =
            context.getOuterRowTypeRangeMap(expr.outerReferenceStepsOut().get());
        final Range<Integer> range = fieldRangeMap.getEntry(field.offset()).getKey();
        final int fieldOffset = field.offset() - range.lowerEndpoint();

        final CorrelationId correlationId =
            relNodeConverter.getRelBuilder().getCluster().createCorrel();
        context.addCorrelationId(expr.outerReferenceStepsOut().get(), correlationId);
        rexInputRef =
            rexBuilder.makeFieldAccess(
                rexBuilder.makeCorrel(fieldRangeMap.get(field.offset()), correlationId),
                fieldOffset);
      } else {
        throw new IllegalArgumentException("Unhandled type: " + segment);
      }

      return rexInputRef;
    }

    return visitFallback(expr, context);
  }

  @Override
  public RexNode visitFallback(Expression expr, Context context) {
    throw new UnsupportedOperationException(
        String.format(
            "Expression %s of type %s not handled by visitor type %s.",
            expr, expr.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }

  @Override
  public RexNode visitExpr(
      SimpleExtension.Function fnDef, int argIdx, Expression e, Context context)
      throws RuntimeException {
    return e.accept(this, context);
  }

  @Override
  public RexNode visitType(SimpleExtension.Function fnDef, int argIdx, Type t, Context context)
      throws RuntimeException {
    throw new UnsupportedOperationException(
        String.format(
            "FunctionArg %s not handled by visitor type %s.",
            t, this.getClass().getCanonicalName()));
  }

  @Override
  public RexNode visitEnumArg(
      SimpleExtension.Function fnDef, int argIdx, EnumArg e, Context context)
      throws RuntimeException {

    return EnumConverter.toRex(rexBuilder, fnDef, argIdx, e)
        .orElseThrow(
            () ->
                new UnsupportedOperationException(
                    String.format(
                        "EnumArg(value=%s) not handled by visitor type %s.",
                        e.value(), this.getClass().getCanonicalName())));
  }

  @Override
  public RexNode visit(ScalarSubquery expr, Context context) throws RuntimeException {
    context.incrementSubqueryDepth();
    RelNode inputRelnode = expr.input().accept(relNodeConverter, context);
    context.decrementSubqueryDepth();
    return RexSubQuery.scalar(inputRelnode);
  }

  @Override
  public RexNode visit(SetPredicate expr, Context context) throws RuntimeException {
    context.incrementSubqueryDepth();
    RelNode inputRelnode = expr.tuples().accept(relNodeConverter, context);
    context.decrementSubqueryDepth();
    switch (expr.predicateOp()) {
      case PREDICATE_OP_EXISTS:
        return RexSubQuery.exists(inputRelnode);
      case PREDICATE_OP_UNIQUE:
        return RexSubQuery.unique(inputRelnode);
      case PREDICATE_OP_UNSPECIFIED:
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot handle SetPredicate when PredicateOp is %s.", expr.predicateOp().name()));
    }
  }
}
