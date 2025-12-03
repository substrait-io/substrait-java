package io.substrait.expression;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.substrait.extension.SimpleExtension;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.type.Type;
import io.substrait.util.DecimalUtil;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ExpressionCreator {

  private ExpressionCreator() {}

  public static Expression.NullLiteral typedNull(final Type t) {
    return Expression.NullLiteral.builder().type(t).build();
  }

  public static Expression.BoolLiteral bool(final boolean nullable, final boolean value) {
    return Expression.BoolLiteral.builder().nullable(nullable).value(value).build();
  }

  public static Expression.I8Literal i8(final boolean nullable, final int value) {
    return Expression.I8Literal.builder().nullable(nullable).value(value).build();
  }

  public static Expression.I16Literal i16(final boolean nullable, final int value) {
    return Expression.I16Literal.builder().nullable(nullable).value(value).build();
  }

  public static Expression.I32Literal i32(final boolean nullable, final int value) {
    return Expression.I32Literal.builder().nullable(nullable).value(value).build();
  }

  public static Expression.I64Literal i64(final boolean nullable, final long value) {
    return Expression.I64Literal.builder().nullable(nullable).value(value).build();
  }

  public static Expression.FP32Literal fp32(final boolean nullable, final float value) {
    return Expression.FP32Literal.builder().nullable(nullable).value(value).build();
  }

  public static Expression.FP64Literal fp64(final boolean nullable, final double value) {
    return Expression.FP64Literal.builder().nullable(nullable).value(value).build();
  }

  public static Expression.StrLiteral string(final boolean nullable, final String value) {
    return Expression.StrLiteral.builder().nullable(nullable).value(value).build();
  }

  public static Expression.BinaryLiteral binary(final boolean nullable, final ByteString value) {
    return Expression.BinaryLiteral.builder().nullable(nullable).value(value).build();
  }

  public static Expression.BinaryLiteral binary(final boolean nullable, final byte[] value) {
    return Expression.BinaryLiteral.builder()
        .nullable(nullable)
        .value(ByteString.copyFrom(value))
        .build();
  }

  public static Expression.DateLiteral date(final boolean nullable, final int value) {
    return Expression.DateLiteral.builder().nullable(nullable).value(value).build();
  }

  public static Expression.TimeLiteral time(final boolean nullable, final long value) {
    return Expression.TimeLiteral.builder().nullable(nullable).value(value).build();
  }

  /**
   * @deprecated Timestamp is deprecated in favor of PrecisionTimestamp
   */
  @Deprecated
  public static Expression.TimestampLiteral timestamp(final boolean nullable, final long value) {
    return Expression.TimestampLiteral.builder().nullable(nullable).value(value).build();
  }

  /**
   * @deprecated Timestamp is deprecated in favor of PrecisionTimestamp
   */
  @Deprecated
  public static Expression.TimestampLiteral timestamp(
      final boolean nullable, final LocalDateTime value) {
    final long epochMicro =
        TimeUnit.SECONDS.toMicros(value.toEpochSecond(ZoneOffset.UTC))
            + TimeUnit.NANOSECONDS.toMicros(value.toLocalTime().getNano());
    return timestamp(nullable, epochMicro);
  }

  /**
   * @deprecated Timestamp is deprecated in favor of PrecisionTimestamp
   */
  @Deprecated
  public static Expression.TimestampLiteral timestamp(
      final boolean nullable,
      final int year,
      final int month,
      final int dayOfMonth,
      final int hour,
      final int minute,
      final int second,
      final int micros) {
    return timestamp(
        nullable,
        LocalDateTime.of(year, month, dayOfMonth, hour, minute, second)
            .withNano((int) TimeUnit.MICROSECONDS.toNanos(micros)));
  }

  /**
   * @deprecated TimestampTZ is deprecated in favor of PrecisionTimestampTZ
   */
  @Deprecated
  public static Expression.TimestampTZLiteral timestampTZ(
      final boolean nullable, final long value) {
    return Expression.TimestampTZLiteral.builder().nullable(nullable).value(value).build();
  }

  /**
   * @deprecated TimestampTZ is deprecated in favor of PrecisionTimestampTZ
   */
  @Deprecated
  public static Expression.TimestampTZLiteral timestampTZ(
      final boolean nullable, final Instant value) {
    final long epochMicro =
        TimeUnit.SECONDS.toMicros(value.getEpochSecond())
            + TimeUnit.NANOSECONDS.toMicros(value.getNano());
    return timestampTZ(nullable, epochMicro);
  }

  public static Expression.PrecisionTimestampLiteral precisionTimestamp(
      final boolean nullable, final long value, final int precision) {
    return Expression.PrecisionTimestampLiteral.builder()
        .nullable(nullable)
        .value(value)
        .precision(precision)
        .build();
  }

  public static Expression.PrecisionTimestampTZLiteral precisionTimestampTZ(
      final boolean nullable, final long value, final int precision) {
    return Expression.PrecisionTimestampTZLiteral.builder()
        .nullable(nullable)
        .value(value)
        .precision(precision)
        .build();
  }

  public static Expression.IntervalYearLiteral intervalYear(
      final boolean nullable, final int years, final int months) {
    return Expression.IntervalYearLiteral.builder()
        .nullable(nullable)
        .years(years)
        .months(months)
        .build();
  }

  public static Expression.IntervalDayLiteral intervalDay(
      final boolean nullable, final int days, final int seconds) {
    return intervalDay(nullable, days, seconds, 0, 0);
  }

  public static Expression.IntervalDayLiteral intervalDay(
      final boolean nullable,
      final int days,
      final int seconds,
      final long subseconds,
      final int precision) {
    return Expression.IntervalDayLiteral.builder()
        .nullable(nullable)
        .days(days)
        .seconds(seconds)
        .subseconds(subseconds)
        .precision(precision)
        .build();
  }

  public static Expression.IntervalCompoundLiteral intervalCompound(
      final boolean nullable,
      final int years,
      final int months,
      final int days,
      final int seconds,
      final long subseconds,
      final int precision) {
    return Expression.IntervalCompoundLiteral.builder()
        .nullable(nullable)
        .years(years)
        .months(months)
        .days(days)
        .seconds(seconds)
        .subseconds(subseconds)
        .precision(precision)
        .build();
  }

  public static Expression.UUIDLiteral uuid(final boolean nullable, final ByteString uuid) {
    final ByteBuffer bb = uuid.asReadOnlyByteBuffer();
    return Expression.UUIDLiteral.builder()
        .nullable(nullable)
        .value(new UUID(bb.getLong(), bb.getLong()))
        .build();
  }

  public static Expression.UUIDLiteral uuid(final boolean nullable, final UUID uuid) {
    return Expression.UUIDLiteral.builder().nullable(nullable).value(uuid).build();
  }

  public static Expression.FixedCharLiteral fixedChar(final boolean nullable, final String str) {
    return Expression.FixedCharLiteral.builder().nullable(nullable).value(str).build();
  }

  public static Expression.VarCharLiteral varChar(
      final boolean nullable, final String str, final int len) {
    return Expression.VarCharLiteral.builder().nullable(nullable).value(str).length(len).build();
  }

  public static Expression.FixedBinaryLiteral fixedBinary(
      final boolean nullable, final ByteString bytes) {
    return Expression.FixedBinaryLiteral.builder().nullable(nullable).value(bytes).build();
  }

  public static Expression.FixedBinaryLiteral fixedBinary(
      final boolean nullable, final byte[] bytes) {
    return Expression.FixedBinaryLiteral.builder()
        .nullable(nullable)
        .value(ByteString.copyFrom(bytes))
        .build();
  }

  public static Expression.DecimalLiteral decimal(
      final boolean nullable, final ByteString value, final int precision, final int scale) {
    return Expression.DecimalLiteral.builder()
        .nullable(nullable)
        .value(value)
        .precision(precision)
        .scale(scale)
        .build();
  }

  public static Expression.DecimalLiteral decimal(
      final boolean nullable, final BigDecimal value, final int precision, final int scale) {
    final byte[] twosComplement = DecimalUtil.encodeDecimalIntoBytes(value, scale, 16);

    return Expression.DecimalLiteral.builder()
        .nullable(nullable)
        .value(ByteString.copyFrom(twosComplement))
        .precision(precision)
        .scale(scale)
        .build();
  }

  public static Expression.MapLiteral map(
      final boolean nullable, final Map<Expression.Literal, Expression.Literal> values) {
    return Expression.MapLiteral.builder().nullable(nullable).putAllValues(values).build();
  }

  public static Expression.EmptyMapLiteral emptyMap(
      final boolean nullable, final Type keyType, final Type valueType) {
    return Expression.EmptyMapLiteral.builder()
        .keyType(keyType)
        .valueType(valueType)
        .nullable(nullable)
        .build();
  }

  public static Expression.ListLiteral list(
      final boolean nullable, final Expression.Literal... values) {
    return Expression.ListLiteral.builder().nullable(nullable).addValues(values).build();
  }

  public static Expression.ListLiteral list(
      final boolean nullable, final Iterable<? extends Expression.Literal> values) {
    return Expression.ListLiteral.builder().nullable(nullable).addAllValues(values).build();
  }

  public static Expression.EmptyListLiteral emptyList(
      final boolean listNullable, final Type elementType) {
    return Expression.EmptyListLiteral.builder()
        .elementType(elementType)
        .nullable(listNullable)
        .build();
  }

  public static Expression.StructLiteral struct(
      final boolean nullable, final Expression.Literal... values) {
    return Expression.StructLiteral.builder().nullable(nullable).addFields(values).build();
  }

  public static Expression.StructLiteral struct(
      final boolean nullable, final Iterable<? extends Expression.Literal> values) {
    return Expression.StructLiteral.builder().nullable(nullable).addAllFields(values).build();
  }

  public static Expression.UserDefinedLiteral userDefinedLiteral(
      final boolean nullable, final String urn, final String name, final Any value) {
    return Expression.UserDefinedLiteral.builder()
        .nullable(nullable)
        .urn(urn)
        .name(name)
        .value(value.toByteString())
        .build();
  }

  public static Expression.Switch switchStatement(
      final Expression match,
      final Expression defaultExpression,
      final Expression.SwitchClause... conditionClauses) {
    return Expression.Switch.builder()
        .match(match)
        .defaultClause(defaultExpression)
        .addSwitchClauses(conditionClauses)
        .build();
  }

  public static Expression.Switch switchStatement(
      final Expression match,
      final Expression defaultExpression,
      final Iterable<? extends Expression.SwitchClause> conditionClauses) {
    return Expression.Switch.builder()
        .match(match)
        .defaultClause(defaultExpression)
        .addAllSwitchClauses(conditionClauses)
        .build();
  }

  public static Expression.SwitchClause switchClause(
      final Expression.Literal expectedValue, final Expression resultExpression) {
    return Expression.SwitchClause.builder()
        .condition(expectedValue)
        .then(resultExpression)
        .build();
  }

  public static Expression.IfThen ifThenStatement(
      final Expression elseExpression, final Expression.IfClause... conditionClauses) {
    return Expression.IfThen.builder()
        .elseClause(elseExpression)
        .addIfClauses(conditionClauses)
        .build();
  }

  public static Expression.IfThen ifThenStatement(
      final Expression elseExpression,
      final Iterable<? extends Expression.IfClause> conditionClauses) {
    return Expression.IfThen.builder()
        .elseClause(elseExpression)
        .addAllIfClauses(conditionClauses)
        .build();
  }

  public static Expression.IfClause ifThenClause(
      final Expression conditionExpression, final Expression resultExpression) {
    return Expression.IfClause.builder()
        .condition(conditionExpression)
        .then(resultExpression)
        .build();
  }

  public static Expression.ScalarFunctionInvocation scalarFunction(
      final SimpleExtension.ScalarFunctionVariant declaration,
      final Type outputType,
      final FunctionArg... arguments) {
    return scalarFunction(declaration, outputType, Arrays.asList(arguments));
  }

  /**
   * Use {@link Expression.ScalarFunctionInvocation#builder()} directly to specify other parameters,
   * e.g. options
   */
  public static Expression.ScalarFunctionInvocation scalarFunction(
      final SimpleExtension.ScalarFunctionVariant declaration,
      final Type outputType,
      final Iterable<? extends FunctionArg> arguments) {
    return Expression.ScalarFunctionInvocation.builder()
        .declaration(declaration)
        .outputType(outputType)
        .addAllArguments(arguments)
        .build();
  }

  /**
   * Use {@link AggregateFunctionInvocation#builder()} directly to specify other parameters, e.g.
   * options
   */
  public static AggregateFunctionInvocation aggregateFunction(
      final SimpleExtension.AggregateFunctionVariant declaration,
      final Type outputType,
      final Expression.AggregationPhase phase,
      final List<Expression.SortField> sort,
      final Expression.AggregationInvocation invocation,
      final Iterable<? extends FunctionArg> arguments) {
    return AggregateFunctionInvocation.builder()
        .declaration(declaration)
        .outputType(outputType)
        .aggregationPhase(phase)
        .sort(sort)
        .invocation(invocation)
        .addAllArguments(arguments)
        .build();
  }

  public static AggregateFunctionInvocation aggregateFunction(
      final SimpleExtension.AggregateFunctionVariant declaration,
      final Type outputType,
      final Expression.AggregationPhase phase,
      final List<Expression.SortField> sort,
      final Expression.AggregationInvocation invocation,
      final FunctionArg... arguments) {
    return aggregateFunction(
        declaration, outputType, phase, sort, invocation, Arrays.asList(arguments));
  }

  /**
   * Use {@link Expression.WindowFunctionInvocation#builder()} directly to specify other parameters,
   * e.g. options
   */
  public static Expression.WindowFunctionInvocation windowFunction(
      final SimpleExtension.WindowFunctionVariant declaration,
      final Type outputType,
      final Expression.AggregationPhase phase,
      final List<Expression.SortField> sort,
      final Expression.AggregationInvocation invocation,
      final List<Expression> partitionBy,
      final Expression.WindowBoundsType boundsType,
      final WindowBound lowerBound,
      final WindowBound upperBound,
      final Iterable<? extends FunctionArg> arguments) {
    return Expression.WindowFunctionInvocation.builder()
        .declaration(declaration)
        .outputType(outputType)
        .aggregationPhase(phase)
        .sort(sort)
        .partitionBy(partitionBy)
        .boundsType(boundsType)
        .lowerBound(lowerBound)
        .upperBound(upperBound)
        .invocation(invocation)
        .addAllArguments(arguments)
        .build();
  }

  /**
   * Use {@link ConsistentPartitionWindow.WindowRelFunctionInvocation#builder()} directly to specify
   * other parameters, e.g. options
   */
  public static ConsistentPartitionWindow.WindowRelFunctionInvocation windowRelFunction(
      final SimpleExtension.WindowFunctionVariant declaration,
      final Type outputType,
      final Expression.AggregationPhase phase,
      final Expression.AggregationInvocation invocation,
      final Expression.WindowBoundsType boundsType,
      final WindowBound lowerBound,
      final WindowBound upperBound,
      final Iterable<? extends FunctionArg> arguments) {
    return ConsistentPartitionWindow.WindowRelFunctionInvocation.builder()
        .declaration(declaration)
        .outputType(outputType)
        .aggregationPhase(phase)
        .boundsType(boundsType)
        .lowerBound(lowerBound)
        .upperBound(upperBound)
        .invocation(invocation)
        .addAllArguments(arguments)
        .build();
  }

  public static Expression.WindowFunctionInvocation windowFunction(
      final SimpleExtension.WindowFunctionVariant declaration,
      final Type outputType,
      final Expression.AggregationPhase phase,
      final List<Expression.SortField> sort,
      final Expression.AggregationInvocation invocation,
      final List<Expression> partitionBy,
      final Expression.WindowBoundsType boundsType,
      final WindowBound lowerBound,
      final WindowBound upperBound,
      final FunctionArg... arguments) {
    return windowFunction(
        declaration,
        outputType,
        phase,
        sort,
        invocation,
        partitionBy,
        boundsType,
        lowerBound,
        upperBound,
        Arrays.asList(arguments));
  }

  public static Expression cast(
      final Type type,
      final Expression expression,
      final Expression.FailureBehavior failureBehavior) {
    return Expression.Cast.builder()
        .type(type)
        .input(expression)
        .failureBehavior(failureBehavior)
        .build();
  }
}
