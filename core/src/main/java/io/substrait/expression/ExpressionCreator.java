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
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for creating Substrait expression objects.
 *
 * <p>This class provides factory methods for constructing various types of expressions, literals,
 * and function invocations used in Substrait query plans.
 */
public class ExpressionCreator {

  private ExpressionCreator() {}

  /**
   * Creates a null literal expression with a specific type.
   *
   * @param t the type of the null literal
   * @return a NullLiteral expression
   */
  public static Expression.NullLiteral typedNull(Type t) {
    return Expression.NullLiteral.builder().type(t).build();
  }

  /**
   * Creates a boolean literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the boolean value
   * @return a BoolLiteral expression
   */
  public static Expression.BoolLiteral bool(boolean nullable, boolean value) {
    return Expression.BoolLiteral.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates an 8-bit integer literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the integer value
   * @return an I8Literal expression
   */
  public static Expression.I8Literal i8(boolean nullable, int value) {
    return Expression.I8Literal.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates a 16-bit integer literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the integer value
   * @return an I16Literal expression
   */
  public static Expression.I16Literal i16(boolean nullable, int value) {
    return Expression.I16Literal.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates a 32-bit integer literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the integer value
   * @return an I32Literal expression
   */
  public static Expression.I32Literal i32(boolean nullable, int value) {
    return Expression.I32Literal.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates a 64-bit integer literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the long value
   * @return an I64Literal expression
   */
  public static Expression.I64Literal i64(boolean nullable, long value) {
    return Expression.I64Literal.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates a 32-bit floating point literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the float value
   * @return an FP32Literal expression
   */
  public static Expression.FP32Literal fp32(boolean nullable, float value) {
    return Expression.FP32Literal.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates a 64-bit floating point literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the double value
   * @return an FP64Literal expression
   */
  public static Expression.FP64Literal fp64(boolean nullable, double value) {
    return Expression.FP64Literal.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates a string literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the string value
   * @return a StrLiteral expression
   */
  public static Expression.StrLiteral string(boolean nullable, String value) {
    return Expression.StrLiteral.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates a binary literal expression from a ByteString.
   *
   * @param nullable whether the literal can be null
   * @param value the ByteString value
   * @return a BinaryLiteral expression
   */
  public static Expression.BinaryLiteral binary(boolean nullable, ByteString value) {
    return Expression.BinaryLiteral.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates a binary literal expression from a byte array.
   *
   * @param nullable whether the literal can be null
   * @param value the byte array value
   * @return a BinaryLiteral expression
   */
  public static Expression.BinaryLiteral binary(boolean nullable, byte[] value) {
    return Expression.BinaryLiteral.builder()
        .nullable(nullable)
        .value(ByteString.copyFrom(value))
        .build();
  }

  /**
   * Creates a date literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the date value as days since epoch
   * @return a DateLiteral expression
   */
  public static Expression.DateLiteral date(boolean nullable, int value) {
    return Expression.DateLiteral.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates a time literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the time value in microseconds since midnight
   * @return a TimeLiteral expression
   * @deprecated Time is deprecated in favor of PrecisionTime
   */
  @Deprecated
  public static Expression.TimeLiteral time(boolean nullable, long value) {
    return Expression.TimeLiteral.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates a precision time literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the time value
   * @param precision the precision of the time
   * @return a PrecisionTimeLiteral expression
   */
  public static Expression.PrecisionTimeLiteral precisionTime(
      boolean nullable, long value, int precision) {
    return Expression.PrecisionTimeLiteral.builder()
        .nullable(nullable)
        .value(value)
        .precision(precision)
        .build();
  }

  /**
   * Creates a precision time literal from a LocalTime value.
   *
   * <p>This method converts a Java {@link LocalTime} to a Substrait precision time literal with
   * nanosecond precision (precision = 9). The time value is represented as nanoseconds since
   * midnight (00:00:00).
   *
   * @param nullable whether the literal can be null
   * @param value the LocalTime value to convert
   * @return a PrecisionTimeLiteral with nanosecond precision representing the given time
   * @see #precisionTime(boolean, long, int) for creating precision time with custom precision
   */
  public static Expression.PrecisionTimeLiteral precisionTime(boolean nullable, LocalTime value) {
    long epochNano = value.toNanoOfDay();
    return precisionTime(nullable, epochNano, 9);
  }

  /**
   * Creates a timestamp literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the timestamp value in microseconds since epoch
   * @return a TimestampLiteral expression
   * @deprecated Timestamp is deprecated in favor of PrecisionTimestamp
   */
  @Deprecated
  public static Expression.TimestampLiteral timestamp(boolean nullable, long value) {
    return Expression.TimestampLiteral.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates a timestamp literal expression from a LocalDateTime.
   *
   * @param nullable whether the literal can be null
   * @param value the LocalDateTime value (interpreted as UTC)
   * @return a TimestampLiteral expression
   * @deprecated Timestamp is deprecated in favor of PrecisionTimestamp
   */
  @Deprecated
  public static Expression.TimestampLiteral timestamp(boolean nullable, LocalDateTime value) {
    long epochMicro =
        TimeUnit.SECONDS.toMicros(value.toEpochSecond(ZoneOffset.UTC))
            + TimeUnit.NANOSECONDS.toMicros(value.getNano());
    return timestamp(nullable, epochMicro);
  }

  /**
   * Creates a timestamp literal expression from date/time components.
   *
   * @param nullable whether the literal can be null
   * @param year the year
   * @param month the month (1-12)
   * @param dayOfMonth the day of month (1-31)
   * @param hour the hour (0-23)
   * @param minute the minute (0-59)
   * @param second the second (0-59)
   * @param micros the microseconds (0-999999)
   * @return a TimestampLiteral expression
   * @deprecated Timestamp is deprecated in favor of PrecisionTimestamp
   */
  @Deprecated
  public static Expression.TimestampLiteral timestamp(
      boolean nullable,
      int year,
      int month,
      int dayOfMonth,
      int hour,
      int minute,
      int second,
      int micros) {
    return timestamp(
        nullable,
        LocalDateTime.of(year, month, dayOfMonth, hour, minute, second)
            .withNano((int) TimeUnit.MICROSECONDS.toNanos(micros)));
  }

  /**
   * Creates a timestamp with timezone literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the timestamp value in microseconds since epoch
   * @return a TimestampTZLiteral expression
   * @deprecated TimestampTZ is deprecated in favor of PrecisionTimestampTZ
   */
  @Deprecated
  public static Expression.TimestampTZLiteral timestampTZ(boolean nullable, long value) {
    return Expression.TimestampTZLiteral.builder().nullable(nullable).value(value).build();
  }

  /**
   * Creates a timestamp with timezone literal expression from an Instant.
   *
   * @param nullable whether the literal can be null
   * @param value the Instant value
   * @return a TimestampTZLiteral expression
   * @deprecated TimestampTZ is deprecated in favor of PrecisionTimestampTZ
   */
  @Deprecated
  public static Expression.TimestampTZLiteral timestampTZ(boolean nullable, Instant value) {
    long epochMicro =
        TimeUnit.SECONDS.toMicros(value.getEpochSecond())
            + TimeUnit.NANOSECONDS.toMicros(value.getNano());
    return timestampTZ(nullable, epochMicro);
  }

  /**
   * Creates a precision timestamp literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the timestamp value
   * @param precision the precision of the timestamp
   * @return a PrecisionTimestampLiteral expression
   */
  public static Expression.PrecisionTimestampLiteral precisionTimestamp(
      boolean nullable, long value, int precision) {
    return Expression.PrecisionTimestampLiteral.builder()
        .nullable(nullable)
        .value(value)
        .precision(precision)
        .build();
  }

  /**
   * Creates a precision timestamp literal from a LocalDateTime value.
   *
   * <p>This method converts a Java {@link LocalDateTime} to a Substrait precision timestamp literal
   * with nanosecond precision (precision = 9). The timestamp value is represented as nanoseconds
   * since the Unix epoch (1970-01-01 00:00:00 UTC), assuming the LocalDateTime is in UTC timezone.
   *
   * @param nullable whether the literal can be null
   * @param value the LocalDateTime value to convert (interpreted as UTC)
   * @return a PrecisionTimestampLiteral with nanosecond precision representing the given timestamp
   * @see #precisionTimestamp(boolean, long, int) for creating precision timestamp with custom
   *     precision
   */
  public static Expression.PrecisionTimestampLiteral precisionTimestamp(
      boolean nullable, LocalDateTime value) {
    long epochNano =
        TimeUnit.SECONDS.toNanos(value.toEpochSecond(ZoneOffset.UTC)) + value.getNano();
    return precisionTimestamp(nullable, epochNano, 9);
  }

  /**
   * Creates a precision timestamp with timezone literal expression.
   *
   * @param nullable whether the literal can be null
   * @param value the timestamp value
   * @param precision the precision of the timestamp
   * @return a PrecisionTimestampTZLiteral expression
   */
  public static Expression.PrecisionTimestampTZLiteral precisionTimestampTZ(
      boolean nullable, long value, int precision) {
    return Expression.PrecisionTimestampTZLiteral.builder()
        .nullable(nullable)
        .value(value)
        .precision(precision)
        .build();
  }

  /**
   * Creates an interval year literal expression.
   *
   * @param nullable whether the literal can be null
   * @param years the number of years
   * @param months the number of months
   * @return an IntervalYearLiteral expression
   */
  public static Expression.IntervalYearLiteral intervalYear(
      boolean nullable, int years, int months) {
    return Expression.IntervalYearLiteral.builder()
        .nullable(nullable)
        .years(years)
        .months(months)
        .build();
  }

  /**
   * Creates an interval day literal expression with default subseconds and precision.
   *
   * @param nullable whether the literal can be null
   * @param days the number of days
   * @param seconds the number of seconds
   * @return an IntervalDayLiteral expression
   */
  public static Expression.IntervalDayLiteral intervalDay(boolean nullable, int days, int seconds) {
    return intervalDay(nullable, days, seconds, 0, 0);
  }

  /**
   * Creates an interval day literal expression with subseconds and precision.
   *
   * @param nullable whether the literal can be null
   * @param days the number of days
   * @param seconds the number of seconds
   * @param subseconds the subsecond component
   * @param precision the precision of subseconds
   * @return an IntervalDayLiteral expression
   */
  public static Expression.IntervalDayLiteral intervalDay(
      boolean nullable, int days, int seconds, long subseconds, int precision) {
    return Expression.IntervalDayLiteral.builder()
        .nullable(nullable)
        .days(days)
        .seconds(seconds)
        .subseconds(subseconds)
        .precision(precision)
        .build();
  }

  /**
   * Creates an interval compound literal expression.
   *
   * @param nullable whether the literal can be null
   * @param years the number of years
   * @param months the number of months
   * @param days the number of days
   * @param seconds the number of seconds
   * @param subseconds the subsecond component
   * @param precision the precision of subseconds
   * @return an IntervalCompoundLiteral expression
   */
  public static Expression.IntervalCompoundLiteral intervalCompound(
      boolean nullable,
      int years,
      int months,
      int days,
      int seconds,
      long subseconds,
      int precision) {
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

  /**
   * Creates a UUID literal expression from a ByteString.
   *
   * @param nullable whether the literal can be null
   * @param uuid the UUID value as a ByteString
   * @return a UUIDLiteral expression
   */
  public static Expression.UUIDLiteral uuid(boolean nullable, ByteString uuid) {
    ByteBuffer bb = uuid.asReadOnlyByteBuffer();
    return Expression.UUIDLiteral.builder()
        .nullable(nullable)
        .value(new UUID(bb.getLong(), bb.getLong()))
        .build();
  }

  /**
   * Creates a UUID literal expression from a UUID object.
   *
   * @param nullable whether the literal can be null
   * @param uuid the UUID value
   * @return a UUIDLiteral expression
   */
  public static Expression.UUIDLiteral uuid(boolean nullable, UUID uuid) {
    return Expression.UUIDLiteral.builder().nullable(nullable).value(uuid).build();
  }

  /**
   * Creates a fixed-length character literal expression.
   *
   * @param nullable whether the literal can be null
   * @param str the string value
   * @return a FixedCharLiteral expression
   */
  public static Expression.FixedCharLiteral fixedChar(boolean nullable, String str) {
    return Expression.FixedCharLiteral.builder().nullable(nullable).value(str).build();
  }

  /**
   * Creates a variable-length character literal expression.
   *
   * @param nullable whether the literal can be null
   * @param str the string value
   * @param len the maximum length
   * @return a VarCharLiteral expression
   */
  public static Expression.VarCharLiteral varChar(boolean nullable, String str, int len) {
    return Expression.VarCharLiteral.builder().nullable(nullable).value(str).length(len).build();
  }

  /**
   * Creates a fixed-length binary literal expression from a ByteString.
   *
   * @param nullable whether the literal can be null
   * @param bytes the ByteString value
   * @return a FixedBinaryLiteral expression
   */
  public static Expression.FixedBinaryLiteral fixedBinary(boolean nullable, ByteString bytes) {
    return Expression.FixedBinaryLiteral.builder().nullable(nullable).value(bytes).build();
  }

  /**
   * Creates a fixed-length binary literal expression from a byte array.
   *
   * @param nullable whether the literal can be null
   * @param bytes the byte array value
   * @return a FixedBinaryLiteral expression
   */
  public static Expression.FixedBinaryLiteral fixedBinary(boolean nullable, byte[] bytes) {
    return Expression.FixedBinaryLiteral.builder()
        .nullable(nullable)
        .value(ByteString.copyFrom(bytes))
        .build();
  }

  /**
   * Creates a decimal literal expression from a ByteString.
   *
   * @param nullable whether the literal can be null
   * @param value the ByteString value containing the decimal in two's complement format
   * @param precision the precision of the decimal
   * @param scale the scale of the decimal
   * @return a DecimalLiteral expression
   */
  public static Expression.DecimalLiteral decimal(
      boolean nullable, ByteString value, int precision, int scale) {
    return Expression.DecimalLiteral.builder()
        .nullable(nullable)
        .value(value)
        .precision(precision)
        .scale(scale)
        .build();
  }

  /**
   * Creates a decimal literal expression from a BigDecimal.
   *
   * @param nullable whether the literal can be null
   * @param value the BigDecimal value
   * @param precision the precision of the decimal
   * @param scale the scale of the decimal
   * @return a DecimalLiteral expression
   */
  public static Expression.DecimalLiteral decimal(
      boolean nullable, BigDecimal value, int precision, int scale) {
    byte[] twosComplement = DecimalUtil.encodeDecimalIntoBytes(value, scale, 16);

    return Expression.DecimalLiteral.builder()
        .nullable(nullable)
        .value(ByteString.copyFrom(twosComplement))
        .precision(precision)
        .scale(scale)
        .build();
  }

  /**
   * Creates a map literal expression.
   *
   * @param nullable whether the literal can be null
   * @param values the map of key-value pairs
   * @return a MapLiteral expression
   */
  public static Expression.MapLiteral map(
      boolean nullable, Map<Expression.Literal, Expression.Literal> values) {
    return Expression.MapLiteral.builder().nullable(nullable).putAllValues(values).build();
  }

  /**
   * Creates an empty map literal expression.
   *
   * @param nullable whether the literal can be null
   * @param keyType the type of map keys
   * @param valueType the type of map values
   * @return an EmptyMapLiteral expression
   */
  public static Expression.EmptyMapLiteral emptyMap(
      boolean nullable, Type keyType, Type valueType) {
    return Expression.EmptyMapLiteral.builder()
        .keyType(keyType)
        .valueType(valueType)
        .nullable(nullable)
        .build();
  }

  /**
   * Creates a list literal expression from varargs.
   *
   * @param nullable whether the literal can be null
   * @param values the literal values in the list
   * @return a ListLiteral expression
   */
  public static Expression.ListLiteral list(boolean nullable, Expression.Literal... values) {
    return Expression.ListLiteral.builder().nullable(nullable).addValues(values).build();
  }

  /**
   * Creates a list literal expression from an iterable.
   *
   * @param nullable whether the literal can be null
   * @param values the literal values in the list
   * @return a ListLiteral expression
   */
  public static Expression.ListLiteral list(
      boolean nullable, Iterable<? extends Expression.Literal> values) {
    return Expression.ListLiteral.builder().nullable(nullable).addAllValues(values).build();
  }

  /**
   * Creates an empty list literal expression.
   *
   * @param listNullable whether the list can be null
   * @param elementType the type of list elements
   * @return an EmptyListLiteral expression
   */
  public static Expression.EmptyListLiteral emptyList(boolean listNullable, Type elementType) {
    return Expression.EmptyListLiteral.builder()
        .elementType(elementType)
        .nullable(listNullable)
        .build();
  }

  /**
   * Creates a struct literal expression from varargs.
   *
   * @param nullable whether the literal can be null
   * @param values the literal field values
   * @return a StructLiteral expression
   */
  public static Expression.StructLiteral struct(boolean nullable, Expression.Literal... values) {
    return Expression.StructLiteral.builder().nullable(nullable).addFields(values).build();
  }

  /**
   * Creates a nested list expression with one or more elements.
   *
   * <p>Note: This method cannot be used to construct an empty list. To create an empty list, use
   * {@link ExpressionCreator#emptyList(boolean, Type)} which returns an {@link
   * Expression.EmptyListLiteral}.
   *
   * @param nullable whether the literal can be null
   * @param values the expression values in the nested list
   * @return a NestedList expression
   */
  public static Expression.NestedList nestedList(boolean nullable, List<Expression> values) {
    return Expression.NestedList.builder().nullable(nullable).addAllValues(values).build();
  }

  /**
   * Creates a struct literal expression from an iterable.
   *
   * @param nullable whether the literal can be null
   * @param values the literal field values
   * @return a StructLiteral expression
   */
  public static Expression.StructLiteral struct(
      boolean nullable, Iterable<? extends Expression.Literal> values) {
    return Expression.StructLiteral.builder().nullable(nullable).addAllFields(values).build();
  }

  /**
   * Creates a nested struct expression from an iterable.
   *
   * @param nullable whether the struct can be null
   * @param fields the expression fields
   * @return a NestedStruct expression
   */
  public static Expression.NestedStruct nestedStruct(
      boolean nullable, Iterable<Expression> fields) {
    return Expression.NestedStruct.builder().nullable(nullable).addAllFields(fields).build();
  }

  /**
   * Creates a nested struct expression from varargs.
   *
   * @param nullable whether the struct can be null
   * @param fields the expression fields
   * @return a NestedStruct expression
   */
  public static Expression.NestedStruct nestedStruct(boolean nullable, Expression... fields) {
    return Expression.NestedStruct.builder().nullable(nullable).addFields(fields).build();
  }

  /**
   * Create a UserDefinedAnyLiteral with google.protobuf.Any representation.
   *
   * @param nullable whether the literal is nullable
   * @param urn the URN of the user-defined type
   * @param name the name of the user-defined type
   * @param typeParameters the type parameters for the user-defined type (can be an empty list)
   * @param value the value, encoded as google.protobuf.Any
   * @return a UserDefinedAnyLiteral expression
   */
  public static Expression.UserDefinedAnyLiteral userDefinedLiteralAny(
      boolean nullable,
      String urn,
      String name,
      java.util.List<io.substrait.type.Type.Parameter> typeParameters,
      Any value) {
    return Expression.UserDefinedAnyLiteral.builder()
        .nullable(nullable)
        .urn(urn)
        .name(name)
        .addAllTypeParameters(typeParameters)
        .value(value)
        .build();
  }

  /**
   * Create a UserDefinedStructLiteral with Struct representation.
   *
   * @param nullable whether the literal is nullable
   * @param urn the URN of the user-defined type
   * @param name the name of the user-defined type
   * @param typeParameters the type parameters for the user-defined type (can be an empty list)
   * @param fields the fields, as a list of Literal values
   * @return a UserDefinedStructLiteral expression
   */
  public static Expression.UserDefinedStructLiteral userDefinedLiteralStruct(
      boolean nullable,
      String urn,
      String name,
      java.util.List<io.substrait.type.Type.Parameter> typeParameters,
      java.util.List<Expression.Literal> fields) {
    return Expression.UserDefinedStructLiteral.builder()
        .nullable(nullable)
        .urn(urn)
        .name(name)
        .addAllTypeParameters(typeParameters)
        .addAllFields(fields)
        .build();
  }

  /**
   * Creates a switch statement with varargs clauses.
   *
   * @param match the expression to match against
   * @param defaultExpression the default expression if no clause matches
   * @param conditionClauses the switch clauses
   * @return a Switch expression
   */
  public static Expression.Switch switchStatement(
      Expression match, Expression defaultExpression, Expression.SwitchClause... conditionClauses) {
    return Expression.Switch.builder()
        .match(match)
        .defaultClause(defaultExpression)
        .addSwitchClauses(conditionClauses)
        .build();
  }

  /**
   * Creates a switch statement with an iterable of clauses.
   *
   * @param match the expression to match against
   * @param defaultExpression the default expression if no clause matches
   * @param conditionClauses the switch clauses
   * @return a Switch expression
   */
  public static Expression.Switch switchStatement(
      Expression match,
      Expression defaultExpression,
      Iterable<? extends Expression.SwitchClause> conditionClauses) {
    return Expression.Switch.builder()
        .match(match)
        .defaultClause(defaultExpression)
        .addAllSwitchClauses(conditionClauses)
        .build();
  }

  /**
   * Creates a switch clause.
   *
   * @param expectedValue the literal value to match
   * @param resultExpression the result expression if matched
   * @return a SwitchClause
   */
  public static Expression.SwitchClause switchClause(
      Expression.Literal expectedValue, Expression resultExpression) {
    return Expression.SwitchClause.builder()
        .condition(expectedValue)
        .then(resultExpression)
        .build();
  }

  /**
   * Creates an if-then statement with varargs clauses.
   *
   * @param elseExpression the else clause expression
   * @param conditionClauses the if clauses
   * @return an IfThen expression
   */
  public static Expression.IfThen ifThenStatement(
      Expression elseExpression, Expression.IfClause... conditionClauses) {
    return Expression.IfThen.builder()
        .elseClause(elseExpression)
        .addIfClauses(conditionClauses)
        .build();
  }

  /**
   * Creates an if-then statement with an iterable of clauses.
   *
   * @param elseExpression the else clause expression
   * @param conditionClauses the if clauses
   * @return an IfThen expression
   */
  public static Expression.IfThen ifThenStatement(
      Expression elseExpression, Iterable<? extends Expression.IfClause> conditionClauses) {
    return Expression.IfThen.builder()
        .elseClause(elseExpression)
        .addAllIfClauses(conditionClauses)
        .build();
  }

  /**
   * Creates an if-then clause.
   *
   * @param conditionExpression the condition expression
   * @param resultExpression the result expression if condition is true
   * @return an IfClause
   */
  public static Expression.IfClause ifThenClause(
      Expression conditionExpression, Expression resultExpression) {
    return Expression.IfClause.builder()
        .condition(conditionExpression)
        .then(resultExpression)
        .build();
  }

  /**
   * Creates a scalar function invocation with varargs.
   *
   * @param declaration the function declaration
   * @param outputType the output type of the function
   * @param arguments the function arguments
   * @return a ScalarFunctionInvocation expression
   */
  public static Expression.ScalarFunctionInvocation scalarFunction(
      SimpleExtension.ScalarFunctionVariant declaration,
      Type outputType,
      FunctionArg... arguments) {
    return scalarFunction(declaration, outputType, Arrays.asList(arguments));
  }

  /**
   * Creates a scalar function invocation.
   *
   * <p>Use {@link Expression.ScalarFunctionInvocation#builder()} directly to specify other
   * parameters, e.g. options
   *
   * @param declaration the function declaration
   * @param outputType the output type of the function
   * @param arguments the function arguments
   * @return a ScalarFunctionInvocation expression
   */
  public static Expression.ScalarFunctionInvocation scalarFunction(
      SimpleExtension.ScalarFunctionVariant declaration,
      Type outputType,
      Iterable<? extends FunctionArg> arguments) {
    return Expression.ScalarFunctionInvocation.builder()
        .declaration(declaration)
        .outputType(outputType)
        .addAllArguments(arguments)
        .build();
  }

  /**
   * Creates an aggregate function invocation.
   *
   * <p>Use {@link AggregateFunctionInvocation#builder()} directly to specify other parameters, e.g.
   * options
   *
   * @param declaration the function declaration
   * @param outputType the output type of the function
   * @param phase the aggregation phase
   * @param sort the sort fields
   * @param invocation the aggregation invocation type
   * @param arguments the function arguments
   * @return an AggregateFunctionInvocation
   */
  public static AggregateFunctionInvocation aggregateFunction(
      SimpleExtension.AggregateFunctionVariant declaration,
      Type outputType,
      Expression.AggregationPhase phase,
      List<Expression.SortField> sort,
      Expression.AggregationInvocation invocation,
      Iterable<? extends FunctionArg> arguments) {
    return AggregateFunctionInvocation.builder()
        .declaration(declaration)
        .outputType(outputType)
        .aggregationPhase(phase)
        .sort(sort)
        .invocation(invocation)
        .addAllArguments(arguments)
        .build();
  }

  /**
   * Creates an aggregate function invocation with varargs.
   *
   * @param declaration the function declaration
   * @param outputType the output type of the function
   * @param phase the aggregation phase
   * @param sort the sort fields
   * @param invocation the aggregation invocation type
   * @param arguments the function arguments
   * @return an AggregateFunctionInvocation
   */
  public static AggregateFunctionInvocation aggregateFunction(
      SimpleExtension.AggregateFunctionVariant declaration,
      Type outputType,
      Expression.AggregationPhase phase,
      List<Expression.SortField> sort,
      Expression.AggregationInvocation invocation,
      FunctionArg... arguments) {
    return aggregateFunction(
        declaration, outputType, phase, sort, invocation, Arrays.asList(arguments));
  }

  /**
   * Creates a window function invocation.
   *
   * <p>Use {@link Expression.WindowFunctionInvocation#builder()} directly to specify other
   * parameters, e.g. options
   *
   * @param declaration the function declaration
   * @param outputType the output type of the function
   * @param phase the aggregation phase
   * @param sort the sort fields
   * @param invocation the aggregation invocation type
   * @param partitionBy the partition by expressions
   * @param boundsType the window bounds type
   * @param lowerBound the lower bound of the window
   * @param upperBound the upper bound of the window
   * @param arguments the function arguments
   * @return a WindowFunctionInvocation expression
   */
  public static Expression.WindowFunctionInvocation windowFunction(
      SimpleExtension.WindowFunctionVariant declaration,
      Type outputType,
      Expression.AggregationPhase phase,
      List<Expression.SortField> sort,
      Expression.AggregationInvocation invocation,
      List<Expression> partitionBy,
      Expression.WindowBoundsType boundsType,
      WindowBound lowerBound,
      WindowBound upperBound,
      Iterable<? extends FunctionArg> arguments) {
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
   * Creates a window relation function invocation.
   *
   * <p>Use {@link ConsistentPartitionWindow.WindowRelFunctionInvocation#builder()} directly to
   * specify other parameters, e.g. options
   *
   * @param declaration the function declaration
   * @param outputType the output type of the function
   * @param phase the aggregation phase
   * @param invocation the aggregation invocation type
   * @param boundsType the window bounds type
   * @param lowerBound the lower bound of the window
   * @param upperBound the upper bound of the window
   * @param arguments the function arguments
   * @return a WindowRelFunctionInvocation
   */
  public static ConsistentPartitionWindow.WindowRelFunctionInvocation windowRelFunction(
      SimpleExtension.WindowFunctionVariant declaration,
      Type outputType,
      Expression.AggregationPhase phase,
      Expression.AggregationInvocation invocation,
      Expression.WindowBoundsType boundsType,
      WindowBound lowerBound,
      WindowBound upperBound,
      Iterable<? extends FunctionArg> arguments) {
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

  /**
   * Creates a window function invocation with varargs.
   *
   * @param declaration the function declaration
   * @param outputType the output type of the function
   * @param phase the aggregation phase
   * @param sort the sort fields
   * @param invocation the aggregation invocation type
   * @param partitionBy the partition by expressions
   * @param boundsType the window bounds type
   * @param lowerBound the lower bound of the window
   * @param upperBound the upper bound of the window
   * @param arguments the function arguments
   * @return a WindowFunctionInvocation expression
   */
  public static Expression.WindowFunctionInvocation windowFunction(
      SimpleExtension.WindowFunctionVariant declaration,
      Type outputType,
      Expression.AggregationPhase phase,
      List<Expression.SortField> sort,
      Expression.AggregationInvocation invocation,
      List<Expression> partitionBy,
      Expression.WindowBoundsType boundsType,
      WindowBound lowerBound,
      WindowBound upperBound,
      FunctionArg... arguments) {
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

  /**
   * Creates a cast expression.
   *
   * @param type the target type to cast to
   * @param expression the expression to cast
   * @param failureBehavior the failure behavior for the cast
   * @return a Cast expression
   */
  public static Expression cast(
      Type type, Expression expression, Expression.FailureBehavior failureBehavior) {
    return Expression.Cast.builder()
        .type(type)
        .input(expression)
        .failureBehavior(failureBehavior)
        .build();
  }

  /**
   * Creates a {@code CURRENT_TIMESTAMP} execution context variable.
   *
   * @param precision the fractional-second precision of the timestamp
   * @return the current timestamp expression
   */
  public static Expression.CurrentTimestamp currentTimestamp(int precision) {
    return Expression.CurrentTimestamp.builder().precision(precision).build();
  }

  /**
   * Creates a {@code CURRENT_TIMEZONE} execution context variable.
   *
   * @return the current timezone expression
   */
  public static Expression.CurrentTimezone currentTimezone() {
    return Expression.CurrentTimezone.builder().build();
  }

  /**
   * Creates a {@code CURRENT_DATE} execution context variable.
   *
   * @return the current date expression
   */
  public static Expression.CurrentDate currentDate() {
    return Expression.CurrentDate.builder().build();
  }
}
