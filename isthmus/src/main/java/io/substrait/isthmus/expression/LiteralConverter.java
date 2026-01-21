package io.substrait.isthmus.expression;

import com.google.protobuf.ByteString;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

public class LiteralConverter {
  // TODO: Handle conversion of user-defined type literals

  static final DateTimeFormatter CALCITE_LOCAL_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
  static final DateTimeFormatter CALCITE_LOCAL_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter();
  private static final DateTimeFormatter CALCITE_LOCAL_DATETIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(CALCITE_LOCAL_DATE_FORMATTER)
          .appendLiteral(' ')
          .append(CALCITE_LOCAL_TIME_FORMATTER)
          .toFormatter();

  private final TypeConverter typeConverter;

  public LiteralConverter(TypeConverter typeConverter) {
    this.typeConverter = typeConverter;
  }

  private static BigDecimal i(RexLiteral literal) {
    return bd(literal).setScale(0, RoundingMode.HALF_UP);
  }

  private static String s(RexLiteral literal) {
    return ((NlsString) literal.getValue()).getValue();
  }

  private static BigDecimal bd(RexLiteral literal) {
    return (BigDecimal) literal.getValue();
  }

  public Expression.Literal convert(RexLiteral literal) {
    // convert type first to guarantee we can handle the value.
    final Type type = typeConverter.toSubstrait(literal.getType());
    return convert(literal, type.nullable());
  }

  /**
   * Converts a RexLiteral to a Substrait Literal with the specified nullability.
   *
   * <p>This overload is useful when the target nullability should come from the schema rather than
   * the literal's own type. For example, Calcite's LogicalValues may have literals with
   * non-nullable types even when the schema field is nullable.
   *
   * @param literal the RexLiteral to convert
   * @param nullable the nullability to use for the resulting Substrait literal
   * @return the converted Substrait Literal
   */
  public Expression.Literal convert(RexLiteral literal, boolean nullable) {
    final boolean n = nullable;

    if (literal.isNull()) {
      final Type type = typeConverter.toSubstrait(literal.getType());
      final Type typeWithNullability =
          n ? TypeCreator.asNullable(type) : TypeCreator.asNotNullable(type);
      return ExpressionCreator.typedNull(typeWithNullability);
    }

    switch (literal.getType().getSqlTypeName()) {
      case TINYINT:
        return ExpressionCreator.i8(n, i(literal).intValue());
      case SMALLINT:
        return ExpressionCreator.i16(n, i(literal).intValue());
      case INTEGER:
        return ExpressionCreator.i32(n, i(literal).intValue());
      case BIGINT:
        return ExpressionCreator.i64(n, i(literal).longValue());
      case BOOLEAN:
        return ExpressionCreator.bool(n, literal.getValueAs(Boolean.class));
      case CHAR:
        {
          Comparable<?> val = literal.getValue();
          if (val instanceof NlsString) {
            NlsString nls = (NlsString) val;
            return ExpressionCreator.fixedChar(n, nls.getValue());
          }
          throw new UnsupportedOperationException("Unable to handle char type: " + val);
        }
      case FLOAT:
      case DOUBLE:
        return ExpressionCreator.fp64(n, literal.getValueAs(Double.class));
      case REAL:
        return ExpressionCreator.fp32(n, literal.getValueAs(Float.class));

      case DECIMAL:
        {
          BigDecimal bd = bd(literal);
          return ExpressionCreator.decimal(
              n, bd, literal.getType().getPrecision(), literal.getType().getScale());
        }
      case VARCHAR:
        {
          if (literal.getType().getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED) {
            return ExpressionCreator.string(n, s(literal));
          }

          return ExpressionCreator.varChar(n, s(literal), literal.getType().getPrecision());
        }
      case BINARY:
        return ExpressionCreator.fixedBinary(
            n,
            ByteString.copyFrom(
                padRightIfNeeded(
                    literal.getValueAs(org.apache.calcite.avatica.util.ByteString.class),
                    literal.getType().getPrecision())));
      case VARBINARY:
        return ExpressionCreator.binary(n, ByteString.copyFrom(literal.getValueAs(byte[].class)));
      case SYMBOL:
        {
          Object value = literal.getValue();
          if (value instanceof NlsString) {
            return ExpressionCreator.string(n, ((NlsString) value).getValue());
          } else if (value instanceof Enum) {
            Enum<?> v = (Enum<?>) value;

            Optional<Expression.Literal> r =
                EnumConverter.canConvert(v)
                    ? Optional.of(ExpressionCreator.string(n, v.name()))
                    : Optional.empty();
            return r.orElseThrow(
                () -> new UnsupportedOperationException("Unable to handle symbol: " + value));
          } else {
            throw new UnsupportedOperationException("Unable to handle symbol: " + value);
          }
        }
      case DATE:
        {
          DateString date = literal.getValueAs(DateString.class);
          LocalDate localDate = LocalDate.parse(date.toString(), CALCITE_LOCAL_DATE_FORMATTER);
          return ExpressionCreator.date(n, (int) localDate.toEpochDay());
        }
      case TIME:
        {
          TimeString time = literal.getValueAs(TimeString.class);
          LocalTime localTime = LocalTime.parse(time.toString(), CALCITE_LOCAL_TIME_FORMATTER);
          return ExpressionCreator.time(n, TimeUnit.NANOSECONDS.toMicros(localTime.toNanoOfDay()));
        }
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        {
          TimestampString timestamp = literal.getValueAs(TimestampString.class);
          LocalDateTime ldt =
              LocalDateTime.parse(timestamp.toString(), CALCITE_LOCAL_DATETIME_FORMATTER);
          return ExpressionCreator.timestamp(n, ldt);
        }
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        {
          long intervalLength = Objects.requireNonNull(literal.getValueAs(Long.class));
          long years = intervalLength / 12;
          long months = intervalLength - years * 12;
          return ExpressionCreator.intervalYear(n, (int) years, (int) months);
        }
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        {
          // Calcite represents day/time intervals in milliseconds, despite a default scale of 6.
          Long totalMillis = Objects.requireNonNull(literal.getValueAs(Long.class));
          Duration interval = Duration.ofMillis(totalMillis);

          long days = interval.toDays();
          long seconds = interval.minusDays(days).toSeconds();
          int micros = interval.toMillisPart() * 1000;

          return ExpressionCreator.intervalDay(n, (int) days, (int) seconds, micros, 6);
        }

      case ROW:
        {
          List<RexLiteral> literals = (List<RexLiteral>) literal.getValue();
          return ExpressionCreator.struct(
              n, literals.stream().map(this::convert).collect(Collectors.toList()));
        }

      case ARRAY:
        {
          List<RexLiteral> literals = (List<RexLiteral>) literal.getValue();
          return ExpressionCreator.list(
              n, literals.stream().map(this::convert).collect(Collectors.toList()));
        }

      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unable to convert the value of %s of type %s to a literal.",
                literal, literal.getType().getSqlTypeName()));
    }
  }

  public static byte[] padRightIfNeeded(
      org.apache.calcite.avatica.util.ByteString bytes, int length) {
    return padRightIfNeeded(bytes.getBytes(), length);
  }

  public static byte[] padRightIfNeeded(byte[] value, int length) {

    if (length < value.length) {
      throw new IllegalArgumentException(
          "Byte values should either be at or below the expected length.");
    }

    if (length == value.length) {
      return value;
    }

    byte[] newArray = new byte[length];
    System.arraycopy(value, 0, newArray, 0, value.length);
    return newArray;
  }
}
