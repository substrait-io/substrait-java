package io.substrait.isthmus.expression;

import com.google.common.math.LongMath;
import com.google.protobuf.ByteString;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

/**
 * Converts Calcite {@link RexLiteral} values to Substrait {@link Expression.Literal}, using {@link
 * TypeConverter} for type resolution.
 *
 * <p>Supports numeric, boolean, character, binary, temporal, interval, ROW/ARRAY, and selected
 * symbol/enums. Throws {@link UnsupportedOperationException} for unsupported types.
 */
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

  /**
   * The longest text a {@code fixedchar} literal can be padded to.
   *
   * <p>A Java {@link String} holds its characters in an array, and this is the largest one a JVM
   * allocates, so a wider literal cannot be built whatever the heap. {@code String.repeat} reports
   * that as an {@link OutOfMemoryError}, which no {@code catch (Exception)} sees and which names
   * neither the column nor the value. A width the heap alone cannot hold still fails as one.
   *
   * <p>The width a fixedchar may declare is the spec's {@code [1..2147483647]}; what a plan's
   * target engine actually supports is a dialect's answer, not this.
   */
  private static final int MAX_PADDED_LENGTH = Integer.MAX_VALUE - 8;

  private final TypeConverter typeConverter;

  /**
   * Creates a converter that uses the given {@link TypeConverter}.
   *
   * @param typeConverter converter for {@link RelDataType} to Substrait {@link Type}
   */
  public LiteralConverter(TypeConverter typeConverter) {
    this.typeConverter = typeConverter;
  }

  private static BigDecimal i(RexLiteral literal) {
    return bd(literal).setScale(0, RoundingMode.HALF_UP);
  }

  private static String s(RexLiteral literal) {
    return ((NlsString) literal.getValue()).getValue();
  }

  /**
   * Builds a character literal of the Substrait type the containing conversion already produced,
   * rather than deriving the form a second time from the Calcite type. A {@link
   * io.substrait.isthmus.UserTypeMapper} can map a Calcite character type to any of the three, and
   * only the first derivation consults it.
   *
   * <p>A mapping to something outside the character family keeps the form Calcite declares. Its
   * type has no character literal form to build, and reaching one would need the value's encoding
   * in that type, which {@link io.substrait.isthmus.UserTypeMapper} has no way to give.
   *
   * <p>Whether the literal is nullable comes from the mapped type either way, which is what the
   * containing conversion took it from before the type reached here.
   *
   * @param mappedType the Substrait type the containing conversion produced
   * @param value the literal's text
   * @param calciteType the Calcite type the literal was declared as
   * @return the literal
   */
  private static Expression.Literal characterLiteral(
      Type mappedType, String value, RelDataType calciteType) {
    boolean nullable = mappedType.nullable();
    Type type =
        mappedType instanceof Type.Str
                || mappedType instanceof Type.VarChar
                || mappedType instanceof Type.FixedChar
            ? mappedType
            : TypeConverter.DEFAULT.toSubstrait(calciteType);
    if (type instanceof Type.Str) {
      return ExpressionCreator.string(nullable, value);
    }
    if (type instanceof Type.VarChar) {
      // Unlike a fixedchar, a varchar literal carries a length of its own, so a value shorter than
      // it is what the type means. A value longer than it is malformed, and nothing rejects it --
      // here or in the POJO that holds it.
      return ExpressionCreator.varChar(nullable, value, ((Type.VarChar) type).length());
    }
    if (type instanceof Type.FixedChar) {
      // A fixedchar literal carries no length of its own — Expression.FixedCharLiteral derives it
      // from the text — so the text has to be the declared width or the two disagree. Padding is
      // also what CHAR(n) means: 'a' in a CHAR(3) is 'a  '.
      // In characters rather than UTF-16 code units: the spec gives a fixedchar its length in
      // characters, where it spells a string's out in UTF-8 bytes.
      int length = ((Type.FixedChar) type).length();
      // Only the negative end. The spec puts a fixedchar's width in [1..2147483647], but Calcite
      // types the empty character literal as a CHAR(0) and its DDL parser takes a CHAR(0) column,
      // so refusing a zero width here would stop ordinary SQL converting.
      if (length < 0) {
        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "A fixedchar cannot declare a negative width, and this one is %d",
                length));
      }
      int characters = value.codePointCount(0, value.length());
      if (characters > length) {
        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "Character value '%s' is longer than the fixedchar<%d> it is declared as",
                value,
                length));
      }
      long padded = (long) value.length() + ((long) length - characters);
      if (padded > MAX_PADDED_LENGTH) {
        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "A fixedchar<%d> literal cannot be built from '%s': padding it to that width takes "
                    + "%d UTF-16 code units, more than a Java String holds",
                length,
                value,
                padded));
      }
      return ExpressionCreator.fixedChar(nullable, value + " ".repeat(length - characters));
    }
    throw new IllegalStateException(
        String.format(
            "A Calcite character type converted to %s, which is not a character type", type));
  }

  private static BigDecimal bd(RexLiteral literal) {
    return (BigDecimal) literal.getValue();
  }

  /**
   * Converts a Calcite {@link RexLiteral} to a Substrait {@link Expression.Literal}.
   *
   * <p>Type conversion is performed first to ensure value compatibility. Null literals return a
   * typed NULL. Unsupported cases throw an exception.
   *
   * @param literal the Calcite literal to convert
   * @return the corresponding Substrait literal
   * @throws UnsupportedOperationException if the literal type/value cannot be handled
   */
  public Expression.Literal convert(RexLiteral literal) {
    return convert(literal, literal.getType());
  }

  /**
   * Converts a RexLiteral to a Substrait Literal carrying the given result type.
   *
   * <p>This overload is useful when the target type comes from a containing schema rather than the
   * literal itself. Calcite may infer a narrower type for a value in a LogicalValues tuple than for
   * the corresponding row field. Nullability is taken from {@code resultType}, so callers that need
   * a nullability other than the literal's own should widen the Calcite type with {@link
   * org.apache.calcite.rel.type.RelDataTypeFactory#createTypeWithNullability} before calling.
   *
   * @param literal the RexLiteral to convert
   * @param resultType the Calcite type required by the containing schema
   * @return the converted Substrait Literal
   */
  public Expression.Literal convert(RexLiteral literal, RelDataType resultType) {
    // convert type first to guarantee we can handle the value.
    final Type type = typeConverter.toSubstrait(resultType);
    final boolean nullable = type.nullable();
    if (literal.isNull()) {
      final Type typeWithNullability =
          nullable ? TypeCreator.asNullable(type) : TypeCreator.asNotNullable(type);
      return ExpressionCreator.typedNull(typeWithNullability);
    }

    switch (resultType.getSqlTypeName()) {
      case TINYINT:
        return ExpressionCreator.i8(nullable, i(literal).intValue());
      case SMALLINT:
        return ExpressionCreator.i16(nullable, i(literal).intValue());
      case INTEGER:
        return ExpressionCreator.i32(nullable, i(literal).intValue());
      case BIGINT:
        return ExpressionCreator.i64(nullable, i(literal).longValue());
      case BOOLEAN:
        return ExpressionCreator.bool(nullable, literal.getValueAs(Boolean.class));
      case CHAR:
        {
          Comparable<?> val = literal.getValue();
          if (val instanceof NlsString) {
            return characterLiteral(type, ((NlsString) val).getValue(), resultType);
          }
          throw new UnsupportedOperationException("Unable to handle char type: " + val);
        }
      case FLOAT:
      case DOUBLE:
        return ExpressionCreator.fp64(nullable, literal.getValueAs(Double.class));
      case REAL:
        return ExpressionCreator.fp32(nullable, literal.getValueAs(Float.class));

      case DECIMAL:
        {
          BigDecimal bd = bd(literal);
          return ExpressionCreator.decimal(
              nullable, bd, resultType.getPrecision(), resultType.getScale());
        }
      case VARCHAR:
        return characterLiteral(type, s(literal), resultType);
      case BINARY:
        return ExpressionCreator.fixedBinary(
            nullable,
            ByteString.copyFrom(
                padRightIfNeeded(
                    literal.getValueAs(org.apache.calcite.avatica.util.ByteString.class),
                    resultType.getPrecision())));
      case VARBINARY:
        return ExpressionCreator.binary(
            nullable, ByteString.copyFrom(literal.getValueAs(byte[].class)));
      case SYMBOL:
        {
          Object value = literal.getValue();
          if (value instanceof NlsString) {
            return ExpressionCreator.string(nullable, ((NlsString) value).getValue());
          } else if (value instanceof Enum) {
            Enum<?> v = (Enum<?>) value;

            Optional<Expression.Literal> r =
                EnumConverter.canConvert(v)
                    ? Optional.of(ExpressionCreator.string(nullable, v.name()))
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
          return ExpressionCreator.date(nullable, (int) localDate.toEpochDay());
        }
      case TIME:
        {
          TimeString time = literal.getValueAs(TimeString.class);
          LocalTime localTime = LocalTime.parse(time.toString(), CALCITE_LOCAL_TIME_FORMATTER);
          int precision = TypeConverter.precisionOf(resultType);
          return ExpressionCreator.precisionTime(
              nullable, rescaleNanos(localTime.toNanoOfDay(), precision), precision);
        }
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        {
          TimestampString timestamp = literal.getValueAs(TimestampString.class);
          LocalDateTime localDateTime =
              LocalDateTime.parse(timestamp.toString(), CALCITE_LOCAL_DATETIME_FORMATTER);
          int precision = TypeConverter.precisionOf(resultType);
          long value =
              localDateTime.toEpochSecond(ZoneOffset.UTC) * LongMath.pow(10, precision)
                  + rescaleNanos(localDateTime.getNano(), precision);
          // toEpochSecond floors, and the nanosecond part it leaves behind is always positive, so
          // a pre-epoch timestamp narrowed to a coarser precision moves back in time rather than
          // towards the epoch. That is what a timestamp wants — 1969-12-31 23:59:59.5 at second
          // precision is 23:59:59 — and it is the opposite of the interval case, where the value
          // is a duration and narrowing shortens it.
          //
          // The SqlTypeName below is the same one toSubstrait maps to precision_timestamp_tz, so
          // the literal has to agree with the type its own conversion produced.
          return resultType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
              ? ExpressionCreator.precisionTimestampTZ(nullable, value, precision)
              : ExpressionCreator.precisionTimestamp(nullable, value, precision);
        }
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        {
          long intervalLength = Objects.requireNonNull(literal.getValueAs(Long.class));
          long years = intervalLength / 12;
          long months = intervalLength - years * 12;
          return ExpressionCreator.intervalYear(nullable, (int) years, (int) months);
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
          // Report the interval at the precision the Calcite type declares, so a Substrait
          // interval_day<P> keeps its P across a round trip. The value is narrowed to whole
          // milliseconds here — Calcite day-time interval values may carry fractional ones, and
          // getValueAs(Long.class) is what discards them — and P only sets the unit the subseconds
          // component is expressed in.
          long totalMillis = Objects.requireNonNull(literal.getValueAs(Long.class));
          int precision = resultType.getScale();

          // Decompose in milliseconds and scale only the sub-second remainder. Scaling the whole
          // value first overflows a long past ~292 years at P=9, well inside the spec's
          // [-3,650,000..3,650,000] day range. Integer division truncates towards zero and the
          // remainder keeps the dividend's sign, so a negative interval still narrows towards zero.
          long millisPerDay = TimeUnit.DAYS.toMillis(1);
          long days = totalMillis / millisPerDay;
          long remainder = totalMillis - days * millisPerDay;
          long seconds = remainder / 1_000L;
          long millisPart = remainder - seconds * 1_000L;
          long subseconds =
              precision > 3
                  ? millisPart * LongMath.pow(10, precision - 3)
                  : millisPart / LongMath.pow(10, 3 - precision);

          return ExpressionCreator.intervalDay(
              nullable, (int) days, (int) seconds, subseconds, precision);
        }

      case ROW:
        {
          List<RexLiteral> literals = (List<RexLiteral>) literal.getValue();
          return ExpressionCreator.struct(
              nullable,
              IntStream.range(0, literals.size())
                  .mapToObj(
                      i -> convert(literals.get(i), resultType.getFieldList().get(i).getType()))
                  .collect(Collectors.toList()));
        }

      case ARRAY:
        {
          List<RexLiteral> literals = (List<RexLiteral>) literal.getValue();
          RelDataType componentType = Objects.requireNonNull(resultType.getComponentType());
          return ExpressionCreator.list(
              nullable,
              literals.stream()
                  .map(nestedLiteral -> convert(nestedLiteral, componentType))
                  .collect(Collectors.toList()));
        }

      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unable to convert the value of %s of type %s to a literal.",
                literal, resultType.getSqlTypeName()));
    }
  }

  /**
   * Pads a Calcite {@link org.apache.calcite.avatica.util.ByteString} right with zeros to the
   * expected length if needed.
   *
   * @param bytes the Calcite {@code ByteString} value
   * @param length the expected fixed length
   * @return a new byte array of {@code length} with original bytes and trailing zeros if needed
   * @throws IllegalArgumentException if {@code length} is less than {@code bytes.length}
   */
  public static byte[] padRightIfNeeded(
      org.apache.calcite.avatica.util.ByteString bytes, int length) {
    return padRightIfNeeded(bytes.getBytes(), length);
  }

  /**
   * Rescales a nanosecond count to the fractional-second unit a Substrait temporal literal of the
   * given precision is expressed in.
   *
   * @param nanos a non-negative nanosecond count: a whole time of day for a {@code precision_time},
   *     the sub-second remainder for a {@code precision_timestamp}. Non-negativity is the
   *     precondition the division relies on, since it truncates towards zero rather than flooring.
   * @param precision the fractional-second precision of the target literal
   * @return {@code nanos} in units of 10^-precision seconds
   */
  private static long rescaleNanos(long nanos, int precision) {
    return precision >= 9
        ? nanos * LongMath.pow(10, precision - 9)
        : nanos / LongMath.pow(10, 9 - precision);
  }

  /**
   * Pads a byte array right with zeros to the expected length if needed.
   *
   * @param value the byte array value
   * @param length the expected fixed length
   * @return a new byte array of {@code length} with original bytes and trailing zeros if needed
   * @throws IllegalArgumentException if {@code length} is less than {@code value.length}
   */
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
