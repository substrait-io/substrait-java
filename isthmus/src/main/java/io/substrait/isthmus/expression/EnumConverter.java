package io.substrait.isthmus.expression;

import io.substrait.expression.EnumArg;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.extension.SimpleExtension.Argument;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Encapsulate mapping Calcite Enums {@link EnumArg EnumArg} mapping.
 *
 * <ul>
 *   <li>Add the calcite {@link Enum} to {@link EnumConverter#calciteEnumMap calciteEnumMap}.
 *       Substrait Enum represented by {ArgAnchor}
 *   <li>Add logic to {@link EnumConverter#constructValue constructValue} to construct the {@link
 *       Enum} instance
 * </ul>
 */
public class EnumConverter {

  private static final Map<ArgAnchor, Class<? extends Enum<?>>> calciteEnumMap = new HashMap<>();

  static {
    // deprecated {@link io.substrait.type.Type.Timestamp}
    calciteEnumMap.put(
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_DATETIME, "extract:req_ts", 0),
        TimeUnitRange.class);
    // deprecated {@link io.substrait.type.Type.TimestampTZ}
    calciteEnumMap.put(
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_DATETIME, "extract:req_tstz_str", 0),
        TimeUnitRange.class);

    calciteEnumMap.put(
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_DATETIME, "extract:req_pts", 0),
        TimeUnitRange.class);
    calciteEnumMap.put(
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_DATETIME, "extract:req_ptstz_str", 0),
        TimeUnitRange.class);
    calciteEnumMap.put(
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_DATETIME, "extract:req_date", 0),
        TimeUnitRange.class);
    calciteEnumMap.put(
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_DATETIME, "extract:req_time", 0),
        TimeUnitRange.class);

    calciteEnumMap.put(
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_STRING, "trim:vchar_vchar", 0),
        SqlTrimFunction.Flag.class);
    calciteEnumMap.put(
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_STRING, "trim:str_str", 0),
        SqlTrimFunction.Flag.class);
    calciteEnumMap.put(
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_STRING, "ltrim:vchar_vchar", 0),
        SqlTrimFunction.Flag.class);
    calciteEnumMap.put(
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_STRING, "ltrim:str_str", 0),
        SqlTrimFunction.Flag.class);
    calciteEnumMap.put(
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_STRING, "rtrim:vchar_vchar", 0),
        SqlTrimFunction.Flag.class);
    calciteEnumMap.put(
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_STRING, "rtrim:str_str", 0),
        SqlTrimFunction.Flag.class);
  }

  private static Optional<Enum<?>> constructValue(
      final Class<? extends Enum<?>> cls, final Supplier<Optional<String>> option) {
    if (cls.isAssignableFrom(TimeUnitRange.class)) {
      return option.get().map(TimeUnitRange::valueOf);
    }

    if (cls.isAssignableFrom(SqlTrimFunction.Flag.class)) {
      return option.get().map(SqlTrimFunction.Flag::valueOf);
    }

    return Optional.empty();
  }

  static Optional<RexLiteral> toRex(
      final RexBuilder rexBuilder,
      final SimpleExtension.Function fnDef,
      final int argIdx,
      final EnumArg e) {
    final ArgAnchor aAnch = argAnchor(fnDef, argIdx);
    final Optional<Class<? extends Enum<?>>> v =
        Optional.ofNullable(calciteEnumMap.getOrDefault(aAnch, null));

    final Supplier<Optional<String>> sOptionVal =
        () -> {
          if (e.value().isPresent()) {
            return Optional.of(e.value().get());
          } else {
            return findEnumArg(fnDef, aAnch).map(ea -> ea.options().get(0));
          }
        };

    return v.flatMap(cls -> constructValue(cls, sOptionVal)).map(en -> rexBuilder.makeFlag(en));
  }

  private static Optional<SimpleExtension.EnumArgument> findEnumArg(
      final SimpleExtension.Function function, final ArgAnchor enumAnchor) {

    if (enumAnchor.fn == function.getAnchor()) {
      return Optional.empty();
    } else {

      final List<Argument> args = function.args();
      if (args.size() <= enumAnchor.argIdx) {
        return Optional.empty();
      }
      final Argument arg = args.get(enumAnchor.argIdx);
      if (arg instanceof SimpleExtension.EnumArgument) {
        return Optional.of((SimpleExtension.EnumArgument) arg);
      } else {
        return Optional.empty();
      }
    }
  }

  static Optional<EnumArg> fromRex(
      final SimpleExtension.Function function, final RexLiteral literal, final int argIdx) {
    switch (literal.getType().getSqlTypeName()) {
      case SYMBOL:
        {
          final Object v = literal.getValue();
          if (!literal.isNull() && (v instanceof Enum)) {
            final Enum<?> value = (Enum<?>) v;
            final ArgAnchor enumAnchor = argAnchor(function, argIdx);
            return findEnumArg(function, enumAnchor).map(ea -> EnumArg.of(ea, value.name()));
          }

          return Optional.empty();
        }
      default:
        return Optional.empty();
    }
  }

  static boolean canConvert(final Enum<?> value) {
    return value != null && calciteEnumMap.containsValue(value.getClass());
  }

  static boolean isEnumValue(final RexNode value) {
    return value instanceof RexLiteral && value.getType().getSqlTypeName() == SqlTypeName.SYMBOL;
  }

  private static class ArgAnchor {
    public final SimpleExtension.FunctionAnchor fn;
    public final int argIdx;

    public ArgAnchor(final SimpleExtension.FunctionAnchor fn, final int argIdx) {
      this.fn = fn;
      this.argIdx = argIdx;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fn, argIdx);
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ArgAnchor)) {
        return false;
      }
      final ArgAnchor other = (ArgAnchor) obj;
      return Objects.equals(fn, other.fn) && argIdx == other.argIdx;
    }
  }

  private static ArgAnchor argAnchor(final String fnNS, final String fnSig, final int argIdx) {
    return new ArgAnchor(SimpleExtension.FunctionAnchor.of(fnNS, fnSig), argIdx);
  }

  private static ArgAnchor argAnchor(final SimpleExtension.Function fnDef, final int argIdx) {
    return new ArgAnchor(
        SimpleExtension.FunctionAnchor.of(fnDef.getAnchor().urn(), fnDef.getAnchor().key()),
        argIdx);
  }
}
