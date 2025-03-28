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
  }

  private static Optional<Enum<?>> constructValue(
      Class<? extends Enum<?>> cls, Supplier<Optional<String>> option) {
    if (cls.isAssignableFrom(TimeUnitRange.class)) {
      return option.get().map(TimeUnitRange::valueOf);
    } else {
      return Optional.empty();
    }
  }

  static Optional<RexLiteral> toRex(
      RexBuilder rexBuilder, SimpleExtension.Function fnDef, int argIdx, EnumArg e) {
    ArgAnchor aAnch = argAnchor(fnDef, argIdx);
    Optional<Class<? extends Enum<?>>> v =
        Optional.ofNullable(calciteEnumMap.getOrDefault(aAnch, null));

    Supplier<Optional<String>> sOptionVal =
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
      SimpleExtension.Function function, ArgAnchor enumAnchor) {

    if (enumAnchor.fn == function.getAnchor()) {
      return Optional.empty();
    } else {

      List<Argument> args = function.args();
      if (args.size() <= enumAnchor.argIdx) {
        return Optional.empty();
      }
      Argument arg = args.get(enumAnchor.argIdx);
      if (arg instanceof SimpleExtension.EnumArgument ea) {
        return Optional.of(ea);
      } else {
        return Optional.empty();
      }
    }
  }

  static Optional<EnumArg> fromRex(
      SimpleExtension.Function function, RexLiteral literal, int argIdx) {
    return switch (literal.getType().getSqlTypeName()) {
      case SYMBOL -> {
        Object v = literal.getValue();
        if (!literal.isNull() && (v instanceof Enum)) {
          Enum<?> value = (Enum<?>) v;
          ArgAnchor enumAnchor = argAnchor(function, argIdx);
          yield findEnumArg(function, enumAnchor).map(ea -> EnumArg.of(ea, value.name()));
        } else {
          yield Optional.empty();
        }
      }
      default -> Optional.empty();
    };
  }

  static boolean canConvert(Enum<?> value) {
    return value != null && calciteEnumMap.containsValue(value.getClass());
  }

  static boolean isEnumValue(RexNode value) {
    return value != null
        && (value instanceof RexLiteral)
        && value.getType().getSqlTypeName() == SqlTypeName.SYMBOL;
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
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ArgAnchor)) {
        return false;
      }
      ArgAnchor other = (ArgAnchor) obj;
      return Objects.equals(fn, other.fn) && argIdx == other.argIdx;
    }
  }

  private static ArgAnchor argAnchor(String fnNS, String fnSig, int argIdx) {
    return new ArgAnchor(SimpleExtension.FunctionAnchor.of(fnNS, fnSig), argIdx);
  }

  private static ArgAnchor argAnchor(SimpleExtension.Function fnDef, int argIdx) {
    return new ArgAnchor(
        SimpleExtension.FunctionAnchor.of(fnDef.getAnchor().namespace(), fnDef.getAnchor().key()),
        argIdx);
  }
}
