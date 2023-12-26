package io.substrait.isthmus.expression;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.substrait.expression.EnumArg;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
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

  private static final BiMap<Class<? extends Enum>, ArgAnchor> calciteEnumMap = HashBiMap.create();

  static {
    calciteEnumMap.put(
        TimeUnitRange.class,
        argAnchor(DefaultExtensionCatalog.FUNCTIONS_DATETIME, "extract:req_ts", 0));
  }

  private static Optional<Enum> constructValue(
      Class<? extends Enum> cls, Supplier<Optional<String>> option) {
    if (cls.isAssignableFrom(TimeUnitRange.class)) {
      return option.get().map(TimeUnitRange::valueOf);
    } else {
      return Optional.empty();
    }
  }

  static Optional<RexLiteral> toRex(
      RexBuilder rexBuilder, SimpleExtension.Function fnDef, int argIdx, EnumArg e) {
    var aAnch = argAnchor(fnDef, argIdx);
    var v = Optional.ofNullable(calciteEnumMap.inverse().getOrDefault(aAnch, null));

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

      var args = function.args();
      if (args.size() <= enumAnchor.argIdx) {
        return Optional.empty();
      }
      var arg = args.get(enumAnchor.argIdx);
      if (arg instanceof SimpleExtension.EnumArgument ea) {
        return Optional.of(ea);
      } else {
        return Optional.empty();
      }
    }
  }

  static Optional<EnumArg> fromRex(SimpleExtension.Function function, RexLiteral literal) {
    return switch (literal.getType().getSqlTypeName()) {
      case SYMBOL -> {
        Object v = literal.getValue();
        if (!literal.isNull() && (v instanceof Enum)) {
          Enum value = (Enum) v;
          Optional<ArgAnchor> enumAnchor =
              Optional.ofNullable(calciteEnumMap.getOrDefault(value.getClass(), null));
          yield enumAnchor
              .flatMap(en -> findEnumArg(function, en))
              .map(ea -> EnumArg.of(ea, value.name()));
        } else {
          yield Optional.empty();
        }
      }
      default -> Optional.empty();
    };
  }

  static boolean canConvert(Enum value) {
    return value != null && calciteEnumMap.containsKey(value.getClass());
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
