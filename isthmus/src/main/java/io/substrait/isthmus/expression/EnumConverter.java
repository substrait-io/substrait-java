package io.substrait.isthmus.expression;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.substrait.expression.EnumArg;
import io.substrait.function.SimpleExtension;
import java.util.Optional;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Encapsulate mapping Calcite Enums {@link EnumArg EnumArg}.
 *
 * <ul>
 *   <li>Add the calcite {@link Enum} to {@link EnumConverter#calciteEnumMap calciteEnumMap}.
 *       Substrait Enum represented by the name in {@link
 *       io.substrait.function.SimpleExtension.EnumArgument}
 *   <li>Add logic to {@link EnumConverter#constructValue constructValue} to construct the {@link
 *       Enum} instance
 * </ul>
 */
public class EnumConverter {

  private static final BiMap<Class<? extends Enum>, String> calciteEnumMap = HashBiMap.create();

  static {
    calciteEnumMap.put(TimeUnitRange.class, "The part of the value to extract.");
  }

  private static Optional<Enum> constructValue(Class<? extends Enum> cls, String option) {
    if (cls.isAssignableFrom(TimeUnitRange.class)) {
      return Optional.of(TimeUnitRange.valueOf(option));
    } else {
      return Optional.empty();
    }
  }

  static Optional<RexLiteral> convert(RexBuilder rexBuilder, EnumArg e) {
    var v = Optional.ofNullable(calciteEnumMap.inverse().getOrDefault(e.enumArg().name(), null));
    return v.flatMap(cls -> constructValue(cls, e.option())).map(en -> rexBuilder.makeFlag(en));
  }

  private static Optional<SimpleExtension.EnumArgument> findEnumArg(
      SimpleExtension.Function function, String enumName) {
    return function.args().stream()
        .filter(a -> a instanceof SimpleExtension.EnumArgument)
        .map(SimpleExtension.EnumArgument.class::cast)
        .filter(a -> a.name().equals(enumName))
        .findFirst();
  }

  static Optional<EnumArg> convert(SimpleExtension.Function function, RexLiteral literal) {
    return switch (literal.getType().getSqlTypeName()) {
      case SYMBOL -> {
        Object v = literal.getValue();
        if (!literal.isNull() && (v instanceof Enum)) {
          Enum value = (Enum) v;
          Optional<String> enumName =
              Optional.ofNullable(calciteEnumMap.getOrDefault(value.getClass(), null));
          yield enumName
              .flatMap(n -> findEnumArg(function, n))
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
        && ((RexLiteral) value).getType().getSqlTypeName() == SqlTypeName.SYMBOL;
  }
}
