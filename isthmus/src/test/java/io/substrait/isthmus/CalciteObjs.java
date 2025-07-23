package io.substrait.isthmus;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/** Set of classes/methods that make it easier to work with Calcite. */
public abstract class CalciteObjs {

  final RelDataTypeFactory type = SubstraitTypeSystem.createTypeFactory();
  final RexBuilder rex = new RexBuilder(type);

  RelDataType t(SqlTypeName typeName, int... vals) {
    switch (vals.length) {
      case 0:
        return type.createSqlType(typeName);
      case 1:
        return type.createSqlType(typeName, vals[0]);
      case 2:
        return type.createSqlType(typeName, vals[0], vals[1]);
      default:
        throw new IllegalArgumentException();
    }
  }

  RelDataType tN(SqlTypeName typeName, int... vals) {
    return type.createTypeWithNullability(t(typeName, vals), true);
  }

  public RexNode makeCalciteLiteral(
      boolean nullable, SqlTypeName typeName, Object value, int... vals) {
    return rex.makeLiteral(value, nullable ? tN(typeName, vals) : t(typeName, vals), true, false);
  }

  public RexNode c(Object value, SqlTypeName typeName, int... vals) {
    return makeCalciteLiteral(false, typeName, value, vals);
  }

  public RexNode cN(Object value, SqlTypeName typeName, int... vals) {
    return makeCalciteLiteral(true, typeName, value, vals);
  }
}
