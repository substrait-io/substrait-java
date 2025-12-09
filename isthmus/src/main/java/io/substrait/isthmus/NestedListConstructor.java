package io.substrait.isthmus;

import static java.util.Objects.requireNonNull;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlMultisetValueConstructor;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

/**
 * Substrait-specific constructor to map back to the Expression NestedList type in Substrait. This
 * constructor creates a special type of SqlKind.ARRAY_VALUE_CONSTRUCTOR for lists that store
 * non-literal expressions.
 */
public class NestedListConstructor extends SqlMultisetValueConstructor {

  public NestedListConstructor() {
    super("NESTEDLIST", SqlKind.ARRAY_VALUE_CONSTRUCTOR);
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    RelDataType type =
        getComponentType(opBinding.getTypeFactory(), opBinding.collectOperandTypes());
    requireNonNull(type, "inferred array element type");

    // explicit cast elements to component type if they are not same
    SqlValidatorUtil.adjustTypeForArrayConstructor(type, opBinding);

    return SqlTypeUtil.createArrayType(opBinding.getTypeFactory(), type, false);
  }
}
