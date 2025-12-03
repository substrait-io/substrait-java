package io.substrait.isthmus.utils;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Utility factory for creating user-defined types implementing {@link RelDataTypeImpl}, correctly.
 * Used for creating types as part of testing.
 */
public class UserTypeFactory {
  private final InnerType N;
  private final InnerType R;
  private final String urn;
  private final String name;

  public UserTypeFactory(final String urn, final String name) {
    this.urn = urn;
    this.name = name;
    this.N = new InnerType(true, name);
    this.R = new InnerType(false, name);
  }

  public RelDataType createCalcite(final boolean nullable) {
    if (nullable) {
      return N;
    } else {
      return R;
    }
  }

  public Type createSubstrait(final boolean nullable) {
    return TypeCreator.of(nullable).userDefined(urn, name);
  }

  public boolean isTypeFromFactory(final RelDataType type) {
    return type == N || type == R;
  }

  private static class InnerType extends RelDataTypeImpl {
    private final boolean nullable;
    private final String name;

    private InnerType(final boolean nullable, final String name) {
      computeDigest();
      this.nullable = nullable;
      this.name = name;
    }

    @Override
    public boolean isNullable() {
      return nullable;
    }

    @Override
    public SqlTypeName getSqlTypeName() {
      return SqlTypeName.OTHER;
    }

    @Override
    protected void generateTypeString(final StringBuilder sb, final boolean withDetail) {
      sb.append(name);
    }
  }
}
