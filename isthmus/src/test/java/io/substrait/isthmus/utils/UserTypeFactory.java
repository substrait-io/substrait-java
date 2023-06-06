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
  private final String uri;
  private final String name;

  public UserTypeFactory(String uri, String name) {
    this.uri = uri;
    this.name = name;
    this.N = new InnerType(true, name);
    this.R = new InnerType(false, name);
  }

  public RelDataType createCalcite(boolean nullable) {
    if (nullable) {
      return N;
    } else {
      return R;
    }
  }

  public Type createSubstrait(boolean nullable) {
    return TypeCreator.of(nullable).userDefined(uri, name);
  }

  public boolean isTypeFromFactory(RelDataType type) {
    return type == N || type == R;
  }

  private static class InnerType extends RelDataTypeImpl {
    private final boolean nullable;
    private final String name;

    private InnerType(boolean nullable, String name) {
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
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
      sb.append(name);
    }
  }
}
