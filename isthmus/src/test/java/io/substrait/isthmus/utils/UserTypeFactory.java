package io.substrait.isthmus.utils;

import io.substrait.isthmus.type.SubstraitUserDefinedType;
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

  public UserTypeFactory(String urn, String name) {
    this.urn = urn;
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
    return TypeCreator.of(nullable).userDefined(urn, name);
  }

  /**
   * Test-specific variant of the core implementation that treats Calcite-copied types as
   * equivalent, even when they are not the same Java instance. Calcite often clones UDTs during
   * planning, so reference equality alone would fail in tests.
   */
  public boolean isTypeFromFactory(RelDataType type) {
    return matchesSubstraitType(type) || type == N || type == R || matchesCalciteAlias(type);
  }

  /**
   * Detects Substrait-backed Calcite types by interrogating their metadata.
   *
   * <p>If Calcite preserves the original {@link SubstraitUserDefinedType}, the urn/name are both
   * available directly. Otherwise, Calcite may create an anonymous {@link RelDataTypeImpl} copy,
   * exposing only its alias string. In that case we fall back to comparing the formatted alias (see
   * {@link InnerType#generateTypeString}).
   */
  private boolean matchesSubstraitType(RelDataType type) {
    if (type instanceof SubstraitUserDefinedType) {
      SubstraitUserDefinedType udt = (SubstraitUserDefinedType) type;
      return this.urn.equals(udt.getUrn()) && this.name.equals(udt.getName());
    }
    return false;
  }

  /**
   * Calcite may copy a user-defined type into an anonymous {@link RelDataTypeImpl} where the only
   * identifier left is its alias string. This helper captures the "find by alias" fallback so it’s
   * clear we’re matching against the formatted <code>urn::name</code> when the rich metadata is not
   * available.
   */
  private boolean matchesCalciteAlias(RelDataType type) {
    return type != null
        && (type.getSqlTypeName() == SqlTypeName.OTHER || type.getSqlTypeName() == SqlTypeName.ROW)
        && type.toString().equals(calciteDisplayName());
  }

  private String calciteDisplayName() {
    return String.format("%s::%s", this.urn, this.name);
  }

  private class InnerType extends RelDataTypeImpl {
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
      sb.append(UserTypeFactory.this.urn).append("::").append(name);
    }
  }
}
