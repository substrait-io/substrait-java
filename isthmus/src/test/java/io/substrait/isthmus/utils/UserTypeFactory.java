package io.substrait.isthmus.utils;

import io.substrait.type.Type;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
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
    this.N = new InnerType(urn, name, true, Collections.emptyList());
    this.R = new InnerType(urn, name, false, Collections.emptyList());
  }

  public RelDataType createCalcite(boolean nullable) {
    return createCalcite(nullable, Collections.emptyList());
  }

  public RelDataType createCalcite(boolean nullable, List<Type.Parameter> typeParameters) {
    if (typeParameters.isEmpty()) {
      return nullable ? N : R;
    }

    return new InnerType(urn, name, nullable, typeParameters);
  }

  public Type createSubstrait(boolean nullable) {
    return createSubstrait(nullable, Collections.emptyList());
  }

  public Type createSubstrait(boolean nullable, List<Type.Parameter> typeParameters) {
    return Type.UserDefined.builder()
        .nullable(nullable)
        .urn(urn)
        .name(name)
        .addAllTypeParameters(typeParameters)
        .build();
  }

  public boolean isTypeFromFactory(RelDataType type) {
    // We may return cached instances (N/R) or fresh InnerType instances with parameters.
    // Use instanceof to recognize any of them and match by urn/name so custom UDT mappings work.
    if (type instanceof InnerType) {
      InnerType inner = (InnerType) type;
      return urn.equals(inner.urn) && name.equals(inner.name);
    }
    return false;
  }

  public List<Type.Parameter> getTypeParameters(RelDataType type) {
    if (type instanceof InnerType) {
      return ((InnerType) type).typeParameters;
    }
    return Collections.emptyList();
  }

  private static class InnerType extends RelDataTypeImpl {
    private final boolean nullable;
    private final String urn;
    private final String name;
    private final List<Type.Parameter> typeParameters;

    private InnerType(
        String urn, String name, boolean nullable, List<Type.Parameter> typeParameters) {
      this.urn = urn;
      this.name = name;
      this.nullable = nullable;
      this.typeParameters = Collections.unmodifiableList(typeParameters);
      computeDigest();
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
      sb.append(urn).append(":").append(name);

      if (!typeParameters.isEmpty()) {
        sb.append("<");
        sb.append(typeParameters.stream().map(Object::toString).collect(Collectors.joining(",")));
        sb.append(">");
      }
    }
  }
}
