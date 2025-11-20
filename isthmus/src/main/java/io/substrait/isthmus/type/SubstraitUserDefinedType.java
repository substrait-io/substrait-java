package io.substrait.isthmus.type;

import io.substrait.type.Type;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Custom Calcite {@link RelDataType} for Substrait user-defined types.
 *
 * <p>This type preserves all UDT metadata (URN, name, type parameters) during Calcite roundtrips.
 * It is used when converting types without literal context. For literals, specialized subclasses
 * provide representation-specific handling:
 *
 * <ul>
 *   <li>{@link SubstraitUserDefinedAnyType} - For opaque binary UDT literals (wraps protobuf Any)
 *   <li>{@link SubstraitUserDefinedStructType} - For structured UDT literals with accessible fields
 * </ul>
 *
 * @see SubstraitUserDefinedAnyType
 * @see SubstraitUserDefinedStructType
 * @see io.substrait.expression.Expression.UserDefinedAny
 * @see io.substrait.expression.Expression.UserDefinedStruct
 */
public class SubstraitUserDefinedType extends RelDataTypeImpl {

  private final String urn;
  private final String name;
  private final List<io.substrait.proto.Type.Parameter> typeParameters;
  private final boolean nullable;

  public SubstraitUserDefinedType(
      String urn,
      String name,
      List<io.substrait.proto.Type.Parameter> typeParameters,
      boolean nullable) {
    this.urn = urn;
    this.name = name;
    this.typeParameters =
        typeParameters != null ? typeParameters : java.util.Collections.emptyList();
    this.nullable = nullable;
    computeDigest();
  }

  public String getUrn() {
    return urn;
  }

  public String getName() {
    return name;
  }

  public List<io.substrait.proto.Type.Parameter> getTypeParameters() {
    return typeParameters;
  }

  @Override
  public boolean isNullable() {
    return nullable;
  }

  @Override
  public SqlTypeName getSqlTypeName() {
    return SqlTypeName.OTHER;
  }

  /** Converts this Calcite type back to a Substrait {@link Type.UserDefined}. */
  public Type.UserDefined toSubstraitType() {
    return Type.UserDefined.builder()
        .urn(urn)
        .name(name)
        .typeParameters(typeParameters)
        .nullable(nullable)
        .build();
  }

  /** Creates a SubstraitUserDefinedType from a Substrait Type.UserDefined. */
  public static SubstraitUserDefinedType from(io.substrait.type.Type.UserDefined type) {
    return new SubstraitUserDefinedType(
        type.urn(), type.name(), type.typeParameters(), type.nullable());
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(name);
    if (!typeParameters.isEmpty()) {
      sb.append("<");
      sb.append(String.join(", ", java.util.Collections.nCopies(typeParameters.size(), "_")));
      sb.append(">");
    }
  }

  /**
   * Custom Calcite type representing a Substrait {@link
   * io.substrait.expression.Expression.UserDefinedAny} type.
   *
   * <p>This type wraps opaque binary data (protobuf Any) and preserves all UDT metadata including
   * type parameters during Calcite roundtrips.
   *
   * <p>Note: The actual value (protobuf Any) is not stored in the type itself - it's stored in the
   * literal. This type only carries the metadata (URN, name, type parameters).
   *
   * <p>{@link io.substrait.expression.Expression.UserDefinedAny UserDefinedAny} literals use this
   * type when passing through Calcite, as they need to be serialized to binary with REINTERPRET.
   * {@link io.substrait.expression.Expression.UserDefinedStruct UserDefinedStruct} literals use
   * {@link SubstraitUserDefinedStructType} instead to preserve field structure.
   *
   * @see SubstraitUserDefinedStructType
   * @see io.substrait.expression.Expression.UserDefinedAny
   */
  public static class SubstraitUserDefinedAnyType extends SubstraitUserDefinedType {

    public SubstraitUserDefinedAnyType(
        String urn,
        String name,
        List<io.substrait.proto.Type.Parameter> typeParameters,
        boolean nullable) {
      super(urn, name, typeParameters, nullable);
    }

    /** Creates a SubstraitUserDefinedAnyType from a Substrait Type.UserDefined. */
    public static SubstraitUserDefinedAnyType from(io.substrait.type.Type.UserDefined type) {
      return new SubstraitUserDefinedAnyType(
          type.urn(), type.name(), type.typeParameters(), type.nullable());
    }
  }

  /**
   * Custom Calcite type representing a Substrait {@link
   * io.substrait.expression.Expression.UserDefinedStruct} type.
   *
   * <p>This type represents a structured UDT with explicitly defined fields. Unlike {@link
   * SubstraitUserDefinedAnyType}, the fields are accessible and can be represented as a Calcite
   * STRUCT/ROW type with additional UDT metadata (URN, name, type parameters).
   *
   * <p>{@link io.substrait.expression.Expression.UserDefinedStruct UserDefinedStruct} literals use
   * this type when passing through Calcite, preserving field structure and enabling field access.
   * The fields are converted to Calcite literals and wrapped in a ROW type with synthetic field
   * names (f0, f1, f2, etc.).
   *
   * @see SubstraitUserDefinedAnyType
   * @see io.substrait.expression.Expression.UserDefinedStruct
   */
  public static class SubstraitUserDefinedStructType extends SubstraitUserDefinedType {

    private final List<RelDataType> fieldTypes;
    private final List<String> fieldNames;

    public SubstraitUserDefinedStructType(
        String urn,
        String name,
        List<io.substrait.proto.Type.Parameter> typeParameters,
        boolean nullable,
        List<RelDataType> fieldTypes,
        List<String> fieldNames) {
      super(urn, name, typeParameters, nullable);
      if (fieldTypes.size() != fieldNames.size()) {
        throw new IllegalArgumentException("Field types and names must have same length");
      }
      this.fieldTypes = fieldTypes;
      this.fieldNames = fieldNames;
    }

    @Override
    public List<RelDataTypeField> getFieldList() {
      java.util.List<RelDataTypeField> fields = new java.util.ArrayList<>();
      for (int i = 0; i < fieldTypes.size(); i++) {
        fields.add(new RelDataTypeFieldImpl(fieldNames.get(i), i, fieldTypes.get(i)));
      }
      return fields;
    }

    @Override
    public int getFieldCount() {
      return fieldTypes.size();
    }

    @Override
    public RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {
      for (int i = 0; i < fieldNames.size(); i++) {
        String name = fieldNames.get(i);
        if (caseSensitive ? name.equals(fieldName) : name.equalsIgnoreCase(fieldName)) {
          return new RelDataTypeFieldImpl(name, i, fieldTypes.get(i));
        }
      }
      return null;
    }

    public List<RelDataType> getFieldTypes() {
      return fieldTypes;
    }

    @Override
    public List<String> getFieldNames() {
      return fieldNames;
    }

    @Override
    public SqlTypeName getSqlTypeName() {
      // Can be considered as ROW since it has structure
      return SqlTypeName.ROW;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
      sb.append(getName());
      if (!getTypeParameters().isEmpty()) {
        sb.append("<");
        sb.append(
            String.join(", ", java.util.Collections.nCopies(getTypeParameters().size(), "_")));
        sb.append(">");
      }
      if (withDetail && fieldNames != null) {
        sb.append("(");
        sb.append(
            java.util.stream.IntStream.range(0, fieldNames.size())
                .mapToObj(i -> fieldNames.get(i) + ": " + fieldTypes.get(i))
                .collect(java.util.stream.Collectors.joining(", ")));
        sb.append(")");
      }
    }

    /**
     * Creates a SubstraitUserDefinedStructType from a Substrait Type.UserDefined and field
     * information.
     */
    public static SubstraitUserDefinedStructType from(
        io.substrait.type.Type.UserDefined type,
        List<RelDataType> fieldTypes,
        List<String> fieldNames) {
      return new SubstraitUserDefinedStructType(
          type.urn(), type.name(), type.typeParameters(), type.nullable(), fieldTypes, fieldNames);
    }
  }
}
