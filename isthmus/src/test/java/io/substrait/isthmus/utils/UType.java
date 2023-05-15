package io.substrait.isthmus.utils;

import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.UserTypeMapper;
import io.substrait.type.Type;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Implementation of a user-defined type for testing. Includes a {@link TypeConverter} that handles
 * it.
 */
public class UType extends RelDataTypeImpl {

  public static final String URI = "/custom_types";
  public static final String NAME = "utype";

  public static UType NULLABLE = new UType(true);
  public static UType REQUIRED = new UType(false);
  private final boolean nullable;

  private UType(boolean nullable) {
    computeDigest();
    this.nullable = nullable;
  }

  public static UType create(boolean nullable) {
    return nullable ? NULLABLE : REQUIRED;
  }

  @Override
  public boolean isNullable() {
    return nullable;
  }

  @Override
  public SqlTypeName getSqlTypeName() {
    return SqlTypeName.VARCHAR;
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(NAME);
  }

  public static TypeConverter typeConverter =
      new TypeConverter(
          new UserTypeMapper() {
            @Nullable
            @Override
            public Type toSubstrait(RelDataType relDataType) {
              if (relDataType instanceof UType) {
                return Type.withNullability(relDataType.isNullable())
                    .userDefined(UType.URI, UType.NAME);
              }
              return null;
            }

            @Nullable
            @Override
            public RelDataType toCalcite(Type.UserDefined type) {
              if (type.uri().equals(UType.URI) && type.name().equals(UType.NAME)) {
                return new UType(type.nullable());
              }
              return null;
            }
          });
}
