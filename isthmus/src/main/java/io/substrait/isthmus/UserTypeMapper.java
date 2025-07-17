package io.substrait.isthmus;

import io.substrait.type.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Defines conversion of user-defined types between Substrait and Calcite */
public interface UserTypeMapper {
  /**
   * @param relDataType the Calcite {@link RelDataType} type to convert
   * @return the Substrait representation of the input type
   */
  @Nullable Type toSubstrait(RelDataType relDataType);

  /**
   * @param type the Subtrait {@link Type.UserDefined} type to convert
   * @return the Calcite {@link RelDataType} representing the input type
   */
  @Nullable RelDataType toCalcite(Type.UserDefined type);
}
