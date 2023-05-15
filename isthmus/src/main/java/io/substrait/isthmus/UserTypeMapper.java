package io.substrait.isthmus;

import io.substrait.type.Type;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;

/** Defines conversion of user-defined types between Substrait and Calcite */
public interface UserTypeMapper {
  @Nullable
  Type toSubstrait(RelDataType relDataType);

  @Nullable
  RelDataType toCalcite(Type.UserDefined type);
}
