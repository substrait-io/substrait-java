package io.substrait.isthmus.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.substrait.proto.Type;
import java.util.List;
import org.junit.jupiter.api.Test;

class SubstraitUserDefinedTypeTest {
  private static final String URN = "extension:io.substrait:test";
  private static final String NAME = "custom";

  @Test
  void differentTypeParametersProduceDifferentDigests() {
    Type.Parameter integerParam = Type.Parameter.newBuilder().setInteger(1).build();
    Type.Parameter enumParam = Type.Parameter.newBuilder().setEnum("value").build();

    SubstraitUserDefinedType typeWithInteger =
        new SubstraitUserDefinedType.SubstraitUserDefinedAnyType(
            URN, NAME, List.of(integerParam), false);
    SubstraitUserDefinedType typeWithEnum =
        new SubstraitUserDefinedType.SubstraitUserDefinedAnyType(
            URN, NAME, List.of(enumParam), false);

    assertNotEquals(typeWithInteger, typeWithEnum);
    assertNotEquals(typeWithInteger.toString(), typeWithEnum.toString());
  }

  @Test
  void sameParametersRemainEqual() {
    Type.Parameter integerParam = Type.Parameter.newBuilder().setInteger(7).build();
    SubstraitUserDefinedType left =
        new SubstraitUserDefinedType.SubstraitUserDefinedAnyType(
            URN, NAME, List.of(integerParam), true);
    SubstraitUserDefinedType right =
        new SubstraitUserDefinedType.SubstraitUserDefinedAnyType(
            URN, NAME, List.of(integerParam), true);

    assertEquals(left, right);
  }
}
