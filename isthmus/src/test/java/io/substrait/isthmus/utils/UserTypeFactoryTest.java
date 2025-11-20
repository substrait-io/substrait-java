package io.substrait.isthmus.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.type.SubstraitUserDefinedType;
import io.substrait.type.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.jupiter.api.Test;

class UserTypeFactoryTest {

  private static final String URN = "extension:io.substrait:test";
  private static final String NAME = "custom_type";

  @Test
  void detectsSubstraitUserDefinedType() {
    UserTypeFactory factory = new UserTypeFactory(URN, NAME);
    RelDataType substraitType =
        SubstraitUserDefinedType.from(
            Type.UserDefined.builder().nullable(true).urn(URN).name(NAME).build());

    assertTrue(factory.isTypeFromFactory(substraitType));
  }

  @Test
  void rejectsDifferentUrnOrName() {
    UserTypeFactory factory = new UserTypeFactory(URN, NAME);
    RelDataType differentType =
        SubstraitUserDefinedType.from(
            Type.UserDefined.builder().nullable(true).urn(URN).name("other").build());

    assertFalse(factory.isTypeFromFactory(differentType));
  }
}
