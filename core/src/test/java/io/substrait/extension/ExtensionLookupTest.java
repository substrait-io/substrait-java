package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURN;
import org.junit.jupiter.api.Test;

class ExtensionLookupTest {

  @Test
  void functionReferencingMissingUrnAnchorThrows() {
    // function declaration references URN anchor 99, but no URN is registered at that anchor
    Plan plan =
        Plan.newBuilder()
            .addExtensions(
                SimpleExtensionDeclaration.newBuilder()
                    .setExtensionFunction(
                        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                            .setFunctionAnchor(1)
                            .setName("add:i32_i32")
                            .setExtensionUrnReference(99)))
            .build();

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> ImmutableExtensionLookup.builder().from(plan).build());
    assertTrue(e.getMessage().contains("no URN is registered at that anchor"));
  }

  @Test
  void typeReferencingMissingUrnAnchorThrows() {
    // type declaration references URN anchor 99, but no URN is registered at that anchor
    Plan plan =
        Plan.newBuilder()
            .addExtensions(
                SimpleExtensionDeclaration.newBuilder()
                    .setExtensionType(
                        SimpleExtensionDeclaration.ExtensionType.newBuilder()
                            .setTypeAnchor(1)
                            .setName("point")
                            .setExtensionUrnReference(99)))
            .build();

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> ImmutableExtensionLookup.builder().from(plan).build());
    assertTrue(e.getMessage().contains("no URN is registered at that anchor"));
  }

  @Test
  void lookupOfUnregisteredFunctionAnchorThrows() {
    // build a lookup with one function at anchor 1, then ask for anchor 999
    Plan plan =
        Plan.newBuilder()
            .addExtensionUrns(
                SimpleExtensionURN.newBuilder().setExtensionUrnAnchor(1).setUrn("extension:x:y"))
            .addExtensions(
                SimpleExtensionDeclaration.newBuilder()
                    .setExtensionFunction(
                        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                            .setFunctionAnchor(1)
                            .setName("add:i32_i32")
                            .setExtensionUrnReference(1)))
            .build();

    ExtensionLookup lookup = ImmutableExtensionLookup.builder().from(plan).build();
    SimpleExtension.ExtensionCollection extensions =
        SimpleExtension.ExtensionCollection.builder().build();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> lookup.getScalarFunction(999, extensions));
    assertTrue(e.getMessage().contains("Unknown function id"));
  }

  @Test
  void lookupOfUnregisteredTypeAnchorThrows() {
    Plan plan = Plan.newBuilder().build();

    ExtensionLookup lookup = ImmutableExtensionLookup.builder().from(plan).build();
    SimpleExtension.ExtensionCollection extensions =
        SimpleExtension.ExtensionCollection.builder().build();

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> lookup.getType(999, extensions));
    assertTrue(e.getMessage().contains("Unknown type id"));
  }
}
