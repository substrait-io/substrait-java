package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
import io.substrait.proto.SimpleExtensionURN;
import org.junit.jupiter.api.Test;

public class ImmutableExtensionLookupUriUrnTest {

  @Test
  public void testUrnResolutionWorks() {
    // Create URN-only plan (normal case)
    SimpleExtensionURN urnProto =
        SimpleExtensionURN.newBuilder()
            .setExtensionUrnAnchor(1)
            .setUrn("extension:test:urn")
            .build();

    SimpleExtensionDeclaration.ExtensionFunction func =
        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
            .setFunctionAnchor(1)
            .setName("test_func")
            .setExtensionUrnReference(1)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionFunction(func).build();

    Plan plan = Plan.newBuilder().addExtensionUrns(urnProto).addExtensions(decl).build();

    // Test with no ExtensionCollection (no URI/URN mapping available)
    ImmutableExtensionLookup lookup = ImmutableExtensionLookup.builder().from(plan).build();

    assertEquals("extension:test:urn", lookup.functionAnchorMap.get(1).urn());
    assertEquals("test_func", lookup.functionAnchorMap.get(1).key());
  }

  @Test
  public void testUriToUrnFallbackWorks() {
    // Create an ExtensionCollection with URI/URN mapping
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("http://example.com/extensions/test", "extension:test:mapped");

    SimpleExtension.ExtensionCollection extensionCollection =
        SimpleExtension.ExtensionCollection.builder().uriUrnMap(uriUrnMap).build();

    // Create URI-only plan (legacy case)
    SimpleExtensionURI uriProto =
        SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(1)
            .setUri("http://example.com/extensions/test")
            .build();

    SimpleExtensionDeclaration.ExtensionFunction func =
        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
            .setFunctionAnchor(1)
            .setName("legacy_func")
            .setExtensionUriReference(1) // References the URI anchor (deprecated field)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionFunction(func).build();

    Plan plan = Plan.newBuilder().addExtensionUris(uriProto).addExtensions(decl).build();

    // Test with URI/URN mapping - should resolve URI to URN
    ImmutableExtensionLookup lookup =
        ImmutableExtensionLookup.builder(extensionCollection).from(plan).build();

    assertEquals("extension:test:mapped", lookup.functionAnchorMap.get(1).urn());
    assertEquals("legacy_func", lookup.functionAnchorMap.get(1).key());
  }

  @Test
  public void testUriWithoutMappingThrowsError() {
    // Create URI-only plan without mapping
    SimpleExtensionURI uriProto =
        SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(1)
            .setUri("http://example.com/unmapped")
            .build();

    SimpleExtensionDeclaration.ExtensionFunction func =
        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
            .setFunctionAnchor(1)
            .setName("unmapped_func")
            .setExtensionUriReference(1) // References the URI anchor
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionFunction(func).build();

    Plan plan = Plan.newBuilder().addExtensionUris(uriProto).addExtensions(decl).build();

    // Should throw error - URI present but no mapping available
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> {
              ImmutableExtensionLookup.builder().from(plan).build();
            });

    assertTrue(exception.getMessage().contains("could not be resolved to a URN"));
    assertTrue(exception.getMessage().contains("http://example.com/unmapped"));
    assertTrue(exception.getMessage().contains("URI <-> URN mapping"));
  }

  @Test
  public void testMissingUrnAndUriThrowsError() {
    // Create plan with missing URN/URI reference
    SimpleExtensionDeclaration.ExtensionFunction func =
        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
            .setFunctionAnchor(1)
            .setName("missing_func")
            .setExtensionUrnReference(999) // Non-existent reference
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionFunction(func).build();

    Plan plan = Plan.newBuilder().addExtensions(decl).build();

    // Should throw error - neither URN nor URI found
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> {
              ImmutableExtensionLookup.builder().from(plan).build();
            });

    assertTrue(exception.getMessage().contains("no URN is registered at that anchor"));
    assertTrue(exception.getMessage().contains("999")); // The missing anchor reference
  }

  // ==========================================================================
  // Simple tests for all 5 resolution cases - Functions
  // ==========================================================================

  @Test
  public void testFunctionCase1_NonZeroUrnReference() {
    // Case 1: Non-zero URN reference resolves
    SimpleExtensionURN urnProto =
        SimpleExtensionURN.newBuilder()
            .setExtensionUrnAnchor(1)
            .setUrn("extension:test:case1")
            .build();

    SimpleExtensionDeclaration.ExtensionFunction func =
        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
            .setFunctionAnchor(1)
            .setName("case1_func")
            .setExtensionUrnReference(1)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionFunction(func).build();

    Plan plan = Plan.newBuilder().addExtensionUrns(urnProto).addExtensions(decl).build();

    ImmutableExtensionLookup lookup = ImmutableExtensionLookup.builder().from(plan).build();

    assertEquals("extension:test:case1", lookup.functionAnchorMap.get(1).urn());
    assertEquals("case1_func", lookup.functionAnchorMap.get(1).key());
  }

  @Test
  public void testFunctionCase2_NonZeroUriReference() {
    // Case 2: Non-zero URI reference resolves via mapping
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("http://example.com/case2", "extension:test:case2");

    SimpleExtension.ExtensionCollection extensionCollection =
        SimpleExtension.ExtensionCollection.builder().uriUrnMap(uriUrnMap).build();

    SimpleExtensionURI uriProto =
        SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(1)
            .setUri("http://example.com/case2")
            .build();

    SimpleExtensionDeclaration.ExtensionFunction func =
        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
            .setFunctionAnchor(1)
            .setName("case2_func")
            .setExtensionUriReference(1)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionFunction(func).build();

    Plan plan = Plan.newBuilder().addExtensionUris(uriProto).addExtensions(decl).build();

    ImmutableExtensionLookup lookup =
        ImmutableExtensionLookup.builder(extensionCollection).from(plan).build();

    assertEquals("extension:test:case2", lookup.functionAnchorMap.get(1).urn());
    assertEquals("case2_func", lookup.functionAnchorMap.get(1).key());
  }

  @Test
  public void testFunctionCase3_ZeroBothResolveConsistent() {
    // Case 3: Both 0 references resolve to consistent URN
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("http://example.com/case3", "extension:test:case3");

    SimpleExtension.ExtensionCollection extensionCollection =
        SimpleExtension.ExtensionCollection.builder().uriUrnMap(uriUrnMap).build();

    SimpleExtensionURN urnProto =
        SimpleExtensionURN.newBuilder()
            .setExtensionUrnAnchor(0)
            .setUrn("extension:test:case3")
            .build();

    SimpleExtensionURI uriProto =
        SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(0)
            .setUri("http://example.com/case3")
            .build();

    SimpleExtensionDeclaration.ExtensionFunction func =
        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
            .setFunctionAnchor(1)
            .setName("case3_func")
            .setExtensionUrnReference(0)
            .setExtensionUriReference(0)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionFunction(func).build();

    Plan plan =
        Plan.newBuilder()
            .addExtensionUrns(urnProto)
            .addExtensionUris(uriProto)
            .addExtensions(decl)
            .build();

    ImmutableExtensionLookup lookup =
        ImmutableExtensionLookup.builder(extensionCollection).from(plan).build();

    assertEquals("extension:test:case3", lookup.functionAnchorMap.get(1).urn());
    assertEquals("case3_func", lookup.functionAnchorMap.get(1).key());
  }

  @Test
  public void testFunctionCase3_ZeroBothResolveConflict() {
    // Case 3: Both 0 references resolve but to different URNs - should throw
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("http://example.com/conflict", "extension:test:different");

    SimpleExtension.ExtensionCollection extensionCollection =
        SimpleExtension.ExtensionCollection.builder().uriUrnMap(uriUrnMap).build();

    SimpleExtensionURN urnProto =
        SimpleExtensionURN.newBuilder()
            .setExtensionUrnAnchor(0)
            .setUrn("extension:test:original")
            .build();

    SimpleExtensionURI uriProto =
        SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(0)
            .setUri("http://example.com/conflict")
            .build();

    SimpleExtensionDeclaration.ExtensionFunction func =
        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
            .setFunctionAnchor(1)
            .setName("conflict_func")
            .setExtensionUrnReference(0)
            .setExtensionUriReference(0)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionFunction(func).build();

    Plan plan =
        Plan.newBuilder()
            .addExtensionUrns(urnProto)
            .addExtensionUris(uriProto)
            .addExtensions(decl)
            .build();

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> {
              ImmutableExtensionLookup.builder(extensionCollection).from(plan).build();
            });

    assertTrue(exception.getMessage().contains("Conflicting URI/URN mapping"));
    assertTrue(exception.getMessage().contains("These must be consistent"));
  }

  @Test
  public void testFunctionCase4_ZeroUrnOnly() {
    // Case 4: Only 0 URN reference resolves
    SimpleExtensionURN urnProto =
        SimpleExtensionURN.newBuilder()
            .setExtensionUrnAnchor(0)
            .setUrn("extension:test:case4")
            .build();

    SimpleExtensionDeclaration.ExtensionFunction func =
        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
            .setFunctionAnchor(1)
            .setName("case4_func")
            .setExtensionUrnReference(0)
            .setExtensionUriReference(0)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionFunction(func).build();

    Plan plan = Plan.newBuilder().addExtensionUrns(urnProto).addExtensions(decl).build();

    ImmutableExtensionLookup lookup = ImmutableExtensionLookup.builder().from(plan).build();

    assertEquals("extension:test:case4", lookup.functionAnchorMap.get(1).urn());
    assertEquals("case4_func", lookup.functionAnchorMap.get(1).key());
  }

  @Test
  public void testFunctionCase5_ZeroUriOnly() {
    // Case 5: Only 0 URI reference resolves
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("http://example.com/case5", "extension:test:case5");

    SimpleExtension.ExtensionCollection extensionCollection =
        SimpleExtension.ExtensionCollection.builder().uriUrnMap(uriUrnMap).build();

    SimpleExtensionURI uriProto =
        SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(0)
            .setUri("http://example.com/case5")
            .build();

    SimpleExtensionDeclaration.ExtensionFunction func =
        SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
            .setFunctionAnchor(1)
            .setName("case5_func")
            .setExtensionUrnReference(0)
            .setExtensionUriReference(0)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionFunction(func).build();

    Plan plan = Plan.newBuilder().addExtensionUris(uriProto).addExtensions(decl).build();

    ImmutableExtensionLookup lookup =
        ImmutableExtensionLookup.builder(extensionCollection).from(plan).build();

    assertEquals("extension:test:case5", lookup.functionAnchorMap.get(1).urn());
    assertEquals("case5_func", lookup.functionAnchorMap.get(1).key());
  }

  // ==========================================================================
  // Simple tests for all 5 resolution cases - Types
  // ==========================================================================

  @Test
  public void testTypeCase1_NonZeroUrnReference() {
    // Case 1: Non-zero URN reference resolves
    SimpleExtensionURN urnProto =
        SimpleExtensionURN.newBuilder()
            .setExtensionUrnAnchor(1)
            .setUrn("extension:test:case1")
            .build();

    SimpleExtensionDeclaration.ExtensionType type =
        SimpleExtensionDeclaration.ExtensionType.newBuilder()
            .setTypeAnchor(1)
            .setName("case1_type")
            .setExtensionUrnReference(1)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionType(type).build();

    Plan plan = Plan.newBuilder().addExtensionUrns(urnProto).addExtensions(decl).build();

    ImmutableExtensionLookup lookup = ImmutableExtensionLookup.builder().from(plan).build();

    assertEquals("extension:test:case1", lookup.typeAnchorMap.get(1).urn());
    assertEquals("case1_type", lookup.typeAnchorMap.get(1).key());
  }

  @Test
  public void testTypeCase2_NonZeroUriReference() {
    // Case 2: Non-zero URI reference resolves via mapping
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("http://example.com/case2", "extension:test:case2");

    SimpleExtension.ExtensionCollection extensionCollection =
        SimpleExtension.ExtensionCollection.builder().uriUrnMap(uriUrnMap).build();

    SimpleExtensionURI uriProto =
        SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(1)
            .setUri("http://example.com/case2")
            .build();

    SimpleExtensionDeclaration.ExtensionType type =
        SimpleExtensionDeclaration.ExtensionType.newBuilder()
            .setTypeAnchor(1)
            .setName("case2_type")
            .setExtensionUriReference(1)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionType(type).build();

    Plan plan = Plan.newBuilder().addExtensionUris(uriProto).addExtensions(decl).build();

    ImmutableExtensionLookup lookup =
        ImmutableExtensionLookup.builder(extensionCollection).from(plan).build();

    assertEquals("extension:test:case2", lookup.typeAnchorMap.get(1).urn());
    assertEquals("case2_type", lookup.typeAnchorMap.get(1).key());
  }

  @Test
  public void testTypeCase3_ZeroBothResolveConsistent() {
    // Case 3: Both 0 references resolve to consistent URN
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("http://example.com/case3", "extension:test:case3");

    SimpleExtension.ExtensionCollection extensionCollection =
        SimpleExtension.ExtensionCollection.builder().uriUrnMap(uriUrnMap).build();

    SimpleExtensionURN urnProto =
        SimpleExtensionURN.newBuilder()
            .setExtensionUrnAnchor(0)
            .setUrn("extension:test:case3")
            .build();

    SimpleExtensionURI uriProto =
        SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(0)
            .setUri("http://example.com/case3")
            .build();

    SimpleExtensionDeclaration.ExtensionType type =
        SimpleExtensionDeclaration.ExtensionType.newBuilder()
            .setTypeAnchor(1)
            .setName("case3_type")
            .setExtensionUrnReference(0)
            .setExtensionUriReference(0)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionType(type).build();

    Plan plan =
        Plan.newBuilder()
            .addExtensionUrns(urnProto)
            .addExtensionUris(uriProto)
            .addExtensions(decl)
            .build();

    ImmutableExtensionLookup lookup =
        ImmutableExtensionLookup.builder(extensionCollection).from(plan).build();

    assertEquals("extension:test:case3", lookup.typeAnchorMap.get(1).urn());
    assertEquals("case3_type", lookup.typeAnchorMap.get(1).key());
  }

  @Test
  public void testTypeCase3_ZeroBothResolveConflict() {
    // Case 3: Both 0 references resolve but to different URNs - should throw
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("http://example.com/conflict", "extension:test:different");

    SimpleExtension.ExtensionCollection extensionCollection =
        SimpleExtension.ExtensionCollection.builder().uriUrnMap(uriUrnMap).build();

    SimpleExtensionURN urnProto =
        SimpleExtensionURN.newBuilder()
            .setExtensionUrnAnchor(0)
            .setUrn("extension:test:original")
            .build();

    SimpleExtensionURI uriProto =
        SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(0)
            .setUri("http://example.com/conflict")
            .build();

    SimpleExtensionDeclaration.ExtensionType type =
        SimpleExtensionDeclaration.ExtensionType.newBuilder()
            .setTypeAnchor(1)
            .setName("conflict_type")
            .setExtensionUrnReference(0)
            .setExtensionUriReference(0)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionType(type).build();

    Plan plan =
        Plan.newBuilder()
            .addExtensionUrns(urnProto)
            .addExtensionUris(uriProto)
            .addExtensions(decl)
            .build();

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> {
              ImmutableExtensionLookup.builder(extensionCollection).from(plan).build();
            });

    assertTrue(exception.getMessage().contains("Conflicting URI/URN mapping"));
    assertTrue(exception.getMessage().contains("These must be consistent"));
  }

  @Test
  public void testTypeCase4_ZeroUrnOnly() {
    // Case 4: Only 0 URN reference resolves
    SimpleExtensionURN urnProto =
        SimpleExtensionURN.newBuilder()
            .setExtensionUrnAnchor(0)
            .setUrn("extension:test:case4")
            .build();

    SimpleExtensionDeclaration.ExtensionType type =
        SimpleExtensionDeclaration.ExtensionType.newBuilder()
            .setTypeAnchor(1)
            .setName("case4_type")
            .setExtensionUrnReference(0)
            .setExtensionUriReference(0)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionType(type).build();

    Plan plan = Plan.newBuilder().addExtensionUrns(urnProto).addExtensions(decl).build();

    ImmutableExtensionLookup lookup = ImmutableExtensionLookup.builder().from(plan).build();

    assertEquals("extension:test:case4", lookup.typeAnchorMap.get(1).urn());
    assertEquals("case4_type", lookup.typeAnchorMap.get(1).key());
  }

  @Test
  public void testTypeCase5_ZeroUriOnly() {
    // Case 5: Only 0 URI reference resolves
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("http://example.com/case5", "extension:test:case5");

    SimpleExtension.ExtensionCollection extensionCollection =
        SimpleExtension.ExtensionCollection.builder().uriUrnMap(uriUrnMap).build();

    SimpleExtensionURI uriProto =
        SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(0)
            .setUri("http://example.com/case5")
            .build();

    SimpleExtensionDeclaration.ExtensionType type =
        SimpleExtensionDeclaration.ExtensionType.newBuilder()
            .setTypeAnchor(1)
            .setName("case5_type")
            .setExtensionUrnReference(0)
            .setExtensionUriReference(0)
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionType(type).build();

    Plan plan = Plan.newBuilder().addExtensionUris(uriProto).addExtensions(decl).build();

    ImmutableExtensionLookup lookup =
        ImmutableExtensionLookup.builder(extensionCollection).from(plan).build();

    assertEquals("extension:test:case5", lookup.typeAnchorMap.get(1).urn());
    assertEquals("case5_type", lookup.typeAnchorMap.get(1).key());
  }

  @Test
  public void testTypeUriToUrnFallbackWorks() {
    // Test the same logic but for types instead of functions
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("http://example.com/types/test", "extension:types:mapped");

    SimpleExtension.ExtensionCollection extensionCollection =
        SimpleExtension.ExtensionCollection.builder().uriUrnMap(uriUrnMap).build();

    SimpleExtensionURI uriProto =
        SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(1)
            .setUri("http://example.com/types/test")
            .build();

    SimpleExtensionDeclaration.ExtensionType type =
        SimpleExtensionDeclaration.ExtensionType.newBuilder()
            .setTypeAnchor(1)
            .setName("legacy_type")
            .setExtensionUriReference(1) // References the URI anchor
            .build();

    SimpleExtensionDeclaration decl =
        SimpleExtensionDeclaration.newBuilder().setExtensionType(type).build();

    Plan plan = Plan.newBuilder().addExtensionUris(uriProto).addExtensions(decl).build();

    ImmutableExtensionLookup lookup =
        ImmutableExtensionLookup.builder(extensionCollection).from(plan).build();

    assertEquals("extension:types:mapped", lookup.typeAnchorMap.get(1).urn());
    assertEquals("legacy_type", lookup.typeAnchorMap.get(1).key());
  }
}
