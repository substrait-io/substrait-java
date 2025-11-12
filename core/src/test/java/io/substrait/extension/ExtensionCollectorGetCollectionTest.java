package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ExtensionCollectorGetCollectionTest {

  @Test
  public void getExtensionCollection_containsOnlyTrackedTypes() {
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("https://example.com/catalog", "extension:example:catalog");

    SimpleExtension.Type catalogType1 =
        SimpleExtension.Type.of("extension:example:catalog", "type1");
    SimpleExtension.Type catalogType2 =
        SimpleExtension.Type.of("extension:example:catalog", "type2");
    SimpleExtension.Type catalogType3 =
        SimpleExtension.Type.of("extension:example:catalog", "type3");

    SimpleExtension.ExtensionCollection catalog =
        SimpleExtension.ExtensionCollection.builder()
            .addTypes(catalogType1, catalogType2, catalogType3)
            .uriUrnMap(uriUrnMap)
            .build();

    ExtensionCollector collector = new ExtensionCollector(catalog);

    collector.getTypeReference(catalogType1.getAnchor());
    collector.getTypeReference(catalogType2.getAnchor());

    SimpleExtension.ExtensionCollection result = collector.getExtensionCollection();

    assertEquals(2, result.types().size());
    assertEquals("type1", result.types().get(0).name());
    assertEquals("type2", result.types().get(1).name());
  }

  @Test
  public void getExtensionCollection_containsOnlyTrackedFunctions() {
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("https://example.com/catalog", "extension:example:catalog");

    SimpleExtension.ScalarFunctionVariant func1 =
        ImmutableSimpleExtension.ScalarFunctionVariant.builder()
            .urn("extension:example:catalog")
            .name("func1")
            .returnType(io.substrait.function.TypeExpressionCreator.REQUIRED.BOOLEAN)
            .build();

    SimpleExtension.ScalarFunctionVariant func2 =
        ImmutableSimpleExtension.ScalarFunctionVariant.builder()
            .urn("extension:example:catalog")
            .name("func2")
            .returnType(io.substrait.function.TypeExpressionCreator.REQUIRED.BOOLEAN)
            .build();

    SimpleExtension.ScalarFunctionVariant func3 =
        ImmutableSimpleExtension.ScalarFunctionVariant.builder()
            .urn("extension:example:catalog")
            .name("func3")
            .returnType(io.substrait.function.TypeExpressionCreator.REQUIRED.BOOLEAN)
            .build();

    SimpleExtension.ExtensionCollection catalog =
        SimpleExtension.ExtensionCollection.builder()
            .addScalarFunctions(func1, func2, func3)
            .uriUrnMap(uriUrnMap)
            .build();

    ExtensionCollector collector = new ExtensionCollector(catalog);

    collector.getFunctionReference(func1);
    collector.getFunctionReference(func3);

    SimpleExtension.ExtensionCollection result = collector.getExtensionCollection();

    assertEquals(2, result.scalarFunctions().size());
    assertEquals("func1", result.scalarFunctions().get(0).name());
    assertEquals("func3", result.scalarFunctions().get(1).name());
  }

  @Test
  public void getExtensionCollection_includesCustomTypes() {
    SimpleExtension.ExtensionCollection emptyCatalog =
        SimpleExtension.ExtensionCollection.builder().build();

    ExtensionCollector collector = new ExtensionCollector(emptyCatalog);

    SimpleExtension.TypeAnchor customType =
        SimpleExtension.TypeAnchor.of("extension:test:custom", "MyCustomType");

    collector.getTypeReference(customType);

    SimpleExtension.ExtensionCollection result = collector.getExtensionCollection();

    assertEquals(1, result.types().size());
    assertEquals("MyCustomType", result.types().get(0).name());
    assertEquals("extension:test:custom", result.types().get(0).urn());
  }

  @Test
  public void getExtensionCollection_includesOnlyUsedUriUrnMappings() {
    BidiMap<String, String> uriUrnMap = new BidiMap<>();
    uriUrnMap.put("https://example.com/urn1", "extension:example:urn1");
    uriUrnMap.put("https://example.com/urn2", "extension:example:urn2");
    uriUrnMap.put("https://example.com/urn3", "extension:example:urn3");

    SimpleExtension.Type type1 = SimpleExtension.Type.of("extension:example:urn1", "type1");
    SimpleExtension.Type type2 = SimpleExtension.Type.of("extension:example:urn2", "type2");
    SimpleExtension.Type type3 = SimpleExtension.Type.of("extension:example:urn3", "type3");

    SimpleExtension.ExtensionCollection catalog =
        SimpleExtension.ExtensionCollection.builder()
            .addTypes(type1, type2, type3)
            .uriUrnMap(uriUrnMap)
            .build();

    ExtensionCollector collector = new ExtensionCollector(catalog);

    collector.getTypeReference(type1.getAnchor());
    collector.getTypeReference(type3.getAnchor());

    SimpleExtension.ExtensionCollection result = collector.getExtensionCollection();

    assertEquals(2, result.uriUrnMap().forwardEntrySet().size());
    assertNotNull(result.getUriFromUrn("extension:example:urn1"));
    assertNotNull(result.getUriFromUrn("extension:example:urn3"));
    assertEquals("https://example.com/urn1", result.getUriFromUrn("extension:example:urn1"));
    assertEquals("https://example.com/urn3", result.getUriFromUrn("extension:example:urn3"));
  }

  @Test
  public void getExtensionCollection_emptyWhenNothingTracked() {
    SimpleExtension.ExtensionCollection catalog =
        SimpleExtension.ExtensionCollection.builder().build();

    ExtensionCollector collector = new ExtensionCollector(catalog);

    SimpleExtension.ExtensionCollection result = collector.getExtensionCollection();

    assertTrue(result.types().isEmpty());
    assertTrue(result.scalarFunctions().isEmpty());
    assertTrue(result.aggregateFunctions().isEmpty());
    assertTrue(result.windowFunctions().isEmpty());
  }

  @Test
  public void getExtensionCollection_throwsWhenFunctionNotInCatalog() {
    SimpleExtension.ExtensionCollection emptyCatalog =
        SimpleExtension.ExtensionCollection.builder().build();

    ExtensionCollector collector = new ExtensionCollector(emptyCatalog);

    SimpleExtension.ScalarFunctionVariant func =
        ImmutableSimpleExtension.ScalarFunctionVariant.builder()
            .urn("extension:missing:catalog")
            .name("missing_func")
            .returnType(io.substrait.function.TypeExpressionCreator.REQUIRED.BOOLEAN)
            .build();

    collector.getFunctionReference(func);

    IllegalArgumentException exception =
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class, () -> collector.getExtensionCollection());

    assertTrue(exception.getMessage().contains("extension:missing:catalog::missing_func"));
    assertTrue(exception.getMessage().contains("not found in catalog"));
  }
}
