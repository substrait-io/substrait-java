package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.proto.Plan;
import org.junit.jupiter.api.Test;

class ExtensionCollectorUriUrnTest {

  @Test
  void testExtensionCollectorScalarFuncWithoutURI() {
    final String uri = "test://uri";
    final BidiMap<String, String> uriUrnMap = new BidiMap<String, String>();
    uriUrnMap.put(uri, "extension:test:basic");

    final SimpleExtension.ExtensionCollection extensionCollection =
        SimpleExtension.ExtensionCollection.builder().uriUrnMap(uriUrnMap).build();

    final ExtensionCollector collector = new ExtensionCollector(extensionCollection);

    final SimpleExtension.ScalarFunctionVariant func =
        ImmutableSimpleExtension.ScalarFunctionVariant.builder()
            .urn("extension:test:basic")
            .name("test_func")
            .returnType(io.substrait.function.TypeExpressionCreator.REQUIRED.BOOLEAN)
            .build();

    final int functionRef = collector.getFunctionReference(func);
    assertEquals(1, functionRef);

    final Plan.Builder planBuilder = Plan.newBuilder();
    collector.addExtensionsToPlan(planBuilder);

    final Plan plan = planBuilder.build();
    assertEquals(1, plan.getExtensionUrnsCount());
    assertEquals("extension:test:basic", plan.getExtensionUrns(0).getUrn());

    assertEquals(1, plan.getExtensionUrisCount());
    assertEquals("test://uri", plan.getExtensionUris(0).getUri());
  }
}
