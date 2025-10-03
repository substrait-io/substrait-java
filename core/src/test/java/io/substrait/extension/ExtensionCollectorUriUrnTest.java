package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.proto.Plan;
import org.junit.jupiter.api.Test;

public class ExtensionCollectorUriUrnTest {

  @Test
  public void testExtensionCollectorScalarFuncWithoutURI() {
    String uri = "test://uri";
    BidiMap<String, String> uriUrnMap = new BidiMap<String, String>();
    uriUrnMap.put(uri, "extension:test:basic");

    SimpleExtension.ExtensionCollection extensionCollection =
        SimpleExtension.ExtensionCollection.builder().uriUrnMap(uriUrnMap).build();

    ExtensionCollector collector = new ExtensionCollector(extensionCollection);

    SimpleExtension.ScalarFunctionVariant func =
        ImmutableSimpleExtension.ScalarFunctionVariant.builder()
            .urn("extension:test:basic")
            .name("test_func")
            .returnType(io.substrait.function.TypeExpressionCreator.REQUIRED.BOOLEAN)
            .build();

    int functionRef = collector.getFunctionReference(func);
    assertEquals(1, functionRef);

    Plan.Builder planBuilder = Plan.newBuilder();
    collector.addExtensionsToPlan(planBuilder);

    Plan plan = planBuilder.build();
    assertEquals(1, plan.getExtensionUrnsCount());
    assertEquals("extension:test:basic", plan.getExtensionUrns(0).getUrn());

    assertEquals(1, plan.getExtensionUrisCount());
    assertEquals("test://uri", plan.getExtensionUris(0).getUri());
  }
}
