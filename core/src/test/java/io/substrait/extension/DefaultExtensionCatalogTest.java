package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class DefaultExtensionCatalogTest {

  @Test
  void defaultCollectionLoads() {
    assertNotNull(DefaultExtensionCatalog.DEFAULT_COLLECTION);
  }
}
