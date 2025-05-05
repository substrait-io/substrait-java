package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.substrait.consumercatalog.ConsumerCatalog;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class CalciteCatalogTest {

  @Test
  void loadCatalog() throws IOException {
    String catalog =
        Resources.toString(Resources.getResource("calcite_catalog.yaml"), Charsets.UTF_8);
    ConsumerCatalog.Catalog calciteCatalog = ConsumerCatalog.load(catalog);
    assertNotNull(calciteCatalog);
  }

  @Test
  void loadMappings() throws IOException {
    var cm = new CatalogMapper();
    assertNotNull(cm);
  }
}
