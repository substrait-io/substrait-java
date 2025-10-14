package io.substrait.isthmus;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import java.util.List;
import org.apache.calcite.prepare.Prepare;
import org.junit.jupiter.api.Test;

public class UdfSqlSubstraitTest extends PlanTestBase {

  private static final String CUSTOM_FUNCTION_PATH = "/extensions/scalar_functions_custom.yaml";

  UdfSqlSubstraitTest() {
    super(loadExtensions(List.of(CUSTOM_FUNCTION_PATH)));
  }

  @Test
  public void customUdfTest() throws Exception {

    final Prepare.CatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "CREATE TABLE t(x VARCHAR NOT NULL)");

    assertSqlSubstraitRelRoundTripWorkaroundOptimizer(
        "SELECT regexp_extract(x, 'ab') from t", catalogReader);
    assertSqlSubstraitRelRoundTripWorkaroundOptimizer(
        "SELECT format_text('UPPER', x) FROM t", catalogReader);
    assertSqlSubstraitRelRoundTripWorkaroundOptimizer(
        "SELECT system_property_get(x) FROM t", catalogReader);
    assertSqlSubstraitRelRoundTripWorkaroundOptimizer(
        "SELECT safe_divide(10,0) FROM t", catalogReader);
  }

  private static SimpleExtension.ExtensionCollection loadExtensions(
      List<String> yamlFunctionFiles) {
    SimpleExtension.ExtensionCollection extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION;
    if (yamlFunctionFiles != null && !yamlFunctionFiles.isEmpty()) {
      SimpleExtension.ExtensionCollection customExtensions =
          SimpleExtension.load(yamlFunctionFiles);
      extensions = extensions.merge(customExtensions);
    }
    return extensions;
  }
}
