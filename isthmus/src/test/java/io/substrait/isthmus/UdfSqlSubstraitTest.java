package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import java.util.List;
import org.apache.calcite.prepare.Prepare;
import org.junit.jupiter.api.Test;

public class UdfSqlSubstraitTest extends PlanTestBase {

  private static final String CUSTOM_FUNCTION_PATH = "/extensions/functions_string_custom.yaml";

  UdfSqlSubstraitTest() {
    super(loadExtensions(List.of(CUSTOM_FUNCTION_PATH)));
  }

  @Test
  public void customUdfTest() throws Exception {

    final String[] sql = {
      "CREATE TABLE t(x VARCHAR NOT NULL)", "SELECT regexp_extract(x, 'ab') from t"
    };

    final Prepare.CatalogReader catalogReader =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(sql[0]);

    assertSqlSubstraitRelRoundTripWorkaroundOptimizer(sql[1], catalogReader);
  }

  private static SimpleExtension.ExtensionCollection loadExtensions(
      List<String> yamlFunctionFiles) {
    SimpleExtension.ExtensionCollection extensions = SimpleExtension.loadDefaults();
    if (yamlFunctionFiles != null && !yamlFunctionFiles.isEmpty()) {
      SimpleExtension.ExtensionCollection customExtensions =
          SimpleExtension.load(yamlFunctionFiles);
      extensions = extensions.merge(customExtensions);
    }
    return extensions;
  }
}
