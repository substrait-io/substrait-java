package io.substrait.isthmus;

import io.substrait.consumercatalog.ConsumerCatalog;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

// TODO
// * Handle Composite Functions
// * Handle Reverse Mapping
// * Handle Variadic Functions
public class CatalogMapper {

  public static final ConsumerCatalog.Catalog DEFAULT_CATALOG;

  public Map<String, SqlOperator> toCalciteScalarFunction = new HashMap<>();

  static {
    InputStream catalogStream = CatalogMapper.class.getResourceAsStream("/calcite_catalog.yaml");
    DEFAULT_CATALOG = ConsumerCatalog.load(catalogStream);
  }

  CatalogMapper() {
    processCatalog(DEFAULT_CATALOG);
  }

  void processCatalog(ConsumerCatalog.Catalog catalog) {
    for (ConsumerCatalog.ExtensionMapping mapping : catalog.mappings()) {
      String uri = mapping.uri();
      for (var sf : mapping.scalarFunctionMappings()) {
        processScalarFunction(uri, sf);
      }
    }
  }

  public static String SQL_STANDARD_OPERATORS = "SqlStdOperatorTable";
  public static String SQL_LIBRARY_OPERATORS = "SqlLibraryOperators";

  void processScalarFunction(String uri, ConsumerCatalog.ScalarFunctionMapping sfm) {

    // Extract Operator
    Map<String, String> meta = sfm.meta();
    SqlOperator operator = null;
    if (meta.containsKey(SQL_STANDARD_OPERATORS)) {
      operator = getOperator(meta.get(SQL_STANDARD_OPERATORS), SqlStdOperatorTable.class);
    } else if (meta.containsKey(SQL_LIBRARY_OPERATORS)) {
      operator = getOperator(meta.get(SQL_LIBRARY_OPERATORS), SqlLibraryOperators.class);
    } else {
      throw new RuntimeException("Failed to map function");
    }


    // Add Scalar Functions to Map

    String functionName = sfm.name();
    for (ConsumerCatalog.FunctionVariant variant : sfm.variants()) {
      String typeSignature = variant.signature();
      String fullyQualifiedName = String.format("%s/%s:%s", uri, functionName, typeSignature);
      toCalciteScalarFunction.put(fullyQualifiedName, operator);
    }
  }

  SqlOperator getOperator(String fieldName, Object clazz) {
    try {
      Field f = SqlStdOperatorTable.class.getField(fieldName);
      Object o = f.get(SqlStdOperatorTable.class);
      if (o instanceof SqlOperator so) {
        return so;
      } else {
        throw new RuntimeException("Blah");
      }
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
