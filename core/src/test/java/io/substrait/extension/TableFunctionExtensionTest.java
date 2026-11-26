package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.TestBase;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class TableFunctionExtensionTest {

  private static final TypeCreator R = TypeCreator.REQUIRED;

  private static final String URN = "extension:test:table_function_extensions";

  private static final SimpleExtension.ExtensionCollection EXTENSIONS;

  static {
    try {
      EXTENSIONS =
          SimpleExtension.load(TestBase.asString("extensions/table_function_extensions.yaml"));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static SimpleExtension.TableFunctionVariant tableFunction(
      SimpleExtension.ExtensionCollection extensions, String urn, String key) {
    return extensions.getTableFunction(SimpleExtension.FunctionAnchor.of(urn, key));
  }

  @Test
  void defaultCatalogDeclaresGenerateSeries() {
    SimpleExtension.TableFunctionVariant i64 =
        tableFunction(
            DefaultExtensionCatalog.DEFAULT_COLLECTION,
            DefaultExtensionCatalog.FUNCTIONS_TABLE,
            "generate_series:i64_i64_i64");
    SimpleExtension.TableFunctionVariant i32 =
        tableFunction(
            DefaultExtensionCatalog.DEFAULT_COLLECTION,
            DefaultExtensionCatalog.FUNCTIONS_TABLE,
            "generate_series:i32_i32_i32");

    assertEquals(R.struct(R.I64), i64.resolveType(List.of(R.I64, R.I64, R.I64)));
    assertEquals(R.struct(R.I32), i32.resolveType(List.of(R.I32, R.I32, R.I32)));
  }

  @Test
  void nullableArgumentsDoNotMakeTheSchemaNullable() {
    SimpleExtension.TableFunctionVariant series =
        tableFunction(
            DefaultExtensionCatalog.DEFAULT_COLLECTION,
            DefaultExtensionCatalog.FUNCTIONS_TABLE,
            "generate_series:i64_i64_i64");

    assertEquals(SimpleExtension.Nullability.DECLARED_OUTPUT, series.nullability());
    TypeCreator n = TypeCreator.NULLABLE;
    assertEquals(R.struct(R.I64), series.resolveType(List.of(n.I64, n.I64, n.I64)));
  }

  @Test
  void schemaDerivedFromTheArguments() {
    SimpleExtension.TableFunctionVariant unnest =
        tableFunction(EXTENSIONS, URN, "unnest_list:list");

    assertEquals(R.struct(R.STRING), unnest.resolveType(List.of(R.list(R.STRING))));
  }

  @Test
  void schemaLeftToTheProducerCannotBeResolved() {
    SimpleExtension.TableFunctionVariant readJson = tableFunction(EXTENSIONS, URN, "read_json:str");

    assertNull(readJson.returnType());
    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class, () -> readJson.resolveType(List.of(R.STRING)));
    assertTrue(e.getMessage().contains("read_json:str"), e.getMessage());
  }

  @Test
  void tableFunctionsAreNotScalarFunctions() {
    SimpleExtension.FunctionAnchor anchor = SimpleExtension.FunctionAnchor.of(URN, "read_json:str");

    assertThrows(IllegalArgumentException.class, () -> EXTENSIONS.getScalarFunction(anchor));
  }

  @Test
  void mergeKeepsTableFunctions() {
    SimpleExtension.ExtensionCollection merged =
        DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(EXTENSIONS);

    assertTrue(merged.containsUrn(URN));
    assertEquals(URN, tableFunction(merged, URN, "unnest_list:list").urn());
  }
}
