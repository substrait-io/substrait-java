package io.substrait.isthmus.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitTypeSystem;
import io.substrait.isthmus.calcite.SubstraitTable;
import java.util.List;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the CREATE-statement parser sources its type factory and connection configuration
 * from the injected {@link ConverterProvider} rather than from global defaults, for both the
 * catalog-reader and the {@link SubstraitTable} entry points.
 */
class CreateStatementParserProviderConfigTest {

  private static final String CREATE_STATEMENT =
      "CREATE TABLE employees (id BIGINT, name VARCHAR, salary DECIMAL(10, 2))";

  /**
   * A type factory that records the struct types it is asked to build, so a test can tell which
   * factory produced a table's row type and where its column types came from.
   */
  static final class RecordingTypeFactory extends SqlTypeFactoryImpl {

    private boolean createStructTypeCalled;
    private List<RelDataType> recordedFieldTypes = List.of();

    RecordingTypeFactory() {
      super(SubstraitTypeSystem.TYPE_SYSTEM);
    }

    @Override
    public RelDataType createStructType(
        final List<RelDataType> typeList, final List<String> fieldNameList) {
      createStructTypeCalled = true;
      recordedFieldTypes = List.copyOf(typeList);
      return super.createStructType(typeList, fieldNameList);
    }

    boolean wasUsedForStructType() {
      return createStructTypeCalled;
    }

    List<RelDataType> recordedFieldTypes() {
      return recordedFieldTypes;
    }

    /** Exposes the protected interning hook so a test can check a type belongs to this factory. */
    RelDataType canonizeType(final RelDataType type) {
      return canonize(type);
    }
  }

  @Test
  void catalogPathUsesProviderTypeFactory() throws SqlParseException {
    RecordingTypeFactory typeFactory = new RecordingTypeFactory();
    ConverterProvider provider = ConverterProvider.builder().typeFactory(typeFactory).build();

    CalciteCatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(provider, CREATE_STATEMENT);

    assertSame(typeFactory, catalog.getTypeFactory());
    assertTrue(
        typeFactory.wasUsedForStructType(),
        "the provider's type factory should build the table row type");
  }

  @Test
  void tableListPathUsesProviderTypeFactory() throws SqlParseException {
    RecordingTypeFactory typeFactory = new RecordingTypeFactory();
    ConverterProvider provider = ConverterProvider.builder().typeFactory(typeFactory).build();

    List<SubstraitTable> tables =
        SubstraitCreateStatementParser.processCreateStatements(provider, CREATE_STATEMENT);

    assertEquals(1, tables.size());
    assertTrue(
        typeFactory.wasUsedForStructType(),
        "the provider's type factory should build the table row type");
  }

  /**
   * The column types are derived through a validator built from the provider, so they are interned
   * in the provider's type factory too — not just the enclosing struct type.
   */
  @Test
  void columnTypesAreDerivedWithProviderTypeFactory() throws SqlParseException {
    RecordingTypeFactory typeFactory = new RecordingTypeFactory();
    ConverterProvider provider = ConverterProvider.builder().typeFactory(typeFactory).build();

    SubstraitCreateStatementParser.processCreateStatementsToCatalog(provider, CREATE_STATEMENT);

    List<RelDataType> fieldTypes = typeFactory.recordedFieldTypes();
    assertEquals(3, fieldTypes.size());
    for (RelDataType fieldType : fieldTypes) {
      assertSame(
          fieldType,
          typeFactory.canonizeType(fieldType),
          "column types should be interned in the provider's type factory");
    }
  }

  /** The default entry points keep working off {@link ConverterProvider#DEFAULT}. */
  @Test
  void defaultPathStillUsesSystemDefaults() throws SqlParseException {
    CalciteCatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATE_STATEMENT);

    assertSame(SubstraitTypeSystem.TYPE_FACTORY, catalog.getTypeFactory());
    assertFalse(catalog.nameMatcher().isCaseSensitive(), "default config is case-insensitive");
  }
}
