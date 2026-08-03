package io.substrait.isthmus.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitTypeSystem;
import io.substrait.isthmus.calcite.SubstraitTable;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
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
   * A type factory that records the row types it is asked to build and the SQL types it is asked to
   * create, so a test can tell which factory produced a table's row type and which one the
   * validator derived its column types through.
   *
   * <p>Recording the requests is what makes the column-derivation check meaningful: {@code
   * RelDataTypeFactoryImpl.canonize} interns through a {@code static} cache shared by every factory
   * instance, so comparing interned type instances cannot distinguish one factory from another.
   */
  static final class RecordingTypeFactory extends SqlTypeFactoryImpl {

    private boolean createStructTypeCalled;
    private final List<SqlTypeName> requestedSqlTypeNames = new ArrayList<>();

    RecordingTypeFactory() {
      super(SubstraitTypeSystem.TYPE_SYSTEM);
    }

    @Override
    public RelDataType createStructType(
        final List<RelDataType> typeList, final List<String> fieldNameList) {
      createStructTypeCalled = true;
      return super.createStructType(typeList, fieldNameList);
    }

    @Override
    public RelDataType createSqlType(final SqlTypeName typeName) {
      requestedSqlTypeNames.add(typeName);
      return super.createSqlType(typeName);
    }

    @Override
    public RelDataType createSqlType(final SqlTypeName typeName, final int precision) {
      requestedSqlTypeNames.add(typeName);
      return super.createSqlType(typeName, precision);
    }

    @Override
    public RelDataType createSqlType(
        final SqlTypeName typeName, final int precision, final int scale) {
      requestedSqlTypeNames.add(typeName);
      return super.createSqlType(typeName, precision, scale);
    }

    boolean wasUsedForStructType() {
      return createStructTypeCalled;
    }

    List<SqlTypeName> requestedSqlTypeNames() {
      return List.copyOf(requestedSqlTypeNames);
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
   * The validator that derives the column types is built from the provider, so the declared column
   * types are requested from the provider's type factory — covering the validator path, not just
   * the enclosing struct type.
   */
  @Test
  void columnTypesAreDerivedWithProviderTypeFactory() throws SqlParseException {
    RecordingTypeFactory typeFactory = new RecordingTypeFactory();
    ConverterProvider provider = ConverterProvider.builder().typeFactory(typeFactory).build();

    SubstraitCreateStatementParser.processCreateStatementsToCatalog(provider, CREATE_STATEMENT);

    assertTrue(
        typeFactory
            .requestedSqlTypeNames()
            .containsAll(List.of(SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.DECIMAL)),
        "the validator should derive the declared column types through the provider's type "
            + "factory, but it only saw "
            + typeFactory.requestedSqlTypeNames());
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
