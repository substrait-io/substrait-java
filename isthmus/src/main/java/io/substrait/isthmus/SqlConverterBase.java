package io.substrait.isthmus;

import io.substrait.function.ImmutableSimpleExtension;
import io.substrait.function.SimpleExtension;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class SqlConverterBase {

  protected static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION;

  static {
    SimpleExtension.ExtensionCollection defaults =
        ImmutableSimpleExtension.ExtensionCollection.builder().build();
    try {
      defaults = SimpleExtension.loadDefaults();
    } catch (IOException e) {
      throw new RuntimeException("Failure while loading defaults.", e);
    }

    EXTENSION_COLLECTION = defaults;
  }

  protected List<DefinedTable> parseCreateTable(
      RelDataTypeFactory factory, SqlValidator validator, String sql) throws SqlParseException {
    SqlParser parser =
        SqlParser.create(sql, SqlParser.Config.DEFAULT.withParserFactory(SqlDdlParserImpl.FACTORY));
    List<DefinedTable> definedTableList = new ArrayList<>();

    SqlNodeList nodeList = parser.parseStmtList();
    for (SqlNode parsed : nodeList) {
      if (!(parsed instanceof SqlCreateTable)) {
        fail("Not a valid CREATE TABLE statement.");
      }

      SqlCreateTable create = (SqlCreateTable) parsed;
      if (create.name.names.size() > 1) {
        fail("Only simple table names are allowed.", create.name.getParserPosition());
      }

      if (create.query != null) {
        fail("CTAS not supported.", create.name.getParserPosition());
      }

      List<String> names = new ArrayList<>();
      List<RelDataType> columnTypes = new ArrayList<>();

      for (SqlNode node : create.columnList) {
        if (!(node instanceof SqlColumnDeclaration)) {
          fail("Unexpected column list construction.", node.getParserPosition());
        }

        SqlColumnDeclaration col = (SqlColumnDeclaration) node;
        if (col.name.names.size() != 1) {
          fail("Expected simple column names.", col.name.getParserPosition());
        }

        names.add(col.name.names.get(0));
        columnTypes.add(col.dataType.deriveType(validator));
      }

      definedTableList.add(
          new DefinedTable(
              create.name.names.get(0), factory, factory.createStructType(columnTypes, names)));
    }

    return definedTableList;
  }

  protected static SqlParseException fail(String text, SqlParserPos pos) {
    return new SqlParseException(text, pos, null, null, new RuntimeException("fake lineage"));
  }

  protected static SqlParseException fail(String text) {
    return fail(text, SqlParserPos.ZERO);
  }

  protected static final class Validator extends SqlValidatorImpl {

    private Validator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        Config config) {
      super(opTab, catalogReader, typeFactory, config);
    }

    public static Validator create(
        RelDataTypeFactory factory, CalciteCatalogReader catalog, SqlValidator.Config config) {
      return new Validator(SqlStdOperatorTable.instance(), catalog, factory, config);
    }
  }

  /** A fully defined pre-specified table. */
  protected static final class DefinedTable extends AbstractTable {

    private final String name;
    private final RelDataTypeFactory factory;
    private final RelDataType type;

    public DefinedTable(String name, RelDataTypeFactory factory, RelDataType type) {
      this.name = name;
      this.factory = factory;
      this.type = type;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      if (factory != typeFactory) {
        throw new IllegalStateException("Different type factory than previously used.");
      }
      return type;
    }

    public String getName() {
      return name;
    }
  }
}
