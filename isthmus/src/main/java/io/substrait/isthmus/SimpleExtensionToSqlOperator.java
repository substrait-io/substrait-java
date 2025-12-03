package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ParameterizedTypeVisitor;
import io.substrait.function.TypeExpression;
import io.substrait.type.Type;
import io.substrait.type.TypeExpressionEvaluator;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

public final class SimpleExtensionToSqlOperator {

  private static final RelDataTypeFactory DEFAULT_TYPE_FACTORY =
      new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM);

  private SimpleExtensionToSqlOperator() {}

  public static List<SqlOperator> from(SimpleExtension.ExtensionCollection collection) {
    return from(collection, DEFAULT_TYPE_FACTORY);
  }

  public static List<SqlOperator> from(
      SimpleExtension.ExtensionCollection collection, RelDataTypeFactory typeFactory) {
    return from(collection, typeFactory, TypeConverter.DEFAULT);
  }

  public static List<SqlOperator> from(
      SimpleExtension.ExtensionCollection collection,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    // TODO: add support for windows functions
    return Stream.concat(
            collection.scalarFunctions().stream(), collection.aggregateFunctions().stream())
        .map(function -> toSqlFunction(function, typeFactory, typeConverter))
        .collect(Collectors.toList());
  }

  private static SqlFunction toSqlFunction(
      SimpleExtension.Function function,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {

    List<SqlTypeFamily> argFamilies = new ArrayList<>();

    for (SimpleExtension.Argument arg : function.requiredArguments()) {
      if (arg instanceof SimpleExtension.ValueArgument) {
        SimpleExtension.ValueArgument valueArg = (SimpleExtension.ValueArgument) arg;
        SqlTypeName typeName = valueArg.value().accept(new CalciteTypeVisitor());
        argFamilies.add(typeName.getFamily());
      } else if (arg instanceof SimpleExtension.EnumArgument) {
        // Treat an EnumArgument as a required string literal.
        argFamilies.add(SqlTypeFamily.STRING);
      }
    }

    SqlReturnTypeInference returnTypeInference =
        new SubstraitReturnTypeInference(function, typeFactory, typeConverter);

    return new SqlFunction(
        function.name(),
        SqlKind.OTHER_FUNCTION,
        returnTypeInference,
        null,
        OperandTypes.family(argFamilies),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  private static class SubstraitReturnTypeInference implements SqlReturnTypeInference {

    private final SimpleExtension.Function function;
    private final RelDataTypeFactory typeFactory;
    private final TypeConverter typeConverter;

    private SubstraitReturnTypeInference(
        SimpleExtension.Function function,
        RelDataTypeFactory typeFactory,
        TypeConverter typeConverter) {
      this.function = function;
      this.typeFactory = typeFactory;
      this.typeConverter = typeConverter;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      List<Type> substraitArgTypes =
          opBinding.collectOperandTypes().stream()
              .map(typeConverter::toSubstrait)
              .collect(Collectors.toList());

      TypeExpression returnExpression = function.returnType();
      Type resolvedSubstraitType =
          TypeExpressionEvaluator.evaluateExpression(
              returnExpression, function.args(), substraitArgTypes);

      boolean finalIsNullable;
      switch (function.nullability()) {
        case MIRROR:
          // If any input is nullable, the output is nullable.
          finalIsNullable =
              opBinding.collectOperandTypes().stream().anyMatch(RelDataType::isNullable);
          break;
        case DISCRETE:
        case DECLARED_OUTPUT:
        default:
          // Use the nullability declared on the resolved Substrait type.
          finalIsNullable = resolvedSubstraitType.nullable();
          break;
      }

      RelDataType baseCalciteType = typeConverter.toCalcite(typeFactory, resolvedSubstraitType);

      return typeFactory.createTypeWithNullability(baseCalciteType, finalIsNullable);
    }
  }

  private static class CalciteTypeVisitor
      extends ParameterizedTypeVisitor.ParameterizedTypeThrowsVisitor<
          SqlTypeName, RuntimeException> {

    private CalciteTypeVisitor() {
      super("Type not supported for Calcite conversion.");
    }

    @Override
    public SqlTypeName visit(Type.Bool expr) {
      return SqlTypeName.BOOLEAN;
    }

    @Override
    public SqlTypeName visit(Type.I8 expr) {
      return SqlTypeName.TINYINT;
    }

    @Override
    public SqlTypeName visit(Type.I16 expr) {
      return SqlTypeName.SMALLINT;
    }

    @Override
    public SqlTypeName visit(Type.I32 expr) {
      return SqlTypeName.INTEGER;
    }

    @Override
    public SqlTypeName visit(Type.I64 expr) {
      return SqlTypeName.BIGINT;
    }

    @Override
    public SqlTypeName visit(Type.FP32 expr) {
      return SqlTypeName.FLOAT;
    }

    @Override
    public SqlTypeName visit(Type.FP64 expr) {
      return SqlTypeName.DOUBLE;
    }

    @Override
    public SqlTypeName visit(Type.Str expr) {
      return SqlTypeName.VARCHAR;
    }

    @Override
    public SqlTypeName visit(Type.Binary expr) {
      return SqlTypeName.VARBINARY;
    }

    @Override
    public SqlTypeName visit(Type.Date expr) {
      return SqlTypeName.DATE;
    }

    @Override
    public SqlTypeName visit(Type.Time expr) {
      return SqlTypeName.TIME;
    }

    @Override
    public SqlTypeName visit(Type.TimestampTZ expr) {
      return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    @Override
    public SqlTypeName visit(Type.Timestamp expr) {
      return SqlTypeName.TIMESTAMP;
    }

    @Override
    public SqlTypeName visit(Type.IntervalYear year) {
      return SqlTypeName.INTERVAL_YEAR_MONTH;
    }

    @Override
    public SqlTypeName visit(Type.IntervalDay day) {
      return SqlTypeName.INTERVAL_DAY;
    }

    @Override
    public SqlTypeName visit(Type.UUID expr) {
      return SqlTypeName.UUID;
    }

    @Override
    public SqlTypeName visit(Type.Struct struct) {
      return SqlTypeName.ROW;
    }

    @Override
    public SqlTypeName visit(Type.ListType listType) {
      return SqlTypeName.ARRAY;
    }

    @Override
    public SqlTypeName visit(Type.Map map) {
      return SqlTypeName.MAP;
    }

    @Override
    public SqlTypeName visit(ParameterizedType.FixedChar expr) {
      return SqlTypeName.CHAR;
    }

    @Override
    public SqlTypeName visit(ParameterizedType.VarChar expr) {
      return SqlTypeName.VARCHAR;
    }

    @Override
    public SqlTypeName visit(ParameterizedType.FixedBinary expr) {
      return SqlTypeName.BINARY;
    }

    @Override
    public SqlTypeName visit(ParameterizedType.Decimal expr) {
      return SqlTypeName.DECIMAL;
    }

    @Override
    public SqlTypeName visit(ParameterizedType.Struct expr) {
      return SqlTypeName.ROW;
    }

    @Override
    public SqlTypeName visit(ParameterizedType.ListType expr) {
      return SqlTypeName.ARRAY;
    }

    @Override
    public SqlTypeName visit(ParameterizedType.Map expr) {
      return SqlTypeName.MAP;
    }

    @Override
    public SqlTypeName visit(ParameterizedType.PrecisionTimestamp expr) {
      return SqlTypeName.TIMESTAMP;
    }

    @Override
    public SqlTypeName visit(ParameterizedType.PrecisionTimestampTZ expr) {
      return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    @Override
    public SqlTypeName visit(ParameterizedType.PrecisionTime expr) {
      return SqlTypeName.TIME;
    }

    @Override
    public SqlTypeName visit(ParameterizedType.IntervalDay expr) {
      return SqlTypeName.INTERVAL_DAY;
    }

    @Override
    public SqlTypeName visit(ParameterizedType.StringLiteral expr) {
      String type = expr.value().toUpperCase();

      if (type.startsWith("ANY")) {
        return SqlTypeName.ANY;
      }

      switch (type) {
        case "BOOLEAN":
          return SqlTypeName.BOOLEAN;
        case "I8":
          return SqlTypeName.TINYINT;
        case "I16":
          return SqlTypeName.SMALLINT;
        case "I32":
          return SqlTypeName.INTEGER;
        case "I64":
          return SqlTypeName.BIGINT;
        case "FP32":
          return SqlTypeName.FLOAT;
        case "FP64":
          return SqlTypeName.DOUBLE;
        case "STRING":
          return SqlTypeName.VARCHAR;
        case "BINARY":
          return SqlTypeName.VARBINARY;
        case "TIMESTAMP":
          return SqlTypeName.TIMESTAMP;
        case "TIMESTAMP_TZ":
          return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        case "DATE":
          return SqlTypeName.DATE;
        case "TIME":
          return SqlTypeName.TIME;
        case "UUID":
          return SqlTypeName.UUID;
        default:
          if (type.startsWith("DECIMAL")) {
            return SqlTypeName.DECIMAL;
          }
          if (type.startsWith("STRUCT")) {
            return SqlTypeName.ROW;
          }
          if (type.startsWith("LIST")) {
            return SqlTypeName.ARRAY;
          }
          return super.visit(expr);
      }
    }
  }
}
