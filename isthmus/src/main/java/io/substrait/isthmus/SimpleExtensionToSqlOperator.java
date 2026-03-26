package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ParameterizedTypeVisitor;
import io.substrait.function.TypeExpression;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for converting Substrait {@link SimpleExtension} function definitions (scalar and
 * aggregate) into Calcite {@link SqlOperator}s.
 *
 * <p>This enables Calcite to recognize and use Substrait-defined functions during query planning
 * and execution. Conversion includes:
 *
 * <ul>
 *   <li>Mapping Substrait types to Calcite {@link SqlTypeName}
 *   <li>Building {@link SqlFunction} instances with proper argument families
 *   <li>Inferring return types based on Substrait type expressions and nullability rules
 * </ul>
 *
 * <p>Currently supports scalar and aggregate functions; window functions are not yet implemented.
 */
public final class SimpleExtensionToSqlOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleExtensionToSqlOperator.class);

  private static final RelDataTypeFactory DEFAULT_TYPE_FACTORY =
      new JavaTypeFactoryImpl(SubstraitTypeSystem.TYPE_SYSTEM);

  private static final CalciteTypeVisitor CALCITE_TYPE_VISITOR = new CalciteTypeVisitor();

  private SimpleExtensionToSqlOperator() {}

  /**
   * Converts all functions in a Substrait {@link SimpleExtension.ExtensionCollection} (scalar and
   * aggregate) into Calcite {@link SqlOperator}s using the default type factory.
   *
   * @param collection The Substrait extension collection containing function definitions.
   * @return A list of Calcite {@link SqlOperator}s corresponding to the Substrait functions.
   */
  public static List<SqlOperator> from(SimpleExtension.ExtensionCollection collection) {
    return from(collection, DEFAULT_TYPE_FACTORY);
  }

  /**
   * Converts all functions in a Substrait {@link SimpleExtension.ExtensionCollection} (scalar and
   * aggregate) into Calcite {@link SqlOperator}s using a provided type factory.
   *
   * @param collection The Substrait extension collection containing function definitions.
   * @param typeFactory Calcite {@link RelDataTypeFactory} for type creation and inference.
   * @return A list of Calcite {@link SqlOperator}s corresponding to the Substrait functions.
   */
  public static List<SqlOperator> from(
      SimpleExtension.ExtensionCollection collection, RelDataTypeFactory typeFactory) {
    return from(collection, typeFactory, TypeConverter.DEFAULT);
  }

  /**
   * Converts all functions in a Substrait {@link SimpleExtension.ExtensionCollection} (scalar and
   * aggregate) into Calcite {@link SqlOperator}s with a custom type factory and {@link
   * TypeConverter}.
   *
   * @param collection The Substrait extension collection containing function definitions.
   * @param typeFactory Calcite {@link RelDataTypeFactory} for type creation and inference.
   * @param typeConverter Converter for Substrait/Calcite type mappings.
   * @return A list of Calcite {@link SqlOperator}s corresponding to the Substrait functions.
   */
  public static List<SqlOperator> from(
      SimpleExtension.ExtensionCollection collection,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    List<? extends SimpleExtension.Function> functions =
        Stream.of(
                collection.scalarFunctions(),
                collection.aggregateFunctions(),
                collection.windowFunctions())
            .flatMap(List::stream)
            .collect(Collectors.toList());
    return from(functions, typeFactory, typeConverter);
  }

  /**
   * Converts a list of functions to SqlOperators. Handles scalar, aggregate, and window functions.
   *
   * @param functions list of functions to convert
   * @param typeFactory the Calcite type factory
   * @return list of SqlOperators
   */
  public static List<SqlOperator> from(
      List<? extends SimpleExtension.Function> functions, RelDataTypeFactory typeFactory) {
    return from(functions, typeFactory, TypeConverter.DEFAULT);
  }

  /**
   * Converts a list of functions to SqlOperators. Handles scalar, aggregate, and window functions.
   *
   * <p>Each function variant is converted to a separate SqlOperator. Functions with the same base
   * name but different type signatures (e.g., strftime:ts_str, strftime:ts_string) are ALL added to
   * the operator table. Calcite will try to match the function call arguments against all available
   * operators and select the one that matches. This allows functions with multiple signatures to be
   * used correctly without explicit deduplication.
   *
   * @param functions list of functions to convert
   * @param typeFactory the Calcite type factory
   * @param typeConverter the type converter
   * @return list of SqlOperators
   */
  public static List<SqlOperator> from(
      List<? extends SimpleExtension.Function> functions,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    return functions.stream()
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
        SqlTypeName typeName = valueArg.value().accept(CALCITE_TYPE_VISITOR);
        argFamilies.add(typeName.getFamily());
      } else if (arg instanceof SimpleExtension.EnumArgument) {
        // Treat an EnumArgument as a required string literal.
        argFamilies.add(SqlTypeFamily.STRING);
      }
    }

    // Create appropriate return type inference based on function type
    SqlReturnTypeInference returnTypeInference;
    if (function instanceof SimpleExtension.AggregateFunctionVariant) {
      returnTypeInference = new AggregateReturnTypeInference(function, typeFactory, typeConverter);
      return new DynamicSqlAggFunction(
          function.name(), returnTypeInference, OperandTypes.family(argFamilies));
    } else if (function instanceof SimpleExtension.WindowFunctionVariant) {
      returnTypeInference = new WindowReturnTypeInference(function, typeFactory, typeConverter);
      return new SqlFunction(
          function.name(),
          SqlKind.OTHER_FUNCTION,
          returnTypeInference,
          null,
          OperandTypes.family(argFamilies),
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    } else {
      // Scalar function
      returnTypeInference = new ScalarReturnTypeInference(function, typeFactory, typeConverter);
      return new SqlFunction(
          function.name(),
          SqlKind.OTHER_FUNCTION,
          returnTypeInference,
          null,
          OperandTypes.family(argFamilies),
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }
  }

  /** Concrete SqlAggFunction implementation for dynamically mapped aggregate functions. */
  private static class DynamicSqlAggFunction extends SqlAggFunction {
    public DynamicSqlAggFunction(
        String name,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeChecker operandTypeChecker) {
      super(
          name,
          null,
          SqlKind.OTHER_FUNCTION,
          returnTypeInference,
          null,
          operandTypeChecker,
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          Optionality.FORBIDDEN);
    }
  }

  /** Base class for return type inference with common logic for handling concrete types. */
  private abstract static class BaseReturnTypeInference implements SqlReturnTypeInference {
    protected final SimpleExtension.Function function;
    protected final RelDataTypeFactory typeFactory;
    protected final TypeConverter typeConverter;

    protected BaseReturnTypeInference(
        SimpleExtension.Function function,
        RelDataTypeFactory typeFactory,
        TypeConverter typeConverter) {
      this.function = function;
      this.typeFactory = typeFactory;
      this.typeConverter = typeConverter;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      TypeExpression returnExpression = function.returnType();

      // If return type is a concrete Type, use it directly
      if (returnExpression instanceof Type) {
        return inferConcreteReturnType((Type) returnExpression, opBinding);
      }

      // For parameterized types, delegate to subclass
      return inferParameterizedReturnType(opBinding);
    }

    /**
     * Infers return type for concrete (non-parameterized) type expressions.
     *
     * @param resolvedSubstraitType the concrete Substrait type
     * @param opBinding the operator binding with operand information
     * @return the inferred Calcite return type
     */
    private RelDataType inferConcreteReturnType(
        Type resolvedSubstraitType, SqlOperatorBinding opBinding) {
      boolean finalIsNullable = determineNullability(resolvedSubstraitType, opBinding);
      RelDataType baseCalciteType = typeConverter.toCalcite(typeFactory, resolvedSubstraitType);
      return typeFactory.createTypeWithNullability(baseCalciteType, finalIsNullable);
    }

    /**
     * Determines the nullability of the return type based on function nullability rules.
     *
     * @param resolvedSubstraitType the resolved Substrait type
     * @param opBinding the operator binding with operand information
     * @return true if the return type should be nullable
     */
    private boolean determineNullability(Type resolvedSubstraitType, SqlOperatorBinding opBinding) {
      switch (function.nullability()) {
        case MIRROR:
          // If any input is nullable, the output is nullable
          return opBinding.collectOperandTypes().stream().anyMatch(RelDataType::isNullable);
        case DISCRETE:
        case DECLARED_OUTPUT:
          // Use the nullability declared on the resolved Substrait type
          return resolvedSubstraitType.nullable();
        default:
          return resolvedSubstraitType.nullable();
      }
    }

    /**
     * Infer return type for parameterized type expressions (e.g., any1, T). Each function type
     * implements its own logic.
     */
    protected abstract RelDataType inferParameterizedReturnType(SqlOperatorBinding opBinding);
  }

  /**
   * Return type inference for scalar functions. Scalar functions typically return the same type as
   * their first argument.
   */
  private static final class ScalarReturnTypeInference extends BaseReturnTypeInference {
    private ScalarReturnTypeInference(
        SimpleExtension.Function function,
        RelDataTypeFactory typeFactory,
        TypeConverter typeConverter) {
      super(function, typeFactory, typeConverter);
    }

    @Override
    protected RelDataType inferParameterizedReturnType(SqlOperatorBinding opBinding) {
      List<RelDataType> operandTypes = opBinding.collectOperandTypes();
      if (operandTypes.isEmpty()) {
        throw new IllegalStateException(
            String.format(
                "Scalar function '%s' has parameterized return type but no arguments to infer from",
                function.name()));
      }

      RelDataType firstArgType = operandTypes.get(0);
      return applyNullabilityRules(firstArgType);
    }

    private RelDataType applyNullabilityRules(RelDataType baseType) {
      if (function.nullability() == SimpleExtension.Nullability.DECLARED_OUTPUT) {
        return typeFactory.createTypeWithNullability(baseType, true);
      }
      // MIRROR and other cases: keep original nullability
      return baseType;
    }
  }

  /**
   * Return type inference for aggregate functions. Aggregate functions often return nullable types
   * and may differ from input type.
   */
  private static final class AggregateReturnTypeInference extends BaseReturnTypeInference {
    private AggregateReturnTypeInference(
        SimpleExtension.Function function,
        RelDataTypeFactory typeFactory,
        TypeConverter typeConverter) {
      super(function, typeFactory, typeConverter);
    }

    @Override
    protected RelDataType inferParameterizedReturnType(SqlOperatorBinding opBinding) {
      List<RelDataType> operandTypes = opBinding.collectOperandTypes();
      if (operandTypes.isEmpty()) {
        // Fallback for aggregates without arguments (e.g., COUNT(*))
        return createNullableBigInt();
      }

      RelDataType firstArgType = operandTypes.get(0);
      return applyAggregateNullabilityRules(firstArgType);
    }

    private RelDataType applyAggregateNullabilityRules(RelDataType baseType) {
      // Aggregates typically return nullable types
      if (function.nullability() == SimpleExtension.Nullability.MIRROR) {
        return baseType; // Keep original nullability
      }
      // DECLARED_OUTPUT and other cases: always nullable
      return typeFactory.createTypeWithNullability(baseType, true);
    }

    private RelDataType createNullableBigInt() {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.BIGINT), true);
    }
  }

  /**
   * Return type inference for window functions. Window functions have diverse return types
   * depending on their category.
   */
  private static final class WindowReturnTypeInference extends BaseReturnTypeInference {
    private WindowReturnTypeInference(
        SimpleExtension.Function function,
        RelDataTypeFactory typeFactory,
        TypeConverter typeConverter) {
      super(function, typeFactory, typeConverter);
    }

    @Override
    protected RelDataType inferParameterizedReturnType(SqlOperatorBinding opBinding) {
      if (isRankingFunction()) {
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      }

      List<RelDataType> operandTypes = opBinding.collectOperandTypes();
      if (operandTypes.isEmpty()) {
        // Fallback for window functions without arguments
        return createNullableBigInt();
      }

      RelDataType firstArgType = operandTypes.get(0);
      return applyWindowNullabilityRules(firstArgType);
    }

    private boolean isRankingFunction() {
      String funcName = function.name().toLowerCase();
      return funcName.contains("rank") || "row_number".equals(funcName) || "ntile".equals(funcName);
    }

    private RelDataType applyWindowNullabilityRules(RelDataType baseType) {
      if (function.nullability() == SimpleExtension.Nullability.MIRROR) {
        return baseType; // Keep original nullability
      }
      // DECLARED_OUTPUT and other cases: always nullable
      return typeFactory.createTypeWithNullability(baseType, true);
    }

    private RelDataType createNullableBigInt() {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.BIGINT), true);
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
          LOGGER.warn("Unsupported type literal for Calcite conversion: {}", type);
          return SqlTypeName.ANY;
      }
    }
  }
}
