package io.substrait.isthmus;

import static java.util.Objects.requireNonNull;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.FunctionSqlType;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlLambdaScope;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Provides Calcite operators for Substrait list functions that accept lambdas.
 *
 * <p>Calcite initially validates lambda parameters as {@code ANY}. These operators revalidate the
 * lambda using the input array's element type so that its body and return type are concrete before
 * conversion to Substrait.
 */
public final class ListFunctions {

  /** Applies a lambda to every array element. */
  public static final SqlFunction TRANSFORM =
      SqlBasicFunction.create(
          "transform",
          binding -> {
            FunctionSqlType lambdaType = (FunctionSqlType) binding.getOperandType(1);
            RelDataType arrayType =
                binding.getTypeFactory().createArrayType(lambdaType.getReturnType(), -1);
            return binding
                .getTypeFactory()
                .createTypeWithNullability(arrayType, binding.getOperandType(0).isNullable());
          },
          new ArrayLambdaOperandTypeChecker(SqlTypeFamily.ANY));

  /** Keeps array elements for which a predicate returns true. */
  public static final SqlFunction FILTER =
      SqlBasicFunction.create(
          "filter", ReturnTypes.ARG0, new ArrayLambdaOperandTypeChecker(SqlTypeFamily.BOOLEAN));

  /** Returns whether a predicate matches any array element. */
  public static final SqlFunction ANY_MATCH =
      SqlBasicFunction.create(
          "any_match",
          ReturnTypes.BOOLEAN_NULLABLE,
          new ArrayLambdaOperandTypeChecker(SqlTypeFamily.BOOLEAN));

  /** Returns whether a predicate matches every array element. */
  public static final SqlFunction ALL_MATCH =
      SqlBasicFunction.create(
          "all_match",
          ReturnTypes.BOOLEAN_NULLABLE,
          new ArrayLambdaOperandTypeChecker(SqlTypeFamily.BOOLEAN));

  private ListFunctions() {}

  private static final class ArrayLambdaOperandTypeChecker implements SqlOperandTypeChecker {
    private final SqlTypeFamily returnTypeFamily;

    private ArrayLambdaOperandTypeChecker(SqlTypeFamily returnTypeFamily) {
      this.returnTypeFamily = returnTypeFamily;
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      SqlNode array = callBinding.operand(0);
      if (!OperandTypes.ARRAY.checkSingleOperandType(callBinding, array, 0, throwOnFailure)) {
        return false;
      }

      SqlNode lambdaOperand = callBinding.operand(1);
      if (!(lambdaOperand instanceof SqlLambda)) {
        return failValidation(callBinding, throwOnFailure);
      }
      SqlLambda lambda = (SqlLambda) lambdaOperand;
      if (lambda.getParameters().size() != 1) {
        return failValidation(callBinding, throwOnFailure);
      }

      RelDataType arrayType = SqlTypeUtil.deriveType(callBinding, array);
      RelDataType elementType = requireNonNull(arrayType.getComponentType(), "componentType");
      SqlValidator validator = callBinding.getValidator();
      SqlLambdaScope lambdaScope = (SqlLambdaScope) validator.getLambdaScope(lambda);
      SqlNode parameter = lambda.getParameters().get(0);
      lambdaScope.getParameterTypes().put(parameter.toString(), elementType);
      lambda.accept(new ValidatedTypeRemover(validator));
      validator.validateLambda(lambda);

      FunctionSqlType lambdaType = (FunctionSqlType) validator.getValidatedNodeType(lambda);
      SqlTypeName returnType = lambdaType.getReturnType().getSqlTypeName();
      if (returnTypeFamily != SqlTypeFamily.ANY
          && !returnTypeFamily.getTypeNames().contains(returnType)) {
        return failValidation(callBinding, throwOnFailure);
      }
      return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      return SqlOperandCountRanges.of(2);
    }

    @Override
    public String getAllowedSignatures(SqlOperator operator, String operatorName) {
      return operatorName + "(<ARRAY>, <FUNCTION(ARRAY_ELEMENT_TYPE)->" + returnTypeFamily + ">)";
    }

    private static boolean failValidation(SqlCallBinding callBinding, boolean throwOnFailure) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }
  }

  private static final class ValidatedTypeRemover extends SqlBasicVisitor<Void> {
    private final SqlValidator validator;

    private ValidatedTypeRemover(SqlValidator validator) {
      this.validator = validator;
    }

    @Override
    public Void visit(SqlIdentifier identifier) {
      validator.removeValidatedNodeType(identifier);
      return super.visit(identifier);
    }

    @Override
    public Void visit(SqlCall call) {
      validator.removeValidatedNodeType(call);
      return super.visit(call);
    }
  }
}
