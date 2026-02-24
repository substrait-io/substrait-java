package io.substrait.isthmus;


import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for Lambda expression conversion between Substrait and Calcite.
 * Note: Calcite does not support nested lambda expressions for the moment, so all tests use stepsOut=0.
 */

public class LambdaExpressionTest extends PlanTestBase{

    final Rel emptyTable = sb.emptyVirtualTableScan();

    /**
     * Test that lambdas with no parameters are valid.
     * Building: () -> i32(42) : func<() -> i32>
     */
    @Test
    void lambdaExpressionZeroParameters(){
        Type.Struct params = Type.Struct.builder()
                .nullable(false)
                .build();

        Expression body = ExpressionCreator.i32(false, 42);
        List<Expression> expressionList = new ArrayList<>();
        Expression.Lambda lambda = Expression.Lambda.builder()
                .parameters(params)
                .body(body)
                .build();
        expressionList.add(lambda);

        Project project = Project.builder().expressions(expressionList).input(emptyTable).build();
        assertFullRoundTrip(project);

    }

    /**
     * Test valid field index with multiple parameters.
     * Building: ($0: i32, $1: i64, $2: string) -> $2 : func<(i32, i64, string) -> string>
     */
    @Test
    void validFieldIndex() {
        Type.Struct params = Type.Struct.builder()
                .nullable(false)
                .addFields(R.I32, R.I64, R.STRING)
                .build();

        FieldReference paramRef = FieldReference.newLambdaParameterReference(0, params, 0);

        List<Expression> expressionList = new ArrayList<>();
        Expression.Lambda lambda = Expression.Lambda.builder()
                .parameters(params)
                .body(paramRef)
                .build();

        expressionList.add(lambda);


        Project project = Project.builder().expressions(expressionList).input(emptyTable).build();
        assertFullRoundTrip(project);
    }

    /**
     * Test deeply nested field ref inside Cast.
     * Building: ($0: i32) -> cast($0 as i64) : func<i32 -> i64>
     */
    @Test
    void deeplyNestedFieldRef() {
        Type.Struct params = Type.Struct.builder()
                .nullable(false)
                .addFields(R.I32)
                .build();
        FieldReference paramRef = FieldReference.newLambdaParameterReference(0, params, 0);

        Expression.Cast castExpr = (Expression.Cast)
                ExpressionCreator.cast(R.I64, paramRef, Expression.FailureBehavior.THROW_EXCEPTION);
        List<Expression> expressionList = new ArrayList<>();


        Expression.Lambda lambda = Expression.Lambda.builder()
                .parameters(params)
                .body(castExpr)
                .build();

        expressionList.add(lambda);
        Project project = Project.builder().expressions(expressionList).input(emptyTable).build();
        assertFullRoundTrip(project);
    }

    /**
     * Test doubly nested field ref (Cast(Cast(LambdaParamRef))).
     * Building: ($0: i32) -> cast(cast($0 as i64) as string) : func<i32 -> string>
     */
    @Test
    void doublyNestedFieldRef() {
        Type.Struct params = Type.Struct.builder()
                .nullable(false)
                .addFields(R.I32)
                .build();

        FieldReference paramRef = FieldReference.newLambdaParameterReference(0, params, 0);
        Expression.Cast innerCast = (Expression.Cast)
                ExpressionCreator.cast(R.I64, paramRef, Expression.FailureBehavior.THROW_EXCEPTION);
        Expression.Cast outerCast = (Expression.Cast)
                ExpressionCreator.cast(R.STRING, innerCast, Expression.FailureBehavior.THROW_EXCEPTION);

        List<Expression> expressionList = new ArrayList<>();

        Expression.Lambda lambda = Expression.Lambda.builder()
                .parameters(params)
                .body(outerCast)
                .build();

        expressionList.add(lambda);
        Project project = Project.builder().expressions(expressionList).input(emptyTable).build();
        assertFullRoundTrip(project);
    }

    /**
     * Test lambda with literal body (no parameter references).
     * Building: ($0: i32) -> 42 : func<i32 -> i32>
     */
    @Test
    void lambdaWithLiteralBody() {
        Type.Struct params = Type.Struct.builder()
                .nullable(false)
                .addFields(R.I32)
                .build();

        Expression body = ExpressionCreator.i32(false, 42);
        List<Expression> expressionList = new ArrayList<>();

        Expression.Lambda lambda = Expression.Lambda.builder()
                .parameters(params)
                .body(body)
                .build();

        expressionList.add(lambda);
        Project project = Project.builder().expressions(expressionList).input(emptyTable).build();
        assertFullRoundTrip(project);
    }

    /**
     * Test that nested lambda (stepsOut > 0) throws UnsupportedOperationException.
     * Calcite does not support nested lambda expressions.
     */
    @Test
    void nestedLambdaThrowsUnsupportedOperation() {
        Type.Struct outerParams = Type.Struct.builder()
                .nullable(false)
                .addFields(R.I64)
                .build();

        Type.Struct innerParams = Type.Struct.builder()
                .nullable(false)
                .addFields(R.I32)
                .build();

        // Inner lambda references outer's parameter with stepsOut=1
        FieldReference outerRef = FieldReference.newLambdaParameterReference(0, outerParams, 1);

        Expression.Lambda innerLambda = Expression.Lambda.builder()
                .parameters(innerParams)
                .body(outerRef)
                .build();

        List<Expression> expressionList = new ArrayList<>();

        Expression.Lambda outerLambda = Expression.Lambda.builder()
                .parameters(outerParams)
                .body(innerLambda)
                .build();

        expressionList.add(outerLambda);
        Project project = Project.builder().expressions(expressionList).input(emptyTable).build();
        assertThrows(UnsupportedOperationException.class, () -> assertFullRoundTrip(project));

    }
    }
