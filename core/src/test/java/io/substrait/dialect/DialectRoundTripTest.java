package io.substrait.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.networknt.schema.Error;
import io.substrait.dialect.Dialect.CastFailureOption;
import io.substrait.dialect.Dialect.DdlWriteType;
import io.substrait.dialect.Dialect.DialectDocument;
import io.substrait.dialect.Dialect.DialectFunction;
import io.substrait.dialect.Dialect.ExchangeKind;
import io.substrait.dialect.Dialect.ExecutionBehavior;
import io.substrait.dialect.Dialect.ExpandFieldType;
import io.substrait.dialect.Dialect.ExpressionKind;
import io.substrait.dialect.Dialect.JoinType;
import io.substrait.dialect.Dialect.NestedType;
import io.substrait.dialect.Dialect.Notation;
import io.substrait.dialect.Dialect.ReadType;
import io.substrait.dialect.Dialect.RelationKind;
import io.substrait.dialect.Dialect.SetOperation;
import io.substrait.dialect.Dialect.SubqueryType;
import io.substrait.dialect.Dialect.SupportedExpression;
import io.substrait.dialect.Dialect.SupportedRelation;
import io.substrait.dialect.Dialect.SupportedType;
import io.substrait.dialect.Dialect.SystemFunctionMetadata;
import io.substrait.dialect.Dialect.SystemTypeMetadata;
import io.substrait.dialect.Dialect.TypeKind;
import io.substrait.dialect.Dialect.VariableEvaluationMode;
import io.substrait.dialect.Dialect.VariableType;
import io.substrait.dialect.Dialect.WriteType;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Builds a dialect in Java exercising every union and configuration, serializes it, validates the
 * YAML against the published schema, and parses it back to assert a lossless round-trip.
 */
class DialectRoundTripTest {

  private static DialectDocument sampleDialect() {
    return DialectDocument.builder()
        .name("Round Trip Dialect")
        .putDependencies("arithmetic", "extension:io.substrait:functions_arithmetic")
        .putDependencies("spark", "extension:substrait:spark")
        // Types: bare, precision (max_precision), system_metadata, and user-defined.
        .addSupportedTypes(SupportedType.of(TypeKind.BOOL))
        .addSupportedTypes(
            SupportedType.builder()
                .type(TypeKind.PRECISION_TIMESTAMP)
                .maxPrecision(9)
                .systemMetadata(
                    SystemTypeMetadata.builder()
                        .name("TimestampNTZType")
                        .supportedAsColumn(true)
                        .build())
                .build())
        .addSupportedTypes(
            SupportedType.builder()
                .type(TypeKind.USER_DEFINED)
                .source("spark")
                .name("interval")
                .build())
        // Relations: bare, join, read, set, write, ddl, exchange, expand, extension.
        .addSupportedRelations(SupportedRelation.of(RelationKind.FILTER))
        .addSupportedRelations(
            SupportedRelation.builder()
                .relation(RelationKind.JOIN)
                .addJoinTypes(JoinType.INNER, JoinType.LEFT)
                .build())
        .addSupportedRelations(
            SupportedRelation.builder()
                .relation(RelationKind.READ)
                .addReadTypes(ReadType.NAMED_TABLE, ReadType.VIRTUAL_TABLE)
                .build())
        .addSupportedRelations(
            SupportedRelation.builder()
                .relation(RelationKind.SET)
                .addOperations(SetOperation.UNION_ALL)
                .build())
        .addSupportedRelations(
            SupportedRelation.builder()
                .relation(RelationKind.WRITE)
                .addWriteTypes(WriteType.NAMED_TABLE)
                .build())
        .addSupportedRelations(
            SupportedRelation.builder()
                .relation(RelationKind.DDL)
                .addDdlWriteTypes(DdlWriteType.NAMED_OBJECT)
                .build())
        .addSupportedRelations(
            SupportedRelation.builder()
                .relation(RelationKind.EXCHANGE)
                .addKinds(ExchangeKind.ROUND_ROBIN, ExchangeKind.BROADCAST)
                .build())
        .addSupportedRelations(
            SupportedRelation.builder()
                .relation(RelationKind.EXPAND)
                .addFieldTypes(ExpandFieldType.SWITCHING_FIELD)
                .build())
        .addSupportedRelations(
            SupportedRelation.builder()
                .relation(RelationKind.EXTENSION_SINGLE)
                .addMessageTypes("type.googleapis.com/google.profile.Person")
                .build())
        // Expressions: bare, cast, subquery, nested, execution-context-variable.
        .addSupportedExpressions(SupportedExpression.of(ExpressionKind.LITERAL))
        .addSupportedExpressions(
            SupportedExpression.builder()
                .expression(ExpressionKind.CAST)
                .addFailureOptions(CastFailureOption.RETURN_NULL)
                .build())
        .addSupportedExpressions(
            SupportedExpression.builder()
                .expression(ExpressionKind.SUBQUERY)
                .addSubqueryTypes(SubqueryType.SCALAR, SubqueryType.IN_PREDICATE)
                .build())
        .addSupportedExpressions(
            SupportedExpression.builder()
                .expression(ExpressionKind.NESTED)
                .addNestedTypes(NestedType.STRUCT, NestedType.LIST)
                .build())
        .addSupportedExpressions(
            SupportedExpression.builder()
                .expression(ExpressionKind.EXECUTION_CONTEXT_VARIABLE)
                .addVariableTypes(VariableType.CURRENT_DATE, VariableType.CURRENT_TIMESTAMP)
                .build())
        // Functions across the three categories.
        .addSupportedScalarFunctions(
            DialectFunction.builder()
                .source("arithmetic")
                .name("add")
                .systemMetadata(
                    SystemFunctionMetadata.builder().name("+").notation(Notation.INFIX).build())
                .addSupportedImpls("i32_i32", "i64_i64")
                .build())
        .addSupportedAggregateFunctions(
            DialectFunction.builder()
                .source("arithmetic")
                .name("sum")
                .addSupportedImpls("i64")
                .build())
        .addSupportedWindowFunctions(
            DialectFunction.builder()
                .source("arithmetic")
                .name("row_number")
                .addSupportedImpls("")
                .build())
        .supportedExecutionBehavior(
            ExecutionBehavior.builder()
                .addSupportedVariableEvaluationMode(VariableEvaluationMode.PER_PLAN)
                .build())
        .build();
  }

  @Test
  void roundTripThroughYamlAndSchema() {
    DialectDocument original = sampleDialect();

    String yaml = Dialect.toYaml(original);

    List<Error> errors = SchemaValidator.validate(yaml);
    assertTrue(errors.isEmpty(), () -> "Generated dialect failed schema validation: " + errors);

    DialectDocument parsed = Dialect.load(yaml);
    assertEquals(original, parsed);
  }
}
