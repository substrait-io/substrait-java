package io.substrait.type.proto;

import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.relation.NamedUpdate;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class UpdateRelRoundtripTest extends TestBase {

  @Test
  void update() {
    final NamedStruct schema =
        NamedStruct.of(
            Stream.of("column1", "column2").collect(Collectors.toList()), R.struct(R.I64, R.I64));

    final List<NamedUpdate.TransformExpression> transformations =
        Arrays.asList(
            NamedUpdate.TransformExpression.builder()
                .columnTarget(0)
                .transformation(fnAdd(1))
                .build(),
            NamedUpdate.TransformExpression.builder()
                .columnTarget(1)
                .transformation(fnAdd(2))
                .build());

    final Expression condition = ExpressionCreator.bool(false, true);

    final NamedUpdate command =
        NamedUpdate.builder()
            .tableSchema(schema)
            .names(Stream.of("table").collect(Collectors.toList()))
            .addAllTransformations(transformations)
            .condition(condition)
            .build();

    verifyRoundTrip(command);
  }

  private Expression.ScalarFunctionInvocation fnAdd(final int value) {
    return defaultExtensionCollection.scalarFunctions().stream()
        .filter(s -> s.name().equalsIgnoreCase("add"))
        .findFirst()
        .map(
            declaration ->
                ExpressionCreator.scalarFunction(
                    declaration,
                    TypeCreator.REQUIRED.BOOLEAN,
                    ImmutableFieldReference.builder()
                        .addSegments(FieldReference.StructField.of(0))
                        .type(TypeCreator.REQUIRED.I64)
                        .build(),
                    ExpressionCreator.i32(false, value)))
        .get();
  }
}
