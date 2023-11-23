package io.substrait.extended.expression;

import com.google.protobuf.util.JsonFormat;
import io.substrait.expression.*;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.ExtendedExpression;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.type.proto.TypeProtoConverter;
import java.io.IOException;
import java.util.*;

public class ExtendedExpressionProtoConverter {
  public ExtendedExpression toProto(
      io.substrait.extended.expression.ExtendedExpression extendedExpressionPojo) {

    ExtendedExpression.Builder extendedExpressionBuilder = ExtendedExpression.newBuilder();
    ExtensionCollector functionCollector = new ExtensionCollector();

    final ExpressionProtoConverter expressionProtoConverter =
        new ExpressionProtoConverter(functionCollector, null);

    // convert expression pojo into expression protobuf
    io.substrait.proto.Expression expressionProto =
        expressionProtoConverter.visit(
            (Expression.ScalarFunctionInvocation) extendedExpressionPojo.getReferredExpr().get(0));

    ExpressionReference.Builder expressionReferenceBuilder =
        ExpressionReference.newBuilder().setExpression(expressionProto).addOutputNames("column-01");

    extendedExpressionBuilder.addReferredExpr(0, expressionReferenceBuilder);
    extendedExpressionBuilder.setBaseSchema(
        extendedExpressionPojo.getBaseSchema().toProto(new TypeProtoConverter(functionCollector)));


      functionCollector.addExtensionsToPlan(extendedExpressionBuilder);
      if (extendedExpressionPojo.getAdvancedExtension().isPresent()) {
      extendedExpressionBuilder.setAdvancedExtensions(
          extendedExpressionPojo.getAdvancedExtension().get());
    }
    return extendedExpressionBuilder.build();
  }

  public static void main(String[] args) throws IOException {
    SimpleExtension.ExtensionCollection defaultExtensionCollection = SimpleExtension.loadDefaults();
    System.out.println(
        "defaultExtensionCollection.scalarFunctions(): "
            + defaultExtensionCollection.scalarFunctions());
    System.out.println(
        "defaultExtensionCollection.windowFunctions(): "
            + defaultExtensionCollection.windowFunctions());
    System.out.println(
        "defaultExtensionCollection.aggregateFunctions(): "
            + defaultExtensionCollection.aggregateFunctions());

    Optional<Expression.ScalarFunctionInvocation> equal =
        defaultExtensionCollection.scalarFunctions().stream()
            .filter(
                s -> {
                  return s.name().equalsIgnoreCase("add");
                })
            .findFirst()
            .map(
                declaration ->
                    ExpressionCreator.scalarFunction(
                        declaration,
                        TypeCreator.REQUIRED.BOOLEAN,
                        ImmutableFieldReference.builder()
                            .addSegments(FieldReference.StructField.of(0))
                            .type(TypeCreator.REQUIRED.I32)
                            .build(),
                        ExpressionCreator.i32(false, 183)));

    Map<Integer, Expression> indexToExpressionMap = new HashMap<>();
    indexToExpressionMap.put(0, equal.get());
    List<String> columnNames = Arrays.asList("N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT");
    List<Type> dataTypes =
        Arrays.asList(
            TypeCreator.NULLABLE.I32,
            TypeCreator.NULLABLE.STRING,
            TypeCreator.NULLABLE.I32,
            TypeCreator.NULLABLE.STRING);
    NamedStruct namedStruct =
        NamedStruct.of(
            columnNames, Type.Struct.builder().fields(dataTypes).nullable(false).build());
    ImmutableExtendedExpression.Builder builder =
        ImmutableExtendedExpression.builder()
            .putAllReferredExpr(indexToExpressionMap)
            .baseSchema(namedStruct);

    ExtendedExpression proto = new ExtendedExpressionProtoConverter().toProto(builder.build());

    System.out.println(
        "JsonFormat.printer().print(getFilterExtendedExpression): "
            + JsonFormat.printer().print(proto));
  }

  public static ExtendedExpression createExtendedExpression(
      io.substrait.expression.Expression.ScalarFunctionInvocation expr) {
    ExtendedExpression.Builder extendedExpressionBuilder = ExtendedExpression.newBuilder();

    io.substrait.proto.Expression expression = new ExpressionProtoConverter(null, null).visit(expr);
    ExpressionReference.Builder expressionReferenceBuilder =
        ExpressionReference.newBuilder()
            .setExpression(expression.toBuilder())
            .addOutputNames("col-01");

    extendedExpressionBuilder.addReferredExpr(0, expressionReferenceBuilder);

    return extendedExpressionBuilder.build();
  }

  public static void createExtendedExpressionManually() {

    Map<String, Expression> nameToExpressionMap = new HashMap<>();
    ImmutableExpression.I32Literal build = Expression.I32Literal.builder().value(10).build();
    nameToExpressionMap.put("out_01", build);

    List<Expression> expressionList = new ArrayList<>();
    expressionList.add(0, null);

    // nation table
    List<String> columnNames = Arrays.asList("N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT");
    List<Type> dataTypes =
        Arrays.asList(
            TypeCreator.NULLABLE.I32,
            TypeCreator.NULLABLE.STRING,
            TypeCreator.NULLABLE.I32,
            TypeCreator.NULLABLE.STRING);
    NamedStruct namedStruct =
        NamedStruct.of(
            columnNames, Type.Struct.builder().fields(dataTypes).nullable(false).build());

    ExtensionCollector functionCollector = new ExtensionCollector();

    new ExpressionProtoConverter(new ExtensionCollector(), null);

    FunctionArg functionArg =
        new FunctionArg() {
          @Override
          public <R, E extends Throwable> R accept(
              SimpleExtension.Function fnDef, int argIdx, FuncArgVisitor<R, E> fnArgVisitor)
              throws E {
            return null;
          }
        };

    // var argVisitor = FunctionArg.toProto(new TypeProtoConverter(new ExtensionCollector()), this);

    /*

    // FIXME! setFunctionReference, addArguments(index: 0, 1)
    io.substrait.proto.Expression.Builder expressionBuilder =
            io.substrait.proto.Expression.newBuilder()
                    .setScalarFunction(
                            io.substrait.proto.Expression.ScalarFunction.newBuilder()
                                    .setFunctionReference(1)
                                    .setOutputType(output)
                                    .addArguments(
                                            0,
                                            FunctionArgument.newBuilder().setValue(result.referenceBuilder()))
                                    .addArguments(
                                            1,
                                            FunctionArgument.newBuilder()
                                                    .setValue(result.expressionBuilderLiteral())));
    io.substrait.proto.ExpressionReference.Builder expressionReferenceBuilder =
            ExpressionReference.newBuilder()
                    .setExpression(expressionBuilder)
                    .addOutputNames(result.ref().getName());

     */

    /*

    io.substrait.extended.expression.ExtendedExpression extendedExpression = new io.substrait.extended.expression.ExtendedExpression() {
        @Override
        public List<ExpressionReference> getReferredExpr() {
            io.substrait.extended.expression.ExpressionReference

        @Override
        public NamedStruct getBaseSchema() {
            return null;
        }

        @Override
        public List<String> getExpectedTypeUrls() {
            return null;
        }

        @Override
        public Optional<AdvancedExtension> getAdvancedExtension() {
            return Optional.empty();
        }
    };

    System.out.println("inicio");
    System.out.println(extendedExpression.getReferredExpr().get(0));
    System.out.println(extendedExpression.getReferredExpr().get(0).getType());
    System.out.println("fin");

    ExpressionReferenceOrBuilder

     */

  }
}
