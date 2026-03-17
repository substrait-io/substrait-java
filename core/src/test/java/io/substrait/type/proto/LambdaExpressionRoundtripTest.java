package io.substrait.type.proto;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.util.JsonFormat;
import io.substrait.TestBase;
import io.substrait.expression.Expression;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class LambdaExpressionRoundtripTest extends TestBase {

  static Stream<String> validLambdaExpressions() throws IOException {
    return listJsonResources("expressions/lambda/valid");
  }

  static Stream<String> invalidLambdaExpressions() throws IOException {
    return listJsonResources("expressions/lambda/invalid");
  }

  @ParameterizedTest
  @MethodSource("validLambdaExpressions")
  void validLambdaExpressionRoundtrip(String resourcePath) throws IOException {
    Expression deserialized = deserializeExpression(resourcePath);
    assertInstanceOf(Expression.Lambda.class, deserialized);
    verifyRoundTrip(deserialized);
  }

  @ParameterizedTest
  @MethodSource("invalidLambdaExpressions")
  void invalidLambdaExpressionRejected(String resourcePath) {
    assertThrows(Exception.class, () -> deserializeExpression(resourcePath));
  }

  private static Stream<String> listJsonResources(String dirPath) throws IOException {
    Path dir =
        Paths.get(
            LambdaExpressionRoundtripTest.class.getClassLoader().getResource(dirPath).getPath());
    return Files.list(dir)
        .filter(p -> p.toString().endsWith(".json"))
        .map(p -> dirPath + "/" + p.getFileName().toString())
        .sorted();
  }

  private Expression deserializeExpression(String resourcePath) throws IOException {
    String json = asString(resourcePath);
    io.substrait.proto.Expression.Builder builder = io.substrait.proto.Expression.newBuilder();
    JsonFormat.parser().merge(json, builder);
    return protoExpressionConverter.from(builder.build());
  }
}
