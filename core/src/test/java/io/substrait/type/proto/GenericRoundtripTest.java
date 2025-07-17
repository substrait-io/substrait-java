package io.substrait.type.proto;

import static io.substrait.expression.proto.ProtoExpressionConverter.EMPTY_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.protobuf.ByteString;
import io.substrait.TestBase;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.ProtoExpressionConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.util.EmptyVisitationContext;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class GenericRoundtripTest extends TestBase {

  static Random rand = new Random(123);

  @ParameterizedTest
  @MethodSource("generateInvocations")
  /**
   * This tests executes a roundtrip for {@link Method} m invoked with a {@link List<Object>} of
   * parameters. If the param generation has failed the {@link UnsupportedTypeGenerationException} e
   * is populated, and the test will be ignored (kept here for tracking).
   */
  public void roundtripTest(Method m, List<Object> paramInst, UnsupportedTypeGenerationException e)
      throws InvocationTargetException, IllegalAccessException {

    // If there is an UncoveredTypeGenerationException we  ignore this test
    if (e != null) {
      Assumptions.assumeTrue(false, e.getMessage());
    }

    // roundtrip to protobuff and back and check equality
    Expression val = (Expression) m.invoke(null, paramInst.toArray(new Object[0]));

    ExpressionProtoConverter to = new ExpressionProtoConverter(null, null);
    ProtoExpressionConverter from =
        new ProtoExpressionConverter(
            null,
            null,
            EMPTY_TYPE,
            new ProtoRelConverter(new ExtensionCollector(), defaultExtensionCollection));
    assertEquals(val, from.from(val.accept(to, EmptyVisitationContext.INSTANCE)));
  }

  // Parametrized case generator
  private static Collection generateInvocations() {

    ArrayList<Arguments> invocations = new ArrayList<>();

    // We list all public and static methods of ExpressionCreator
    List<Method> methodsToTest = getMethods(ExpressionCreator.class, true, true);

    // We generate synthetic input params (for a subset of types we support)
    for (Method m : methodsToTest) {
      try {
        invocations.add(arguments(m, instantiateParams(m), null));
      } catch (UnsupportedTypeGenerationException e) {
        invocations.add(arguments(m, null, e));
      }
    }
    return invocations;
  }

  private static List<Object> instantiateParams(Method m)
      throws UnsupportedTypeGenerationException {
    List<Object> l = new ArrayList<>();
    for (Class param : m.getParameterTypes()) {
      Object val = valGenerator(param);
      if (val == null) {
        throw new UnsupportedTypeGenerationException(
            "We can't yet handle generation for type: " + param.getName());
      }
      l.add(valGenerator(param));
    }
    return l;
  }

  private static List<Method> getMethods(
      Class c, boolean filterPublicOnly, boolean filterStaticOnly) {
    Method[] allMethods = c.getMethods();
    List<Method> selectedMethods = new ArrayList<>();
    for (Method m : allMethods) {
      if ((filterPublicOnly && !Modifier.isPublic(m.getModifiers()))
          || (filterStaticOnly && !Modifier.isStatic(m.getModifiers()))) {
        continue;
      }
      selectedMethods.add(m);
    }
    return selectedMethods;
  }

  private static Object valGenerator(Class<?> type) {
    // For each "type" generate some random value

    if (type.equals(Boolean.TYPE) || type.equals(Boolean.class)) {
      return rand.nextBoolean();
    } else if (type.equals((Integer.TYPE)) || type.equals(Integer.class)) {
      // we generate always numbers in 1-12 as this is often use for timestamp
      // construction and need to respect the days/months/years formats.
      // we would not test all values, but get ok coverage of basics
      return rand.nextInt(11) + 1;
    } else if (type.equals(Long.TYPE) || type.equals(Long.class)) {
      return rand.nextLong();
    } else if (type.equals(Double.TYPE) || type.equals(Double.class)) {
      return rand.nextDouble();
    } else if (type.equals(Float.TYPE) || type.equals(Float.class)) {
      return rand.nextFloat();
    } else if (type.equals(String.class)) {
      return UUID.randomUUID().toString();
    } else if (type.equals(UUID.class)) {
      return UUID.randomUUID();
    } else if (type.equals(LocalDateTime.class)) {
      return LocalDateTime.now();
    } else if (type.equals(ByteString.class)) {
      return ByteString.copyFromUtf8(UUID.randomUUID().toString());
    } else if (type.equals(BigDecimal.class)) {
      return BigDecimal.valueOf(rand.nextLong());
    } else if (type.equals(Instant.class)) {
      return Instant.now();
    }

    return null;
  }

  // Class used to propagate type generation errors from param generator to test cases
  private static class UnsupportedTypeGenerationException extends Exception {
    public UnsupportedTypeGenerationException(String s) {
      super(s);
    }
  }
}
