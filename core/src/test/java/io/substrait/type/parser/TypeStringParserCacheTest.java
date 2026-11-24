package io.substrait.type.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.TypeExpression;
import io.substrait.type.Deserializers;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TypeStringParserCacheTest {

  private static final String URN_A = "extension:test:a";
  private static final String URN_B = "extension:test:b";

  private static final List<String> DEFAULT_EXTENSION_RESOURCES =
      List.of(
          "/substrait/extensions/functions_boolean.yaml",
          "/substrait/extensions/functions_aggregate_generic.yaml",
          "/substrait/extensions/functions_aggregate_approx.yaml",
          "/substrait/extensions/functions_arithmetic_decimal.yaml",
          "/substrait/extensions/functions_arithmetic.yaml",
          "/substrait/extensions/functions_comparison.yaml",
          "/substrait/extensions/functions_datetime.yaml",
          "/substrait/extensions/functions_list.yaml",
          "/substrait/extensions/functions_logarithmic.yaml",
          "/substrait/extensions/functions_rounding.yaml",
          "/substrait/extensions/functions_rounding_decimal.yaml",
          "/substrait/extensions/functions_set.yaml",
          "/substrait/extensions/functions_string.yaml",
          "/substrait/extensions/unsigned_integers.yaml");

  @Test
  void modeIsolation() {
    // Wildcards and parameterized types succeed in PARAMETERIZED and EXPRESSION modes,
    // but must fail in SIMPLE mode even after being warmed in the other modes.
    ParameterizedType anyParam = TypeStringParser.parseParameterized("any", URN_A);
    assertEquals(TypeStringParser.parse("any", URN_A, ParseToPojo::parameterizedType), anyParam);
    assertEquals(anyParam, TypeStringParser.parseExpression("any", URN_A));
    assertThrows(ClassCastException.class, () -> TypeStringParser.parseSimple("any", URN_A));
    assertThrows(ClassCastException.class, () -> TypeStringParser.parseSimple("any1?", URN_A));

    ParameterizedType decParam = TypeStringParser.parseParameterized("decimal<P,S>", URN_A);
    assertEquals(
        TypeStringParser.parse("decimal<P,S>", URN_A, ParseToPojo::parameterizedType), decParam);
    assertEquals(decParam, TypeStringParser.parseExpression("decimal<P,S>", URN_A));
    assertThrows(
        UnsupportedOperationException.class,
        () -> TypeStringParser.parseSimple("decimal<P,S>", URN_A));

    // Derivation expressions succeed in EXPRESSION mode but must fail in PARAMETERIZED and SIMPLE.
    String expr = "FIXEDCHAR<L1+L2>";
    TypeExpression parsedExpr = TypeStringParser.parseExpression(expr, URN_A);
    assertEquals(TypeStringParser.parse(expr, URN_A, ParseToPojo::typeExpression), parsedExpr);
    assertThrows(
        UnsupportedOperationException.class,
        () -> TypeStringParser.parseParameterized(expr, URN_A));
    assertThrows(
        UnsupportedOperationException.class, () -> TypeStringParser.parseSimple(expr, URN_A));
  }

  @Test
  void udtUrnIsolation() throws IOException {
    // Direct parser API: same UDT string under distinct URNs must carry distinct URNs.
    for (String typeStr :
        Arrays.asList("u!foo", "U!foo?", "list<u!foo>", "alias.u!bar<i32>", "!a ? u!foo : i32")) {
      TypeExpression exprA = TypeStringParser.parseExpression(typeStr, URN_A);
      TypeExpression exprB = TypeStringParser.parseExpression(typeStr, URN_B);
      assertNotEquals(exprA, exprB, typeStr);
      assertEquals(TypeStringParser.parse(typeStr, URN_A, ParseToPojo::typeExpression), exprA);
      assertEquals(TypeStringParser.parse(typeStr, URN_B, ParseToPojo::typeExpression), exprB);
    }

    Type simpleA = TypeStringParser.parseSimple("u!foo", URN_A);
    Type simpleB = TypeStringParser.parseSimple("u!foo", URN_B);
    assertEquals(TypeCreator.REQUIRED.userDefined(URN_A, "foo"), simpleA);
    assertEquals(TypeCreator.REQUIRED.userDefined(URN_B, "foo"), simpleB);
    assertNotEquals(simpleA, simpleB);

    ParameterizedType paramA = TypeStringParser.parseParameterized("list<U!foo?>", URN_A);
    ParameterizedType paramB = TypeStringParser.parseParameterized("list<U!foo?>", URN_B);
    assertNotEquals(paramA, paramB);

    // Jackson Deserializers path: verify URN injection stays isolated across ObjectMappers.
    ObjectMapper mapperA = mapperWithUrn(URN_A);
    ObjectMapper mapperB = mapperWithUrn(URN_B);
    assertEquals(
        TypeCreator.REQUIRED.userDefined(URN_A, "foo"), mapperA.readValue("\"u!foo\"", Type.class));
    assertEquals(
        TypeCreator.REQUIRED.userDefined(URN_B, "foo"), mapperB.readValue("\"u!foo\"", Type.class));
    assertEquals(
        TypeCreator.REQUIRED.userDefined(URN_A, "foo"),
        mapperA.readValue("\"u!foo\"", ParameterizedType.class));
    assertEquals(
        TypeCreator.REQUIRED.userDefined(URN_B, "foo"),
        mapperB.readValue("\"u!foo\"", ParameterizedType.class));
  }

  @Test
  void fastPathSingletonsAndFallbackParity() throws IOException {
    // Canonical scalars return shared singletons directly.
    assertSame(TypeCreator.REQUIRED.I32, TypeStringParser.parseSimple("i32", URN_A));
    assertSame(TypeCreator.NULLABLE.I32, TypeStringParser.parseSimple("i32?", URN_A));
    assertSame(TypeCreator.REQUIRED.STRING, TypeStringParser.parseParameterized("string", URN_A));
    assertSame(TypeCreator.NULLABLE.BOOLEAN, TypeStringParser.parseExpression("boolean?", URN_A));

    // Non-canonical case and whitespace must match ANTLR output identically.
    for (String variant :
        Arrays.asList("I32", "i32 ", " i32?", "BOOLEAN", "Boolean?", "ANY", "ANY1?", "  any9  ")) {
      assertEquals(
          TypeStringParser.parse(variant, URN_A, ParseToPojo::parameterizedType),
          TypeStringParser.parseParameterized(variant, URN_A),
          variant);
      assertEquals(
          TypeStringParser.parse(variant, URN_A, ParseToPojo::typeExpression),
          TypeStringParser.parseExpression(variant, URN_A),
          variant);
    }

    // Invalid syntax must still throw ParseError (and JsonMappingException via Jackson).
    for (String invalid : Arrays.asList("", "decimal<", "list<i32", "i32??", "1 +")) {
      assertThrows(
          TypeStringParser.ParseError.class, () -> TypeStringParser.parseSimple(invalid, URN_A));
      assertThrows(
          TypeStringParser.ParseError.class,
          () -> TypeStringParser.parseParameterized(invalid, URN_A));
      assertThrows(
          TypeStringParser.ParseError.class,
          () -> TypeStringParser.parseExpression(invalid, URN_A));
    }

    ObjectMapper mapper = mapperWithUrn(URN_A);
    assertThrows(
        JsonMappingException.class, () -> mapper.readValue("\"decimal<\"", TypeExpression.class));
  }

  @Test
  void allCanonicalScalarsAndWildcardsMatchAntlr() {
    List<String> scalars =
        List.of(
            "boolean",
            "i8",
            "i16",
            "i32",
            "i64",
            "fp32",
            "fp64",
            "string",
            "binary",
            "date",
            "interval_year",
            "uuid");
    for (String base : scalars) {
      for (String candidate : List.of(base, base + "?")) {
        Type expected = TypeStringParser.parse(candidate, URN_A, ParseToPojo::type);
        assertSame(expected, TypeStringParser.parseSimple(candidate, URN_A), candidate);
        assertSame(expected, TypeStringParser.parseParameterized(candidate, URN_A), candidate);
        assertSame(expected, TypeStringParser.parseExpression(candidate, URN_A), candidate);
      }
    }

    List<String> wildcards =
        List.of(
            "any", "any0", "any1", "any2", "any3", "any4", "any5", "any6", "any7", "any8", "any9");
    for (String base : wildcards) {
      for (String candidate : List.of(base, base + "?")) {
        ParameterizedType expected =
            TypeStringParser.parse(candidate, URN_A, ParseToPojo::parameterizedType);
        assertEquals(expected, TypeStringParser.parseParameterized(candidate, URN_A), candidate);
        assertEquals(expected, TypeStringParser.parseExpression(candidate, URN_A), candidate);
        assertThrows(
            ClassCastException.class,
            () -> TypeStringParser.parseSimple(candidate, URN_A),
            candidate);
      }
    }
  }

  @Test
  void defaultExtensionYamlTypeParity() throws IOException {
    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    Set<String> argTypeStrings = new LinkedHashSet<>();
    Set<String> returnTypeStrings = new LinkedHashSet<>();

    for (String resource : DEFAULT_EXTENSION_RESOURCES) {
      try (InputStream stream = DefaultExtensionCatalog.class.getResourceAsStream(resource)) {
        Map<?, ?> doc = yamlMapper.readValue(stream, Map.class);
        collectFunctionTypes(doc.get("scalar_functions"), argTypeStrings, returnTypeStrings);
        collectFunctionTypes(doc.get("aggregate_functions"), argTypeStrings, returnTypeStrings);
        collectFunctionTypes(doc.get("window_functions"), argTypeStrings, returnTypeStrings);
      }
    }

    assertFalse(argTypeStrings.isEmpty());
    assertFalse(returnTypeStrings.isEmpty());

    // Verify cold + warm parity against uncached ANTLR for every type string in default YAMLs.
    for (int pass = 0; pass < 2; pass++) {
      for (String argType : argTypeStrings) {
        ParameterizedType expected =
            TypeStringParser.parse(argType, URN_A, ParseToPojo::parameterizedType);
        assertEquals(expected, TypeStringParser.parseParameterized(argType, URN_A), argType);
        assertEquals(
            expected,
            TypeStringParser.parse(argType, ParseToPojo.Visitor.parameterized(URN_A)),
            argType);
      }
      for (String returnType : returnTypeStrings) {
        TypeExpression expected =
            TypeStringParser.parse(returnType, URN_A, ParseToPojo::typeExpression);
        assertEquals(expected, TypeStringParser.parseExpression(returnType, URN_A), returnType);
        assertEquals(
            expected,
            TypeStringParser.parse(returnType, ParseToPojo.Visitor.expression(URN_A)),
            returnType);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void collectFunctionTypes(
      Object section, Set<String> argTypes, Set<String> returnTypes) {
    if (!(section instanceof List<?>)) {
      return;
    }
    for (Object fnObj : (List<?>) section) {
      Map<String, Object> fn = (Map<String, Object>) fnObj;
      Object implsObj = fn.get("impls");
      if (!(implsObj instanceof List<?>)) {
        continue;
      }
      for (Object implObj : (List<?>) implsObj) {
        Map<String, Object> impl = (Map<String, Object>) implObj;
        Object ret = impl.get("return");
        if (ret instanceof String) {
          returnTypes.add((String) ret);
        }
        Object intermediate = impl.get("intermediate");
        if (intermediate instanceof String) {
          returnTypes.add((String) intermediate);
        }
        Object argsObj = impl.get("args");
        if (argsObj instanceof List<?>) {
          for (Object argObj : (List<?>) argsObj) {
            if (argObj instanceof Map<?, ?>) {
              Map<String, Object> arg = (Map<String, Object>) argObj;
              if (arg.get("value") instanceof String) {
                argTypes.add((String) arg.get("value"));
              }
              if (arg.get("type") instanceof String) {
                argTypes.add((String) arg.get("type"));
              }
            }
          }
        }
      }
    }
  }

  private static ObjectMapper mapperWithUrn(String urn) {
    InjectableValues.Std iv = new InjectableValues.Std();
    iv.addValue(SimpleExtension.URN_LOCATOR_KEY, urn);
    SimpleModule module =
        new SimpleModule()
            .addDeserializer(Type.class, Deserializers.TYPE)
            .addDeserializer(ParameterizedType.class, Deserializers.PARAMETERIZED_TYPE)
            .addDeserializer(TypeExpression.class, Deserializers.DERIVATION_EXPRESSION);
    return new ObjectMapper().registerModule(module).setInjectableValues(iv);
  }
}
