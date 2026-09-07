package io.substrait.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.FunctionBindingResolver;
import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.InvalidFunctionBindingException;
import io.substrait.extension.ResolvedArgument;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ParameterizedTypeCreator;
import io.substrait.function.TypeExpression;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ContainerReturnTypeTest {
  private static final TypeCreator R = TypeCreator.REQUIRED;
  private static final TypeCreator N = TypeCreator.NULLABLE;
  private static final ParameterizedTypeCreator P = ParameterizedTypeCreator.REQUIRED;
  private static final ParameterizedTypeCreator Q = ParameterizedTypeCreator.NULLABLE;
  private static final ParameterizedType ANY1 = P.parameter("any1");

  static Stream<Arguments> catalogReturns() {
    return Stream.of(
        Arguments.of(
            "string_split:vchar_vchar",
            R.list(R.varChar(20)),
            List.of(R.varChar(20), R.varChar(20))),
        Arguments.of(
            "regexp_string_split:vchar_vchar",
            R.list(R.varChar(20)),
            List.of(R.varChar(20), R.varChar(20))),
        Arguments.of(
            "regexp_match_substring_all:vchar_vchar_i64_i64",
            R.list(R.varChar(20)),
            List.of(R.varChar(20), R.varChar(20), R.I64, R.I64)),
        Arguments.of("sort:list", R.list(N.I32), List.of(R.list(N.I32))),
        Arguments.of("sort:list", N.list(R.I32), List.of(N.list(R.I32))),
        Arguments.of(
            "filter:list_func",
            R.list(N.I32),
            List.of(R.list(N.I32), R.func(List.of(N.I32), N.BOOLEAN))),
        Arguments.of(
            "transform:list_func",
            R.list(N.varChar(30)),
            List.of(R.list(N.I32), R.func(List.of(N.I32), N.varChar(30)))));
  }

  @ParameterizedTest
  @MethodSource("catalogReturns")
  void derivesCatalogListReturns(String key, Type expected, List<Type> actual) {
    SimpleExtension.Function function =
        DefaultExtensionCatalog.DEFAULT_COLLECTION.scalarFunctions().stream()
            .filter(f -> f.key().equals(key))
            .findFirst()
            .orElseThrow();
    assertDerives(function, expected, actual);
  }

  @Test
  void recursesThroughMapsStructsAndFunctions() {
    ParameterizedType declaration =
        P.mapE(
            P.varCharE("L"),
            P.structE(P.listE(ANY1), P.funcE(List.of(ANY1), P.decimalE("P", "S"))));
    Type actual =
        R.map(R.varChar(12), R.struct(R.list(N.I64), R.func(List.of(N.I64), R.decimal(15, 3))));
    assertDerives(function(declaration, declaration), actual, List.of(actual));
  }

  @Test
  void concreteNestedReturnTypesNeedNoParameters() {
    assertDerives(function(P.listE(R.I32)), R.list(R.I32), List.of());
  }

  @Test
  void sharedNestedWildcardsKeepInnerNullability() {
    SimpleExtension.Function pair = function(P.listE(ANY1), P.listE(ANY1), P.listE(ANY1));
    assertDerives(pair, N.list(N.I32), List.of(N.list(N.I32), R.list(N.I32)));
    assertInvalid(pair, R.list(R.I32), R.list(N.I32));
    assertInvalid(pair, R.list(R.I32), R.list(R.I64));
  }

  @Test
  void nullableWildcardMarkersAreSubstitutedAcrossArgumentShapes() {
    ParameterizedType nullableElement = P.listE(Q.parameter("any1"));
    // These are the scalar-binding examples for j(any1, list<any1?>), in both argument orders.
    SimpleExtension.Function forward = function(nullableElement, ANY1, nullableElement);
    SimpleExtension.Function reverse = function(nullableElement, nullableElement, ANY1);
    assertDerives(forward, R.list(N.I32), List.of(R.I32, R.list(N.I32)));
    assertDerives(reverse, R.list(N.I32), List.of(R.list(N.I32), R.I32));
    assertInvalid(forward, R.I32, R.list(R.I32));
    assertInvalid(forward, R.I32, R.list(N.I64));
    assertInvalid(reverse, R.list(N.I64), R.I32);
    // A nullable marker does not remove nullability already bound by an unmarked nested wildcard.
    assertDerives(
        function(P.listE(ANY1), nullableElement, P.listE(ANY1)),
        R.list(N.I32),
        List.of(R.list(N.I32), R.list(N.I32)));
  }

  @Test
  void nestedIntegerParametersAndLiteralsAreChecked() {
    ParameterizedType list = P.listE(P.decimalE("P", "0"));
    SimpleExtension.Function pair = function(P.listE(P.decimalE("P", "0")), list, list);
    assertDerives(
        pair,
        R.list(R.decimal(12, 0)),
        List.of(R.list(R.decimal(12, 0)), R.list(R.decimal(12, 0))));
    assertInvalid(pair, R.list(R.decimal(12, 0)), R.list(R.decimal(13, 0)));
    assertInvalid(pair, R.list(R.decimal(12, 0)), R.list(R.decimal(12, 1)));
  }

  @Test
  void rejectsWrongContainerShapesAndArity() {
    assertInvalid(function(R.I64, P.listE(ANY1)), R.I64);
    assertInvalid(function(R.I64, P.mapE(ANY1, ANY1)), R.list(R.I32));
    assertInvalid(function(R.I64, P.structE(ANY1, ANY1)), R.struct(R.I32));
    assertInvalid(
        function(R.I64, P.funcE(List.of(ANY1), ANY1)), R.func(List.of(R.I32, R.I32), R.I32));
    assertInvalid(function(R.I64, P.listE(P.listE(ANY1))), R.list(N.list(R.I32)));
    assertInvalid(function(R.I64, P.listE(R.I32)), R.list(N.I32));
  }

  @Test
  void concreteReturnsStillRejectIncorrectNestedMembers() {
    List<ParameterizedType> patterns =
        List.of(
            P.mapE(R.STRING, ANY1),
            P.mapE(ANY1, R.I32),
            P.structE(R.I32, ANY1),
            P.structE(ANY1, R.I32),
            P.funcE(List.of(R.I32), ANY1),
            P.funcE(List.of(ANY1), R.BOOLEAN));
    List<Type> actual =
        List.of(
            R.map(R.I64, R.I32),
            R.map(R.STRING, N.I32),
            R.struct(R.I64, R.I32),
            R.struct(R.I32, N.I32),
            R.func(List.of(N.I32), R.I64),
            R.func(List.of(R.I32), N.BOOLEAN));
    for (int index = 0; index < patterns.size(); index++) {
      SimpleExtension.Function function = function(R.I64, patterns.get(index));
      Type argument = actual.get(index);
      assertThrows(
          UnsupportedOperationException.class, () -> function.resolveType(List.of(argument)));
      assertInvalid(function, argument);
      assertThrows(
          InvalidFunctionBindingException.class,
          () ->
              FunctionBindingResolver.resolveAndValidate(
                  function, List.of(ResolvedArgument.value(argument)), List.of(), R.I64));
    }
  }

  @Test
  void containerOuterNullabilityFollowsTheFunctionPolicy() {
    for (SimpleExtension.Nullability policy : SimpleExtension.Nullability.values()) {
      SimpleExtension.Function function =
          ImmutableSimpleExtension.ScalarFunctionVariant.builder()
              .from(function(P.listE(ANY1), P.listE(ANY1)))
              .nullability(policy)
              .build();
      assertDerives(function, R.list(N.I32), List.of(R.list(N.I32)));
      if (policy == SimpleExtension.Nullability.DISCRETE) {
        assertThrows(
            InvalidFunctionBindingException.class,
            () ->
                FunctionBindingResolver.resolveAndValidate(
                    function,
                    List.of(ResolvedArgument.value(N.list(N.I32))),
                    List.of(),
                    R.list(N.I32)));
      } else {
        Type expected = TypeCreator.of(policy == SimpleExtension.Nullability.MIRROR).list(N.I32);
        assertDerives(function, expected, List.of(N.list(N.I32)));
      }
    }
  }

  @Test
  void validatesTheDerivedElementTypeAndNullability() {
    SimpleExtension.Function function = function(P.listE(P.varCharE("L")), P.varCharE("L"));
    List<ResolvedArgument> arguments = List.of(ResolvedArgument.value(R.varChar(20)));
    for (Type wrong :
        List.of(R.list(R.varChar(19)), R.list(N.varChar(20)), N.list(R.varChar(20)))) {
      assertThrows(
          InvalidFunctionBindingException.class,
          () -> FunctionBindingResolver.resolveAndValidate(function, arguments, List.of(), wrong));
    }
  }

  @Test
  void aPlainAnyStillHasNoReturnBinding() {
    SimpleExtension.Function function = function(P.listE(P.parameter("any")), P.parameter("any"));
    assertThrows(UnsupportedOperationException.class, () -> function.resolveType(List.of(R.I32)));
    assertInvalid(function, R.I32);
  }

  @Test
  void catalogQuantileStillHasAnUnboundElementType() {
    SimpleExtension.Function quantile =
        DefaultExtensionCatalog.DEFAULT_COLLECTION.aggregateFunctions().stream()
            .filter(f -> f.key().equals("quantile:req_req_i64_any"))
            .findFirst()
            .orElseThrow();
    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class, () -> quantile.resolveType(List.of(R.I64, R.I32)));
    assertTrue(error.getMessage().contains("Unbound type parameter 'any'"), error.getMessage());
  }

  @Test
  void aNullableMarkerAloneCannotDetermineTheVariablesOwnNullability() {
    ParameterizedType nullableElement = P.listE(Q.parameter("any1"));
    assertDerives(
        function(nullableElement, nullableElement), R.list(N.I32), List.of(R.list(N.I32)));
    // Both any1=i32 and any1=i32? satisfy list<any1?>. Without another occurrence, the
    // nullability of an unmarked return element is not determined by the argument.
    assertInvalid(function(P.listE(ANY1), nullableElement), R.list(N.I32));
  }

  @Test
  void containerTypeArgumentsAlsoBindParameters() {
    ParameterizedType list = P.listE(P.varCharE("L"));
    SimpleExtension.Function function =
        ImmutableSimpleExtension.ScalarFunctionVariant.builder()
            .from(function(list))
            .args(List.of(SimpleExtension.TypeArgument.builder().type(list).build()))
            .build();
    assertEquals(
        R.list(R.varChar(17)),
        FunctionBindingResolver.deriveOutputType(
            function, List.of(ResolvedArgument.type(R.list(R.varChar(17))))));
  }

  @Test
  void variadicContainersRespectParameterConsistencyAndLiteralConstraints() {
    ParameterizedType list = P.listE(P.decimalE("P", "0"));
    for (SimpleExtension.VariadicBehavior.ParameterConsistency consistency :
        SimpleExtension.VariadicBehavior.ParameterConsistency.values()) {
      SimpleExtension.Function function =
          ImmutableSimpleExtension.ScalarFunctionVariant.builder()
              .from(function(list, list))
              .variadic(
                  ImmutableSimpleExtension.VariadicBehavior.builder()
                      .min(1)
                      .parameterConsistency(consistency)
                      .build())
              .build();
      List<Type> actual = List.of(R.list(R.decimal(12, 0)), R.list(R.decimal(15, 0)));
      if (consistency == SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT) {
        assertInvalid(function, actual.toArray(new Type[0]));
      } else {
        assertDerives(function, actual.get(0), actual);
      }
      assertInvalid(function, actual.get(0), R.list(R.decimal(15, 1)));
    }
  }

  private static SimpleExtension.ScalarFunctionVariant function(
      TypeExpression result, ParameterizedType... parameters) {
    return ImmutableSimpleExtension.ScalarFunctionVariant.builder()
        .urn("extension:io.substrait:container_test")
        .name("container")
        .returnType(result)
        .args(
            Arrays.stream(parameters)
                .map(p -> SimpleExtension.ValueArgument.builder().value(p).build())
                .collect(Collectors.toList()))
        .build();
  }

  private static void assertDerives(
      SimpleExtension.Function function, Type expected, List<Type> actual) {
    assertEquals(expected, function.resolveType(actual));
    List<ResolvedArgument> arguments =
        actual.stream().map(ResolvedArgument::value).collect(Collectors.toList());
    assertEquals(expected, FunctionBindingResolver.deriveOutputType(function, arguments));
    FunctionBindingResolver.resolveAndValidate(function, arguments, List.of(), expected);
  }

  private static void assertInvalid(SimpleExtension.Function function, Type... actual) {
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.deriveOutputType(
                function,
                Arrays.stream(actual).map(ResolvedArgument::value).collect(Collectors.toList())));
  }
}
