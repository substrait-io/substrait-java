package io.substrait.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.FunctionBindingResolver;
import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.InvalidFunctionBindingException;
import io.substrait.extension.ResolvedAggregateBinding;
import io.substrait.extension.ResolvedArgument;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ParameterizedTypeCreator;
import io.substrait.function.TypeExpression;
import io.substrait.type.parser.TypeStringParser;
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
  void catalogIndexInBindsItsElementExactly() {
    // index_in(any1, list<any1>): the value's own nullability is stripped before binding, as the
    // spec does for an outermost argument, while the element keeps its, so a nullable element
    // binds any1 to a different type than the value does.
    SimpleExtension.Function function =
        DefaultExtensionCatalog.DEFAULT_COLLECTION.scalarFunctions().stream()
            .filter(f -> f.key().equals("index_in:any_list"))
            .findFirst()
            .orElseThrow();
    for (Type value : List.of(R.I32, N.I32)) {
      assertDerives(function, N.I64, List.of(value, R.list(R.I32)));
      assertInvalid(function, value, R.list(N.I32));
    }
    assertInvalid(function, R.FP64, R.list(R.I32));
    assertInvalid(function, R.I32, R.list(N.FP64));
  }

  @Test
  void topLevelWildcardsBindExactlyInEitherOrder() {
    ParameterizedType list = P.listE(ANY1);
    SimpleExtension.Function forward = function(list, ANY1, list);
    SimpleExtension.Function reverse = function(list, list, ANY1);
    for (Type value : List.of(R.I32, N.I32)) {
      Type expected = TypeCreator.of(value.nullable()).list(R.I32);
      assertDerives(forward, expected, List.of(value, R.list(R.I32)));
      assertDerives(reverse, expected, List.of(R.list(R.I32), value));
      assertInvalid(forward, value, R.list(N.I32));
      assertInvalid(reverse, R.list(N.I32), value);
    }
    assertInvalid(forward, R.I32, R.list(N.FP64));
    assertInvalid(reverse, R.list(N.FP64), R.I32);
    assertInvalid(function(list, ANY1, list, list), R.I32, R.list(R.I32), R.list(N.I32));
    assertInvalid(function(list, list, ANY1, list), R.list(R.I32), R.I32, R.list(N.I32));
    assertInvalid(function(list, list, list, ANY1), R.list(R.I32), R.list(N.I32), R.I32);
  }

  @Test
  void aWildcardBoundOnlyAtTheTopLevelDerivesANestedReturn() {
    // f(any1) -> list<any1>: the element takes the argument's type without its own nullability,
    // which the function's nullability handling applies to the list.
    SimpleExtension.Function wrap = function(P.listE(ANY1), ANY1);
    assertDerives(wrap, R.list(R.I32), List.of(R.I32));
    assertDerives(wrap, N.list(R.I32), List.of(N.I32));
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
    assertDerives(function, R.list(R.varChar(20)), List.of(R.varChar(20)));
    for (Type wrong :
        List.of(R.list(R.varChar(19)), R.list(N.varChar(20)), N.list(R.varChar(20)))) {
      assertThrows(
          InvalidFunctionBindingException.class,
          () -> FunctionBindingResolver.resolveAndValidate(function, arguments, List.of(), wrong));
    }
  }

  @Test
  void catalogFunctionArgumentsRequireFunctionTypes() {
    for (String key : List.of("all_match:list_func", "any_match:list_func")) {
      SimpleExtension.Function function =
          DefaultExtensionCatalog.DEFAULT_COLLECTION.scalarFunctions().stream()
              .filter(f -> f.key().equals(key))
              .findFirst()
              .orElseThrow();
      assertDerives(function, N.BOOLEAN, List.of(R.list(R.I64), R.func(List.of(R.I64), N.BOOLEAN)));
      assertThrows(
          UnsupportedOperationException.class,
          () -> function.resolveType(List.of(R.list(R.I64), N.BOOLEAN)));
      assertInvalid(function, R.list(R.I64), N.BOOLEAN);
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

  @Test
  void aWildcardBoundFromAnElementKeepsItsNullabilityWhereverTheReturnNamesIt() {
    // Under DECLARED_OUTPUT the return's nullability is the declaration's, so a nullable element
    // bound into any1 has to survive a bare any1, a local assigned from it, and a conditional.
    for (String program : List.of("any1", "t = any1\nlist<t>", "list<1 > 0 ? any1 : any1>")) {
      SimpleExtension.Function function =
          ImmutableSimpleExtension.ScalarFunctionVariant.builder()
              .from(
                  function(
                      TypeStringParser.parseExpression(program, "extension:test"), P.listE(ANY1)))
              .nullability(SimpleExtension.Nullability.DECLARED_OUTPUT)
              .build();
      for (Type element : List.of(R.I32, N.I32)) {
        Type expected = program.endsWith(">") ? R.list(element) : element;
        assertDerives(function, expected, List.of(R.list(element)));
      }
    }
  }

  @Test
  void signatureValidationChecksWhatTheArgumentsShare() {
    // matchesDeclaration validates the signature without deriving a type, so it has to bind the
    // parameters the way derivation does: a shared nested wildcard, an integer parameter's literal.
    ParameterizedType list = P.listE(ANY1);
    assertFalse(matches(aggregate(list, list), R.list(R.I32), R.list(R.I64)));
    assertTrue(matches(aggregate(list, list), R.list(R.I32), R.list(R.I32)));
    ParameterizedType decimals = P.listE(P.decimalE("P", "0"));
    assertFalse(matches(aggregate(decimals), R.list(R.decimal(12, 1))));
    assertTrue(matches(aggregate(decimals), R.list(R.decimal(12, 0))));
  }

  @Test
  void theUnboundTypeMatchesNoDeclaredShape() {
    Type unbound = Type.Unbound.builder().build();
    assertInvalid(function(ANY1, P.listE(ANY1)), R.list(unbound));
    assertInvalid(function(R.I64, ANY1, P.listE(ANY1)), R.I32, unbound);
    // Reported as a binding that does not match rather than escaping from a nullability check.
    assertFalse(matches(aggregate(P.listE(Q.parameter("any1"))), R.list(unbound)));
    assertFalse(matches(aggregate(ANY1), unbound));
  }

  private static SimpleExtension.AggregateFunctionVariant aggregate(
      ParameterizedType... parameters) {
    return ImmutableSimpleExtension.AggregateFunctionVariant.builder()
        .urn("extension:io.substrait:container_test")
        .name("container_aggregate")
        .returnType(R.I64)
        .args(
            Arrays.stream(parameters)
                .map(p -> SimpleExtension.ValueArgument.builder().value(p).build())
                .collect(Collectors.toList()))
        .build();
  }

  private static boolean matches(
      SimpleExtension.AggregateFunctionVariant declaration, Type... actual) {
    return FunctionBindingResolver.matchesDeclaration(
        ResolvedAggregateBinding.builder()
            .function(
                FunctionBindingResolver.resolve(
                    declaration,
                    Arrays.stream(actual).map(ResolvedArgument::value).collect(Collectors.toList()),
                    List.of()))
            .phase(Expression.AggregationPhase.INITIAL_TO_RESULT)
            .invocation(Expression.AggregationInvocation.ALL)
            .build());
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
