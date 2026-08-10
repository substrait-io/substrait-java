package io.substrait.extension;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.io.Resources;
import io.substrait.expression.FunctionOption;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class FunctionBindingResolverTest {

  static final TypeCreator R = TypeCreator.REQUIRED;
  static final TypeCreator N = TypeCreator.NULLABLE;

  /** Declarations that the standard extensions do not provide (plain wildcards, DISCRETE). */
  static final SimpleExtension.ExtensionCollection TEST_EXTENSIONS = loadTestExtensions();

  final SimpleExtension.ExtensionCollection extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION;

  private static SimpleExtension.ExtensionCollection loadTestExtensions() {
    try {
      return SimpleExtension.load(
          Resources.toString(
              Resources.getResource("extensions/binding_extensions.yaml"), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private SimpleExtension.AggregateFunctionVariant aggregate(String urn, String key) {
    return extensions.getAggregateFunction(SimpleExtension.FunctionAnchor.of(urn, key));
  }

  private SimpleExtension.ScalarFunctionVariant scalar(String urn, String key) {
    return extensions.getScalarFunction(SimpleExtension.FunctionAnchor.of(urn, key));
  }

  private ResolvedFunctionBinding binding(
      SimpleExtension.Function declaration, List<ResolvedArgument> arguments) {
    return ResolvedFunctionBinding.builder()
        .anchor(declaration.getAnchor())
        .declaration(declaration)
        .arguments(arguments)
        .options(List.of())
        .build();
  }

  @Test
  void resolvesConcreteIntegerSum() {
    SimpleExtension.AggregateFunctionVariant sum =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "sum:i32");
    ResolvedFunctionBinding binding =
        FunctionBindingResolver.resolveAndValidate(
            sum, List.of(ResolvedArgument.value(R.I32)), List.of(), N.I64);
    assertEquals(N.I64, binding.outputType());
    assertEquals(sum.getAnchor(), binding.anchor());
  }

  @Test
  void resolvesDecimalSumWidth() {
    SimpleExtension.AggregateFunctionVariant sum =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL, "sum:dec");
    ResolvedFunctionBinding binding =
        FunctionBindingResolver.resolveAndValidate(
            sum, List.of(ResolvedArgument.value(R.decimal(10, 2))), List.of(), N.decimal(38, 2));
    assertEquals(N.decimal(38, 2), binding.outputType());
  }

  @Test
  void appliesMirrorNullabilityForScalars() {
    // add:i32_i32 has MIRROR nullability: a nullable operand makes the result nullable.
    SimpleExtension.ScalarFunctionVariant add =
        scalar(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "add:i32_i32");
    ResolvedFunctionBinding required =
        FunctionBindingResolver.resolve(
            add, List.of(ResolvedArgument.value(R.I32), ResolvedArgument.value(R.I32)), List.of());
    assertFalse(required.outputType().nullable());
    ResolvedFunctionBinding nullable =
        FunctionBindingResolver.resolve(
            add, List.of(ResolvedArgument.value(N.I32), ResolvedArgument.value(R.I32)), List.of());
    assertTrue(nullable.outputType().nullable());
  }

  @Test
  void derivesWildcardReturnFromArgument() {
    // any_value(any1) -> any1?: the wildcard return resolves to the argument type, made nullable.
    SimpleExtension.AggregateFunctionVariant anyValue =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_AGGREGATE_GENERIC, "any_value:any");
    assertEquals(
        N.I32,
        FunctionBindingResolver.deriveOutputType(anyValue, List.of(ResolvedArgument.value(R.I32))));
  }

  @Test
  void rejectsDeclaredOutputTypeThatDivergesFromDeclaration() {
    SimpleExtension.AggregateFunctionVariant sum =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "sum:i32");
    // The standard declaration derives i64? for sum(i32); a plan declaring required i64 is invalid.
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                sum, List.of(ResolvedArgument.value(R.I32)), List.of(), R.I64));
  }

  @Test
  void rejectsIncompatibleArgumentType() {
    SimpleExtension.AggregateFunctionVariant sum =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "sum:i32");
    // Validation is opt-in: resolve() alone captures identity and does not check the signature.
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                sum, List.of(ResolvedArgument.value(R.FP64)), List.of(), N.I64));
  }

  @Test
  void rejectsInconsistentNumberedWildcard() {
    // nullif(any1, any1): the numbered wildcard must bind to one type; i32 + string is invalid.
    SimpleExtension.ScalarFunctionVariant nullif =
        scalar(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "nullif:any_any");
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                nullif,
                List.of(ResolvedArgument.value(R.I32), ResolvedArgument.value(R.STRING)),
                List.of(),
                R.I32));
  }

  @Test
  void rejectsWrongArity() {
    SimpleExtension.AggregateFunctionVariant sum =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "sum:i32");
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                sum,
                List.of(ResolvedArgument.value(R.I32), ResolvedArgument.value(R.I32)),
                List.of(),
                N.I64));
  }

  @Test
  void outputTypeIsDerivedNotSettable() {
    SimpleExtension.AggregateFunctionVariant sum =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "sum:i32");
    // There is no builder setter for outputType; it is always derived from the declaration.
    assertEquals(N.I64, binding(sum, List.of(ResolvedArgument.value(R.I32))).outputType());
  }

  @Test
  void identityIncludesArgumentsAndEnumValues() {
    SimpleExtension.AggregateFunctionVariant sum =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "sum:i32");
    assertEquals(
        binding(sum, List.of(ResolvedArgument.value(R.I32))),
        binding(sum, List.of(ResolvedArgument.value(R.I32))));
    // Different value-argument types are distinct identities.
    assertNotEquals(
        binding(sum, List.of(ResolvedArgument.value(R.I32))),
        binding(sum, List.of(ResolvedArgument.value(R.FP64))));
    // Different enum options are distinct identities: std_dev declares a leading "distribution"
    // enum argument, so POPULATION and SAMPLE are two different functions on the same anchor.
    SimpleExtension.AggregateFunctionVariant stdDev = stdDev();
    assertNotEquals(
        binding(stdDev, stdDevArguments("POPULATION")), binding(stdDev, stdDevArguments("SAMPLE")));
  }

  @Test
  void identityKeepsEnumOptionsAsSpelled() {
    SimpleExtension.AggregateFunctionVariant stdDev = stdDev();
    // Identity is conservative rather than spec-exact: the plan's spelling is preserved verbatim
    // (see acceptsEnumOptionRegardlessOfCase for the case-insensitive matching the spec requires),
    // so two spellings of one option stay distinct instead of one of them being rewritten.
    assertNotEquals(
        binding(stdDev, stdDevArguments("SAMPLE")), binding(stdDev, stdDevArguments("sample")));
    // An unspecified option is not the same as any specified one either.
    assertNotEquals(
        binding(
            stdDev,
            List.of(ResolvedArgument.unspecifiedEnumOption(), ResolvedArgument.value(R.FP32))),
        binding(stdDev, stdDevArguments("SAMPLE")));
  }

  @Test
  void optionsAreKeptAsSpelledButMatchedCaseInsensitively() {
    SimpleExtension.AggregateFunctionVariant count =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_AGGREGATE_GENERIC, "count:any");
    ResolvedFunctionBinding binding =
        FunctionBindingResolver.resolveAndValidate(
            count,
            List.of(ResolvedArgument.value(R.I32)),
            List.of(FunctionOption.builder().name("Overflow").addValues("ERROR").build()),
            R.I64);
    // The declaration spells the option "overflow" with values [SILENT, SATURATE, ERROR]; matching
    // ignores case, but the binding still reports what the plan asked for.
    assertEquals(
        List.of(FunctionOption.builder().name("Overflow").addValues("ERROR").build()),
        binding.options());
  }

  @Test
  void acceptsEnumOptionRegardlessOfCase() {
    SimpleExtension.AggregateFunctionVariant stdDev = stdDev();
    ResolvedFunctionBinding binding =
        FunctionBindingResolver.resolveAndValidate(
            stdDev, stdDevArguments("sample"), List.of(), N.FP32);
    assertEquals(N.FP32, binding.outputType());
  }

  @Test
  void rejectsUnknownEnumOption() {
    SimpleExtension.AggregateFunctionVariant stdDev = stdDev();
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                stdDev, stdDevArguments("MEDIAN"), List.of(), N.FP32));
  }

  @Test
  void rejectsUnspecifiedRequiredEnumOption() {
    SimpleExtension.AggregateFunctionVariant stdDev = stdDev();
    // std_dev's distribution argument is required, so leaving it unspecified does not satisfy the
    // declaration — but it is reported as a missing option, not as an unknown one.
    InvalidFunctionBindingException e =
        assertThrows(
            InvalidFunctionBindingException.class,
            () ->
                FunctionBindingResolver.resolveAndValidate(
                    stdDev,
                    List.of(
                        ResolvedArgument.unspecifiedEnumOption(), ResolvedArgument.value(R.FP32)),
                    List.of(),
                    N.FP32));
    assertTrue(e.getMessage().contains("left unspecified"), e.getMessage());
  }

  @Test
  void plainWildcardArgumentsBindIndependently() {
    // pair(any, any): plain wildcards are matched per argument, so two different types are valid.
    SimpleExtension.ScalarFunctionVariant pair = testScalar("pair:any_any");
    assertDoesNotThrow(
        () ->
            FunctionBindingResolver.resolveAndValidate(
                pair,
                List.of(ResolvedArgument.value(R.I32), ResolvedArgument.value(R.STRING)),
                List.of(),
                R.BOOLEAN));
  }

  @Test
  void numberedWildcardArgumentsMustAgree() {
    // same_pair(any1, any1): the numbered wildcard has to bind to one type across the invocation.
    SimpleExtension.ScalarFunctionVariant samePair = testScalar("same_pair:any_any");
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                samePair,
                List.of(ResolvedArgument.value(R.I32), ResolvedArgument.value(R.STRING)),
                List.of(),
                R.BOOLEAN));
  }

  @Test
  void discreteDeclarationFixesWildcardNullability() {
    // discrete_wildcard(any1) -> any1 with DISCRETE nullability: the declared required argument
    // does not accept a nullable one, and the derived type keeps the declared nullability.
    SimpleExtension.ScalarFunctionVariant discrete = testScalar("discrete_wildcard:any");
    assertEquals(
        R.I32,
        FunctionBindingResolver.resolveAndValidate(
                discrete, List.of(ResolvedArgument.value(R.I32)), List.of(), R.I32)
            .outputType());
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                discrete, List.of(ResolvedArgument.value(N.I32)), List.of(), N.I32));
  }

  @Test
  void discreteDeclarationFixesNullabilityOfParameterizedTypes() {
    // discrete_decimal(DECIMAL?<P,S>) with DISCRETE nullability: the declared nullable argument is
    // part of the signature, so a required decimal does not satisfy it either.
    SimpleExtension.ScalarFunctionVariant discrete = testScalar("discrete_decimal:dec");
    assertDoesNotThrow(
        () ->
            FunctionBindingResolver.resolveAndValidate(
                discrete, List.of(ResolvedArgument.value(N.decimal(10, 2))), List.of(), R.BOOLEAN));
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                discrete, List.of(ResolvedArgument.value(R.decimal(10, 2))), List.of(), R.BOOLEAN));
  }

  @Test
  void numericLiteralParameterConstrainsTheActualType() {
    // zero_scale(DECIMAL<P,0>): the literal 0 is a constraint, not a parameter name — an actual
    // decimal whose scale is not 0 must be rejected, not silently accepted.
    SimpleExtension.ScalarFunctionVariant zeroScale = testScalar("zero_scale:dec");
    assertDoesNotThrow(
        () ->
            FunctionBindingResolver.resolveAndValidate(
                zeroScale,
                List.of(ResolvedArgument.value(R.decimal(10, 0))),
                List.of(),
                R.BOOLEAN));
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                zeroScale,
                List.of(ResolvedArgument.value(R.decimal(10, 2))),
                List.of(),
                R.BOOLEAN));
  }

  @Test
  void caseCollidingDeclaredOptionNamesAreRejected() {
    // cased_options declares both "rounding" and "ROUNDING"; option names match
    // case-insensitively, so the declaration is ambiguous and validation refuses to guess.
    SimpleExtension.ScalarFunctionVariant cased = testScalar("cased_options:i32");
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                cased, List.of(ResolvedArgument.value(R.I32)), List.of(), R.BOOLEAN));
  }

  @Test
  void typeArgumentDeclaredTypeIsValidated() {
    // typed(<type: i32>, x: i64): the declared pattern constrains the supplied type argument, not
    // just its kind — a string type argument must be rejected.
    SimpleExtension.ScalarFunctionVariant typed = testScalar("typed:type_i64");
    assertDoesNotThrow(
        () ->
            FunctionBindingResolver.resolveAndValidate(
                typed,
                List.of(ResolvedArgument.type(R.I32), ResolvedArgument.value(R.I64)),
                List.of(),
                R.I64));
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                typed,
                List.of(ResolvedArgument.type(R.STRING), ResolvedArgument.value(R.I64)),
                List.of(),
                R.I64));
  }

  @Test
  void typeArgumentBindsReturnTypeParameters() {
    // typed_decimal(<type: DECIMAL<P,S>>) -> DECIMAL<P,S>: the supplied type argument binds P and
    // S, so the return derives from it rather than failing as unbound.
    SimpleExtension.ScalarFunctionVariant typedDecimal = testScalar("typed_decimal:type");
    assertEquals(
        R.decimal(10, 2),
        FunctionBindingResolver.deriveOutputType(
            typedDecimal, List.of(ResolvedArgument.type(R.decimal(10, 2)))));
  }

  @Test
  void inconsistentVariadicKeepsLiteralConstraints() {
    // zero_scale_variadic(DECIMAL<P,0>...) is INCONSISTENT: P may differ between repetitions, but
    // the literal scale 0 constrains every repetition, not only the first.
    SimpleExtension.ScalarFunctionVariant zeroScale = testScalar("zero_scale_variadic:dec");
    assertDoesNotThrow(
        () ->
            FunctionBindingResolver.resolveAndValidate(
                zeroScale,
                List.of(
                    ResolvedArgument.value(R.decimal(10, 0)),
                    ResolvedArgument.value(R.decimal(12, 0))),
                List.of(),
                R.BOOLEAN));
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                zeroScale,
                List.of(
                    ResolvedArgument.value(R.decimal(10, 0)),
                    ResolvedArgument.value(R.decimal(10, 2))),
                List.of(),
                R.BOOLEAN));
  }

  @Test
  void failsClosedOnANestedShapeItCannotCheck() {
    SimpleExtension.ScalarFunctionVariant listPair = testScalar("list_pair:list_list");
    // A declared list<any1> against a non-list actual used to be accepted silently; a strict
    // validator must reject a shape it cannot check rather than pass it.
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                listPair,
                List.of(ResolvedArgument.value(R.I32), ResolvedArgument.value(R.I32)),
                List.of(),
                R.BOOLEAN));
    // list<i32> vs list<i32?> must not bind either: nested nullability is part of the structural
    // match, which is exactly the check this validator cannot do yet — so it fails closed here too.
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                listPair,
                List.of(
                    ResolvedArgument.value(R.list(R.I32)), ResolvedArgument.value(R.list(N.I32))),
                List.of(),
                R.BOOLEAN));
  }

  @Test
  void variadicArgumentsBindBeyondTheDeclaredTail() {
    // coalesce(any1...) declares its argument once but accepts it repeatedly; every actual
    // argument has to agree on the wildcard, including the ones past the declared tail.
    SimpleExtension.ScalarFunctionVariant coalesce =
        scalar(DefaultExtensionCatalog.FUNCTIONS_COMPARISON, "coalesce:any");
    assertEquals(
        R.I32,
        FunctionBindingResolver.deriveOutputType(
            coalesce,
            List.of(
                ResolvedArgument.value(R.I32),
                ResolvedArgument.value(R.I32),
                ResolvedArgument.value(R.I32))));
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.deriveOutputType(
                coalesce,
                List.of(
                    ResolvedArgument.value(R.I32),
                    ResolvedArgument.value(R.I32),
                    ResolvedArgument.value(R.STRING))));
  }

  @Test
  void sharedTypeParametersMustAgreeEvenWithAConcreteReturn() {
    // decimal_pair(DECIMAL<P,S>, DECIMAL<P,S>) -> boolean: nothing about the return depends on P
    // and S, but one signature cannot bind them to two different values.
    SimpleExtension.ScalarFunctionVariant pair = testScalar("decimal_pair:dec_dec");
    assertDoesNotThrow(
        () ->
            FunctionBindingResolver.resolveAndValidate(
                pair,
                List.of(
                    ResolvedArgument.value(R.decimal(10, 2)),
                    ResolvedArgument.value(R.decimal(10, 2))),
                List.of(),
                R.BOOLEAN));
    assertThrows(
        InvalidFunctionBindingException.class,
        () ->
            FunctionBindingResolver.resolveAndValidate(
                pair,
                List.of(
                    ResolvedArgument.value(R.decimal(10, 2)),
                    ResolvedArgument.value(R.decimal(20, 5))),
                List.of(),
                R.BOOLEAN));
  }

  @Test
  void optionsCannotChangeAfterTheBindingIsBuilt() {
    SimpleExtension.AggregateFunctionVariant count =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_AGGREGATE_GENERIC, "count:any");
    List<FunctionOption> options =
        new ArrayList<>(
            List.of(FunctionOption.builder().name("overflow").addValues("error").build()));
    ResolvedFunctionBinding binding =
        FunctionBindingResolver.resolve(count, List.of(ResolvedArgument.value(R.I32)), options);
    // Mutating the list the caller handed over must not change the binding's identity: the option
    // list is copied and every FunctionOption in it is immutable in its own right.
    options.add(FunctionOption.builder().name("overflow").addValues("silent").build());
    assertEquals(1, binding.options().size());
    // Lookup by name matches case-insensitively, as the spec requires, while the stored option
    // keeps the plan's spelling.
    assertEquals(Optional.of(List.of("error")), binding.option("OVERFLOW"));
  }

  @Test
  void repeatedOptionNamesAreKept() {
    // A plan may repeat an option name; a map keyed by name would silently drop all but one.
    SimpleExtension.AggregateFunctionVariant count =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_AGGREGATE_GENERIC, "count:any");
    List<FunctionOption> options =
        List.of(
            FunctionOption.builder().name("overflow").addValues("error").build(),
            FunctionOption.builder().name("overflow").addValues("silent").build());
    ResolvedFunctionBinding binding =
        FunctionBindingResolver.resolve(count, List.of(ResolvedArgument.value(R.I32)), options);
    assertEquals(options, binding.options());
  }

  @Test
  void identityDistinguishesExtensionsByAnchor() {
    SimpleExtension.AggregateFunctionVariant sumI32 =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "sum:i32");
    SimpleExtension.AggregateFunctionVariant sumI64 =
        aggregate(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "sum:i64");
    assertNotEquals(
        binding(sumI32, List.of(ResolvedArgument.value(R.I32))),
        binding(sumI64, List.of(ResolvedArgument.value(R.I64))));
  }

  private SimpleExtension.AggregateFunctionVariant stdDev() {
    return aggregate(DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC, "std_dev:req_fp32");
  }

  private SimpleExtension.ScalarFunctionVariant testScalar(String key) {
    return TEST_EXTENSIONS.getScalarFunction(
        SimpleExtension.FunctionAnchor.of("extension:test:binding_extensions", key));
  }

  private List<ResolvedArgument> stdDevArguments(String distribution) {
    return List.of(ResolvedArgument.enumOption(distribution), ResolvedArgument.value(R.FP32));
  }
}
