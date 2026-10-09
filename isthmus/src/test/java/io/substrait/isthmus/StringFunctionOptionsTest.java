package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.FunctionOption;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.relation.Project;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.calcite.DataContexts;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class StringFunctionOptionsTest extends PlanTestBase {
  private final ScalarFunctionConverter scalar =
      new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory);
  private final WindowFunctionConverter window =
      new WindowFunctionConverter(extensions.windowFunctions(), typeFactory);
  private final ExpressionRexConverter toRex =
      new ExpressionRexConverter(typeFactory, scalar, window, TypeConverter.DEFAULT);
  private final RexExpressionConverter fromRex =
      new RexExpressionConverter(
          null,
          Stream.concat(CallConverters.defaults(TypeConverter.DEFAULT).stream(), Stream.of(scalar))
              .toList(),
          window,
          TypeConverter.DEFAULT);

  private static final class Case {
    private final String name;
    private final String query;
    private final String option;
    private final String supported;
    private final String unsupported;
    private final String expected;

    private Case(
        String name,
        String query,
        String option,
        String supported,
        String unsupported,
        String expected) {
      this.name = name;
      this.query = query;
      this.option = option;
      this.supported = supported;
      this.unsupported = unsupported;
      this.expected = expected;
    }
  }

  static Stream<Case> cases() {
    return Stream.of(
        new Case(
            "like", "a LIKE b", "case_sensitivity", "CASE_SENSITIVE", "CASE_INSENSITIVE", "false"),
        new Case(
            "replace",
            "replace(a,b,a)",
            "case_sensitivity",
            "CASE_SENSITIVE",
            "CASE_INSENSITIVE",
            "Abc"),
        new Case(
            "starts_with",
            "starts_with(a,b)",
            "case_sensitivity",
            "CASE_SENSITIVE",
            "CASE_INSENSITIVE_ASCII",
            "false"),
        new Case(
            "ends_with",
            "ends_with(a,b)",
            "case_sensitivity",
            "CASE_SENSITIVE",
            "CASE_INSENSITIVE",
            "false"),
        new Case(
            "strpos",
            "position(b IN a)",
            "case_sensitivity",
            "CASE_SENSITIVE",
            "CASE_INSENSITIVE",
            "0"),
        new Case(
            "substring",
            "substring(a,-1,4)",
            "negative_start",
            "LEFT_OF_BEGINNING",
            "WRAP_FROM_END",
            "ab"),
        new Case("lower", "lower(a)", "char_set", "UTF8", "ASCII_ONLY", "école"),
        new Case("upper", "upper(a)", "char_set", "UTF8", "ASCII_ONLY", "ÉCOLE"),
        new Case("initcap", "initcap(a)", "char_set", "ASCII_ONLY", "UTF8", "éCole"));
  }

  private FunctionOption option(Case c, String... values) {
    return FunctionOption.builder().name(c.option).addValues(values).build();
  }

  private Expression.ScalarFunctionInvocation call(Case c, List<FunctionOption> options) {
    List<FunctionArg> args;
    Type output;
    String key;
    switch (c.name) {
      case "lower", "upper", "initcap" -> {
        args =
            List.of(
                ExpressionCreator.string(
                    false,
                    c.name.equals("initcap")
                        ? "éCOLE"
                        : c.name.equals("upper") ? "école" : "ÉCOLE"));
        output = R.STRING;
        key = c.name + ":str";
      }
      case "substring" -> {
        args =
            List.of(
                ExpressionCreator.string(false, "abcdef"),
                ExpressionCreator.i32(false, -1),
                ExpressionCreator.i32(false, 4));
        output = R.STRING;
        key = "substring:str_i32_i32";
      }
      default -> {
        args =
            new ArrayList<>(
                List.of(
                    ExpressionCreator.string(false, c.name.equals("ends_with") ? "AbC" : "Abc"),
                    ExpressionCreator.string(
                        false,
                        c.name.equals("like") ? "a%" : c.name.equals("ends_with") ? "c" : "a")));
        output = c.name.equals("strpos") ? R.I64 : R.BOOLEAN;
        key = c.name + ":str_str";
        if (c.name.equals("replace")) {
          args.add(ExpressionCreator.string(false, "x"));
          output = R.STRING;
          key += "_str";
        }
      }
    }
    return Expression.ScalarFunctionInvocation.builder()
        .from(sb.scalarFn(DefaultExtensionCatalog.FUNCTIONS_STRING, key, output, args, List.of()))
        .options(options)
        .build();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void exportPinsTheOperatorsBehavior(Case c) throws Exception {
    Project project =
        (Project)
            new SqlToSubstrait()
                .convert(
                    "SELECT " + c.query + " FROM strings",
                    SubstraitCreateStatementParser.processCreateStatementsToCatalog(
                        "CREATE TABLE strings (a VARCHAR, b VARCHAR)"))
                .getRoots()
                .get(0)
                .getInput();
    Expression.ScalarFunctionInvocation expression =
        assertInstanceOf(
            Expression.ScalarFunctionInvocation.class, project.getExpressions().get(0));
    assertEquals(List.of(option(c, c.supported)), expression.options());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void importUsesTheSupportedPreferenceAndPreservesItOnExport(Case c) {
    RexNode rex =
        call(c, List.of(option(c, c.unsupported, c.supported))).accept(toRex, Context.newContext());
    Expression.ScalarFunctionInvocation back =
        assertInstanceOf(Expression.ScalarFunctionInvocation.class, rex.accept(fromRex));
    assertEquals(List.of(option(c, c.supported)), back.options());
    List<RexNode> reduced = new ArrayList<>();
    new RexExecutorImpl(DataContexts.EMPTY).reduce(creator.rex(), List.of(rex), reduced);
    RexLiteral literal = assertInstanceOf(RexLiteral.class, reduced.get(0));
    String result =
        c.name.equals("strpos")
            ? literal.getValueAs(Long.class).toString()
            : c.expected.equals("false")
                ? literal.getValueAs(Boolean.class).toString()
                : literal.getValueAs(String.class);
    assertEquals(c.expected, result);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void rejectsAnIncompatibleOption(Case c) {
    assertThrows(
        UnsupportedOperationException.class,
        () -> call(c, List.of(option(c, c.unsupported))).accept(toRex, Context.newContext()));
  }

  @Test
  void validatesOptionsAgainstTheOperatorChosenByACustomConverter() {
    ScalarFunctionConverter custom =
        new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory) {
          @Override
          public Optional<SqlOperator> getSqlOperatorFromSubstraitFunc(
              String key, Type outputType) {
            return Optional.of(SqlLibraryOperators.ILIKE);
          }
        };
    ExpressionRexConverter converter =
        new ExpressionRexConverter(typeFactory, custom, window, TypeConverter.DEFAULT);
    Case c = cases().filter(sample -> sample.name.equals("like")).findFirst().orElseThrow();
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            call(c, List.of(option(c, "CASE_SENSITIVE"))).accept(converter, Context.newContext()));
  }

  @Test
  void customOptionPolicyPreservesCaseInsensitiveLikeInBothDirections() {
    Case c = cases().filter(sample -> sample.name.equals("like")).findFirst().orElseThrow();
    FunctionOption insensitive = option(c, "CASE_INSENSITIVE");
    ScalarFunctionConverter custom =
        new ScalarFunctionConverter(
            extensions.scalarFunctions(),
            List.of(new FunctionMappings.Sig(SqlLibraryOperators.ILIKE, "like")),
            typeFactory,
            TypeConverter.DEFAULT) {
          @Override
          public Optional<SqlOperator> getSqlOperatorFromSubstraitFunc(
              String key, Type outputType) {
            if (key.equals("like:str_str")) {
              return Optional.of(SqlLibraryOperators.ILIKE);
            }
            return super.getSqlOperatorFromSubstraitFunc(key, outputType);
          }

          @Override
          protected SqlOperator resolveOptions(
              Expression.ScalarFunctionInvocation expression, SqlOperator operator) {
            if (operator == SqlLibraryOperators.ILIKE
                && expression.options().stream()
                    .allMatch(
                        o ->
                            o.getName().equalsIgnoreCase("case_sensitivity")
                                && o.values().stream()
                                    .anyMatch("CASE_INSENSITIVE"::equalsIgnoreCase))) {
              return operator;
            }
            return super.resolveOptions(expression, operator);
          }

          @Override
          protected List<FunctionOption> options(RexCall call, ScalarFunctionVariant function) {
            if (DefaultExtensionCatalog.FUNCTIONS_STRING.equals(function.urn())
                && function.key().equals("like:str_str")
                && call.getOperator() == SqlLibraryOperators.ILIKE) {
              return List.of(insensitive);
            }
            return super.options(call, function);
          }
        };
    ExpressionRexConverter importer =
        new ExpressionRexConverter(typeFactory, custom, window, TypeConverter.DEFAULT);
    RexExpressionConverter exporter =
        new RexExpressionConverter(
            null,
            Stream.concat(
                    CallConverters.defaults(TypeConverter.DEFAULT).stream(), Stream.of(custom))
                .toList(),
            window,
            TypeConverter.DEFAULT);
    RexNode imported = call(c, List.of(insensitive)).accept(importer, Context.newContext());
    assertEquals(
        SqlLibraryOperators.ILIKE, assertInstanceOf(RexCall.class, imported).getOperator());
    Expression.ScalarFunctionInvocation exported =
        assertInstanceOf(Expression.ScalarFunctionInvocation.class, imported.accept(exporter));
    assertEquals(List.of(insensitive), exported.options());
    List<RexNode> reduced = new ArrayList<>();
    new RexExecutorImpl(DataContexts.EMPTY).reduce(creator.rex(), List.of(imported), reduced);
    assertEquals(
        true, assertInstanceOf(RexLiteral.class, reduced.get(0)).getValueAs(Boolean.class));
    assertThrows(
        UnsupportedOperationException.class,
        () -> call(c, List.of(option(c, "CASE_SENSITIVE"))).accept(importer, Context.newContext()));
  }
}
