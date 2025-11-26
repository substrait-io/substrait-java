package io.substrait.type.parser;

import io.substrait.function.ImmutableTypeExpression;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ParameterizedTypeCreator;
import io.substrait.function.TypeExpression;
import io.substrait.function.TypeExpressionCreator;
import io.substrait.type.SubstraitTypeParser;
import io.substrait.type.SubstraitTypeVisitor;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.IntFunction;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

public class ParseToPojo {

  public static Type type(String urn, SubstraitTypeParser.StartContext ctx) {
    Visitor visitor = Visitor.simple(urn);
    return (Type) ctx.accept(visitor);
  }

  public static ParameterizedType parameterizedType(
      String urn, SubstraitTypeParser.StartContext ctx) {
    return (ParameterizedType) ctx.accept(Visitor.parameterized(urn));
  }

  public static TypeExpression typeExpression(String urn, SubstraitTypeParser.StartContext ctx) {
    return ctx.accept(Visitor.expression(urn));
  }

  public static class Visitor implements SubstraitTypeVisitor<TypeExpression> {
    private final VisitorType expressionType;
    private final String urn;

    public static Visitor simple(String urn) {
      return new Visitor(VisitorType.SIMPLE, urn);
    }

    public static Visitor parameterized(String urn) {
      return new Visitor(VisitorType.PARAMETERIZED, urn);
    }

    public static Visitor expression(String urn) {
      return new Visitor(VisitorType.EXPRESSION, urn);
    }

    private Visitor(VisitorType exprType, String urn) {
      this.expressionType = exprType;
      this.urn = urn;
    }

    enum VisitorType {
      SIMPLE,
      PARAMETERIZED,
      EXPRESSION;
    }

    private void checkParameterizedOrExpression() {
      if (this.expressionType != VisitorType.EXPRESSION
          && this.expressionType != VisitorType.PARAMETERIZED) {
        throw new UnsupportedOperationException(
            "This construct can only be used in Parameterized Types or Type Expressions.");
      }
    }

    private void checkExpression() {
      if (this.expressionType != VisitorType.EXPRESSION) {
        throw new UnsupportedOperationException(
            "This construct can only be used in Type Expressions.");
      }
    }

    @Override
    public TypeExpression visitStart(final SubstraitTypeParser.StartContext ctx) {
      return ctx.expr().accept(this);
    }

    @Override
    public Type visitBoolean(final SubstraitTypeParser.BooleanContext ctx) {
      return withNull(ctx).BOOLEAN;
    }

    @Override
    public Type visitI8(final SubstraitTypeParser.I8Context ctx) {
      return withNull(ctx).I8;
    }

    @Override
    public Type visitI16(final SubstraitTypeParser.I16Context ctx) {
      return withNull(ctx).I16;
    }

    @Override
    public Type visitI32(final SubstraitTypeParser.I32Context ctx) {
      return withNull(ctx).I32;
    }

    @Override
    public Type visitI64(final SubstraitTypeParser.I64Context ctx) {
      return withNull(ctx).I64;
    }

    @Override
    public TypeExpression visitTypeLiteral(final SubstraitTypeParser.TypeLiteralContext ctx) {
      return ctx.type().accept(this);
    }

    @Override
    public Type visitFp32(final SubstraitTypeParser.Fp32Context ctx) {
      return withNull(ctx).FP32;
    }

    @Override
    public Type visitFp64(final SubstraitTypeParser.Fp64Context ctx) {
      return withNull(ctx).FP64;
    }

    @Override
    public Type visitString(final SubstraitTypeParser.StringContext ctx) {
      return withNull(ctx).STRING;
    }

    @Override
    public Type visitBinary(final SubstraitTypeParser.BinaryContext ctx) {
      return withNull(ctx).BINARY;
    }

    @Override
    public Type visitTimestamp(final SubstraitTypeParser.TimestampContext ctx) {
      return withNull(ctx).TIMESTAMP;
    }

    @Override
    public Type visitTimestampTz(final SubstraitTypeParser.TimestampTzContext ctx) {
      return withNull(ctx).TIMESTAMP_TZ;
    }

    @Override
    public Type visitDate(final SubstraitTypeParser.DateContext ctx) {
      return withNull(ctx).DATE;
    }

    @Override
    public Type visitTime(final SubstraitTypeParser.TimeContext ctx) {
      return withNull(ctx).TIME;
    }

    @Override
    public Type visitIntervalYear(final SubstraitTypeParser.IntervalYearContext ctx) {
      return withNull(ctx).INTERVAL_YEAR;
    }

    @Override
    public TypeExpression visitIntervalDay(final SubstraitTypeParser.IntervalDayContext ctx) {
      boolean nullable = ctx.isnull != null;
      Object precision = i(ctx.precision);
      if (precision instanceof Integer) {
        return withNull(nullable).intervalDay((Integer) precision);
      }
      if (precision instanceof String) {
        checkParameterizedOrExpression();
        return withNullP(nullable).intervalDayE((String) precision);
      }

      checkExpression();
      return withNullE(nullable).intervalDayE(ctx.precision.accept(this));
    }

    @Override
    public TypeExpression visitIntervalCompound(
        final SubstraitTypeParser.IntervalCompoundContext ctx) {
      boolean nullable = ctx.isnull != null;
      Object precision = i(ctx.precision);
      if (precision instanceof Integer) {
        return withNull(nullable).intervalCompound((Integer) precision);
      }
      if (precision instanceof String) {
        checkParameterizedOrExpression();
        return withNullP(nullable).intervalCompoundE((String) precision);
      }

      checkExpression();
      return withNullE(nullable).intervalCompoundE(ctx.precision.accept(this));
    }

    @Override
    public Type visitUuid(final SubstraitTypeParser.UuidContext ctx) {
      return withNull(ctx).UUID;
    }

    @Override
    public Type visitUserDefined(SubstraitTypeParser.UserDefinedContext ctx) {
      String name = ctx.Identifier().getSymbol().getText();
      return withNull(ctx).userDefined(urn, name);
    }

    @Override
    public TypeExpression visitFixedChar(final SubstraitTypeParser.FixedCharContext ctx) {
      boolean nullable = ctx.isnull != null;
      return of(
          ctx.len,
          withNull(nullable)::fixedChar,
          withNullP(nullable)::fixedCharE,
          withNullE(nullable)::fixedCharE);
    }

    private TypeExpression of(
        SubstraitTypeParser.NumericParameterContext ctx,
        IntFunction<TypeExpression> intFunc,
        Function<String, TypeExpression> strFunc,
        Function<TypeExpression, TypeExpression> exprFunc) {
      TypeExpression type = ctx.accept(this);
      if (type instanceof TypeExpression.IntegerLiteral) {
        return intFunc.apply(((TypeExpression.IntegerLiteral) type).value());
      }
      if (type instanceof ParameterizedType.StringLiteral) {
        checkParameterizedOrExpression();
        return strFunc.apply(((ParameterizedType.StringLiteral) type).value());
      }
      checkExpression();
      return exprFunc.apply(type);
    }

    @Override
    public TypeExpression visitVarChar(final SubstraitTypeParser.VarCharContext ctx) {
      boolean nullable = ctx.isnull != null;
      return of(
          ctx.len,
          withNull(nullable)::varChar,
          withNullP(nullable)::varCharE,
          withNullE(nullable)::varCharE);
    }

    @Override
    public TypeExpression visitFixedBinary(final SubstraitTypeParser.FixedBinaryContext ctx) {
      boolean nullable = ctx.isnull != null;
      return of(
          ctx.len,
          withNull(nullable)::fixedBinary,
          withNullP(nullable)::fixedBinaryE,
          withNullE(nullable)::fixedBinaryE);
    }

    @Override
    public TypeExpression visitDecimal(final SubstraitTypeParser.DecimalContext ctx) {
      boolean nullable = ctx.isnull != null;
      Object precision = i(ctx.precision);
      Object scale = i(ctx.scale);
      if (precision instanceof Integer && scale instanceof Integer) {
        return withNull(nullable).decimal((int) precision, (int) scale);
      }

      if (precision instanceof String && scale instanceof String) {
        checkParameterizedOrExpression();
        return withNullP(nullable).decimalE((String) precision, (String) scale);
      }

      if (precision instanceof String && scale instanceof Integer) {
        checkParameterizedOrExpression();
        return withNullP(nullable).decimalE((String) precision, String.valueOf(scale));
      }

      if (precision instanceof Integer && scale instanceof String) {
        checkParameterizedOrExpression();
        return withNullP(nullable).decimalE(String.valueOf(precision), (String) scale);
      }

      checkExpression();
      return withNullE(nullable).decimalE(ctx.precision.accept(this), ctx.scale.accept(this));
    }

    @Override
    public TypeExpression visitPrecisionTimestamp(
        final SubstraitTypeParser.PrecisionTimestampContext ctx) {
      boolean nullable = ctx.isnull != null;
      Object precision = i(ctx.precision);
      if (precision instanceof Integer) {
        return withNull(nullable).precisionTimestamp((Integer) precision);
      }
      if (precision instanceof String) {
        checkParameterizedOrExpression();
        return withNullP(nullable).precisionTimestampE((String) precision);
      }

      checkExpression();
      return withNullE(nullable).precisionTimestampE(ctx.precision.accept(this));
    }

    @Override
    public TypeExpression visitPrecisionTimestampTZ(
        final SubstraitTypeParser.PrecisionTimestampTZContext ctx) {
      boolean nullable = ctx.isnull != null;
      Object precision = i(ctx.precision);
      if (precision instanceof Integer) {
        return withNull(nullable).precisionTimestampTZ((Integer) precision);
      }
      if (precision instanceof String) {
        checkParameterizedOrExpression();
        return withNullP(nullable).precisionTimestampTZE((String) precision);
      }

      checkExpression();
      return withNullE(nullable).precisionTimestampTZE(ctx.precision.accept(this));
    }

    private Object i(SubstraitTypeParser.NumericParameterContext ctx) {
      TypeExpression type = ctx.accept(this);
      if (type instanceof TypeExpression.IntegerLiteral) {
        return ((TypeExpression.IntegerLiteral) type).value();
      } else if (type instanceof ParameterizedType.StringLiteral) {
        checkParameterizedOrExpression();
        return ((ParameterizedType.StringLiteral) type).value();
      } else {
        checkExpression();
        return type;
      }
    }

    @Override
    public TypeExpression visitStruct(final SubstraitTypeParser.StructContext ctx) {
      boolean nullable = ctx.isnull != null;
      List<TypeExpression> types =
          ctx.expr().stream()
              .map(t -> t.accept(this))
              .collect(java.util.stream.Collectors.toList());
      if (types.stream().allMatch(t -> t instanceof Type)) {
        return withNull(nullable)
            .struct(
                types.stream().map(t -> ((Type) t)).collect(java.util.stream.Collectors.toList()));
      }

      if (types.stream().allMatch(t -> t instanceof ParameterizedType)) {
        checkParameterizedOrExpression();
        return withNullP(nullable)
            .structE(
                types.stream()
                    .map(t -> ((ParameterizedType) t))
                    .collect(java.util.stream.Collectors.toList()));
      }

      checkExpression();
      return withNullE(nullable).structE(types);
    }

    @Override
    public TypeExpression visitNStruct(final SubstraitTypeParser.NStructContext ctx) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TypeExpression visitList(final SubstraitTypeParser.ListContext ctx) {
      boolean nullable = ctx.isnull != null;
      TypeExpression element = ctx.expr().accept(this);
      if (element instanceof Type) {
        return withNull(nullable).list((Type) element);
      }

      if (element instanceof ParameterizedType) {
        checkParameterizedOrExpression();
        return withNullP(nullable).listE((ParameterizedType) element);
      }

      checkExpression();
      return withNullE(nullable).listE(element);
    }

    @Override
    public TypeExpression visitMap(final SubstraitTypeParser.MapContext ctx) {
      boolean nullable = ctx.isnull != null;
      TypeExpression key = ctx.key.accept(this);
      TypeExpression value = ctx.value.accept(this);
      if (key instanceof Type && value instanceof Type) {
        return withNull(nullable).map((Type) key, (Type) value);
      }

      if (key instanceof ParameterizedType && value instanceof ParameterizedType) {
        checkParameterizedOrExpression();
        return withNullP(nullable).mapE((ParameterizedType) key, (ParameterizedType) value);
      }
      checkExpression();
      return withNullE(nullable).mapE(key, value);
    }

    private TypeCreator withNull(SubstraitTypeParser.ScalarTypeContext required) {
      return Type.withNullability(
          ((SubstraitTypeParser.TypeContext) required.parent).isnull != null);
    }

    private TypeCreator withNull(boolean nullable) {
      return Type.withNullability(nullable);
    }

    private TypeExpressionCreator withNullE(boolean nullable) {
      return TypeExpression.withNullability(nullable);
    }

    private ParameterizedTypeCreator withNullP(boolean nullable) {
      return ParameterizedType.withNullability(nullable);
    }

    @Override
    public TypeExpression visitType(final SubstraitTypeParser.TypeContext ctx) {
      if (ctx.scalarType() != null) {
        return ctx.scalarType().accept(this);
      } else if (ctx.parameterizedType() != null) {
        return ctx.parameterizedType().accept(this);
      } else {

        return ctx.anyType().accept(this);
      }
    }

    @Override
    public TypeExpression visitTypeParam(final SubstraitTypeParser.TypeParamContext ctx) {
      checkParameterizedOrExpression();
      boolean nullable = ctx.isnull != null;
      return ParameterizedType.StringLiteral.builder()
          .nullable(nullable)
          .value(ctx.getText())
          .build();
    }

    @Override
    public TypeExpression visitParenExpression(
        final SubstraitTypeParser.ParenExpressionContext ctx) {
      return ctx.expr().accept(this);
    }

    @Override
    public TypeExpression visitIfExpr(final SubstraitTypeParser.IfExprContext ctx) {
      checkExpression();
      return TypeExpression.IfOperation.builder()
          .ifCondition(ctx.ifExpr.accept(this))
          .thenExpr(ctx.thenExpr.accept(this))
          .elseExpr(ctx.elseExpr.accept(this))
          .build();
    }

    @Override
    public TypeExpression visitTernary(final SubstraitTypeParser.TernaryContext ctx) {
      checkExpression();
      return TypeExpression.IfOperation.builder()
          .ifCondition(ctx.ifExpr.accept(this))
          .thenExpr(ctx.thenExpr.accept(this))
          .elseExpr(ctx.elseExpr.accept(this))
          .build();
    }

    @Override
    public TypeExpression visitMultilineDefinition(
        final SubstraitTypeParser.MultilineDefinitionContext ctx) {
      checkExpression();
      List<TypeExpression> exprs =
          ctx.expr().stream()
              .map(t -> t.accept(this))
              .collect(java.util.stream.Collectors.toList());
      List<String> identifiers =
          ctx.Identifier().stream()
              .map(t -> t.getText())
              .collect(java.util.stream.Collectors.toList());
      TypeExpression finalExpr = ctx.finalType.accept(this);

      ImmutableTypeExpression.ReturnProgram.Builder bldr = TypeExpression.ReturnProgram.builder();
      for (int i = 0; i < exprs.size(); i++) {
        bldr.addAssignments(
            TypeExpression.ReturnProgram.Assignment.builder()
                .expr(exprs.get(i))
                .name(identifiers.get(i))
                .build());
      }

      bldr.finalExpression(finalExpr);
      return bldr.build();
    }

    @Override
    public TypeExpression visitBinaryExpr(final SubstraitTypeParser.BinaryExprContext ctx) {
      checkExpression();
      TypeExpression.BinaryOperation.OpType type = getBinaryExpressionType(ctx.op);
      return TypeExpression.BinaryOperation.builder()
          .opType(type)
          .left(ctx.left.accept(this))
          .right(ctx.right.accept(this))
          .build();
    }

    private TypeExpression.BinaryOperation.OpType getBinaryExpressionType(Token token) {
      switch (token.getText().toUpperCase(Locale.ROOT)) {
        case "+":
          return TypeExpression.BinaryOperation.OpType.ADD;
        case "-":
          return TypeExpression.BinaryOperation.OpType.SUBTRACT;
        case "*":
          return TypeExpression.BinaryOperation.OpType.MULTIPLY;
        case "/":
          return TypeExpression.BinaryOperation.OpType.DIVIDE;
        case ">":
          return TypeExpression.BinaryOperation.OpType.GT;
        case "<":
          return TypeExpression.BinaryOperation.OpType.LT;
        case "AND":
          return TypeExpression.BinaryOperation.OpType.AND;
        case "OR":
          return TypeExpression.BinaryOperation.OpType.OR;
        case "=":
          return TypeExpression.BinaryOperation.OpType.EQ;
        case ":=":
          return TypeExpression.BinaryOperation.OpType.COVERS;
        default:
          throw new IllegalStateException("Unexpected value: " + token.getText());
      }
    }

    @Override
    public TypeExpression visitNumericLiteral(final SubstraitTypeParser.NumericLiteralContext ctx) {
      return TypeExpression.IntegerLiteral.builder().value(Integer.parseInt(ctx.getText())).build();
    }

    @Override
    public TypeExpression visitNumericParameterName(
        final SubstraitTypeParser.NumericParameterNameContext ctx) {
      checkParameterizedOrExpression();
      return ParameterizedType.StringLiteral.builder().nullable(false).value(ctx.getText()).build();
    }

    @Override
    public TypeExpression visitNumericExpression(
        final SubstraitTypeParser.NumericExpressionContext ctx) {
      return ctx.expr().accept(this);
    }

    @Override
    public TypeExpression visitAnyType(SubstraitTypeParser.AnyTypeContext anyType) {
      boolean nullable = ((SubstraitTypeParser.TypeContext) anyType.parent).isnull != null;
      return withNullP(nullable).parameter("any");
    }

    @Override
    public TypeExpression visitFunctionCall(final SubstraitTypeParser.FunctionCallContext ctx) {
      checkExpression();
      if (ctx.expr().size() != 2) {
        throw new IllegalStateException("Only two argument functions exist for type expressions.");
      }
      TypeExpression.BinaryOperation.OpType type = getFunctionType(ctx.Identifier().getSymbol());
      return TypeExpression.BinaryOperation.builder()
          .opType(type)
          .left(ctx.expr(0).accept(this))
          .right(ctx.expr(1).accept(this))
          .build();
    }

    private TypeExpression.BinaryOperation.OpType getFunctionType(Token token) {
      switch (token.getText().toUpperCase(Locale.ROOT)) {
        case "MIN":
          return TypeExpression.BinaryOperation.OpType.MIN;
        case "MAX":
          return TypeExpression.BinaryOperation.OpType.MAX;
        default:
          throw new IllegalStateException(
              "The following operation was unrecognized: " + token.getText());
      }
    }

    @Override
    public TypeExpression visitNotExpr(final SubstraitTypeParser.NotExprContext ctx) {
      return TypeExpression.NotOperation.builder().inner(ctx.expr().accept(this)).build();
    }

    @Override
    public TypeExpression visitLiteralNumber(final SubstraitTypeParser.LiteralNumberContext ctx) {
      return i(Integer.parseInt(ctx.getText()));
    }

    protected TypeExpression i(int val) {
      return TypeExpression.IntegerLiteral.builder().value(val).build();
    }

    @Override
    public Type visit(final ParseTree tree) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Type visitChildren(final RuleNode node) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Type visitTerminal(final TerminalNode node) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Type visitErrorNode(final ErrorNode node) {
      throw new UnsupportedOperationException();
    }
  }
}
