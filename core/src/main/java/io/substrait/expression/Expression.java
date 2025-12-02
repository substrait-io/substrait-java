package io.substrait.expression;

import com.google.protobuf.ByteString;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.AggregateFunction;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.immutables.value.Value;

@Value.Enclosing
public interface Expression extends FunctionArg {

  Type getType();

  @Override
  default <R, C extends VisitationContext, E extends Throwable> R accept(
      SimpleExtension.Function fnDef, int argIdx, FuncArgVisitor<R, C, E> fnArgVisitor, C context)
      throws E {
    return fnArgVisitor.visitExpr(fnDef, argIdx, this, context);
  }

  interface Literal extends Expression {
    @Value.Default
    default boolean nullable() {
      return false;
    }
  }

  interface Nested extends Expression {
    @Value.Default
    default boolean nullable() {
      return false;
    }
  }

  <R, C extends VisitationContext, E extends Throwable> R accept(
      ExpressionVisitor<R, C, E> visitor, C context) throws E;

  @Value.Immutable
  abstract class NullLiteral implements Literal {
    public abstract Type type();

    @Override
    public Type getType() {
      return type();
    }

    public static ImmutableExpression.NullLiteral.Builder builder() {
      return ImmutableExpression.NullLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class BoolLiteral implements Literal {
    public abstract Boolean value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).BOOLEAN;
    }

    public static ImmutableExpression.BoolLiteral.Builder builder() {
      return ImmutableExpression.BoolLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class I8Literal implements Literal {
    public abstract int value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).I8;
    }

    public static ImmutableExpression.I8Literal.Builder builder() {
      return ImmutableExpression.I8Literal.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class I16Literal implements Literal {
    public abstract int value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).I16;
    }

    public static ImmutableExpression.I16Literal.Builder builder() {
      return ImmutableExpression.I16Literal.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class I32Literal implements Literal {
    public abstract int value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).I32;
    }

    public static ImmutableExpression.I32Literal.Builder builder() {
      return ImmutableExpression.I32Literal.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class I64Literal implements Literal {
    public abstract long value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).I64;
    }

    public static ImmutableExpression.I64Literal.Builder builder() {
      return ImmutableExpression.I64Literal.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class FP32Literal implements Literal {
    public abstract float value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).FP32;
    }

    public static ImmutableExpression.FP32Literal.Builder builder() {
      return ImmutableExpression.FP32Literal.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class FP64Literal implements Literal {
    public abstract double value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).FP64;
    }

    public static ImmutableExpression.FP64Literal.Builder builder() {
      return ImmutableExpression.FP64Literal.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class StrLiteral implements Literal {
    public abstract String value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).STRING;
    }

    public static ImmutableExpression.StrLiteral.Builder builder() {
      return ImmutableExpression.StrLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class BinaryLiteral implements Literal {
    public abstract ByteString value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).BINARY;
    }

    public static ImmutableExpression.BinaryLiteral.Builder builder() {
      return ImmutableExpression.BinaryLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class TimestampLiteral implements Literal {
    public abstract long value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).TIMESTAMP;
    }

    public static ImmutableExpression.TimestampLiteral.Builder builder() {
      return ImmutableExpression.TimestampLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class TimeLiteral implements Literal {
    public abstract long value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).TIME;
    }

    public static ImmutableExpression.TimeLiteral.Builder builder() {
      return ImmutableExpression.TimeLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class DateLiteral implements Literal {
    public abstract int value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).DATE;
    }

    public static ImmutableExpression.DateLiteral.Builder builder() {
      return ImmutableExpression.DateLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class TimestampTZLiteral implements Literal {
    public abstract long value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).TIMESTAMP_TZ;
    }

    public static ImmutableExpression.TimestampTZLiteral.Builder builder() {
      return ImmutableExpression.TimestampTZLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class PrecisionTimestampLiteral implements Literal {
    public abstract long value();

    public abstract int precision();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).precisionTimestamp(precision());
    }

    public static ImmutableExpression.PrecisionTimestampLiteral.Builder builder() {
      return ImmutableExpression.PrecisionTimestampLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class PrecisionTimestampTZLiteral implements Literal {
    public abstract long value();

    public abstract int precision();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).precisionTimestampTZ(precision());
    }

    public static ImmutableExpression.PrecisionTimestampTZLiteral.Builder builder() {
      return ImmutableExpression.PrecisionTimestampTZLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class IntervalYearLiteral implements Literal {
    public abstract int years();

    public abstract int months();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).INTERVAL_YEAR;
    }

    public static ImmutableExpression.IntervalYearLiteral.Builder builder() {
      return ImmutableExpression.IntervalYearLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class IntervalDayLiteral implements Literal {
    public abstract int days();

    public abstract int seconds();

    public abstract long subseconds();

    public abstract int precision();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).intervalDay(precision());
    }

    public static ImmutableExpression.IntervalDayLiteral.Builder builder() {
      return ImmutableExpression.IntervalDayLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class IntervalCompoundLiteral implements Literal {
    // Flattened IntervalYearLiteral
    public abstract int years();

    public abstract int months();

    // Flattened IntervalDayLiteral
    public abstract int days();

    public abstract int seconds();

    public abstract long subseconds();

    public abstract int precision();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).intervalCompound(precision());
    }

    public static ImmutableExpression.IntervalCompoundLiteral.Builder builder() {
      return ImmutableExpression.IntervalCompoundLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class UUIDLiteral implements Literal {
    public abstract UUID value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).UUID;
    }

    public static ImmutableExpression.UUIDLiteral.Builder builder() {
      return ImmutableExpression.UUIDLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }

    public ByteString toBytes() {
      ByteBuffer bb = ByteBuffer.allocate(16);
      bb.putLong(value().getMostSignificantBits());
      bb.putLong(value().getLeastSignificantBits());
      bb.flip();
      return ByteString.copyFrom(bb);
    }
  }

  @Value.Immutable
  abstract class FixedCharLiteral implements Literal {
    public abstract String value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).fixedChar(value().length());
    }

    public static ImmutableExpression.FixedCharLiteral.Builder builder() {
      return ImmutableExpression.FixedCharLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class VarCharLiteral implements Literal {
    public abstract String value();

    public abstract int length();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).varChar(length());
    }

    public static ImmutableExpression.VarCharLiteral.Builder builder() {
      return ImmutableExpression.VarCharLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class FixedBinaryLiteral implements Literal {
    public abstract ByteString value();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).fixedBinary(value().size());
    }

    public static ImmutableExpression.FixedBinaryLiteral.Builder builder() {
      return ImmutableExpression.FixedBinaryLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class DecimalLiteral implements Literal {
    public abstract ByteString value();

    public abstract int precision();

    public abstract int scale();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).decimal(precision(), scale());
    }

    public static ImmutableExpression.DecimalLiteral.Builder builder() {
      return ImmutableExpression.DecimalLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class MapLiteral implements Literal {
    public abstract Map<Literal, Literal> values();

    @Override
    public Type getType() {
      return Type.withNullability(nullable())
          .map(
              values().keySet().iterator().next().getType(),
              values().values().iterator().next().getType());
    }

    public static ImmutableExpression.MapLiteral.Builder builder() {
      return ImmutableExpression.MapLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class EmptyMapLiteral implements Literal {
    public abstract Type keyType();

    public abstract Type valueType();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).map(keyType(), valueType());
    }

    public static ImmutableExpression.EmptyMapLiteral.Builder builder() {
      return ImmutableExpression.EmptyMapLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class ListLiteral implements Literal {
    public abstract List<Literal> values();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).list(values().get(0).getType());
    }

    public static ImmutableExpression.ListLiteral.Builder builder() {
      return ImmutableExpression.ListLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class EmptyListLiteral implements Literal {
    public abstract Type elementType();

    @Override
    public Type.ListType getType() {
      return Type.withNullability(nullable()).list(elementType());
    }

    public static ImmutableExpression.EmptyListLiteral.Builder builder() {
      return ImmutableExpression.EmptyListLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class StructLiteral implements Literal {
    public abstract List<Literal> fields();

    @Override
    public Type getType() {
      return Type.withNullability(nullable())
          .struct(
              fields().stream()
                  .map(Literal::getType)
                  .collect(java.util.stream.Collectors.toList()));
    }

    public static ImmutableExpression.StructLiteral.Builder builder() {
      return ImmutableExpression.StructLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class UserDefinedLiteral implements Literal {
    public abstract ByteString value();

    public abstract String urn();

    public abstract String name();

    @Override
    public Type getType() {
      return Type.withNullability(nullable()).userDefined(urn(), name());
    }

    public static ImmutableExpression.UserDefinedLiteral.Builder builder() {
      return ImmutableExpression.UserDefinedLiteral.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class Switch implements Expression {
    public abstract Expression match();

    public abstract List<SwitchClause> switchClauses();

    public abstract Expression defaultClause();

    @Override
    public Type getType() {
      return defaultClause().getType();
    }

    public static ImmutableExpression.Switch.Builder builder() {
      return ImmutableExpression.Switch.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class SwitchClause {
    public abstract Literal condition();

    public abstract Expression then();

    public static ImmutableExpression.SwitchClause.Builder builder() {
      return ImmutableExpression.SwitchClause.builder();
    }
  }

  @Value.Immutable
  abstract class IfThen implements Expression {
    public abstract List<IfClause> ifClauses();

    public abstract Expression elseClause();

    @Override
    public Type getType() {
      Type elseType = elseClause().getType();

      // If any of the clauses are nullable, the whole expression is also nullable.
      if (ifClauses().stream().anyMatch(clause -> clause.then().getType().nullable())) {
        return TypeCreator.asNullable(elseType);
      }
      return elseType;
    }

    public static ImmutableExpression.IfThen.Builder builder() {
      return ImmutableExpression.IfThen.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class IfClause {
    public abstract Expression condition();

    public abstract Expression then();

    public static ImmutableExpression.IfClause.Builder builder() {
      return ImmutableExpression.IfClause.builder();
    }
  }

  @Value.Immutable
  abstract class Cast implements Expression {
    public abstract Type type();

    public abstract Expression input();

    public abstract FailureBehavior failureBehavior();

    @Override
    public Type getType() {
      return type();
    }

    public static ImmutableExpression.Cast.Builder builder() {
      return ImmutableExpression.Cast.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class ScalarFunctionInvocation implements Expression {
    public abstract SimpleExtension.ScalarFunctionVariant declaration();

    public abstract List<FunctionArg> arguments();

    public abstract List<FunctionOption> options();

    public abstract Type outputType();

    @Override
    public Type getType() {
      return outputType();
    }

    public static ImmutableExpression.ScalarFunctionInvocation.Builder builder() {
      return ImmutableExpression.ScalarFunctionInvocation.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class WindowFunctionInvocation implements Expression {

    public abstract SimpleExtension.WindowFunctionVariant declaration();

    public abstract List<FunctionArg> arguments();

    public abstract List<FunctionOption> options();

    public abstract AggregationPhase aggregationPhase();

    public abstract List<Expression> partitionBy();

    public abstract List<SortField> sort();

    public abstract WindowBoundsType boundsType();

    public abstract WindowBound lowerBound();

    public abstract WindowBound upperBound();

    public abstract Type outputType();

    @Override
    public Type getType() {
      return outputType();
    }

    public abstract AggregationInvocation invocation();

    public static ImmutableExpression.WindowFunctionInvocation.Builder builder() {
      return ImmutableExpression.WindowFunctionInvocation.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  enum WindowBoundsType {
    UNSPECIFIED(io.substrait.proto.Expression.WindowFunction.BoundsType.BOUNDS_TYPE_UNSPECIFIED),
    ROWS(io.substrait.proto.Expression.WindowFunction.BoundsType.BOUNDS_TYPE_ROWS),
    RANGE(io.substrait.proto.Expression.WindowFunction.BoundsType.BOUNDS_TYPE_RANGE);

    private final io.substrait.proto.Expression.WindowFunction.BoundsType proto;

    WindowBoundsType(io.substrait.proto.Expression.WindowFunction.BoundsType proto) {
      this.proto = proto;
    }

    public io.substrait.proto.Expression.WindowFunction.BoundsType toProto() {
      return proto;
    }

    public static WindowBoundsType fromProto(
        io.substrait.proto.Expression.WindowFunction.BoundsType proto) {
      for (WindowBoundsType v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  @Value.Immutable
  abstract class SingleOrList implements Expression {
    public abstract Expression condition();

    public abstract List<Expression> options();

    @Override
    public Type getType() {
      return TypeCreator.NULLABLE.BOOLEAN;
    }

    public static ImmutableExpression.SingleOrList.Builder builder() {
      return ImmutableExpression.SingleOrList.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class MultiOrList implements Expression {
    public abstract List<Expression> conditions();

    public abstract List<MultiOrListRecord> optionCombinations();

    @Override
    public Type getType() {
      return TypeCreator.NULLABLE.BOOLEAN;
    }

    public static ImmutableExpression.MultiOrList.Builder builder() {
      return ImmutableExpression.MultiOrList.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class NestedList implements Nested {
    public abstract List<Expression> values();

    @Value.Check
    protected void check() {
      assert values().stream().map(Expression::getType).distinct().count() <= 1
          : "All values in NestedList must have the same type";
    }

    @Override
    public Type getType() {
      check();
      return Type.withNullability(nullable()).list(values().get(0).getType());
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }

    public static ImmutableExpression.NestedList.Builder builder() {
      return ImmutableExpression.NestedList.builder();
    }
  }

  @Value.Immutable
  abstract class MultiOrListRecord {
    public abstract List<Expression> values();

    public static ImmutableExpression.MultiOrListRecord.Builder builder() {
      return ImmutableExpression.MultiOrListRecord.builder();
    }
  }

  @Value.Immutable
  abstract class SortField {
    public abstract Expression expr();

    public abstract SortDirection direction();

    public static ImmutableExpression.SortField.Builder builder() {
      return ImmutableExpression.SortField.builder();
    }
  }

  interface Subquery extends Expression {}

  @Value.Immutable
  abstract class SetPredicate implements Subquery {
    public abstract PredicateOp predicateOp();

    public abstract Rel tuples();

    @Override
    public Type getType() {
      return TypeCreator.REQUIRED.BOOLEAN;
    }

    public static ImmutableExpression.SetPredicate.Builder builder() {
      return ImmutableExpression.SetPredicate.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class ScalarSubquery implements Subquery {
    public abstract Rel input();

    public static ImmutableExpression.ScalarSubquery.Builder builder() {
      return ImmutableExpression.ScalarSubquery.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  @Value.Immutable
  abstract class InPredicate implements Subquery {
    public abstract Rel haystack();

    public abstract List<Expression> needles();

    @Override
    public Type getType() {
      return TypeCreator.REQUIRED.BOOLEAN;
    }

    public static ImmutableExpression.InPredicate.Builder builder() {
      return ImmutableExpression.InPredicate.builder();
    }

    @Override
    public <R, C extends VisitationContext, E extends Throwable> R accept(
        ExpressionVisitor<R, C, E> visitor, C context) throws E {
      return visitor.visit(this, context);
    }
  }

  enum PredicateOp {
    PREDICATE_OP_UNSPECIFIED(
        io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp.PREDICATE_OP_UNSPECIFIED),
    PREDICATE_OP_EXISTS(
        io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp.PREDICATE_OP_EXISTS),
    PREDICATE_OP_UNIQUE(
        io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp.PREDICATE_OP_UNIQUE);

    private final io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp proto;

    PredicateOp(io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp proto) {
      this.proto = proto;
    }

    public io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp toProto() {
      return proto;
    }

    public static PredicateOp fromProto(
        io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp proto) {
      for (PredicateOp v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  enum AggregationInvocation {
    UNSPECIFIED(AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_UNSPECIFIED),
    ALL(AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL),
    DISTINCT(AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_DISTINCT);

    private final io.substrait.proto.AggregateFunction.AggregationInvocation proto;

    AggregationInvocation(io.substrait.proto.AggregateFunction.AggregationInvocation proto) {
      this.proto = proto;
    }

    public io.substrait.proto.AggregateFunction.AggregationInvocation toProto() {
      return proto;
    }

    public static AggregationInvocation fromProto(AggregateFunction.AggregationInvocation proto) {
      for (AggregationInvocation v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  enum AggregationPhase {
    UNSPECIFIED(io.substrait.proto.AggregationPhase.AGGREGATION_PHASE_UNSPECIFIED),
    INITIAL_TO_INTERMEDIATE(
        io.substrait.proto.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE),
    INTERMEDIATE_TO_INTERMEDIATE(
        io.substrait.proto.AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE),
    INITIAL_TO_RESULT(io.substrait.proto.AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT),
    INTERMEDIATE_TO_RESULT(
        io.substrait.proto.AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT);

    private final io.substrait.proto.AggregationPhase proto;

    AggregationPhase(io.substrait.proto.AggregationPhase proto) {
      this.proto = proto;
    }

    public io.substrait.proto.AggregationPhase toProto() {
      return proto;
    }

    public static AggregationPhase fromProto(io.substrait.proto.AggregationPhase proto) {
      for (AggregationPhase v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  enum SortDirection {
    ASC_NULLS_FIRST(io.substrait.proto.SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST),
    ASC_NULLS_LAST(io.substrait.proto.SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST),
    DESC_NULLS_FIRST(io.substrait.proto.SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_FIRST),
    DESC_NULLS_LAST(io.substrait.proto.SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_LAST),
    CLUSTERED(io.substrait.proto.SortField.SortDirection.SORT_DIRECTION_CLUSTERED);

    private final io.substrait.proto.SortField.SortDirection proto;

    SortDirection(io.substrait.proto.SortField.SortDirection proto) {
      this.proto = proto;
    }

    public io.substrait.proto.SortField.SortDirection toProto() {
      return proto;
    }

    public static SortDirection fromProto(io.substrait.proto.SortField.SortDirection proto) {
      for (SortDirection v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  enum FailureBehavior {
    UNSPECIFIED(io.substrait.proto.Expression.Cast.FailureBehavior.FAILURE_BEHAVIOR_UNSPECIFIED),
    RETURN_NULL(io.substrait.proto.Expression.Cast.FailureBehavior.FAILURE_BEHAVIOR_RETURN_NULL),
    THROW_EXCEPTION(
        io.substrait.proto.Expression.Cast.FailureBehavior.FAILURE_BEHAVIOR_THROW_EXCEPTION);

    private final io.substrait.proto.Expression.Cast.FailureBehavior proto;

    FailureBehavior(io.substrait.proto.Expression.Cast.FailureBehavior proto) {
      this.proto = proto;
    }

    public io.substrait.proto.Expression.Cast.FailureBehavior toProto() {
      return proto;
    }

    public static FailureBehavior fromProto(
        io.substrait.proto.Expression.Cast.FailureBehavior proto) {
      for (FailureBehavior v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }
}
