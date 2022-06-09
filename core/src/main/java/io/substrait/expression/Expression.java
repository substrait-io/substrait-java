package io.substrait.expression;

import com.google.protobuf.ByteString;
import io.substrait.function.SimpleExtension;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Enclosing
public interface Expression extends FunctionArg {

  Type getType();

  @Override
  default <R, E extends Throwable> R acceptFuncArgVis(FuncArgVisitor<R, E> fnArgVisitor) throws E {
    return fnArgVisitor.visitExpr(this);
  }

  interface Literal extends Expression {
    @Value.Default
    default boolean nullable() {
      return false;
    }
  }

  <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E;

  @Value.Immutable
  abstract static class NullLiteral implements Literal {
    public abstract Type type();

    public Type getType() {
      return type();
    }

    public static ImmutableExpression.NullLiteral.Builder builder() {
      return ImmutableExpression.NullLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class BoolLiteral implements Literal {
    public abstract Boolean value();

    public Type getType() {
      return Type.withNullability(nullable()).BOOLEAN;
    }

    public static ImmutableExpression.BoolLiteral.Builder builder() {
      return ImmutableExpression.BoolLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class I8Literal implements Literal {
    public abstract int value();

    public Type getType() {
      return Type.withNullability(nullable()).I8;
    }

    public static ImmutableExpression.I8Literal.Builder builder() {
      return ImmutableExpression.I8Literal.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class I16Literal implements Literal {
    public abstract int value();

    public Type getType() {
      return Type.withNullability(nullable()).I16;
    }

    public static ImmutableExpression.I16Literal.Builder builder() {
      return ImmutableExpression.I16Literal.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class I32Literal implements Literal {
    public abstract int value();

    public Type getType() {
      return Type.withNullability(nullable()).I32;
    }

    public static ImmutableExpression.I32Literal.Builder builder() {
      return ImmutableExpression.I32Literal.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class I64Literal implements Literal {
    public abstract long value();

    public Type getType() {
      return Type.withNullability(nullable()).I64;
    }

    public static ImmutableExpression.I64Literal.Builder builder() {
      return ImmutableExpression.I64Literal.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class FP32Literal implements Literal {
    public abstract float value();

    public Type getType() {
      return Type.withNullability(nullable()).FP32;
    }

    public static ImmutableExpression.FP32Literal.Builder builder() {
      return ImmutableExpression.FP32Literal.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class FP64Literal implements Literal {
    public abstract double value();

    public Type getType() {
      return Type.withNullability(nullable()).FP64;
    }

    public static ImmutableExpression.FP64Literal.Builder builder() {
      return ImmutableExpression.FP64Literal.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class StrLiteral implements Literal {
    public abstract String value();

    public Type getType() {
      return Type.withNullability(nullable()).STRING;
    }

    public static ImmutableExpression.StrLiteral.Builder builder() {
      return ImmutableExpression.StrLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class BinaryLiteral implements Literal {
    public abstract ByteString value();

    public Type getType() {
      return Type.withNullability(nullable()).BINARY;
    }

    public static ImmutableExpression.BinaryLiteral.Builder builder() {
      return ImmutableExpression.BinaryLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class TimestampLiteral implements Literal {
    public abstract long value();

    public Type getType() {
      return Type.withNullability(nullable()).TIMESTAMP;
    }

    public static ImmutableExpression.TimestampLiteral.Builder builder() {
      return ImmutableExpression.TimestampLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class TimeLiteral implements Literal {
    public abstract long value();

    public Type getType() {
      return Type.withNullability(nullable()).TIME;
    }

    public static ImmutableExpression.TimeLiteral.Builder builder() {
      return ImmutableExpression.TimeLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class DateLiteral implements Literal {
    public abstract int value();

    public Type getType() {
      return Type.withNullability(nullable()).DATE;
    }

    public static ImmutableExpression.DateLiteral.Builder builder() {
      return ImmutableExpression.DateLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class TimestampTZLiteral implements Literal {
    public abstract long value();

    public Type getType() {
      return Type.withNullability(nullable()).TIMESTAMP_TZ;
    }

    public static ImmutableExpression.TimestampTZLiteral.Builder builder() {
      return ImmutableExpression.TimestampTZLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class IntervalYearLiteral implements Literal {
    public abstract int years();

    public abstract int months();

    public Type getType() {
      return Type.withNullability(nullable()).INTERVAL_YEAR;
    }

    public static ImmutableExpression.IntervalYearLiteral.Builder builder() {
      return ImmutableExpression.IntervalYearLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class IntervalDayLiteral implements Literal {
    public abstract int days();

    public abstract int seconds();

    public Type getType() {
      return Type.withNullability(nullable()).INTERVAL_DAY;
    }

    public static ImmutableExpression.IntervalDayLiteral.Builder builder() {
      return ImmutableExpression.IntervalDayLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class UUIDLiteral implements Literal {
    public abstract UUID value();

    public Type getType() {
      return Type.withNullability(nullable()).UUID;
    }

    public static ImmutableExpression.UUIDLiteral.Builder builder() {
      return ImmutableExpression.UUIDLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
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
  abstract static class FixedCharLiteral implements Literal {
    public abstract String value();

    public Type getType() {
      return Type.withNullability(nullable()).fixedChar(value().length());
    }

    public static ImmutableExpression.FixedCharLiteral.Builder builder() {
      return ImmutableExpression.FixedCharLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class VarCharLiteral implements Literal {
    public abstract String value();

    public abstract int length();

    public Type getType() {
      return Type.withNullability(nullable()).varChar(length());
    }

    public static ImmutableExpression.VarCharLiteral.Builder builder() {
      return ImmutableExpression.VarCharLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class FixedBinaryLiteral implements Literal {
    public abstract ByteString value();

    public Type getType() {
      return Type.withNullability(nullable()).fixedBinary(value().size());
    }

    public static ImmutableExpression.FixedBinaryLiteral.Builder builder() {
      return ImmutableExpression.FixedBinaryLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class DecimalLiteral implements Literal {
    public abstract ByteString value();

    public abstract int precision();

    public abstract int scale();

    public Type getType() {
      return Type.withNullability(nullable()).decimal(precision(), scale());
    }

    public static ImmutableExpression.DecimalLiteral.Builder builder() {
      return ImmutableExpression.DecimalLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class MapLiteral implements Literal {
    public abstract Map<Literal, Literal> values();

    public Type getType() {
      return Type.withNullability(nullable())
          .map(
              values().keySet().iterator().next().getType(),
              values().values().iterator().next().getType());
    }

    public static ImmutableExpression.MapLiteral.Builder builder() {
      return ImmutableExpression.MapLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class ListLiteral implements Literal {
    public abstract List<Literal> values();

    public Type getType() {
      return Type.withNullability(nullable()).list(values().get(0).getType());
    }

    public static ImmutableExpression.ListLiteral.Builder builder() {
      return ImmutableExpression.ListLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class StructLiteral implements Literal {
    public abstract List<Literal> fields();

    public Type getType() {
      return Type.withNullability(nullable())
          .struct(fields().stream().map(Literal::getType).toList());
    }

    public static ImmutableExpression.StructLiteral.Builder builder() {
      return ImmutableExpression.StructLiteral.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class Switch implements Expression {
    public abstract List<SwitchClause> switchClauses();

    public abstract Expression defaultClause();

    public Type getType() {
      return defaultClause().getType();
    }

    public static ImmutableExpression.Switch.Builder builder() {
      return ImmutableExpression.Switch.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class SwitchClause {
    public abstract Literal condition();

    public abstract Expression then();

    public static ImmutableExpression.SwitchClause.Builder builder() {
      return ImmutableExpression.SwitchClause.builder();
    }
  }

  @Value.Immutable
  abstract static class IfThen implements Expression {
    public abstract List<IfClause> ifClauses();

    public abstract Expression elseClause();

    public Type getType() {
      return elseClause().getType();
    }

    public static ImmutableExpression.IfThen.Builder builder() {
      return ImmutableExpression.IfThen.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class IfClause {
    public abstract Expression condition();

    public abstract Expression then();

    public static ImmutableExpression.IfClause.Builder builder() {
      return ImmutableExpression.IfClause.builder();
    }
  }

  @Value.Immutable
  abstract static class Cast implements Expression {
    public abstract Type type();

    public abstract Expression input();

    public Type getType() {
      return type();
    }

    public static ImmutableExpression.Cast.Builder builder() {
      return ImmutableExpression.Cast.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class ScalarFunctionInvocation implements Expression {
    public abstract SimpleExtension.ScalarFunctionVariant declaration();

    public abstract List<FunctionArg> arguments();

    public final List<Expression> exprArguments() {
      return arguments().stream()
          .filter(f -> f instanceof Expression)
          .map(f -> Expression.class.cast(f))
          .collect(Collectors.toList());
    }

    public abstract Type outputType();

    public Type getType() {
      return outputType();
    }

    public static ImmutableExpression.ScalarFunctionInvocation.Builder builder() {
      return ImmutableExpression.ScalarFunctionInvocation.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class SingleOrList implements Expression {
    public abstract Expression condition();

    public abstract List<Expression> options();

    public Type getType() {
      return Type.NULLABLE.BOOLEAN;
    }

    public static ImmutableExpression.SingleOrList.Builder builder() {
      return ImmutableExpression.SingleOrList.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class MultiOrList implements Expression {
    public abstract List<Expression> conditions();

    public abstract List<MultiOrListRecord> optionCombinations();

    public Type getType() {
      return Type.NULLABLE.BOOLEAN;
    }

    public static ImmutableExpression.MultiOrList.Builder builder() {
      return ImmutableExpression.MultiOrList.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class MultiOrListRecord {
    public abstract List<Expression> values();

    public static ImmutableExpression.MultiOrListRecord.Builder builder() {
      return ImmutableExpression.MultiOrListRecord.builder();
    }
  }

  @Value.Immutable
  abstract static class SortField {
    public abstract Expression expr();

    public abstract SortDirection direction();

    public static ImmutableExpression.SortField.Builder builder() {
      return ImmutableExpression.SortField.builder();
    }
  }

  interface Subquery extends Expression {}

  @Value.Immutable
  abstract static class SetPredicate implements Subquery {
    public abstract PredicateOp predicateOp();

    public abstract Rel tuples();

    public Type getType() {
      return Type.REQUIRED.BOOLEAN;
    }

    public static ImmutableExpression.SetPredicate.Builder builder() {
      return ImmutableExpression.SetPredicate.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class ScalarSubquery implements Subquery {
    public abstract Rel input();

    public static ImmutableExpression.ScalarSubquery.Builder builder() {
      return ImmutableExpression.ScalarSubquery.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Value.Immutable
  abstract static class InPredicate implements Subquery {
    public abstract Rel haystack();

    public abstract List<Expression> needles();

    public static ImmutableExpression.InPredicate.Builder builder() {
      return ImmutableExpression.InPredicate.builder();
    }

    public <R, E extends Throwable> R accept(ExpressionVisitor<R, E> visitor) throws E {
      return visitor.visit(this);
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
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  enum AggregationPhase {
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
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  public enum SortDirection {
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
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }
}
