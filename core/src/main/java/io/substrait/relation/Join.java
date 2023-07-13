package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.proto.JoinRel;
import io.substrait.type.Type;
import io.substrait.type.Type.Binary;
import io.substrait.type.Type.Bool;
import io.substrait.type.Type.Date;
import io.substrait.type.Type.Decimal;
import io.substrait.type.Type.FP32;
import io.substrait.type.Type.FP64;
import io.substrait.type.Type.FixedBinary;
import io.substrait.type.Type.FixedChar;
import io.substrait.type.Type.I16;
import io.substrait.type.Type.I32;
import io.substrait.type.Type.I64;
import io.substrait.type.Type.I8;
import io.substrait.type.Type.IntervalDay;
import io.substrait.type.Type.IntervalYear;
import io.substrait.type.Type.ListType;
import io.substrait.type.Type.Map;
import io.substrait.type.Type.Str;
import io.substrait.type.Type.Struct;
import io.substrait.type.Type.Time;
import io.substrait.type.Type.Timestamp;
import io.substrait.type.Type.TimestampTZ;
import io.substrait.type.Type.UUID;
import io.substrait.type.Type.UserDefined;
import io.substrait.type.Type.VarChar;
import io.substrait.type.TypeCreator;
import io.substrait.type.TypeVisitor;
import java.util.Optional;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Join extends BiRel implements HasExtension {

  public abstract Optional<Expression> getCondition();

  public abstract Optional<Expression> getPostJoinFilter();

  public abstract JoinType getJoinType();

  public static enum JoinType {
    UNKNOWN(JoinRel.JoinType.JOIN_TYPE_UNSPECIFIED),
    INNER(JoinRel.JoinType.JOIN_TYPE_INNER),
    OUTER(JoinRel.JoinType.JOIN_TYPE_OUTER),
    LEFT(JoinRel.JoinType.JOIN_TYPE_LEFT),
    RIGHT(JoinRel.JoinType.JOIN_TYPE_RIGHT),
    SEMI(JoinRel.JoinType.JOIN_TYPE_SEMI),
    ANTI(JoinRel.JoinType.JOIN_TYPE_ANTI);

    private JoinRel.JoinType proto;

    JoinType(JoinRel.JoinType proto) {
      this.proto = proto;
    }

    public JoinRel.JoinType toProto() {
      return proto;
    }

    public static JoinType fromProto(JoinRel.JoinType proto) {
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }

      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  private static final class NullableTypeVisitor implements TypeVisitor<Type, RuntimeException> {

    @Override
    public Type visit(Bool type) throws RuntimeException {
      return TypeCreator.NULLABLE.BOOLEAN;
    }

    @Override
    public Type visit(I8 type) throws RuntimeException {
      return TypeCreator.NULLABLE.I8;
    }

    @Override
    public Type visit(I16 type) throws RuntimeException {
      return TypeCreator.NULLABLE.I16;
    }

    @Override
    public Type visit(I32 type) throws RuntimeException {
      return TypeCreator.NULLABLE.I32;
    }

    @Override
    public Type visit(I64 type) throws RuntimeException {
      return TypeCreator.NULLABLE.I64;
    }

    @Override
    public Type visit(FP32 type) throws RuntimeException {
      return TypeCreator.NULLABLE.FP32;
    }

    @Override
    public Type visit(FP64 type) throws RuntimeException {
      return TypeCreator.NULLABLE.FP64;
    }

    @Override
    public Type visit(Str type) throws RuntimeException {
      return TypeCreator.NULLABLE.STRING;
    }

    @Override
    public Type visit(Binary type) throws RuntimeException {
      return TypeCreator.NULLABLE.BINARY;
    }

    @Override
    public Type visit(Date type) throws RuntimeException {
      return TypeCreator.NULLABLE.DATE;
    }

    @Override
    public Type visit(Time type) throws RuntimeException {
      return TypeCreator.NULLABLE.TIME;
    }

    @Override
    public Type visit(TimestampTZ type) throws RuntimeException {
      return TypeCreator.NULLABLE.TIMESTAMP_TZ;
    }

    @Override
    public Type visit(Timestamp type) throws RuntimeException {
      return TypeCreator.NULLABLE.TIMESTAMP;
    }

    @Override
    public Type visit(IntervalYear type) throws RuntimeException {
      return TypeCreator.NULLABLE.INTERVAL_YEAR;
    }

    @Override
    public Type visit(IntervalDay type) throws RuntimeException {
      return TypeCreator.NULLABLE.INTERVAL_DAY;
    }

    @Override
    public Type visit(UUID type) throws RuntimeException {
      return TypeCreator.NULLABLE.UUID;
    }

    @Override
    public Type visit(FixedChar type) throws RuntimeException {
      return TypeCreator.NULLABLE.fixedChar(type.length());
    }

    @Override
    public Type visit(VarChar type) throws RuntimeException {
      return TypeCreator.NULLABLE.varChar(type.length());
    }

    @Override
    public Type visit(FixedBinary type) throws RuntimeException {
      return TypeCreator.NULLABLE.fixedBinary(type.length());
    }

    @Override
    public Type visit(Decimal type) throws RuntimeException {
      return TypeCreator.NULLABLE.decimal(type.precision(), type.scale());
    }

    @Override
    public Type visit(Struct type) throws RuntimeException {
      return TypeCreator.NULLABLE.struct(type.fields());
    }

    @Override
    public Type visit(ListType type) throws RuntimeException {
      return TypeCreator.NULLABLE.list(type.elementType());
    }

    @Override
    public Type visit(Map type) throws RuntimeException {
      return TypeCreator.NULLABLE.map(type.key(), type.value());
    }

    @Override
    public Type visit(UserDefined type) throws RuntimeException {
      return TypeCreator.NULLABLE.userDefined(type.uri(), type.name());
    }
  }

  @Override
  protected Type.Struct deriveRecordType() {
    var nullable = new NullableTypeVisitor();
    Stream<Type> leftTypes =
        switch (getJoinType()) {
          case RIGHT, OUTER -> getLeft().getRecordType().fields().stream()
              .map(t -> t.accept(nullable));
          default -> getLeft().getRecordType().fields().stream();
        };
    Stream<Type> rightTypes =
        switch (getJoinType()) {
          case LEFT, OUTER -> getRight().getRecordType().fields().stream()
              .map(t -> t.accept(nullable));
          default -> getRight().getRecordType().fields().stream();
        };
    return TypeCreator.REQUIRED.struct(Stream.concat(leftTypes, rightTypes));
  }

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableJoin.Builder builder() {
    return ImmutableJoin.builder();
  }
}
