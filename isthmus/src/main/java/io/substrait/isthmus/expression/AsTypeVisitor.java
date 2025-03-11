package io.substrait.isthmus.expression;

import io.substrait.function.NullableType;
import io.substrait.function.ParameterizedType;
import io.substrait.function.ParameterizedTypeVisitor;
import io.substrait.type.Type;
import io.substrait.type.Type.Binary;
import io.substrait.type.Type.Bool;
import io.substrait.type.Type.Date;
import io.substrait.type.Type.FP32;
import io.substrait.type.Type.FP64;
import io.substrait.type.Type.I16;
import io.substrait.type.Type.I32;
import io.substrait.type.Type.I64;
import io.substrait.type.Type.I8;
import io.substrait.type.Type.IntervalYear;
import io.substrait.type.Type.PrecisionTime;
import io.substrait.type.Type.Str;
import io.substrait.type.Type.Time;
import io.substrait.type.Type.Timestamp;
import io.substrait.type.Type.TimestampTZ;
import io.substrait.type.Type.UUID;
import io.substrait.type.Type.UserDefined;
import io.substrait.type.TypeCreator;
import io.substrait.type.TypeVisitor;
import java.util.Optional;

/**
 * Obtains the Type corresponding to a given target ParameterizedType. To correctly define the
 * target Type, the source Type from which it will be converted must be considered, so that
 * parameters such as string length can be set correctly.
 *
 * <p>The {@code BUILDER} is used as a visitor for the source Type. This returns a visitor that can
 * be used with a target ParameterizedType to determine the corresponding Type.
 */
public interface AsTypeVisitor extends ParameterizedTypeVisitor<Optional<Type>, RuntimeException> {
  Builder BUILDER = new Builder();

  final class Builder implements TypeVisitor<AsTypeVisitor, RuntimeException> {
    private static final AsTypeVisitor DEFAULT = new AsTypeVisitor() {};

    // Private constructor to prevent instantiation
    private Builder() {}

    private static TypeCreator typeCreator(NullableType type) {
      return type.nullable() ? TypeCreator.NULLABLE : TypeCreator.REQUIRED;
    }

    @Override
    public AsTypeVisitor visit(Bool source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(I8 source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(I16 source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(I32 source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(I64 source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(FP32 source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(FP64 source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Str source) {
      return new AsTypeVisitor() {
        @Override
        public Optional<Type> visit(ParameterizedType.StringLiteral target) {
          return Optional.of(source);
        }
      };
    }

    @Override
    public AsTypeVisitor visit(Binary source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Date source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Time source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(TimestampTZ source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Timestamp source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(PrecisionTime type) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Type.PrecisionTimestamp source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Type.PrecisionTimestampTZ source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(IntervalYear source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Type.IntervalDay source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Type.IntervalCompound source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(UUID source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Type.FixedChar source) {
      return new AsTypeVisitor() {
        @Override
        public Optional<Type> visit(ParameterizedType.VarChar target) {
          var result = typeCreator(source).varChar(source.length());
          return Optional.of(result);
        }

        @Override
        public Optional<Type> visit(ParameterizedType.FixedChar target) {
          return Optional.of(source);
        }

        @Override
        public Optional<Type> visit(ParameterizedType.StringLiteral target) {
          var result = typeCreator(source).STRING;
          return Optional.of(result);
        }
      };
    }

    @Override
    public AsTypeVisitor visit(Type.VarChar source) {
      return new AsTypeVisitor() {
        @Override
        public Optional<Type> visit(ParameterizedType.VarChar target) {
          return Optional.of(source);
        }

        @Override
        public Optional<Type> visit(ParameterizedType.FixedChar target) {
          var result = typeCreator(source).fixedChar(source.length());
          return Optional.of(result);
        }

        @Override
        public Optional<Type> visit(ParameterizedType.StringLiteral target) {
          var result = typeCreator(source).STRING;
          return Optional.of(result);
        }
      };
    }

    @Override
    public AsTypeVisitor visit(Type.FixedBinary source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Type.Decimal source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Type.Struct source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Type.ListType source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(Type.Map source) {
      return DEFAULT;
    }

    @Override
    public AsTypeVisitor visit(UserDefined source) {
      return DEFAULT;
    }
  }

  @Override
  default Optional<Type> visit(Type.Bool type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.I8 type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.I16 type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.I32 type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.I64 type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.FP32 type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.FP64 type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.Str type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.Binary type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.Date type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.Time type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.TimestampTZ type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.Timestamp type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.PrecisionTime type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.IntervalYear type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.IntervalDay type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.IntervalCompound type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.UUID type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.UserDefined type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.FixedChar type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.VarChar type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.FixedBinary type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.Decimal type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.PrecisionTimestamp type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.PrecisionTimestampTZ type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.Struct type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.ListType type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(Type.Map type) {
    return Optional.of(type);
  }

  @Override
  default Optional<Type> visit(ParameterizedType.FixedChar expr) {
    return Optional.empty();
  }

  @Override
  default Optional<Type> visit(ParameterizedType.VarChar expr) {
    return Optional.empty();
  }

  @Override
  default Optional<Type> visit(ParameterizedType.FixedBinary expr) {
    return Optional.empty();
  }

  @Override
  default Optional<Type> visit(ParameterizedType.Decimal expr) {
    return Optional.empty();
  }

  @Override
  default Optional<Type> visit(ParameterizedType.IntervalDay expr) {
    return Optional.empty();
  }

  @Override
  default Optional<Type> visit(ParameterizedType.IntervalCompound expr) {
    return Optional.empty();
  }

  @Override
  default Optional<Type> visit(ParameterizedType.PrecisionTime expr) {
    return Optional.empty();
  }

  @Override
  default Optional<Type> visit(ParameterizedType.PrecisionTimestamp expr) {
    return Optional.empty();
  }

  @Override
  default Optional<Type> visit(ParameterizedType.PrecisionTimestampTZ expr) {
    return Optional.empty();
  }

  @Override
  default Optional<Type> visit(ParameterizedType.Struct expr) {
    return Optional.empty();
  }

  @Override
  default Optional<Type> visit(ParameterizedType.ListType expr) {
    return Optional.empty();
  }

  @Override
  default Optional<Type> visit(ParameterizedType.Map expr) {
    return Optional.empty();
  }

  @Override
  default Optional<Type> visit(ParameterizedType.StringLiteral stringLiteral) {
    return Optional.empty();
  }
}
