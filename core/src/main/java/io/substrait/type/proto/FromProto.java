package io.substrait.type.proto;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;

public class FromProto {

  private FromProto() {}

  public static Type from(io.substrait.proto.Type type) {
    switch (type.getKindCase()) {
      case BOOL:
        return n(type.getBool().getNullability()).BOOLEAN;
      case I8:
        return n(type.getI8().getNullability()).I8;
      case I16:
        return n(type.getI16().getNullability()).I16;
      case I32:
        return n(type.getI32().getNullability()).I32;
      case I64:
        return n(type.getI64().getNullability()).I64;
      case FP32:
        return n(type.getFp32().getNullability()).FP32;
      case FP64:
        return n(type.getFp64().getNullability()).FP64;
      case STRING:
        return n(type.getString().getNullability()).STRING;
      case BINARY:
        return n(type.getBinary().getNullability()).BINARY;
      case TIMESTAMP:
        return n(type.getTimestamp().getNullability()).TIMESTAMP;
      case DATE:
        return n(type.getDate().getNullability()).DATE;
      case TIME:
        return n(type.getTime().getNullability()).TIME;
      case INTERVAL_YEAR:
        return n(type.getIntervalYear().getNullability()).INTERVAL_YEAR;
      case INTERVAL_DAY:
        return n(type.getIntervalDay().getNullability()).INTERVAL_DAY;
      case TIMESTAMP_TZ:
        return n(type.getTimestampTz().getNullability()).TIMESTAMP_TZ;
      case UUID:
        return n(type.getUuid().getNullability()).UUID;
      case FIXED_CHAR:
        return n(type.getFixedChar().getNullability()).fixedChar(type.getFixedChar().getLength());
      case VARCHAR:
        return n(type.getVarchar().getNullability()).varChar(type.getVarchar().getLength());
      case FIXED_BINARY:
        return n(type.getFixedBinary().getNullability())
            .fixedBinary(type.getFixedBinary().getLength());
      case DECIMAL:
        return n(type.getDecimal().getNullability())
            .decimal(type.getDecimal().getPrecision(), type.getDecimal().getScale());
      case STRUCT:
        return n(type.getStruct().getNullability())
            .struct(
                type.getStruct().getTypesList().stream()
                    .map(FromProto::from)
                    .collect(java.util.stream.Collectors.toList()));
      case LIST:
        return n(type.getList().getNullability()).list(from(type.getList().getType()));
      case MAP:
        return n(type.getMap().getNullability())
            .map(from(type.getMap().getKey()), from(type.getMap().getValue()));
      case USER_DEFINED_TYPE_REFERENCE:
      case USER_DEFINED:
      case KIND_NOT_SET:
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static boolean isNullable(io.substrait.proto.Type.Nullability nullability) {
    return io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE == nullability;
  }

  private static TypeCreator n(io.substrait.proto.Type.Nullability n) {
    return n == io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE
        ? TypeCreator.NULLABLE
        : TypeCreator.REQUIRED;
  }
}
