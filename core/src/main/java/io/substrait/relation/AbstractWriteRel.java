package io.substrait.relation;

import io.substrait.proto.WriteRel;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;

public abstract class AbstractWriteRel extends SingleInputRel {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractWriteRel.class);

  public abstract NamedStruct getTableSchema();

  public abstract WriteOp getOperation();

  public abstract CreateMode getCreateMode();

  public abstract OutputMode getOutputMode();

  public enum WriteOp {
    UNSPECIFIED(WriteRel.WriteOp.WRITE_OP_UNSPECIFIED),
    INSERT(WriteRel.WriteOp.WRITE_OP_INSERT),
    DELETE(WriteRel.WriteOp.WRITE_OP_DELETE),
    UPDATE(WriteRel.WriteOp.WRITE_OP_UPDATE),
    CTAS(WriteRel.WriteOp.WRITE_OP_CTAS);

    private final WriteRel.WriteOp proto;

    WriteOp(WriteRel.WriteOp proto) {
      this.proto = proto;
    }

    public WriteRel.WriteOp toProto() {
      return proto;
    }

    public static WriteOp fromProto(WriteRel.WriteOp proto) {
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  public enum CreateMode {
    UNSPECIFIED(WriteRel.CreateMode.CREATE_MODE_UNSPECIFIED),
    APPEND_IF_EXISTS(WriteRel.CreateMode.CREATE_MODE_APPEND_IF_EXISTS),
    REPLACE_IF_EXISTS(WriteRel.CreateMode.CREATE_MODE_REPLACE_IF_EXISTS),
    IGNORE_IF_EXISTS(WriteRel.CreateMode.CREATE_MODE_IGNORE_IF_EXISTS),
    ERROR_IF_EXISTS(WriteRel.CreateMode.CREATE_MODE_ERROR_IF_EXISTS);

    private final WriteRel.CreateMode proto;

    CreateMode(WriteRel.CreateMode proto) {
      this.proto = proto;
    }

    public WriteRel.CreateMode toProto() {
      return proto;
    }

    public static CreateMode fromProto(WriteRel.CreateMode proto) {
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  public enum OutputMode {
    UNSPECIFIED(WriteRel.OutputMode.OUTPUT_MODE_UNSPECIFIED),
    NO_OUTPUT(WriteRel.OutputMode.OUTPUT_MODE_NO_OUTPUT),
    MODIFIED_RECORDS(WriteRel.OutputMode.OUTPUT_MODE_MODIFIED_RECORDS);

    private final WriteRel.OutputMode proto;

    OutputMode(WriteRel.OutputMode proto) {
      this.proto = proto;
    }

    public WriteRel.OutputMode toProto() {
      return proto;
    }

    public static OutputMode fromProto(WriteRel.OutputMode proto) {
      for (var v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  @Override
  public Type.Struct deriveRecordType() {
    return getInput().getRecordType();
  }
}
