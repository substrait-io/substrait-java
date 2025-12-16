package io.substrait.relation;

import io.substrait.proto.WriteRel;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;

/**
 * Base class for write relations with a single input.
 *
 * <p>Defines table schema, write operation, create mode, and output mode.
 */
public abstract class AbstractWriteRel extends SingleInputRel implements HasExtension {

  /**
   * Returns the target table schema (names + types).
   *
   * @return target table {@link NamedStruct}
   */
  public abstract NamedStruct getTableSchema();

  /**
   * Returns the write operation (insert/delete/update/ctas).
   *
   * @return write operation
   */
  public abstract WriteOp getOperation();

  /**
   * Returns the table creation behavior when the target exists.
   *
   * @return create mode
   */
  public abstract CreateMode getCreateMode();

  /**
   * Returns the output behavior of the write (e.g., rows returned).
   *
   * @return output mode
   */
  public abstract OutputMode getOutputMode();

  /** Logical write operation kinds. */
  public enum WriteOp {
    /** Unspecified operation. */
    UNSPECIFIED(WriteRel.WriteOp.WRITE_OP_UNSPECIFIED),
    /** Insert rows into the target. */
    INSERT(WriteRel.WriteOp.WRITE_OP_INSERT),
    /** Delete rows from the target. */
    DELETE(WriteRel.WriteOp.WRITE_OP_DELETE),
    /** Update rows in the target. */
    UPDATE(WriteRel.WriteOp.WRITE_OP_UPDATE),
    /** Create table as select (CTAS). */
    CTAS(WriteRel.WriteOp.WRITE_OP_CTAS);

    private final WriteRel.WriteOp proto;

    /**
     * Creates a {@code WriteOp} bound to its protobuf value.
     *
     * @param proto protobuf enum value
     */
    WriteOp(WriteRel.WriteOp proto) {
      this.proto = proto;
    }

    /**
     * Converts this enum to its protobuf counterpart.
     *
     * @return protobuf enum value
     */
    public WriteRel.WriteOp toProto() {
      return proto;
    }

    /**
     * Maps a protobuf value to this enum.
     *
     * @param proto protobuf enum value
     * @return matching {@code WriteOp}
     * @throws IllegalArgumentException if unknown
     */
    public static WriteOp fromProto(WriteRel.WriteOp proto) {
      for (WriteOp v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  /** Behavior when the target table already exists. */
  public enum CreateMode {
    /** Unspecified mode. */
    UNSPECIFIED(WriteRel.CreateMode.CREATE_MODE_UNSPECIFIED),
    /** Append to the existing table if it exists. */
    APPEND_IF_EXISTS(WriteRel.CreateMode.CREATE_MODE_APPEND_IF_EXISTS),
    /** Replace the existing table if it exists. */
    REPLACE_IF_EXISTS(WriteRel.CreateMode.CREATE_MODE_REPLACE_IF_EXISTS),
    /** Ignore the write if the table exists. */
    IGNORE_IF_EXISTS(WriteRel.CreateMode.CREATE_MODE_IGNORE_IF_EXISTS),
    /** Error if the table exists. */
    ERROR_IF_EXISTS(WriteRel.CreateMode.CREATE_MODE_ERROR_IF_EXISTS);

    private final WriteRel.CreateMode proto;

    /**
     * Creates a {@code CreateMode} bound to its protobuf value.
     *
     * @param proto protobuf enum value
     */
    CreateMode(WriteRel.CreateMode proto) {
      this.proto = proto;
    }

    /**
     * Converts this enum to its protobuf counterpart.
     *
     * @return protobuf enum value
     */
    public WriteRel.CreateMode toProto() {
      return proto;
    }

    /**
     * Maps a protobuf value to this enum.
     *
     * @param proto protobuf enum value
     * @return matching {@code CreateMode}
     * @throws IllegalArgumentException if unknown
     */
    public static CreateMode fromProto(WriteRel.CreateMode proto) {
      for (CreateMode v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  /** Output behavior for write operations. */
  public enum OutputMode {
    /** Unspecified output. */
    UNSPECIFIED(WriteRel.OutputMode.OUTPUT_MODE_UNSPECIFIED),
    /** No rows are produced by the write. */
    NO_OUTPUT(WriteRel.OutputMode.OUTPUT_MODE_NO_OUTPUT),
    /** Only modified records are produced. */
    MODIFIED_RECORDS(WriteRel.OutputMode.OUTPUT_MODE_MODIFIED_RECORDS);

    private final WriteRel.OutputMode proto;

    /**
     * Creates an {@code OutputMode} bound to its protobuf value.
     *
     * @param proto protobuf enum value
     */
    OutputMode(WriteRel.OutputMode proto) {
      this.proto = proto;
    }

    /**
     * Converts this enum to its protobuf counterpart.
     *
     * @return protobuf enum value
     */
    public WriteRel.OutputMode toProto() {
      return proto;
    }

    /**
     * Maps a protobuf value to this enum.
     *
     * @param proto protobuf enum value
     * @return matching {@code OutputMode}
     * @throws IllegalArgumentException if unknown
     */
    public static OutputMode fromProto(WriteRel.OutputMode proto) {
      for (OutputMode v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  /**
   * Derives the output record type from the input relation.
   *
   * @return input record {@link Type.Struct}
   */
  @Override
  public Type.Struct deriveRecordType() {
    return getInput().getRecordType();
  }
}
