package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.proto.DdlRel;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.Optional;

/**
 * Base class for DDL relations with no inputs.
 *
 * <p>Defines schema/defaults, DDL object and operation, and optional view definition.
 */
public abstract class AbstractDdlRel extends ZeroInputRel implements HasExtension {

  /**
   * Returns the target table/view schema (names + types).
   *
   * @return target {@link NamedStruct}
   */
  public abstract NamedStruct getTableSchema();

  /**
   * Returns default values for the table columns.
   *
   * @return defaults as {@link Expression.StructLiteral}
   */
  public abstract Expression.StructLiteral getTableDefaults();

  /**
   * Returns the DDL object kind (table or view).
   *
   * @return DDL object
   */
  public abstract DdlObject getObject();

  /**
   * Returns the DDL operation (create/alter/drop, etc.).
   *
   * @return DDL operation
   */
  public abstract DdlOp getOperation();

  /**
   * Returns the view definition when the object is a view.
   *
   * @return optional view definition relation
   */
  public abstract Optional<Rel> getViewDefinition();

  /** DDL object kinds. */
  public enum DdlObject {
    /** Unspecified object. */
    UNSPECIFIED(DdlRel.DdlObject.DDL_OBJECT_UNSPECIFIED),
    /** Table DDL. */
    TABLE(DdlRel.DdlObject.DDL_OBJECT_TABLE),
    /** View DDL. */
    VIEW(DdlRel.DdlObject.DDL_OBJECT_VIEW);

    private final DdlRel.DdlObject proto;

    /**
     * Creates a {@code DdlObject} bound to its protobuf value.
     *
     * @param proto protobuf enum value
     */
    DdlObject(DdlRel.DdlObject proto) {
      this.proto = proto;
    }

    /**
     * Converts this enum to its protobuf counterpart.
     *
     * @return protobuf enum value
     */
    public DdlRel.DdlObject toProto() {
      return proto;
    }

    /**
     * Maps a protobuf value to this enum.
     *
     * @param proto protobuf enum value
     * @return matching {@code DdlObject}
     * @throws IllegalArgumentException if unknown
     */
    public static DdlObject fromProto(DdlRel.DdlObject proto) {
      for (DdlObject v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  /** DDL operations. */
  public enum DdlOp {
    /** Unspecified operation. */
    UNSPECIFIED(DdlRel.DdlOp.DDL_OP_UNSPECIFIED),
    /** Create a new object. */
    CREATE(DdlRel.DdlOp.DDL_OP_CREATE),
    /** Create or replace the object. */
    CREATE_OR_REPLACE(DdlRel.DdlOp.DDL_OP_CREATE_OR_REPLACE),
    /** Alter an existing object. */
    ALTER(DdlRel.DdlOp.DDL_OP_ALTER),
    /** Drop an object. */
    DROP(DdlRel.DdlOp.DDL_OP_DROP),
    /** Drop an object if it exists. */
    DROP_IF_EXIST(DdlRel.DdlOp.DDL_OP_DROP_IF_EXIST);

    private final DdlRel.DdlOp proto;

    /**
     * Creates a {@code DdlOp} bound to its protobuf value.
     *
     * @param proto protobuf enum value
     */
    DdlOp(DdlRel.DdlOp proto) {
      this.proto = proto;
    }

    /**
     * Converts this enum to its protobuf counterpart.
     *
     * @return protobuf enum value
     */
    public DdlRel.DdlOp toProto() {
      return proto;
    }

    /**
     * Maps a protobuf value to this enum.
     *
     * @param proto protobuf enum value
     * @return matching {@code DdlOp}
     * @throws IllegalArgumentException if unknown
     */
    public static DdlOp fromProto(DdlRel.DdlOp proto) {
      for (DdlOp v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  /**
   * Derives the output record type from the declared schema.
   *
   * @return {@link Type.Struct} from {@link #getTableSchema()}
   */
  @Override
  public Type.Struct deriveRecordType() {
    return getTableSchema().struct();
  }
}
