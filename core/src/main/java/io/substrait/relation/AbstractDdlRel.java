package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.proto.DdlRel;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.Optional;

public abstract class AbstractDdlRel extends ZeroInputRel {
  public abstract NamedStruct getTableSchema();

  public abstract Expression.StructLiteral getTableDefaults();

  public abstract DdlObject getObject();

  public abstract DdlOp getOperation();

  public abstract Optional<Rel> getViewDefinition();

  public enum DdlObject {
    UNSPECIFIED(DdlRel.DdlObject.DDL_OBJECT_UNSPECIFIED),
    TABLE(DdlRel.DdlObject.DDL_OBJECT_TABLE),
    VIEW(DdlRel.DdlObject.DDL_OBJECT_VIEW);

    private final DdlRel.DdlObject proto;

    DdlObject(DdlRel.DdlObject proto) {
      this.proto = proto;
    }

    public DdlRel.DdlObject toProto() {
      return proto;
    }

    public static DdlObject fromProto(DdlRel.DdlObject proto) {
      for (DdlObject v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  public enum DdlOp {
    UNSPECIFIED(DdlRel.DdlOp.DDL_OP_UNSPECIFIED),
    CREATE(DdlRel.DdlOp.DDL_OP_CREATE),
    CREATE_OR_REPLACE(DdlRel.DdlOp.DDL_OP_CREATE_OR_REPLACE),
    ALTER(DdlRel.DdlOp.DDL_OP_ALTER),
    DROP(DdlRel.DdlOp.DDL_OP_DROP),
    DROP_IF_EXIST(DdlRel.DdlOp.DDL_OP_DROP_IF_EXIST);

    private final DdlRel.DdlOp proto;

    DdlOp(DdlRel.DdlOp proto) {
      this.proto = proto;
    }

    public DdlRel.DdlOp toProto() {
      return proto;
    }

    public static DdlOp fromProto(DdlRel.DdlOp proto) {
      for (DdlOp v : values()) {
        if (v.proto == proto) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unknown type: " + proto);
    }
  }

  @Override
  public Type.Struct deriveRecordType() {
    return getTableSchema().struct();
  }
}
