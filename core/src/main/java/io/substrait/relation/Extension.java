package io.substrait.relation;

import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.List;

/** Contains tag interfaces for handling {@link com.google.protobuf.Any} types within Substrait. */
public class Extension {

  public interface LeafRelDetail extends ToProto<com.google.protobuf.Any> {
    /**
     * @return the record layout for the associated {@link ExtensionLeaf} relation
     */
    Type.Struct deriveRecordType();
  }

  public interface SingleRelDetail extends ToProto<com.google.protobuf.Any> {
    /**
     * @param input to the associated {@link ExtensionSingle} relation
     * @return the record layout for the associated {@link ExtensionSingle} relation
     */
    Type.Struct deriveRecordType(Rel input);
  }

  public interface MultiRelDetail extends ToProto<com.google.protobuf.Any> {
    /**
     * @param inputs to the associated {@link ExtensionMulti} relation
     * @return the record layout for the associated {@link ExtensionMulti} relation
     */
    Type.Struct deriveRecordType(List<Rel> inputs);
  }

  public interface ExtensionTableDetail extends ToProto<com.google.protobuf.Any> {
    /**
     * @return the table schema for the associated {@link ExtensionTable} relation
     */
    NamedStruct deriveSchema();
  }

  public interface WriteExtensionObject extends ToProto<com.google.protobuf.Any> {}

  public interface DdlExtensionObject extends ToProto<com.google.protobuf.Any> {}
}
