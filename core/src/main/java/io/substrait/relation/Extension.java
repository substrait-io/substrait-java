package io.substrait.relation;

import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import java.util.List;

/** Contains tag interfaces for handling {@link com.google.protobuf.Any} types within Substrait. */
public class Extension {

  /** Detail object carrying the behavior of an {@link ExtensionLeaf} relation. */
  public interface LeafRelDetail extends ToProto<com.google.protobuf.Any> {
    /**
     * @return the record layout for the associated {@link ExtensionLeaf} relation
     */
    Type.Struct deriveRecordType();
  }

  /** Detail object carrying the behavior of an {@link ExtensionSingle} relation. */
  public interface SingleRelDetail extends ToProto<com.google.protobuf.Any> {
    /**
     * @param input to the associated {@link ExtensionSingle} relation
     * @return the record layout for the associated {@link ExtensionSingle} relation
     */
    Type.Struct deriveRecordType(Rel input);
  }

  /** Detail object carrying the behavior of an {@link ExtensionMulti} relation. */
  public interface MultiRelDetail extends ToProto<com.google.protobuf.Any> {
    /**
     * @param inputs to the associated {@link ExtensionMulti} relation
     * @return the record layout for the associated {@link ExtensionMulti} relation
     */
    Type.Struct deriveRecordType(List<Rel> inputs);
  }

  /** Detail object carrying the behavior of an {@link ExtensionTable} relation. */
  public interface ExtensionTableDetail extends ToProto<com.google.protobuf.Any> {
    /**
     * @return the table schema for the associated {@link ExtensionTable} relation
     */
    NamedStruct deriveSchema();
  }

  /** Detail object carrying the behavior of an {@link ExtensionWrite} relation. */
  public interface WriteExtensionObject extends ToProto<com.google.protobuf.Any> {}

  /** Detail object carrying the behavior of an {@link ExtensionDdl} relation. */
  public interface DdlExtensionObject extends ToProto<com.google.protobuf.Any> {}
}
