package io.substrait.spark

import com.google.protobuf
import io.substrait.extension.ExtensionLookup
import io.substrait.relation.{ProtoRelConverter, RelProtoConverter}
import io.substrait.relation.Extension.WriteExtensionObject
import io.substrait.relation.files.FileOrFiles

case class FileHolder(fileOrFiles: FileOrFiles) extends WriteExtensionObject {

  override def toProto(converter: RelProtoConverter): protobuf.Any = {
    protobuf.Any.pack(fileOrFiles.toProto)
  }
}

class FileHolderHandlingProtoRelConverter(lookup: ExtensionLookup)
  extends ProtoRelConverter(lookup) {
  override def detailFromWriteExtensionObject(any: protobuf.Any): WriteExtensionObject = {
    FileHolder(
      newFileOrFiles(any.unpack(classOf[io.substrait.proto.ReadRel.LocalFiles.FileOrFiles])))
  }
}
