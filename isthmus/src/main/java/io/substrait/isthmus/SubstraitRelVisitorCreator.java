package io.substrait.isthmus;

import io.substrait.extension.SimpleExtension;
import org.apache.calcite.rel.type.RelDataTypeFactory;

public interface SubstraitRelVisitorCreator {
  SubstraitRelVisitor create(
      RelDataTypeFactory typeFactory,
      SimpleExtension.ExtensionCollection extensionCollection,
      FeatureBoard featureBoard);
}
