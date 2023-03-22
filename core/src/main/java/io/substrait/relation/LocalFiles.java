package io.substrait.relation;

import io.substrait.io.substrait.extension.AdvancedExtension;
import io.substrait.relation.files.FileOrFiles;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class LocalFiles extends AbstractReadRel {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalFiles.class);

  public abstract List<FileOrFiles> getItems();

  public abstract Optional<AdvancedExtension> getCommonExtension();

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableLocalFiles.Builder builder() {
    return ImmutableLocalFiles.builder();
  }
}
