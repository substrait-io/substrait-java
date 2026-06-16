package io.substrait.relation;

import io.substrait.relation.files.FileOrFiles;
import io.substrait.util.VisitationContext;
import java.util.List;
import org.immutables.value.Value;

/** A read relation that reads data from a set of files or file groups. */
@Value.Immutable
public abstract class LocalFiles extends AbstractReadRel {

  /**
   * Returns the files or file groups to read from.
   *
   * @return the file items
   */
  public abstract List<FileOrFiles> getItems();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link LocalFiles}.
   *
   * @return a new builder
   */
  public static ImmutableLocalFiles.Builder builder() {
    return ImmutableLocalFiles.builder();
  }
}
