package io.substrait.relation;

import io.substrait.relation.files.FileOrFiles;
import io.substrait.util.VisitationContext;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class LocalFiles extends AbstractReadRel {

  public abstract List<FileOrFiles> getItems();

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      final RelVisitor<O, C, E> visitor, final C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableLocalFiles.Builder builder() {
    return ImmutableLocalFiles.builder();
  }
}
