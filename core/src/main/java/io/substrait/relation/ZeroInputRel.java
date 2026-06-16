package io.substrait.relation;

import java.util.Collections;
import java.util.List;

/**
 * Base class for relations that have no input relations, such as leaf relations that read directly
 * from a source.
 */
public abstract class ZeroInputRel extends AbstractRel {

  @Override
  public final List<Rel> getInputs() {
    return Collections.emptyList();
  }
}
