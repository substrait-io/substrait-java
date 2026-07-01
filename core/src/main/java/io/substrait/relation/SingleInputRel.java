package io.substrait.relation;

import java.util.Collections;
import java.util.List;

/** Base class for relations that operate on exactly one input relation. */
public abstract class SingleInputRel extends AbstractRel {

  /**
   * Returns the single input relation.
   *
   * @return the input relation
   */
  public abstract Rel getInput();

  @Override
  public final List<Rel> getInputs() {
    return Collections.singletonList(getInput());
  }
}
