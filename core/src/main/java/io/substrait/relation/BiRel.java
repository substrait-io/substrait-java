package io.substrait.relation;

import java.util.Arrays;
import java.util.List;

/** Abstract base class for binary relations that have exactly two input relations. */
public abstract class BiRel extends AbstractRel {

  /**
   * Returns the left input relation.
   *
   * @return the left input
   */
  public abstract Rel getLeft();

  /**
   * Returns the right input relation.
   *
   * @return the right input
   */
  public abstract Rel getRight();

  @Override
  public final List<Rel> getInputs() {
    return Arrays.asList(getLeft(), getRight());
  }
}
