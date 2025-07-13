package io.substrait.relation;

import java.util.Collections;
import java.util.List;

public abstract class ZeroInputRel extends AbstractRel {

  @Override
  public final List<Rel> getInputs() {
    return Collections.emptyList();
  }
}
