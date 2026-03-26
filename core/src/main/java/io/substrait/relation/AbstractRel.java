package io.substrait.relation;

import io.substrait.type.Type;
import io.substrait.util.Util;
import java.util.function.Supplier;

/**
 * Abstract base class for relations that provides common functionality for deriving and caching
 * record types with optional remapping.
 */
public abstract class AbstractRel implements Rel {

  private Supplier<Type.Struct> recordType =
      Util.memoize(
          () -> {
            Type.Struct s = deriveRecordType();
            return getRemap().map(r -> r.remap(s)).orElse(s);
          });

  /**
   * Derives the record type for this relation before any remapping is applied.
   *
   * @return the derived record type
   */
  protected abstract Type.Struct deriveRecordType();

  @Override
  public final Type.Struct getRecordType() {
    return recordType.get();
  }
}
