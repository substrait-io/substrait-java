package io.substrait.relation;

import io.substrait.extension.AdvancedExtension;
import java.util.Optional;

/** Used to indicate the potential presence of an {@link AdvancedExtension} */
public interface HasExtension {
  /**
   * @return the {@link AdvancedExtension} associated directly with the class
   */
  Optional<AdvancedExtension> getExtension();

  /**
   * Returns a copy of this value with its {@link #getExtension() extension} set to the given
   * optional value ({@link Optional#empty()} clears it).
   *
   * <p>Overridden by the generated Immutables {@code withExtension(Optional)} on every concrete
   * implementor, this provides a type-agnostic way to set the extension a value carries directly —
   * the counterpart of {@link Rel#withCommonExtension(Optional)} for the {@link
   * io.substrait.proto.RelCommon} one — rather than switching on the concrete type and rebuilding
   * through its builder. Hand-written implementors that are not Immutables-backed inherit this
   * throwing default.
   *
   * <p>The return type is {@code HasExtension} rather than {@code Rel} because non-relations
   * ({@link io.substrait.hint.Hint} and its nested types) implement this interface too, so a caller
   * holding a {@link Rel} has to cast the result back. The generated override returns the concrete
   * implementor, so that cast never fails for an Immutables-backed relation.
   *
   * <p>The {@code ? extends} wildcard has to match what Immutables emits exactly. No
   * {@code @Override} links the two, so a narrower parameter type would not override the generated
   * method — it would clash with it, having the same erasure while neither overrides the other.
   *
   * @param extension the extension to set, or empty to clear it
   * @return a copy of this value carrying the given extension
   */
  default HasExtension withExtension(Optional<? extends AdvancedExtension> extension) {
    throw new UnsupportedOperationException(getClass() + " does not support setting an extension");
  }
}
