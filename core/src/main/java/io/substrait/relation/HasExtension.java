package io.substrait.relation;

import io.substrait.io.substrait.extension.AdvancedExtension;
import java.util.Optional;

/**
 * Used to indicate the presence of an {@link AdvancedExtension} directly on a relation
 *
 * <p>This exists outside of {@link Rel} because there are relations that do not have advanced
 * extensions. See {@link ExtensionLeaf}, {@link ExtensionSingle}, {@link ExtensionMulti}).
 */
public interface HasExtension {
  /**
   * @return the {@link AdvancedExtension} associated directly with the relation, if present
   */
  Optional<AdvancedExtension> getRelExtension();
}
