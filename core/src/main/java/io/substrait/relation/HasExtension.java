package io.substrait.relation;

import io.substrait.extension.AdvancedExtension;
import java.util.Optional;

/** Used to indicate the potential presence of an {@link AdvancedExtension} */
public interface HasExtension {
  /**
   * @return the {@link AdvancedExtension} associated directly with the class
   */
  Optional<AdvancedExtension> getExtension();
}
