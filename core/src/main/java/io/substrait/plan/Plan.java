package io.substrait.plan;

import io.substrait.proto.AdvancedExtension;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
import io.substrait.relation.Rel;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Plan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Plan.class);

  public abstract List<Root> getRoots();

  public abstract List<String> getExpectedTypeUrls();

  public abstract List<SimpleExtensionDeclaration> getExtensionDeclarations();

  public abstract List<SimpleExtensionURI> getExtensionUris();

  public abstract Optional<AdvancedExtension> getAdvancedExtension();

  @Value.Immutable
  public abstract static class Root {
    public abstract Rel getInput();

    public abstract List<String> getNames();
  }
}
