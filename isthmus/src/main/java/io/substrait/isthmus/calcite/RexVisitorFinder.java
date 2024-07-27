package io.substrait.isthmus.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;

/**
 * Visitor that finds all instances of a given class in a RexNode tree.
 *
 * @param <T> Class type to find instances.
 */
public class RexVisitorFinder<T> extends RexVisitorImpl<Void> {
  final List<T> found;
  final Class<T> findClass;

  public RexVisitorFinder(Class<T> findClass) {
    super(true);
    this.found = new ArrayList<>();
    this.findClass = findClass;
  }

  @Override
  public void visitEach(Iterable<? extends RexNode> expressions) {
    for (RexNode expr : expressions) {
      if (findClass.isInstance(expr)) {
        found.add(findClass.cast(expr));
      }
    }
    super.visitEach(expressions);
  }

  /**
   * Find all instances of the class in the given call.
   *
   * @param call The call to search
   * @return List of instances of the class
   */
  public List<T> find(RexNode call) {
    if (findClass.isInstance(call)) {
      found.add(findClass.cast(call));
    }
    call.accept(this);
    return found;
  }

  /**
   * Find a unique instance of the class in the given call.
   *
   * <p>Throws an exception if more than one instance is found.
   *
   * @param call The call to search
   * @return Optional of the instance of the class
   */
  public Optional<T> findUnique(RexNode call) {
    this.find(call);

    if (this.found.isEmpty()) {
      return Optional.empty();
    }
    if (this.found.size() > 1) {
      throw new IllegalStateException("Found more than one instance of " + findClass);
    }
    return Optional.of(this.found.get(0));
  }
}
