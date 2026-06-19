package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.List;
import java.util.stream.Stream;
import org.immutables.value.Value;

/**
 * A relation that appends additional computed columns to its input by evaluating a list of
 * expressions for each row.
 */
@Value.Immutable
public abstract class Project extends SingleInputRel implements HasExtension {

  /**
   * Returns the expressions whose results are appended to the input columns.
   *
   * @return the projection expressions
   */
  public abstract List<Expression> getExpressions();

  @Override
  public Type.Struct deriveRecordType() {
    Type.Struct initial = getInput().getRecordType();
    return TypeCreator.of(initial.nullable())
        .struct(
            Stream.concat(
                initial.fields().stream(), getExpressions().stream().map(Expression::getType)));
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link Project}.
   *
   * @return a new builder
   */
  public static ImmutableProject.Builder builder() {
    return ImmutableProject.builder();
  }
}
