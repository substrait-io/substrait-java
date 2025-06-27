package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.List;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Project extends SingleInputRel implements HasExtension {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Project.class);

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

  public static ImmutableProject.Builder builder() {
    return ImmutableProject.builder();
  }
}
