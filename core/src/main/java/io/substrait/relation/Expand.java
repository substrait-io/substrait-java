package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.List;
import org.immutables.value.Value;

@Value.Enclosing
@Value.Immutable
public abstract class Expand extends SingleInputRel {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Expand.class);

  public abstract List<ExpandField> getFields();

  @Override
  public Type.Struct deriveRecordType() {
    Type.Struct initial = getInput().getRecordType();
    return TypeCreator.of(initial.nullable())
        .struct(getFields().stream().map(ExpandField::getType));
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  public static ImmutableExpand.Builder builder() {
    return ImmutableExpand.builder();
  }

  public interface ExpandField {
    Type getType();
  }

  @Value.Immutable
  public abstract static class ConsistentField implements ExpandField {
    public abstract Expression getExpression();

    public Type getType() {
      return getExpression().getType();
    }

    public static ImmutableExpand.ConsistentField.Builder builder() {
      return ImmutableExpand.ConsistentField.builder();
    }
  }

  @Value.Immutable
  public abstract static class SwitchingField implements ExpandField {
    public abstract List<Expression> getDuplicates();

    public Type getType() {
      boolean nullable = getDuplicates().stream().anyMatch(d -> d.getType().nullable());
      Type type = getDuplicates().get(0).getType();
      return nullable ? TypeCreator.asNullable(type) : TypeCreator.asNotNullable(type);
    }

    public static ImmutableExpand.SwitchingField.Builder builder() {
      return ImmutableExpand.SwitchingField.builder();
    }
  }
}
