package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.expression.Expression.SortField;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ConsistentPartitionWindow extends SingleInputRel implements HasExtension {

  public abstract List<Expression.WindowRelFunctionInvocation> getWindowFunctions();

  public abstract List<Expression> getPartitionExpressions();

  public abstract List<SortField> getSorts();

  @Override
  protected Type.Struct deriveRecordType() {
    Type.Struct initial = getInput().getRecordType();
    return TypeCreator.of(initial.nullable())
        .struct(
            Stream.concat(
                initial.fields().stream(),
                getPartitionExpressions().stream().map(Expression::getType)));
  }

  @Override
  public <O, E extends Exception> O accept(RelVisitor<O, E> visitor) throws E {
    return visitor.visit(this);
  }

  public static ImmutableConsistentPartitionWindow.Builder builder() {
    return ImmutableConsistentPartitionWindow.builder();
  }
}
