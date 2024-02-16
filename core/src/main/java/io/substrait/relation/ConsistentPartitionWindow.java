package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.expression.Expression.SortField;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.FunctionOption;
import io.substrait.expression.WindowBound;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
@Value.Enclosing
public abstract class ConsistentPartitionWindow extends SingleInputRel implements HasExtension {

  public abstract List<WindowRelFunctionInvocation> getWindowFunctions();

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

  @Value.Immutable
  public abstract static class WindowRelFunctionInvocation {

    public abstract SimpleExtension.WindowFunctionVariant declaration();

    public abstract List<FunctionArg> arguments();

    public abstract Map<String, FunctionOption> options();

    public abstract Type outputType();

    public abstract Expression.AggregationPhase aggregationPhase();

    public abstract Expression.AggregationInvocation invocation();

    public abstract WindowBound lowerBound();

    public abstract WindowBound upperBound();

    public abstract Expression.WindowBoundsType boundsType();

    public static ImmutableConsistentPartitionWindow.WindowRelFunctionInvocation.Builder builder() {
      return ImmutableConsistentPartitionWindow.WindowRelFunctionInvocation.builder();
    }
  }
}
