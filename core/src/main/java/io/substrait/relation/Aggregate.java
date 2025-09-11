package io.substrait.relation;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Aggregate extends SingleInputRel implements HasExtension {

  public abstract List<Grouping> getGroupings();

  public abstract List<Measure> getMeasures();

  @Override
  protected Type.Struct deriveRecordType() {
    boolean isGroupingSet = getGroupings().size() > 1;

    Stream<Type> groupingTypes =
        getGroupings().stream()
            .flatMap(g -> g.getExpressions().stream())
            .collect(Collectors.toCollection(LinkedHashSet::new))
            .stream()
            .map(
                expr -> {
                  if (isGroupingSet) {
                    return TypeCreator.asNullable(expr.getType());
                  }
                  return expr.getType();
                });

    Stream<Type> measureTypes = getMeasures().stream().map(t -> t.getFunction().getType());

    return TypeCreator.REQUIRED.struct(Stream.concat(groupingTypes, measureTypes));
  }

  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  @Value.Immutable
  public abstract static class Grouping {
    public abstract List<Expression> getExpressions();

    public static ImmutableGrouping.Builder builder() {
      return ImmutableGrouping.builder();
    }
  }

  @Value.Immutable
  public abstract static class Measure {
    public abstract AggregateFunctionInvocation getFunction();

    public abstract Optional<Expression> getPreMeasureFilter();

    public static ImmutableMeasure.Builder builder() {
      return ImmutableMeasure.builder();
    }
  }

  public static ImmutableAggregate.Builder builder() {
    return ImmutableAggregate.builder();
  }
}
