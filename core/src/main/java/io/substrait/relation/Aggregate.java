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
    // If there's only one grouping set (or none), the nullability rule doesn't apply.
    if (getGroupings().size() <= 1) {
      final Stream<Type> groupingTypes =
          getGroupings().stream()
              .flatMap(g -> g.getExpressions().stream())
              .map(Expression::getType);
      final Stream<Type> measureTypes = getMeasures().stream().map(t -> t.getFunction().getType());
      return TypeCreator.REQUIRED.struct(Stream.concat(groupingTypes, measureTypes));
    }

    final LinkedHashSet<Expression> uniqueExpressions =
        getGroupings().stream()
            .flatMap(g -> g.getExpressions().stream())
            .collect(Collectors.toCollection(LinkedHashSet::new));

    // For each unique expression, determine its final nullability based on the spec.
    final Stream<Type> groupingTypes =
        uniqueExpressions.stream()
            .map(
                expr -> {
                  // the code below implements the following statement from the spec
                  // (https://substrait.io/relations/logical_relations/#aggregate-operation):
                  // "The values for the grouping expression columns that are not
                  // part of the grouping set for a particular record will be set to null."
                  final boolean appearsInAllSets =
                      getGroupings().stream().allMatch(g -> g.getExpressions().contains(expr));
                  if (appearsInAllSets) {
                    return expr.getType();
                  } else {
                    return TypeCreator.asNullable(expr.getType());
                  }
                });

    final Stream<Type> measureTypes = getMeasures().stream().map(t -> t.getFunction().getType());

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
