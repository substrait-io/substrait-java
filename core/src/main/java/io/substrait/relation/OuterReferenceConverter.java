package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.plan.Plan;
import io.substrait.util.EmptyVisitationContext;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Converts a Substrait plan between the two outer-reference resolution encodings:
 *
 * <ul>
 *   <li><b>offset-based</b>: {@link FieldReference#outerReferenceStepsOut()} counts subquery
 *       boundaries up to the referenced relation;
 *   <li><b>id-based</b>: {@link FieldReference#outerReferenceRelReference()} names the referenced
 *       relation by its {@link Rel#getRelAnchor() rel anchor}.
 * </ul>
 *
 * <p>The two forms are semantically equivalent for tree-shaped plans; this class translates one
 * into the other so integrations can rely on a single encoding while the wire format keeps both
 * (see the Substrait <a
 * href="https://github.com/substrait-io/substrait/blob/main/site/docs/spec/breaking_change_policy.md">breaking
 * change policy</a>). Anchor assignment requires plan-wide context, which is why this lives in core
 * rather than being duplicated per integration.
 *
 * <p><b>Supported scope:</b> outer references whose binding relation is the input of a single-input
 * expression host ({@link Filter}, {@link Project}). This covers correlated scalar/IN/EXISTS
 * subqueries as produced by the SQL integrations. Multi-input scopes (a correlated reference into a
 * join/set condition) and shared subtrees ({@code ReferenceRel}) are not representable as a single
 * binding relation and cause an {@link UnsupportedOperationException}.
 *
 * <p><b>Anchor allocation</b> starts at {@code 1}; callers must not pass {@code toIdBased} a plan
 * that already carries {@code rel_anchor}s.
 */
public final class OuterReferenceConverter {

  private OuterReferenceConverter() {}

  private enum Direction {
    TO_ID,
    TO_STEPS_OUT
  }

  /**
   * Rewrites offset-based outer references ({@code steps_out}) as id-based references ({@code
   * rel_reference}), assigning a {@link Rel#getRelAnchor() rel anchor} to each referenced relation.
   *
   * @param root the relation tree to convert
   * @return an equivalent tree using id-based outer references
   */
  public static Rel toIdBased(Rel root) {
    return convert(root, Direction.TO_ID);
  }

  /**
   * Rewrites id-based outer references ({@code rel_reference}) as offset-based references ({@code
   * steps_out}), removing the {@link Rel#getRelAnchor() rel anchors} they resolved to.
   *
   * @param root the relation tree to convert
   * @return an equivalent tree using offset-based outer references
   */
  public static Rel toStepsOut(Rel root) {
    return convert(root, Direction.TO_STEPS_OUT);
  }

  /**
   * Applies {@link #toIdBased(Rel)} to every root of the given plan.
   *
   * @param plan the plan to convert
   * @return an equivalent plan using id-based outer references
   */
  public static Plan toIdBased(Plan plan) {
    return convert(plan, Direction.TO_ID);
  }

  /**
   * Applies {@link #toStepsOut(Rel)} to every root of the given plan.
   *
   * @param plan the plan to convert
   * @return an equivalent plan using offset-based outer references
   */
  public static Plan toStepsOut(Plan plan) {
    return convert(plan, Direction.TO_STEPS_OUT);
  }

  private static Plan convert(Plan plan, Direction direction) {
    List<Plan.Root> roots = new ArrayList<>(plan.getRoots().size());
    for (Plan.Root root : plan.getRoots()) {
      roots.add(Plan.Root.builder().from(root).input(convert(root.getInput(), direction)).build());
    }
    return Plan.builder().from(plan).roots(roots).build();
  }

  private static Rel convert(Rel root, Direction direction) {
    State state = new State(direction);
    RelRewriter relRewriter = new RelRewriter(state);
    return root.accept(relRewriter, EmptyVisitationContext.INSTANCE).orElse(root);
  }

  /** Mutable state shared between the paired relation and expression rewriters. */
  private static final class State {
    final Direction direction;

    /**
     * Stack of enclosing scope relations, one entry per subquery boundary. A {@code null} entry
     * marks a multi-input (unsupported) scope. {@code steps_out=N} resolves to the entry {@code N}
     * from the top.
     */
    final List<Rel> outerScopes = new ArrayList<>();

    /**
     * Anchors assigned to binding relations while converting to the id-based form (by identity).
     */
    final Map<Rel, Integer> anchorByRel = new IdentityHashMap<>();

    /** Anchors that were resolved to a {@code steps_out} value and should be stripped from rels. */
    final java.util.Set<Integer> resolvedAnchors = new java.util.HashSet<>();

    /** The input relation whose expressions are currently being rewritten (RootReference scope). */
    Rel currentInput;

    int nextAnchor = 1;

    State(Direction direction) {
      this.direction = direction;
    }

    int allocateAnchor(Rel binding) {
      Integer existing = anchorByRel.get(binding);
      if (existing != null) {
        return existing;
      }
      int anchor = nextAnchor++;
      anchorByRel.put(binding, anchor);
      return anchor;
    }
  }

  /**
   * Relation rewriter that maintains the scope stack and stamps/strips {@code rel_anchor}s. Only
   * single-input expression hosts are handled explicitly; every other relation is traversed by the
   * copy-on-write base class.
   */
  private static final class RelRewriter extends RelCopyOnWriteVisitor<RuntimeException> {
    private final State state;

    RelRewriter(State state) {
      super(relVisitor -> new ExprRewriter(relVisitor, state));
      this.state = state;
    }

    @Override
    public Optional<Rel> visit(Filter filter, EmptyVisitationContext context) {
      // Expressions first, so a reference binding to this filter's input is discovered before the
      // input is (re)built and (for the id-based direction) stamped with its anchor.
      Optional<Expression> condition =
          rewriteInScope(
              filter.getInput(),
              () -> filter.getCondition().accept(getExpressionCopyOnWriteVisitor(), context));
      Rel newInput = rewriteInput(filter.getInput());

      if (!condition.isPresent() && newInput == filter.getInput()) {
        return Optional.empty();
      }
      return Optional.of(
          Filter.builder()
              .from(filter)
              .input(newInput)
              .condition(condition.orElse(filter.getCondition()))
              .build());
    }

    @Override
    public Optional<Rel> visit(Project project, EmptyVisitationContext context) {
      Optional<List<Expression>> expressions =
          rewriteInScope(
              project.getInput(), () -> visitExprList(project.getExpressions(), context));
      Rel newInput = rewriteInput(project.getInput());

      if (!expressions.isPresent() && newInput == project.getInput()) {
        return Optional.empty();
      }
      return Optional.of(
          Project.builder()
              .from(project)
              .input(newInput)
              .expressions(expressions.orElse(project.getExpressions()))
              .build());
    }

    @Override
    public Optional<Rel> visit(NamedDdl ddl, EmptyVisitationContext context) {
      // The copy-on-write base throws for DDL; a view definition is an independent (top-level)
      // query that may itself contain correlated subqueries, so traverse it rather than throw.
      if (!ddl.getViewDefinition().isPresent()) {
        return Optional.empty();
      }
      return ddl.getViewDefinition()
          .get()
          .accept(this, context)
          .map(definition -> NamedDdl.builder().from(ddl).viewDefinition(definition).build());
    }

    /**
     * Runs the given expression rewrite with {@code currentInput} temporarily set to {@code input},
     * so that subqueries entered while rewriting push the correct scope.
     */
    private <T> T rewriteInScope(Rel input, java.util.function.Supplier<T> rewrite) {
      Rel saved = state.currentInput;
      state.currentInput = input;
      try {
        return rewrite.get();
      } finally {
        state.currentInput = saved;
      }
    }

    /**
     * Rebuilds the input and applies the anchor stamp/strip decided while rewriting expressions.
     */
    private Rel rewriteInput(Rel originalInput) {
      Rel rebuilt =
          originalInput.accept(this, EmptyVisitationContext.INSTANCE).orElse(originalInput);
      if (state.direction == Direction.TO_ID) {
        Integer anchor = state.anchorByRel.get(originalInput);
        if (anchor != null) {
          return rebuilt.withRelAnchor(anchor);
        }
      } else {
        Optional<Integer> anchor = originalInput.getRelAnchor();
        if (anchor.isPresent() && state.resolvedAnchors.contains(anchor.get())) {
          return rebuilt.withRelAnchor(Optional.empty());
        }
      }
      return rebuilt;
    }
  }

  /** Expression rewriter that pushes/pops subquery scopes and rewrites outer field references. */
  private static final class ExprRewriter extends ExpressionCopyOnWriteVisitor<RuntimeException> {
    private final State state;

    ExprRewriter(RelCopyOnWriteVisitor<RuntimeException> relVisitor, State state) {
      super(relVisitor);
      this.state = state;
    }

    @Override
    public Optional<Expression> visit(FieldReference fieldReference, EmptyVisitationContext context)
        throws RuntimeException {
      if (state.direction == Direction.TO_ID
          && fieldReference.outerReferenceStepsOut().isPresent()) {
        int anchor = anchorForStepsOut(fieldReference.outerReferenceStepsOut().get());
        return Optional.of(
            ImmutableFieldReference.builder()
                .from(fieldReference)
                .outerReferenceStepsOut(Optional.empty())
                .outerReferenceRelReference(anchor)
                .build());
      }
      if (state.direction == Direction.TO_STEPS_OUT
          && fieldReference.outerReferenceRelReference().isPresent()) {
        int stepsOut = stepsOutForAnchor(fieldReference.outerReferenceRelReference().get());
        return Optional.of(
            ImmutableFieldReference.builder()
                .from(fieldReference)
                .outerReferenceRelReference(Optional.empty())
                .outerReferenceStepsOut(stepsOut)
                .build());
      }

      // Non-outer references: preserve all attributes, rewriting only a nested input expression.
      if (fieldReference.inputExpression().isPresent()) {
        Optional<Expression> newInput =
            fieldReference.inputExpression().get().accept(this, context);
        if (newInput.isPresent()) {
          return Optional.of(
              ImmutableFieldReference.builder()
                  .from(fieldReference)
                  .inputExpression(newInput)
                  .build());
        }
      }
      return Optional.empty();
    }

    private int anchorForStepsOut(int stepsOut) {
      int index = state.outerScopes.size() - stepsOut;
      if (index < 0 || index >= state.outerScopes.size()) {
        throw new IllegalArgumentException(
            "Outer reference steps_out=" + stepsOut + " exceeds the enclosing subquery depth");
      }
      Rel binding = state.outerScopes.get(index);
      if (binding == null) {
        throw new UnsupportedOperationException(
            "Cannot assign a rel_anchor for an outer reference that resolves to a multi-input "
                + "(e.g. join) scope; only single-input scopes are supported");
      }
      return state.allocateAnchor(binding);
    }

    private int stepsOutForAnchor(int relReference) {
      for (int i = state.outerScopes.size() - 1; i >= 0; i--) {
        Rel scope = state.outerScopes.get(i);
        if (scope != null
            && scope.getRelAnchor().isPresent()
            && scope.getRelAnchor().get() == relReference) {
          state.resolvedAnchors.add(relReference);
          return state.outerScopes.size() - i;
        }
      }
      throw new UnsupportedOperationException(
          "Cannot resolve outer reference rel_reference="
              + relReference
              + " to an enclosing scope; the referenced relation is not an ancestor (shared "
              + "subtrees / ReferenceRel are not supported)");
    }

    @Override
    public Optional<Expression> visit(
        Expression.ScalarSubquery scalarSubquery, EmptyVisitationContext context) {
      return withSubqueryScope(() -> super.visit(scalarSubquery, context));
    }

    @Override
    public Optional<Expression> visit(
        Expression.SetPredicate setPredicate, EmptyVisitationContext context) {
      return withSubqueryScope(() -> super.visit(setPredicate, context));
    }

    @Override
    public Optional<Expression> visit(
        Expression.InPredicate inPredicate, EmptyVisitationContext context) {
      // needles are evaluated in the current (outer) scope; the haystack is the subquery boundary.
      Optional<List<Expression>> needles = visitExprList(inPredicate.needles(), context);
      Optional<io.substrait.relation.Rel> haystack =
          withSubqueryScope(
              () -> inPredicate.haystack().accept(getRelCopyOnWriteVisitor(), context));

      if (!needles.isPresent() && !haystack.isPresent()) {
        return Optional.empty();
      }
      return Optional.of(
          Expression.InPredicate.builder()
              .from(inPredicate)
              .haystack(haystack.orElse(inPredicate.haystack()))
              .needles(needles.orElse(inPredicate.needles()))
              .build());
    }

    /** Pushes {@code currentInput} as a new subquery scope, runs the action, then pops it. */
    private <T> T withSubqueryScope(java.util.function.Supplier<T> action) {
      Rel savedInput = state.currentInput;
      state.outerScopes.add(state.currentInput);
      try {
        return action.get();
      } finally {
        state.outerScopes.remove(state.outerScopes.size() - 1);
        state.currentInput = savedInput;
      }
    }
  }
}
