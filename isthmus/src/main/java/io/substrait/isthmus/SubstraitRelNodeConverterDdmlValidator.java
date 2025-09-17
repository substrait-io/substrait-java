package io.substrait.isthmus;

import io.substrait.relation.AbstractDdlRel;
import io.substrait.relation.AbstractRelVisitor;
import io.substrait.relation.AbstractWriteRel;
import io.substrait.relation.NamedDdl;
import io.substrait.relation.NamedWrite;
import io.substrait.relation.Rel;
import io.substrait.util.NoException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.tools.RelBuilder;

public class SubstraitRelNodeConverterDdmlValidator
    extends AbstractRelVisitor<ValidationResult, SubstraitRelNodeConverter.Context, NoException> {
  private static final Set<AbstractWriteRel.WriteOp> NAMED_WRITE_OPERATIONS_SUPPORTED =
      Set.of(
          AbstractWriteRel.WriteOp.INSERT,
          AbstractWriteRel.WriteOp.DELETE,
          AbstractWriteRel.WriteOp.UPDATE,
          AbstractWriteRel.WriteOp.CTAS);

  private final RelBuilder relBuilder;
  private final SubstraitRelNodeConverter substraitRelNodeConverter;

  public SubstraitRelNodeConverterDdmlValidator(
      final RelBuilder relBuilder, SubstraitRelNodeConverter substraitRelNodeConverter) {
    this.relBuilder = relBuilder;
    this.substraitRelNodeConverter = substraitRelNodeConverter;
  }

  @Override
  public ValidationResult visitFallback(Rel rel, SubstraitRelNodeConverter.Context context) {
    return ValidationResult.ok();
  }

  @Override
  public ValidationResult visit(NamedDdl namedDdl, SubstraitRelNodeConverter.Context ctx) {
    final List<String> validationMessages = new LinkedList<>();
    if (namedDdl.getViewDefinition().isEmpty()) {
      validationMessages.add("No view definition found");
    }
    if (!AbstractDdlRel.DdlObject.VIEW.equals(namedDdl.getObject())) {
      validationMessages.add("Unsupported object: " + namedDdl.getObject().name());
    }
    if (!AbstractDdlRel.DdlOp.CREATE.equals(namedDdl.getOperation())) {
      validationMessages.add("Unsupported operation: " + namedDdl.getOperation().name());
    }
    return validationMessages.isEmpty()
        ? ValidationResult.SUCCESS
        : ValidationResult.error(validationMessages);
  }

  @Override
  public ValidationResult visit(NamedWrite namedWrite, SubstraitRelNodeConverter.Context ctx) {
    final List<String> validationMessages = new LinkedList<>();
    if (!NAMED_WRITE_OPERATIONS_SUPPORTED.contains(namedWrite.getOperation())) {
      validationMessages.add(
          "Write operation '"
              + namedWrite.getOperation()
              + "' is not supported by the NamedWrite visitor. "
              + "Check if a more specific relation type (e.g., NamedUpdate) should be used.");
    }
    if (!AbstractWriteRel.WriteOp.CTAS.equals(namedWrite.getOperation())
        && (relBuilder.getRelOptSchema() == null
            || relBuilder.getRelOptSchema().getTableForMember(namedWrite.getNames()) == null)) {
      validationMessages.add("Target table not found in Calcite catalog: " + namedWrite.getNames());
    }

    if (namedWrite.getInput().accept(substraitRelNodeConverter, ctx) == null) {
      validationMessages.add("Only write operations with input supported");
    }
    return validationMessages.isEmpty()
        ? ValidationResult.SUCCESS
        : ValidationResult.error(validationMessages);
  }
}
