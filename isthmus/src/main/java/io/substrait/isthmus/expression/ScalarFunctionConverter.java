package io.substrait.isthmus.expression;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction.Flag;
import org.apache.calcite.sql.type.SqlTypeName;

public class ScalarFunctionConverter
    extends FunctionConverter<
        SimpleExtension.ScalarFunctionVariant,
        Expression,
        ScalarFunctionConverter.WrappedScalarCall>
    implements CallConverter {

  private enum Trim {
    TRIM("trim", Flag.BOTH),
    LTRIM("ltrim", Flag.LEADING),
    RTRIM("rtrim", Flag.TRAILING);

    private final String substraitName;
    private final Flag flag;

    Trim(String substraitName, Flag flag) {
      this.substraitName = substraitName;
      this.flag = flag;
    }

    public String getSubstraitName() {
      return substraitName;
    }

    public Flag getFlag() {
      return flag;
    }

    public static Optional<Trim> from(Flag flag) {
      return Arrays.stream(values()).filter(t -> t.flag == flag).findAny();
    }
  }

  private final Map<Trim, ScalarFunctionVariant> trimFunctions;

  public ScalarFunctionConverter(
      List<SimpleExtension.ScalarFunctionVariant> functions, RelDataTypeFactory typeFactory) {
    this(functions, Collections.emptyList(), typeFactory, TypeConverter.DEFAULT);
  }

  public ScalarFunctionConverter(
      List<SimpleExtension.ScalarFunctionVariant> functions,
      List<FunctionMappings.Sig> additionalSignatures,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    super(functions, additionalSignatures, typeFactory, typeConverter);

    var trims = new HashMap<Trim, ScalarFunctionVariant>();
    for (var t : Trim.values()) {
      var function = findFunction(t.getSubstraitName(), functions);
      function.ifPresent(f -> trims.put(t, f));
    }
    trimFunctions = Collections.unmodifiableMap(trims);
  }

  private Optional<ScalarFunctionVariant> findFunction(
      String name, Collection<ScalarFunctionVariant> functions) {
    return functions.stream().filter(f -> name.equals(f.name().toLowerCase(Locale.ROOT))).findAny();
  }

  @Override
  protected ImmutableList<FunctionMappings.Sig> getSigs() {
    return FunctionMappings.SCALAR_SIGS;
  }

  @Override
  public Optional<Expression> convert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    return customConvert(call, topLevelConverter)
        .or(() -> standardConvert(call, topLevelConverter));
  }

  private Optional<Expression> customConvert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    if (SqlStdOperatorTable.TRIM.equals(call.op)) {
      var trim = getTrimFunction(call);

      return trim.map(
          f -> {
            var finder = new FunctionFinder(f.name(), call.op, List.of(f));
            if (!finder.allowedArgCount(call.getOperands().size() - 1)) {
              return null;
            }

            var wrapped =
                new WrappedScalarCall(call) {
                  @Override
                  public Stream<RexNode> getOperands() {
                    return super.getOperands().skip(1);
                  }
                };

            return finder.attemptMatch(wrapped, topLevelConverter).orElse(null);
          });
    }

    return Optional.empty();
  }

  private Optional<SimpleExtension.ScalarFunctionVariant> getTrimFunction(RexCall call) {
    var trimType = call.operands.get(0);
    if (trimType.getType().getSqlTypeName() != SqlTypeName.SYMBOL) {
      return Optional.empty();
    }

    var value = ((RexLiteral) trimType).getValue();
    if (!(value instanceof Flag)) {
      return Optional.empty();
    }

    var trim = Trim.from((Flag) value);
    return trim.map(trimFunctions::get);
  }

  private Optional<Expression> standardConvert(
      RexCall call, Function<RexNode, Expression> topLevelConverter) {
    FunctionFinder m = signatures.get(call.op);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(call.operands.size())) {
      return Optional.empty();
    }

    var wrapped = new WrappedScalarCall(call);
    return m.attemptMatch(wrapped, topLevelConverter);
  }

  @Override
  protected Expression generateBinding(
      WrappedScalarCall call,
      SimpleExtension.ScalarFunctionVariant function,
      List<? extends FunctionArg> arguments,
      Type outputType) {
    return Expression.ScalarFunctionInvocation.builder()
        .outputType(outputType)
        .declaration(function)
        .addAllArguments(arguments)
        .build();
  }

  public List<FunctionArg> getRexArguments(Expression.ScalarFunctionInvocation expression) {
    var result = new LinkedList<>(expression.arguments());

    var name = expression.declaration().name();
    Arrays.stream(Trim.values())
        .filter(t -> t.getSubstraitName().equals(name))
        .findAny()
        .map(Trim::getFlag)
        .map(Flag::name)
        .map(EnumArg::of)
        .ifPresent(result::addFirst);

    return result;
  }

  static class WrappedScalarCall implements GenericCall {

    private final RexCall delegate;

    private WrappedScalarCall(RexCall delegate) {
      this.delegate = delegate;
    }

    @Override
    public Stream<RexNode> getOperands() {
      return delegate.getOperands().stream();
    }

    @Override
    public RelDataType getType() {
      return delegate.getType();
    }
  }
}
