## Representing and mapping Function arguments
- `FunctionArg` is a marker interface
- subtypes are `Expression`, `Type`, and  the new `EnumArg` class

### EnumArg class
- captured as the `Option` value and a reference to the `Enum`
- Reference to the `Enum` is a reference to an `SimpleExtension.EnumArgument` instance.
  - TODO: should also include the `FunctionAnchor`

## Changes to Function invocation structure
- `ScalarFunctionInvocation`, `AggregateFunctionInvocation`
  - `arguments` is now  of type `List<FunctionArg>`
  - currently retained `List<Expression> exprArguments()` which is a view on `arguments`
    - seems useful. Need feedback..
    - TODO: memonize

## Proto mapping
- **To Proto**
  - map substrait `Expression` and `Type` instances to proto and then wrap them in a `FunctionArgument`
  - map an `EnumArg` instance to a `FunctionArgument.Enum` that only holds the option value.
- **From Proto**
  - map `FunctionArgument`s wrapping `Type` and `Expression` to substrait `Type and `Expression` respectively
  - for an `FunctionArgument.Enum` value at a particular parameter index in the function invocation
    - look within possible `SimpleExtension.EnumArgument` and pick the first one that has the option value
      in its options list.

## Other changes in core
- `RelCopyOnWriteVisitor`
  - TODO: incorporate `FunctionArg` in trees
  - for now replaced `expr.arguments()` with `expr.exprArguments()`
- `ExpressionCreator`
  - factory methods `scalarFunction`, `aggregateFunction` now take `FunctionArg` parameters.

## Calcite mapping
- **EnumConverter**
  - Encapsulate mapping calcite Enums to substrait EnumArgs.
  - provide unctions for
  - convert `EnumArg` to `RexLiteral`
  - convert `RexLiteral` to `EnumArg` within the context of a `SimpleExtension.Function`
- **To Calcite**
  - `ExpressionRexConverter` implements `FunctionArg.FuncArgVisitor`
    - For `Expression` FunctionArg just call `accept`
    - For `Type` FunctionArg throws `UnsupportedException`
      - TODO: how to map Type method parameters to calcite `RexCall`?
    - For `EnumArg` FunctionArg
      - call the `EnumConverter`
    - visit for a `Expression.ScalarFunctionInvocation`
      - map the `expr.arguments` by calling `acceptFuncArgVis`
- **From Calcite**
  - `FunctionFinder:attemptMatch`
    - the `RexLiteral` of `Enum` values still first converted to substrait `String` Literal
    - but then try to match against a function variant with enum arguments.
    - if a match is found, convert operand Expressions into `FunctionArg` and call `generateBinding`
  - 