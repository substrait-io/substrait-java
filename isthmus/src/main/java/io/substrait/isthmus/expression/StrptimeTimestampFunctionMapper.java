package io.substrait.isthmus.expression;

import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import org.apache.calcite.sql.fun.SqlLibraryOperators;

/**
 * Custom mapping for the Calcite {@code PARSE_TIMESTAMP} function to the Substrait {@code
 * strptime_timestamp} function. {@code PARSE_TIMESTAMP(format, timestamp_string)} maps to {@code
 * strptime_timestamp(timestamp_string, format)}.
 */
public final class StrptimeTimestampFunctionMapper extends AbstractStrptimeFunctionMapper {
  private static final String STRPTIME_TIMESTAMP_FUNCTION_NAME = "strptime_timestamp";

  public StrptimeTimestampFunctionMapper(List<ScalarFunctionVariant> functions) {
    super(STRPTIME_TIMESTAMP_FUNCTION_NAME, SqlLibraryOperators.PARSE_TIMESTAMP, functions);
  }
}
