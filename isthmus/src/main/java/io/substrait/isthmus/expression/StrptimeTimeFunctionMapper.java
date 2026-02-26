package io.substrait.isthmus.expression;

import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import org.apache.calcite.sql.fun.SqlLibraryOperators;

/**
 * Custom mapping for the Calcite {@code PARSE_TIME} function to the Substrait {@code strptime_time}
 * function. {@code PARSE_TIME(format, time_string)} maps to {@code strptime_time(time_string,
 * format)}.
 */
public final class StrptimeTimeFunctionMapper extends AbstractStrptimeFunctionMapper {
  private static final String STRPTIME_TIME_FUNCTION_NAME = "strptime_time";

  public StrptimeTimeFunctionMapper(List<ScalarFunctionVariant> functions) {
    super(STRPTIME_TIME_FUNCTION_NAME, SqlLibraryOperators.PARSE_TIME, functions);
  }
}
