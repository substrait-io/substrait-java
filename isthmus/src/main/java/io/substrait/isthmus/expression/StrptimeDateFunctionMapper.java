package io.substrait.isthmus.expression;

import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import org.apache.calcite.sql.fun.SqlLibraryOperators;

/**
 * Custom mapping for the Calcite {@code PARSE_DATE} function to the Substrait {@code strptime_date}
 * function. {@code PARSE_DATE(format, date_string)} maps to {@code strptime_date(date_string,
 * format)}.
 */
public final class StrptimeDateFunctionMapper extends AbstractStrptimeFunctionMapper {
  private static final String STRPTIME_DATE_FUNCTION_NAME = "strptime_date";

  public StrptimeDateFunctionMapper(List<ScalarFunctionVariant> functions) {
    super(STRPTIME_DATE_FUNCTION_NAME, SqlLibraryOperators.PARSE_DATE, functions);
  }
}
