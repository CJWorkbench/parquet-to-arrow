#include <cerrno>
#include <cstdio>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <unistd.h>
#include <sys/types.h>

#include <arrow/api.h>
#include <gflags/gflags.h>

#include "common.h"

static bool validate_delimiter(const char* flagname, const std::string& delimiter)
{
  if (delimiter.size() == 1) {
    return true;
  }
  printf("Invalid value for --%s: must be 1 byte in length");
  return false;
}


DEFINE_uint64(max_rows, std::numeric_limits<uint64_t>::max(), "Skip rows after parsing this many");
DEFINE_uint64(max_columns, std::numeric_limits<uint64_t>::max(), "Skip columns after parsing this many");
DEFINE_uint32(max_bytes_per_value, std::numeric_limits<uint32_t>::max(), "Truncate each value to at most this size");
DEFINE_string(delimiter, ",", "Character separating values on a record");
DEFINE_validator(delimiter, &validate_delimiter);

struct Warnings {
  size_t nRowsSkipped;
  size_t nColumnsSkipped;
  size_t nValuesTruncated; // truncated because the individual value is too big
  size_t firstTruncatedValueRow; // 0-base
  size_t firstTruncatedValueColumn; // 0-base
  size_t nValuesRepaired; // repaired because after a quoted string they had more data
  size_t firstRepairedValueRow; // 0-base
  size_t firstRepairedValueColumn; // 0-base
  bool eofInQuotedValue; // file ended in a quoted value (and we closed the quote automatically)

  Warnings()
    : nRowsSkipped(0)
    , nColumnsSkipped(0)
    , nValuesTruncated(0)
    , firstTruncatedValueRow(0)
    , firstTruncatedValueColumn(0)
    , nValuesRepaired(0)
    , firstRepairedValueRow(0)
    , firstRepairedValueColumn(0)
    , eofInQuotedValue(false)
  {
  }

  void warnSkippedRow()
  {
    this->nRowsSkipped++;
  }

  void warnSkippedColumn(size_t nPastLimit)
  {
    this->nColumnsSkipped = std::max(this->nColumnsSkipped, nPastLimit);
  }

  void warnRepairedValue(size_t row, size_t column)
  {
    if (this->nValuesRepaired == 0) {
      this->firstRepairedValueRow = row;
      this->firstRepairedValueColumn = column;
    }
    this->nValuesRepaired++;
  }

  void warnEofInQuotedValue()
  {
    this->eofInQuotedValue = true;
  }

  void warnTruncatedValue(size_t row, size_t column)
  {
    if (this->nValuesTruncated == 0) {
      this->firstTruncatedValueRow = row;
      this->firstTruncatedValueColumn = column;
    }
    this->nValuesTruncated++;
  }
};

struct ReadCsvResult {
  Warnings warnings;
  std::shared_ptr<arrow::Table> table;
};

struct ColumnBuilder {
  arrow::StringBuilder arrayBuilder;
  size_t nextRowIndex;

  ColumnBuilder() : arrayBuilder(arrow::default_memory_pool()), nextRowIndex(0) {}

  void writeValue(size_t row, const uint8_t* bytes, int32_t nBytes)
  {
    if (row != this->nextRowIndex) {
      ASSERT_ARROW_OK(this->arrayBuilder.AppendNulls(row - this->nextRowIndex), "appending nulls");
      this->nextRowIndex = row;
    }
    ASSERT_ARROW_OK(this->arrayBuilder.Append(bytes, nBytes), "appending value");
    this->nextRowIndex++;
  }
};

struct TableBuilder {
  std::vector<std::unique_ptr<ColumnBuilder> > columnBuilders;

  void writeValue(size_t row, size_t column, const uint8_t* bytes, int32_t nBytes) {
    if (column >= columnBuilders.size()) {
      while (column >= columnBuilders.size()) {
        this->columnBuilders.emplace_back(std::make_unique<ColumnBuilder>());
      }
    }
    this->columnBuilders[column]->writeValue(row, bytes, nBytes);
  }

  /**
   * Destructively build an arrow::Table
   *
   * This resets the TableBuilder to its initial state (and frees RAM).
   */
  std::shared_ptr<arrow::Table> finish() {
    int columnIndex = -1;
    int nRows = 0; // "0" here in case there are 0 columns

    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(this->columnBuilders.size());
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(this->columnBuilders.size());
    std::shared_ptr<arrow::DataType> utf8 = arrow::utf8();

    for (auto& columnBuilder : this->columnBuilders) {
      columnIndex++;
      auto& arrayBuilder = columnBuilder->arrayBuilder;

      if (columnIndex == 0) {
        // Column 0 is the only column guaranteed to have a non-null value
        // in each column
        nRows = arrayBuilder.length();
      } else {
        // If the last row of output has fewer columns than previous rows, pad
        // the rightmost columns with `null`.
        if (arrayBuilder.length() < nRows) {
          arrayBuilder.AppendNulls(nRows - arrayBuilder.length());
        }
      }

      std::string columnName = std::to_string(columnIndex);
      std::shared_ptr<arrow::Field> field = arrow::field(columnName, utf8);
      fields.push_back(field);

      std::shared_ptr<arrow::Array> array;
      ASSERT_ARROW_OK(arrayBuilder.Finish(&array), "Converting column to array");
      columns.push_back(array);
    }

    auto schema = arrow::schema(fields);
    return arrow::Table::Make(schema, columns, nRows);
  }
};

static ReadCsvResult readCsv(const char* csvFilename, const char delimiter) {
  Warnings warnings;
  TableBuilder builder;
  size_t row(0);
  size_t column(0);
  std::unique_ptr<uint8_t[]> buf(new uint8_t[FLAGS_max_bytes_per_value]);
  uint8_t* valuePtr = &buf[0];
  int32_t valuePos(0); // index into buf; int32_t is in arrow/types.h
  int c;

  FILE* file = fopen(csvFilename, "r");
  if (file == NULL) {
    perror("Could not open CSV file");
    std::_Exit(1);
  }

#define NEXT() fgetc_unlocked(file)

#define EMIT_VALUE() \
  do { \
    if (row >= FLAGS_max_rows) { \
      if (column == 0) { \
        warnings.warnSkippedRow(); \
      } \
    } else if (column >= FLAGS_max_columns) { \
      warnings.warnSkippedColumn(column - FLAGS_max_columns + 1); \
    } else { \
      builder.writeValue(row, column, valuePtr, std::min<uint32_t>(valuePos, FLAGS_max_bytes_per_value)); \
    } \
  } while (0)

#define CLEAR_VALUE() valuePos = 0

#define STORE_CHAR(c) \
  do { \
    if (valuePos == FLAGS_max_bytes_per_value) { \
      warnings.warnTruncatedValue(row, column); \
    } else if (valuePos < FLAGS_max_bytes_per_value) { \
      buf[valuePos] = static_cast<uint8_t>(c); \
    } \
    valuePos++; \
  } while (0)

#define ADVANCE_COLUMN() column++

#define ADVANCE_ROW() \
  do { \
    row++; \
    column = 0; \
  } while (0)

  /*
   * Implemented as a state machine:
   *
   * VALUE_BEGIN: start of value (and initial state)
   * IN_UNQUOTED_VALUE: started reading a value without quotation marks
   * IN_QUOTED_VALUE: started reading a value with quotation marks
   * AFTER_QUOTE: either at end of a quoted value, or escaping '"'
   */

VALUE_BEGIN:
  c = NEXT();
  if (c == delimiter) {
    EMIT_VALUE(); // empty string
    ADVANCE_COLUMN();
    goto VALUE_BEGIN;
  }
  switch (c) {
    case EOF:
      goto END;
    case '\r':
    case '\n':
      if (column == 0) {
        // Ignore empty lines; also, treat "\r\n" as a single newline.
        goto VALUE_BEGIN;
      }
      EMIT_VALUE(); // empty string
      ADVANCE_ROW();
      goto VALUE_BEGIN;
    case '"':
      goto IN_QUOTED_VALUE;
    default:
      STORE_CHAR(c);
      goto IN_UNQUOTED_VALUE;
  }

IN_UNQUOTED_VALUE:
  c = NEXT();
  if (c == delimiter) {
    EMIT_VALUE();
    CLEAR_VALUE();
    ADVANCE_COLUMN();
    goto VALUE_BEGIN;
  }
  switch (c) {
    case EOF:
      EMIT_VALUE();
      CLEAR_VALUE();
      goto END;
    case '\r':
    case '\n':
      EMIT_VALUE();
      CLEAR_VALUE();
      ADVANCE_ROW();
      goto VALUE_BEGIN;
    default:
      STORE_CHAR(c);
      goto IN_UNQUOTED_VALUE;
  }

IN_QUOTED_VALUE:
  c = NEXT();
  switch (c) {
    case EOF:
      warnings.warnEofInQuotedValue();
      EMIT_VALUE();
      CLEAR_VALUE();
      goto END;
    case '"':
      goto AFTER_QUOTE;
    default:
      STORE_CHAR(c);
      goto IN_QUOTED_VALUE;
  }

AFTER_QUOTE:
  c = NEXT();
  if (c == delimiter) {
    EMIT_VALUE();
    CLEAR_VALUE();
    ADVANCE_COLUMN();
    goto VALUE_BEGIN;
  }
  switch (c) {
    case EOF:
      EMIT_VALUE();
      CLEAR_VALUE();
      goto END;
    case '"':
      STORE_CHAR('"');
      goto IN_QUOTED_VALUE;
    case '\r':
    case '\n':
      EMIT_VALUE();
      CLEAR_VALUE();
      ADVANCE_ROW();
      goto VALUE_BEGIN;
    default:
      warnings.warnRepairedValue(row, column);
      STORE_CHAR(c);
      goto IN_UNQUOTED_VALUE;
  }

END:
  if (fclose(file) == EOF) {
    perror("Failed to close input CSV");
    std::_Exit(1);
  }

  std::shared_ptr<arrow::Table> table(builder.finish());
  return ReadCsvResult { warnings, table };
}

static void printWarnings(const Warnings& warnings)
{
  if (warnings.nRowsSkipped) {
    printf("skipped %d rows (after row limit of %d)\n", warnings.nRowsSkipped, FLAGS_max_rows);
  }
  if (warnings.nColumnsSkipped) {
    printf("skipped %d columns (after column limit of %d)\n", warnings.nColumnsSkipped, FLAGS_max_columns);
  }
  if (warnings.nValuesTruncated) {
    printf("truncated %d values (value byte limit is %d; see row %d column %d)\n", warnings.nValuesTruncated, FLAGS_max_bytes_per_value, warnings.firstTruncatedValueRow, warnings.firstTruncatedValueColumn);
  }
  if (warnings.nValuesRepaired) {
    printf("repaired %d values (misplaced quotation marks; see row %d column %d)\n", warnings.nValuesRepaired, warnings.firstRepairedValueRow, warnings.firstRepairedValueColumn);
  }
  if (warnings.eofInQuotedValue) {
    printf("repaired last value (missing quotation mark)\n");
  }
}

int main(int argc, char** argv) {
  std::string usage = std::string("Usage: ") + argv[0] + " <CSV_FILENAME> <ARROW_FILENAME>";
  gflags::SetUsageMessage(usage);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 3) {
    gflags::ShowUsageWithFlags(argv[0]);
    std::_Exit(1);
  }

  const char* csvFilename(argv[1]);
  const std::string arrowFilename(argv[2]);

  ReadCsvResult result = readCsv(csvFilename, FLAGS_delimiter[0]);
  printWarnings(result.warnings);
  writeArrowTable(*result.table, arrowFilename);
}
