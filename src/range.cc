#include <charconv>

#include "range.h"

ParseRangeResult
parse_range(const char* begin, const char* end)
{
  uint64_t start = 0; // avoid "uninitialized" compiler warning
  auto [after_start, ec] = std::from_chars(begin, end, start);
  if (ec != std::errc()) {
    return { Range(0, 0), ec };
  }

  if (after_start >= end || *after_start != '-') {
    return { Range(0, 0), std::errc::invalid_argument };
  }

  uint64_t stop = 0; // avoid "uninitialized" compiler warning
  auto [after_stop, ec2] = std::from_chars(after_start + 1, end, stop);
  if (ec != std::errc()) {
    return { Range(0, 0), ec };
  }
  if (after_stop != end) {
    return { Range(0, 0), std::errc::invalid_argument };
  }

  if (start > stop) {
    return { Range(0, 0), std::errc::result_out_of_range };
  }

  return { Range(start, stop), std::errc() };
}

