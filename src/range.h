#include <algorithm>
#include <cstdint>
#include <system_error>

/**
 * A pair of characters on the unsigned-integer number line.
 *
 * * Start comes before stop.
 * * Start is inclusive, and the first valid value is 0.
 * * Stop is exclusive.
 * * If start == stop, the range is empty.
 */
struct Range {
  uint64_t start;
  uint64_t stop;

  Range(uint64_t aStart, uint64_t aStop) : start(aStart), stop(aStop) {}
  Range() : start(0), stop(std::numeric_limits<uint64_t>::max()) {}

  uint64_t size() const {
    return this->stop - this->start;
  }

  Range clip(uint64_t max) const {
    return Range(std::min(this->start, max), std::min(this->stop, max));
  }

  bool includes(uint64_t i) const {
    return i >= this->start && i < this->stop;
  }
};

struct ParseRangeResult {
  const Range range; // if ec == 0, this is 0-0
  std::errc ec;
};

/**
 * Parse a Range from a C string.
 *
 * Return a result with std::errc::invalid_argument or
 * std::errc::out_of_range if the range is not valid.
 */
ParseRangeResult parse_range(const char* begin, const char* end);
