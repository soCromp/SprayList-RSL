#pragma once

#include <cstddef>

/// Value representing the length of the primes array.
static constexpr int NUM_PRIMES = 49;

/// A list of the biggest prime less than or equal to each power two, up to
/// 2^49. (2 itself is the only "or equal to.")
static constexpr size_t PRIMES[NUM_PRIMES] = {2,
                                              3,
                                              7,
                                              13,
                                              31,
                                              61,
                                              127,
                                              251,
                                              509,
                                              1021,
                                              2039,
                                              4093,
                                              8191,
                                              16381,
                                              32749,
                                              65521,
                                              131071,
                                              262139,
                                              524287,
                                              1048573,
                                              2097143,
                                              4194301,
                                              8388593,
                                              16777213,
                                              33554393,
                                              67108859,
                                              134217689,
                                              268435399,
                                              536870909,
                                              1073741789,
                                              2147483647,
                                              4294967291,
                                              8589934583,
                                              17179869143,
                                              34359738337,
                                              68719476731,
                                              137438953447,
                                              274877906899,
                                              549755813881,
                                              1099511627689,
                                              2199023255531,
                                              4398046511093,
                                              8796093022151,
                                              17592186044399,
                                              35184372088777,
                                              70368744177643,
                                              140737488355213,
                                              281474976710597,
                                              562949953421231};

/// Find the smallest entry of the prime array greater than or equal to target.
/// Uses a binary search.
inline size_t find_next_prime_after(size_t target) {

  // Check lower bound.
  if (target <= PRIMES[0]) {
    return PRIMES[0];
  }

  // Check upper bound. Default to largest prime if above it.
  // (Values larger than the second largest prime should also evaluate to the
  // largest prime, and they can be included in this check at no extra cost,
  // and so they are.)
  if (target > PRIMES[NUM_PRIMES - 2]) {
    return PRIMES[NUM_PRIMES - 1];
  }

  int lower_index = 1;              // Index lower bound
  int upper_index = NUM_PRIMES - 2; // Index upper bound

  while (lower_index != upper_index) {
    const int median_index = (lower_index + upper_index) / 2;
    const size_t median = PRIMES[median_index];

    if (median < target) {
      // If the median prime is lower than the target,
      // raise the lower bound and repeat.
      lower_index = median_index + 1;
    } else if (PRIMES[median_index - 1] >= target) {
      // If the prime before the median is greater than or equal to the target,
      // lower the upper bound and repeat.
      upper_index = median_index - 1;
    } else {
      // If the median prime is greater than or equal to the target, and the
      // previous prime is less than the target, the median prime is the
      // smallest prime greater than or equal to the target, so return it.
      return median;
    }
  }

  // If we've gotten here, lower_index must be equal to upper_index,
  // so the range encompasses a single element, which must be the answer.
  return PRIMES[upper_index];
}