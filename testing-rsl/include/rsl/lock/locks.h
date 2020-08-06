#pragma once

#include <atomic>
#include <cstdint>
#include <utility>
#include <x86intrin.h>

#include "loop_counter.h"

/// seqlock_t is a sequence lock that can be used in IHT implementations with
/// baton locks.
class seqlock_t {
  /// The sequence lock
  std::atomic<uint64_t> seqlock;

public:
  /// Construct a sequence lock by initializing its integer to the provided
  /// value
  seqlock_t(uint64_t i) : seqlock(i) {}

  /// Helper method for verification: return if lock is held
  bool is_locked() { return seqlock & 1; }

  /// Acquire the seqlock by atomically increasing it from even to odd
  void acquire() {
    loop_counter ctr;
    while (true) {
      ctr.count();
      uint64_t curr = seqlock;
      if (((curr & 1) == 0) &&
          seqlock.compare_exchange_strong(curr, curr + 1)) {
        break;
      }
    }
  }

  /// Release the lock by setting it to the next even number.  Return the
  /// value, in case we need to spin
  uint64_t release() {
    uint64_t ret = seqlock + 1;
    seqlock = ret;
    return ret;
  }

  /// Release the lock "as if" it was never acquired, and return the seqlock
  /// value, in case we need to spin
  uint64_t drop() {
    uint64_t ret = seqlock - 1;
    seqlock = ret;
    return ret;
  }

  /// Return true if the seqlock no longer matches an expected value (ignore
  /// low bit)
  bool changed(uint64_t expect) { return (seqlock | 1) != (expect | 1); }

  /// Ensure that the sequence lock is acquired, regardless of how the lock
  /// is being held (wrt the queue).
  void forceAcquire(std::pair<int, uint64_t> acqState) {
    if (acqState.first != 1) {
      acquire();
    }
  }
};

/// tle_t is a test-and-set lock that uses HTM for lock elision.  It can be used
/// in IHT implementations with baton locks.
///
/// RETRIES is the number of HTM attempts before falling back to the underlying
/// lock
template <int RETRIES> class tle_t {
  /// The underlying lock
  std::atomic<uint64_t> spinlock;

public:
  /// Construct the tle lock by initializing its integer to 0
  /// TODO: is this correct???
  tle_t(uint64_t) : spinlock(0) {}

  /// Construct the tle lock by initializing its integer to 0
  tle_t() : spinlock(0) {}

  /// Acquire the tle by atomically either swapping in 1, or by calling xbegin
  void acquire() {
    // we need to now how many times the HTM aborted
    int retry_counter = 0;
    uint64_t valid_lock_val, status;
    // On abort, restart from here
  retry:
    // If lock is held, wait until it is released
    do {
      valid_lock_val = spinlock.load(std::memory_order_acquire);
    } while (valid_lock_val & 1);
    // Start hardware transaction
    status = _xbegin();
    if (status == _XBEGIN_STARTED) {
      // check the lock is not held by other threads
      // also, this prevent lock status changing during htm execution
      if (spinlock.load(std::memory_order_acquire) & 1)
        _xabort(66);
      return; // HTM succeeded.  We have an elided lock.
    } else if ((status & _XABORT_EXPLICIT) && _XABORT_CODE(status) == 66) {
      // global lock is on, so we go back to
      // the beginning of transaction and reset the retry counter
      retry_counter = 0;
      goto retry;
    } else if (!(status & _XABORT_CAPACITY) && retry_counter <= RETRIES) {
      // abort reason is not lack of capacity, retry times less than the
      // threshold
      ++retry_counter;
      goto retry;
    } else {
      // slow path, global lock
      while (true) {
        valid_lock_val = spinlock.load(std::memory_order_acquire);
        // acquire the global lock
        if (!(valid_lock_val & 1)) {
          if (spinlock.compare_exchange_strong(valid_lock_val, 1))
            break;
        }
      }
      return; // global lock grabbed
    }
  }

  /// Release the lock, either via xend or by setting the lock to 0
  uint64_t release() {
    // We don't have any thread state to tell us we're in HTM, but in
    // well-formed programs, if the lock is 1, then we must have set it.
    if (spinlock.load(std::memory_order_relaxed) == 0) {
      _xend();
    } else {
      spinlock.store(0);
    }
    return 0; // Unimportant lock value
  }

  /// This is only called from read-only critical sections.  With HTM, the only
  /// thing we can do is mimic a release().
  uint64_t drop() { return release(); }

  /// Due to HTM, we can't tell if things have actually changed, so we just
  /// return true always.
  bool changed(uint64_t) { return true; }

  /// Ensure that the thread is allowed to touch shared data.  This could mean
  /// starting HTM, or acquiring the lock.  The key is that acqState lets us
  /// know if we've already got a lock of any sort.
  void forceAcquire(std::pair<int, uint64_t> acqState) {
    if (acqState.first != 1) {
      acquire();
    }
  }
};
