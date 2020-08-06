#pragma once

#include <atomic>
#include <cassert>

#include "loop_counter.h"

/// An implementation of a sequence lock.
class sequence_lock {
  std::atomic<uint64_t> lock;

  static constexpr uint64_t LOCK_BIT = 0x0000000000000001ull;
  static constexpr uint64_t COUNTER_MASK = 0xFFFFFFFFFFFFFFFEull;

public:
  /// Constructor. Initializes lock to two, which is an even number, signifying
  /// "unlocked." Zero is avoided so that callers can use the special value zero
  /// to avoid special-casing the first call to acquire_if_changed(). Thus,
  /// acquire_if_changed(0) will always successfully acquire (barring overflow.)
  /// acquire_once_changed() has the same behavior.
  sequence_lock() : lock(2) {}

  ~sequence_lock() {}

  /// Check if the sequence lock is held.
  bool is_locked() { return lock.load() % 2 == 1; }

  /// Acquire the sequence lock as a writer.
  /// Busywait until the counter is even,
  /// then (atomically) increment the counter to the next odd number.
  uint64_t acquire() {
    uint64_t read_value = lock.load();
    bool success = false;
    loop_counter ctr;
    while (!success) {
      ctr.count();
      if (read_value % 2 == 0) {
        success = lock.compare_exchange_weak(read_value, read_value + 1);
      } else {
        read_value = lock.load();
      }
    }

    return read_value + 1;
  }

  /// Acquire a sequence lock only if its value has changed.
  /// If value has changed, then this method will acquire the lock, update prev,
  /// and return true.
  /// If it has not changed, return false.
  bool acquire_if_changed(uint64_t &prev) {
    uint64_t read_value = lock.load();
    bool success = false;
    loop_counter ctr;
    while (!success) {
      ctr.count();
      if (read_value % 2 == 0) {
        if (read_value == prev) {
          return false;
        }
        success = lock.compare_exchange_weak(read_value, read_value + 1);
      } else {
        read_value = lock.load();
      }
    }

    prev = read_value;
    return true;
  }

  /// Wait until a sequence lock's value has changed, then acquire it.
  /// This method will acquire the lock and update prev.
  void acquire_once_changed(uint64_t &prev) {
    uint64_t read_value = lock.load();
    bool success = false;
    loop_counter ctr;
    while (!success) {
      ctr.count();
      if (read_value % 2 == 0 && read_value != prev) {
        success = lock.compare_exchange_weak(read_value, read_value + 1);
      } else {
        read_value = lock.load();
      }
    }

    prev = read_value;
  }

  /// Try to upgrade from reader to writer.
  /// Succeeds only if lock's value is unchanged.
  bool upgrade(uint64_t v) {
    // If this method is called with an odd v,
    // this class contains a bug or is being misused.
    assert(v % 2 == 0);
    return lock.compare_exchange_strong(v, v + 1);
  }

  /// Release the sequence lock after making changes to the protected data.
  /// This increments the counter to the next even number.
  /// Should only be called by the thread that acquired.
  uint64_t release() {
    uint64_t read_value = lock.load();

    // Only held locks should be released.
    assert(read_value % 2 == 1);

    lock.store(read_value + 1);
    return read_value + 1;
  }

  /// Release the sequence lock after making no changes to the protected data.
  /// This decrements the counter down to the previous even number.
  /// Should only be called by the thread that acquired.
  uint64_t release_unchanged() {
    uint64_t read_value = lock.load();

    // Only held locks should be released.
    assert(read_value % 2 == 1);

    lock.store(read_value - 1);
    return read_value - 1;
  }

  /// "Acquire" the lock as a reader.
  /// Clears the lock bit, and returns the value.
  /// Should only be done when the shared data allows for safe concurrent reads
  /// and writes, and any unsound results from the access can easily be
  /// discarded, reversed, or repaired.
  uint64_t begin_read() { return lock.load() & COUNTER_MASK; }

  /// "Acquire" a sequence lock as a reader only if its value has changed.
  /// If lock has changed, then this method will update prev and return true.
  /// If lock has not changed, return false.
  /// Should only be done when the shared data allows for safe concurrent reads
  /// and writes, and any unsound results from the access can easily be
  /// discarded, reversed, or repaired.
  bool begin_read_if_changed(uint64_t &prev) {
    uint64_t read_value = lock.load() & COUNTER_MASK;

    if (read_value == prev) {
      return false;
    } else {
      prev = read_value;
      return true;
    }
  }

  /// "Release" the lock as a reader.
  /// Basically just checks if the lock has changed.
  /// Reader must abort and try again if it has.
  bool confirm_read(uint64_t value) {
    std::atomic_thread_fence(std::memory_order_acquire);
    return lock.load(std::memory_order_relaxed) == value;
  }

  /// Directly read the value of the lock.
  /// For debug purposes.
  uint64_t get_value() { return lock.load(); }

  void dump() {
    std::cout << "sequence lock value: " << lock.load() << std::endl;
  }
};
