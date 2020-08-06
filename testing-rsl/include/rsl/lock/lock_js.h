#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <execinfo.h>
#include <unistd.h>

#include "loop_counter.h"

/// A class managing the concurrency strategy for an iterable data structure.
/// This implementation uses locks to offer "java semantics:" every individual
/// "visit" by a foreach() or range() is linearizable, but not the entire
/// operation as a whole.
class lock_js {
  /// Value representing a held partition lock.
  static constexpr int8_t LOCKED = -1;

  /// Value representing an free partition lock.
  static constexpr int8_t UNLOCKED = 1;

  /// A special value indicating a partition lock that has been made permanently
  /// non-lockable.
  static constexpr int8_t NON_LOCKABLE = 0;

public:
  typedef std::atomic<int8_t> lock_t;

  /// Caller-facing abstraction of a traversal.
  /// Dummied out in this implementation.
  typedef bool traversal_t;

  /// lin_set is meaningless for this implementation,
  /// so just use the smallest possible type.
  typedef bool lin_set_t;

  /// Indicates whether this lock class supports late starts and early exits.
  /// (In this case, they are supported vacuously, because this class doesn't
  /// offer linearizable iteration anyway.)
  static constexpr bool LATE_START_SUPPORTED = true;
  static constexpr bool EARLY_EXIT_SUPPORTED = true;

  lock_js(size_t) {}
  ~lock_js() {}

  /// Initialize a new lock to the default value.
  void initialize_lock(lock_t &newlock) { newlock = UNLOCKED; }

  /// Make a target lock a copy of another (except unlocked.)
  void copy_lock(lock_t &dest, lock_t &) { dest.store(UNLOCKED); }

  template <typename K> traversal_t new_traversal(const K &, const K &, bool) {
    // No need to generate unique traversal IDs; just return a dummy value.
    return UNLOCKED;
  }

  /// Acquire a lock for use by a traversal.
  /// If this method returns false, that means the acquire failed because the
  /// lock is non-lockable.
  template <typename EK>
  bool t_acquire(const EK &k, traversal_t, lock_t &lock) {
    return acquire(k, lock);
  }

  template <typename K> bool acquire(const K &k, lin_set_t, lock_t &lock) {
    return acquire(k, lock);
  }

  /// Acquire a lock for use by an elemental access.
  /// If this method returns NON_LOCKABLE, that means the acquire failed because
  /// the lock is non-lockable.
  template <typename K> bool acquire(const K &, lock_t &lock) {
    bool success = false;
    int8_t read_lock = lock.load();

    // If read_lock is greater than or equal to initial_lin, it's OK to grab the
    // lock, so try repeatedly to CAS it to the "locked" state until successful.
    // If read_lock is less than the initial value of lin_id,
    // wait until that changes.
    loop_counter ctr;
    while (!success) {
      ctr.count();
      if (read_lock == UNLOCKED) {
        success = lock.compare_exchange_weak(read_lock, LOCKED);
      } else if (read_lock == NON_LOCKABLE) {
        return false;
      } else {
        read_lock = lock.load();
      }
    }

    return true;
  }

  /// Release a lock acquired by a traversal.
  void t_release(traversal_t, lock_t &lock) { s_release(lock); }

  /// Release a lock acquired by an elemental access.
  void release(lock_t &lock) { s_release(lock); }

  /// Release a lock acquired for purely structural reasons.
  void s_release(lock_t &lock) {
    // Only held locks should be released.
    assert(lock.load() == LOCKED);
    lock.store(UNLOCKED);
  }

  /// Release an acquired lock and make it permanently non-lockable.
  /// This can only be done safely by a thread that currently holds this lock,
  /// or during initialization.
  void make_nonlockable(lock_t &lock) { lock.store(NON_LOCKABLE); }

  /// Check if a lock is nonlockable.
  bool is_nonlockable(const lock_t &lock) { return lock == NON_LOCKABLE; }

  void finish(traversal_t) {}

  /// Get the current value of lin_id.
  /// lin_id doesn't exist in this implementation, so this is a no-op.
  lin_set_t get_lin_set() { return true; }

  /// Delete a lin_set_t object allocated by get_lin_set().
  /// No-op in this implementation.
  static void delete_lin_set(lin_set_t) {}

  /// Wait until a lock is unlocked/nonlockable (but don't acquire it.)
  void wait_until_unlocked(lock_t &lock) {
    loop_counter ctr;
    while (lock.load() < 0)
      ctr.count();
    return;
  }

  /// Debug method. Makes sure this lock class is in a sensible state at the
  /// start and end of execution.
  bool verify() {
    // No op, this class has no state.
    return true;
  }
};