#pragma once

#include <atomic>
#include <cstddef>

/// A class managing the concurrency strategy for an iterable data structure.
/// This implementation offers no concurrency,
/// and is only suitable for use in a single-threaded environment.
/// All methods are no-ops.
class no_lock {
public:
  // These types aren't used, so just use bools as they're small.
  typedef bool lock_t;
  typedef bool traversal_t;
  typedef bool lin_set_t;

  /// Indicates whether this lock class supports late starts and early exits.
  /// (In this case, they are supported vacuously, because this class doesn't
  /// offer linearizable iteration anyway.)
  static constexpr bool LATE_START_SUPPORTED = true;
  static constexpr bool EARLY_EXIT_SUPPORTED = true;

  no_lock(size_t) {}
  ~no_lock() {}
  void initialize_lock(bool &) {}
  void copy_lock(bool &, bool &) {}
  template <typename K> bool new_traversal(const K &, const K &, bool) {
    return false;
  }
  template <typename EK> bool t_acquire(const EK &, bool, bool &) {
    return true;
  }
  template <typename K> bool acquire(const K &, bool &) { return true; }
  template <typename K> bool acquire(const K &, bool, bool &) { return true; }
  void t_release(bool, bool &) {}
  void release(bool &) {}
  void s_release(bool &) {}
  void finish(bool) {}
  void make_nonlockable(bool &) {}
  bool is_nonlockable(bool &) { return false; }
  bool get_lin_set() { return true; }
  static void delete_lin_set(bool) {}
  void wait_until_unlocked(bool &) {}
  bool verify() { return true; }
};