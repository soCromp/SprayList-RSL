#pragma once

#include <atomic>

#include "../lock/lock_dn.h"

/// A lock class exclusively for skipvector_dn.
template <typename K, typename STRATEGY> class sv_dn_lock {

  typedef typename STRATEGY::lock_t lock_t;

  STRATEGY *strategy;
  lock_t lock;

public:
  /// Determine if the node owning this lock is an orphan.
  /// May only be called while lock is held.
  bool is_orphan() { return strategy->is_orphan(lock); }

  /// Determine if the node owning this lock is dead.
  bool is_dead() { return strategy->is_nonlockable(lock); }

  /// Determine if the node owning this lock has reservations.
  bool is_reserved() { return strategy->is_reserved(lock); }

  /// Determine if the node is marked for removal from the structure.
  bool is_doomed() { return (lock & STRATEGY::DOOMED_BIT) != 0; }

  /// Default constructor; begin unlocked and with all flag bits clear.
  /// Initialization is required after invoking this constructor.
  sv_dn_lock() : lock(0) {}

  /// Constructor; begin unlocked and with all flag bits clear.
  sv_dn_lock(STRATEGY *strategy) : strategy(strategy), lock(0) {}

  /// Constructor. Allows caller to manually set the orphan flag.
  sv_dn_lock(STRATEGY *strategy, bool orphan) : strategy(strategy), lock(0) {
    if (orphan) {
      strategy->mark_orphan(lock);
    }
  }

  ~sv_dn_lock() {}

  /// Initializes a node that was constructed with the default constructor.
  void initialize(STRATEGY *_strategy, bool orphan) {
    strategy = _strategy;
    if (orphan) {
      strategy->mark_orphan(lock);
    }
  }

  /// Simply acquire the lock.
  /// No work should be done while the lock is held.
  bool simple_acquire() { return strategy->simple_acquire(lock); }

  // Acquire the lock for an elemental operation.
  bool acquire() { return strategy->simple_acquire(lock); }

  // Acquire the lock for a traversal.
  // Do not reserve; caller is expected to do that.
  template <typename VECTOR> bool t_acquire(const VECTOR &v) {
    return strategy->t_acquire(v, lock, false);
  }

  // Acquire the first lock for a range() operation.
  // Do not reserve; caller is expected to do that.
  template <typename VECTOR> bool range_first_acquire(const VECTOR &v) {
    return strategy->t_acquire(v, lock, true);
  }

  // Release the lock after an elemental operation.
  void release(const K &k, bool readonly) {
    strategy->release(lock, k, readonly);
  }

  // Release the lock after use by a traversal.
  void t_release() {
    strategy->drop_reservation(lock);
    strategy->s_release(lock);
  }

  // Release the lock after a node is used for purely structural purposes.
  void s_release() { strategy->s_release(lock); }

  // Release the lock after an elemental operation as an orphan.
  void release_as_orphan(const K &k, bool readonly) {
    strategy->mark_orphan(lock);
    strategy->release(lock, k, readonly);
  }

  // Release a lock and mark the node as dead.
  // Dead is represented as a nonlockable node.
  void release_as_dead() { strategy->make_nonlockable(lock); }

  // Spin until this lock is available, but do not acquire it.
  void wait_until_unlocked() { strategy->wait_until_unlocked(lock); }

  void reserve() { strategy->reserve(lock); }

  void drop_reservation() { strategy->drop_reservation(lock); }

  int reservation_count() { return lock / STRATEGY::RESERVE_INCREMENT; }

  // Spin until this lock is available, but do not acquire it.
  bool wait_until_deletable() { return strategy->wait_until_deletable(lock); }

  lock_t *get_lock() { return &lock; }

  void dump() {
    // Indicate if orphan or Child
    if (is_orphan()) {
      std::cout << "o";
    } else {
      std::cout << "C";
    }

    // Indicate if Dead or Alive
    if (is_dead()) {
      std::cout << "D";
    } else {
      std::cout << "A";
    }

    // Indicate if Locked or Unlocked
    if ((lock & STRATEGY::LOCK_BIT) != 0) {
      std::cout << "L";
    } else {
      std::cout << "U";
    }

    // Indicate if Doomed or Normal
    if (is_doomed()) {
      std::cout << "D";
    } else {
      std::cout << "N";
    }

    // Print number of reservations
    std::cout << reservation_count();
  }
};
