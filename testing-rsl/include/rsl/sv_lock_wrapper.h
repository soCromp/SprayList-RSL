#pragma once

#include <atomic>

/// A class that wraps a given instance of a lock.
template <typename K, typename STRATEGY> class sv_lock_wrapper {

  typedef typename STRATEGY::lock_t lock_t;
  STRATEGY *strategy;
  lock_t lock;
  bool orphan = false;

public:
  typedef typename STRATEGY::lin_set_t lin_set_t;
  typedef typename STRATEGY::traversal_t traversal_t;

  /// Determine if the node owning this lock is an orphan.
  /// May only be called while lock is held.
  bool is_orphan() { return orphan; }

  /// Determine if the node owning this lock is dead.
  bool is_dead() { return strategy->is_nonlockable(lock); }

  /// Constructor taking no arguments.
  /// Initialization is required after invoking this constructor.
  sv_lock_wrapper() : orphan(orphan) {}

  /// Default constructor; begin unlocked and with all flag bits clear.
  sv_lock_wrapper(STRATEGY *strategy) : strategy(strategy) {
    strategy->initialize_lock(lock);
  }

  /// Constructor. Allows caller to manually set the orphan flag.
  sv_lock_wrapper(STRATEGY *strategy, bool orphan)
      : strategy(strategy), orphan(orphan) {
    strategy->initialize_lock(lock);
  }

  ~sv_lock_wrapper() {}

  /// Initializes a node that was constructed with the default constructor.
  void initialize(STRATEGY *_strategy, bool _orphan) {
    strategy = _strategy;
    orphan = _orphan;
    strategy->initialize_lock(lock);
  }

  /// Acquire the lock for an elemental operation.
  bool acquire(const K &k, const lin_set_t *lin) {
    return strategy->acquire(k, *lin, lock);
  }

  /// Acquire the lock for either an elemental operation or traversal.
  /// If lin is not null, acquire for an elemental operation with that lin set.
  /// If it is null, acquire for traversal t.
  bool acquire(const K &k, const lin_set_t *lin, traversal_t *t) {
    if (lin != nullptr) {
      return strategy->acquire(k, *lin, lock);
    } else {
      return strategy->t_acquire(k, *t, lock);
    }
  }

  /// Acquire the lock for either a traversal.
  /// If lin is not null, acquire for an elemental operation with that lin set.
  /// If it is null, acquire for traversal t.
  bool t_acquire(const K &k, traversal_t *t) {
    return strategy->t_acquire(k, *t, lock);
  }

  /// Release the lock after an elemental operation.
  void release() { strategy->release(lock); }

  /// Release the lock after a node is used for purely structural purposes.
  void s_release() { strategy->s_release(lock); }

  /// Release the lock after an elemental operation (or traversal, if t is
  /// non-null.)
  void release(traversal_t *t) {
    if (t == nullptr) {
      strategy->release(lock);
    } else {
      strategy->t_release(*t, lock);
    }
  }

  /// Release the lock after an elemental operation uses it for purely
  /// structurtal purposes (or release it normally as a traversal, if t is
  /// non-null.)
  void s_release(traversal_t *t) {
    if (t == nullptr) {
      strategy->s_release(lock);
    } else {
      strategy->t_release(*t, lock);
    }
  }

  /// Release the lock after an elemental operation as an orphan.
  void release_as_orphan() {
    orphan = true;
    strategy->release(lock);
  }

  /// Release a lock and mark the node as dead.
  /// Dead is represented as a nonlockable node.
  void release_as_dead() { strategy->make_nonlockable(lock); }

  /// Spin until this lock is available, but do not acquire it.
  void wait_until_unlocked() { strategy->wait_until_unlocked(lock); }

  /// Initialize this lock as a copy of another lock.
  void copy(sv_lock_wrapper &other) { strategy->copy_lock(lock, other.lock); }

  lock_t *get_lock() { return &lock; }

  void dump() { std::cout << +lock; }
};
