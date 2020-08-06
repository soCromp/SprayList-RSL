#include <atomic>

/// A thread hazard pointer context.  This specific context type assumes we
/// are doing hand-over-hand list traversals, and thus only ever need two
/// hazard pointers.
///
/// sv_thread_hp_context_t tracks two things.  First, it has a small number (2)
/// of hazard pointers: shared single-writer, multi-reader locations where a
/// thread can store the locations it is protecting.  Second, it has a large
/// segment of memory (a list of 64-element vectors) that stores retired
/// pointers that are not yet reclaimed (presumably because of a product of
/// laziness and the hazard pointers held by other threads).
///
/// [mfs] Would it be a bad idea to use a std::vector instead of a list of
///       chunk_t objects?
template <typename A, typename B, void *HEAD> class sv_thread_hp_context_t {
  /// The size of a chunk in the thread'd list of to-reclaim locations
  ///
  /// [mfs] Either switch to std::vector or make this a template parameter?
  static const int CHUNKSIZE = 64;

  /// A set of pointers that are unlinked, but not yet reclaimed.
  ///
  /// NB: This is templated on the type, so we can delete() instead of just
  ///     freeing.
  template <typename T> struct chunk_t {
    /// The set of objects to be reclaimed.
    T *ptrs[CHUNKSIZE];

    /// The number of valid pointers in the chunk
    int count = 0;

    /// The next chunk in the linked list of chunks.
    chunk_t<T> *next = nullptr;
  };

  /// A list of chunks of blocks
  chunk_t<A> *a_chunks;
  chunk_t<B> *b_chunks;

  /// The hazard pointers held by this thread.  It is hard-coded at two,
  /// because we are only dealing with a linked list data structure
  std::atomic<void *> hazP[2];

  /// The threads' HP contexts form a linked list.  The order in the list is
  /// not important, but we need a next pointer to create the list.
  sv_thread_hp_context_t *next = nullptr;

  /// Index of curr in hasP.  Value doesn't matter if no hazard pointer on
  /// curr is held.
  int curr_idx = 0;

  /// Check if a pointer is reserved by another thread.
  bool is_reserved(void *ptr) {
    sv_thread_hp_context_t *curr =
        SV::contexts_head.load(std::memory_order_relaxed);
    while (curr) {
      if (curr != this &&
          (curr->hazP[0].load(std::memory_order_relaxed) == ptr ||
           curr->hazP[1].load(std::memory_order_relaxed) == ptr)) {
        return true;
      }
      curr = curr->next;
    }
    return false;
  }

  /// Go through a linked list of chunks, and try to reclaim each pointer
  ///
  /// [mfs] This function has O(dn^2) overhead, where n is the number of
  ///       threads and d is the number of items in the current thread's list.
  ///       That's really high.  If we instead were to sweep through all
  ///       threads' HP lists once and put all currently held elements into a
  ///       vector then we could sort that vector.  The overhead would be O(n
  ///       + nlg(n)) to do the sweep and then the sort.  From there, we could
  ///       either do a binary search for each of the d elements, so O(dlg(n))
  ///       total, or else also sort the d elements (O(dlg(d))) and then do a
  ///       linear match in O(d) time.  Either way would be faster.
  template <typename T> void sweep_hp_list(chunk_t<T> *head) {
    chunk_t<T> *curr = head;
    while (curr) {
      int pos = 0;
      while (pos < curr->count) {
        T *ptr = curr->ptrs[pos];
        if (!is_reserved(ptr)) {
          delete ptr;
          curr->ptrs[pos] = curr->ptrs[--curr->count];
        } else {
          ++pos;
        }
      }
      curr = curr->next;
    }
  }

  /// Go through both linked lists of chunks, and try to reclaim each pointer
  void sweep_hp_lists() {
    sweep_hp_list<A>(a_chunks);
    sweep_hp_list<B>(b_chunks);
  }

  /// When destroying the thread hazard pointer context, we assume that the
  /// program has shifted to a new phase (possibly sequential), and thus
  /// everything that was previously scheduled for deletion is now guaranteed
  /// to be reclaimable.  Given that assumption, wipe_chunks will reclaim
  /// everything, and also reclaim the chunk lists themselves.
  template <typename T> static void wipe_chunks(chunk_t<T> *head) {
    chunk_t<T> *curr = head;
    while (curr) {
      // Reclaim nodes
      //
      // [mfs] Why is it important to decrement curr->count, if we're just
      //       going to delete curr anyway?
      int &count = curr->count;
      for (; count > 0; --count) {
        T *ptr = curr->ptrs[count - 1];
        delete ptr;
      }
      chunk_t<T> *next_chunk = curr->next;
      delete curr;
      curr = next_chunk;
    }
  }

  /// Mark a node of type T for reclamation by adding it to a chunk in the
  /// appropriate linked list
  template <typename T> void reclaim(T *ptr, chunk_t<T> *head) {
    chunk_t<T> *curr = head;
    while (true) {
      if (curr->count < CHUNKSIZE)
        break;
      if (curr->next == nullptr) {
        curr->next = new chunk_t<T>();
        curr = curr->next;
        break;
      }
      curr = curr->next;
    }
    curr->ptrs[curr->count++] = ptr;
    // [mfs] This may have the unintended consequence of causing deletions at
    //       too high of a frequency.  It might be wiser to sweep on every N
    //       calls to reclaim, for some configurable parameter N.
    if (head->count >= CHUNKSIZE)
      sweep_hp_lists();
  }

public:
  /// Create a thread's hazard pointer context by giving it empty lists and
  /// nulling its hazard pointer slots.
  sv_thread_hp_context_t()
      : a_chunks(new chunk_t<A>()), b_chunks(new chunk_t<B>()) {
    hazP[0].store(nullptr, std::memory_order_relaxed);
    hazP[1].store(nullptr, std::memory_order_relaxed);
  }

  /// Sequential-only destructor that frees all hazard pointers without
  /// checking if they are still reserved anywhere.
  ~sv_thread_hp_context_t() {
    // Reclaim all pointers and all chunks
    sv_thread_hp_context_t::wipe_chunks<A>(a_chunks);
    sv_thread_hp_context_t::wipe_chunks<B>(b_chunks);
  }

  /// Tail-recursive method for sweeping this thread's HP lists, and all
  /// threads after this thread in the list.  This should only be called when
  /// tearing down the data structure that is using the hazard pointer
  /// context.
  void sweep_hp_lists_recursive() {
    sweep_hp_lists();

    if (next != nullptr)
      next->sweep_hp_lists_recursive();
  }

  /// Protect a location by taking a hazard pointer on it.  Assumes no hazard
  /// pointers are held by the thread yet.
  void take_first(void *ptr) {
    hazP[0].store(ptr, std::memory_order_relaxed);
    curr_idx = 0;
  }

  /// Protect a location by taking a hazard pointer on it.  Assumes there is
  /// another hazard pointer currently held.
  void take(void *ptr) {
    hazP[1 - curr_idx].store(ptr, std::memory_order_relaxed);
  }

  /// Drop the "oldest" hazard pointer that we currently hold
  void drop_curr() {
    hazP[curr_idx].store(nullptr, std::memory_order_relaxed);
    // Switch curr_slot to the other slot. Necessary if there is a node in
    // the other slot, harmless otherwise.
    curr_idx = 1 - curr_idx;
  }

  /// Drop the hazard pointer on next.
  void drop_next() {
    hazP[1 - curr_idx].store(nullptr, std::memory_order_relaxed);
  }

  /// Drop all hazard pointers held by this thread.
  void drop_all() {
    hazP[0].store(nullptr, std::memory_order_relaxed);
    hazP[1].store(nullptr, std::memory_order_relaxed);
    curr_idx = 0;
  }

  /// Mark an A node for reclamation by adding it to a chunk
  void reclaim(A *ptr) { reclaim<A>(ptr, a_chunks); }

  /// Mark a B node for reclamation by adding it to a chunk
  void reclaim(B *ptr) { reclaim<B>(ptr, b_chunks); }

  // Count the number of hazard pointers held by all threads.
  int count_reserved() {
    int count = 0;
    sv_thread_hp_context_t *curr =
        SV::contexts_head.load(std::memory_order_relaxed);
    while (curr) {
      for (int i = 0; i < 2; ++i) {
        if (curr->hazP[i].load(std::memory_order_relaxed) != nullptr) {
          ++count;
        }
      }
      curr = curr->next;
    }
    return count;
  }

  /// [mfs] It would be nice if we could refactor so that we didn't need the
  ///       remaining two methods.

  /// Return the next context in the list of thread contexts; used only for
  /// shutting down the benchmark.
  sv_thread_hp_context_t *get_next() { return next; }

  /// Set the next pointer to build a list of thread contexts; used when
  /// initializing a new thread.
  void set_next(sv_thread_hp_context_t *_next) { next = _next; }
};