//de-templated RSL
//#pragma once
#include <atomic>
#include <cassert>
#include <cstddef>
#include <functional>
#include "rsl_c.h"
#include "include/rsl/common/config.h"
#include "include/rsl/common/ext.h"
#include "include/rsl/common/rlx_atomic.h"
#include "include/rsl/lehmer64.h"
#include "include/rsl/sv_lock.h"
#include "include/rsl/common/vector_sfra.h"
#include <iostream>

/// RSL is a version of the SkipVector that uses lazy two-phase
/// locking for concurrency control.  That is, it does not lock its entire
/// working right away.  Instead, it begins traversing the SkipVector and
/// locking elements.  As it locks elements, it operates on them.  Finally, when
/// it has finished locking elements and operating on them, then it releases all
/// of its locks.
///
/// RSL uses hazard pointers for memory safety.  As with the other
/// SkipVector implementations in this repository, it lazily merges orphans in
/// the data layer, and it uses non-resizable vectors.
///
/// Template Parameters:
/// int       K         - The type of the key for k/v pairs
/// int       V         - The type of the value for k/v pairs
/// vector_sfra  VECTOR    - A vector type that can hold k/v pairs
/// 5            DATA_EXP  - The log_2 of the target chunk size for data layer vectors
/// 5            INDEX_EXP - The log_2 of the target chunk size for index layer vectors
/// 0            LAYERS    - The number of index layers, 0 is automatically determined


class rsl {
    const static int data_exp = 8;
    const static int index_exp = 8;
    int LAYERS; //num index layers
    /// node_t is used for both the data layer and the index layer(s)
    template <typename T, int EXP> struct node_t {
        /// A lock to protect this node.  sv_lock is a sequence lock with a few
        /// stolen bits
        sv_lock lock;

        static constexpr int get_vector_size() {
            /*if (EXP == 2)  {
                // Special case: if EXP equals 2, we hack the vector size to 1 so it
                // behaves as much like a skip list as possible.
                return 1;
            } else // General case: choose a size that can hold 2 * 2^EXP elements.
                return 2 << EXP;*/
            return EXP == 2 ? 1 : 2 << EXP;
        }

        vector_sfra<slkey_t, T, get_vector_size()> v;

        //pointer to next vector at this layer
        rlx_atomic<node_t *> next = nullptr;

        /// Default constructor; creates node as orphan. This constructor is only
        /// ever used to create leftmost nodes, which are always orphans,
        /// and to initialize minchunk at skipvector initialization
        node_t() : lock(true), v() {}

        /// Constructor; creates node, and stitches it in after a given other node.
        ///
        /// NB: Assumes that the caller has locked /prev/
        node_t(node_t *prev, bool orphan) : lock(orphan), v() {
            next = prev->next.load();
            prev->next = this;
        }

        //destroying a node doesn't require anything special
        ~node_t() {}

        /// Merge the next node into this node, and unlink next node
        ///
        /// NB: The caller is expected to handle reclamation of unlinked node
        void merge() {
            node_t *zombie = next;
            v.merge(&(zombie->v));
            next = zombie->next.load();

            //set the dead bit
            zombie->lock.release_as_dead();
        }

        /// Insert a K/V pair into this node
        ///
        /// NB: May split this node if it's full
        bool insert(const std::pair<const slkey_t, T> &pair) {
            bool overfull = false;
            bool result = v.insert(pair, overfull);
            if(overfull) {
                // Insert failed because the current node was too big,
                // so split it and make an orphan.
                // Note: the orphan's constructor will stitch itself in.
                node_t *new_orphan = new node_t(this, true);
                new_orphan->v.steal_half_and_insert(&v, pair);
                return true;
            }
            return result;
        }

        /// Sequential code for checking if a node is an orphan
        ///
        /// NB: Concurrent methods should read the orphan bit from the seqlock
        bool is_orphan_seq() { return sv_lock::is_orphan(lock.get_value()); }
    };

  /// [mfs] Even though "virtual is bad", we could simplify the code a lot by
  ///       having a base class that has a virtual destuctor, and then making
  ///       index_t and data_t inherit from it.  Among other things, this would
  ///       let us have a single HP list.

  /// type of index nodes.  Since an index node can reference either another
  /// index node, or a data node, we use a generic void*.  Thus the map holds
  /// key/ptr pairs
  typedef node_t<void *, index_exp> index_t; 

  /// type of data nodes.  A data node's vector holds key/val pairs
  typedef node_t<val_t, data_exp> data_t;

  std::atomic <data_t *> minchunk; //chilling chunk for extract min
  std::atomic <int64_t> minchunksz; //size of chilling chunk

  /// A thread hazard pointer context.  This specific context type assumes we
  /// are doing hand-over-hand list traversals, and thus only ever need two
  /// hazard pointers.
  ///
  /// thread_hp_context_t tracks two things.  First, it has a small number (2)
  /// of hazard pointers: shared single-writer, multi-reader locations where a
  /// thread can store the locations it is protecting.  Second, it has a large
  /// segment of memory (a list of 64-element vectors) that stores retired
  /// pointers that are not yet reclaimed (presumably because of a product of
  /// laziness and the hazard pointers held by other threads).
  ///
  /// [mfs] This type should move into its own file.  The templating on index
  ///       and data types will make that tough, though.
  ///
  /// [mfs] Would it be a bad idea to use a std::vector instead of a list of
  ///       chunk_t objects?
  class thread_hp_context_t {
    /// The size of a chunk in the thread'd list of to-reclaim locations
    ///
    /// [mfs] Either switch to std::vector or make this a template parameter?
    static const int CHUNKSIZE = 64;

    /// A set of pointers that are unlinked, but not yet reclaimed.
    ///
    /// NB: This is templated on the type, so we can delete() instead of just
    ///     freeing.
    template <typename T> struct chunk_t {
        //the set of objects to be reclaimed
        T *ptrs[CHUNKSIZE];

        //num valid pointers in the chunk
        int count = 0;

        //the next chunk in the linked list of chunks
        chunk_t<T> *next = nullptr;
    };

    //a list of chunks of index blocks
    chunk_t<index_t> *idx_chunks;

    //a list of chunks of data blocks
    chunk_t<data_t> *data_chunks;

    //the hazard pointers held by this thread. hard coded at 2
    //because only dealing with linked list data structure
    std::atomic<void *> hazP[2];

    //the threads' HP contexts form a linked list. the order in the list is
    //not important but need a next pointer to create the list
    thread_hp_context_t *next = nullptr;

    //index of curr in hasP. val doesn't matter if no hazard pointer on
    //curr is held
    int curr_idx = 0;

    //check if pointer reserved by another thread
    bool is_reserved(void *ptr) {
      thread_hp_context_t *curr = contexts_head.load(std::memory_order_relaxed);
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
    template<typename T> void sweep_hp_list(chunk_t<T> *head) {
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
      sweep_hp_list<index_t>(idx_chunks);
      sweep_hp_list<data_t>(data_chunks);
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
    thread_hp_context_t()
        : idx_chunks(new chunk_t<index_t>()),
          data_chunks(new chunk_t<data_t>()) {
      hazP[0].store(nullptr, std::memory_order_relaxed);
      hazP[1].store(nullptr, std::memory_order_relaxed);
    }

    /// Sequential-only destructor that frees all hazard pointers without
    /// checking if they are still reserved anywhere.
    ~thread_hp_context_t() {
      // Reclaim all pointers and all chunks
      thread_hp_context_t::wipe_chunks<index_t>(idx_chunks);
      thread_hp_context_t::wipe_chunks<data_t>(data_chunks);
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

    /// Mark an index node for reclamation by adding it to a chunk
    void reclaim(index_t *ptr) { reclaim<index_t>(ptr, idx_chunks); }

    /// Mark a data node for reclamation by adding it to a chunk
    void reclaim(data_t *ptr) { reclaim<data_t>(ptr, data_chunks); }

    // Count the number of hazard pointers held by all threads.
    int count_reserved() {
      int count = 0;
      thread_hp_context_t *curr = contexts_head.load(std::memory_order_relaxed);
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
    thread_hp_context_t *get_next() { return next; }

    /// Set the next pointer to build a list of thread contexts; used when
    /// initializing a new thread.
    void set_next(thread_hp_context_t *_next) { next = _next; }
  };

  /// A thread-local pointer to each thread's hazard pointer context.
  inline static thread_local thread_hp_context_t *hp_context;

  // The head of the global linked list of contexts.
  inline static std::atomic<thread_hp_context_t *> contexts_head;

  /// The threshold at which to merge chunks of the skipvector
  const double merge_threshold;

  /// The number of index layers in the skipvector.
  ///
  /// NB: This does not include the data layer
  const int layers;

  /// Array of leftmost index nodes.
  index_t *index_head[100];

  /// Leftmost data vector.
  std::atomic <data_t *> data_head;

  /// Create a context for the thread, if one doesn't exist
  void init_context() {
    if (hp_context == nullptr) {
      hp_context = new thread_hp_context_t();
      while (true) {
        thread_hp_context_t *read_head = contexts_head;
        hp_context->set_next(read_head);
        if (contexts_head.compare_exchange_strong(read_head, hp_context))
          break;
      }
    }
  }

  /// Generate height using a geometric distribution from 0 to layers.
  /// A height of n means it exists in the bottommost n index layers, and also
  /// the data layer. A height of zero means it exists solely in the data layer.
  int random_height() {
    constexpr int target_data_chunk_size = 1 << data_exp;
    constexpr int target_idx_chunk_size = 1 << index_exp;

    static thread_local __uint128_t g_lehmer64_state =
        lehmer64_seed((uint64_t) 0);
        //lehmer64_seed((uint64_t) pthread_self());  
    uint64_t r = lehmer64(g_lehmer64_state);
    int result = 0;

    // The probability that r is divisible by target_chunk_size is exactly 1 /
    // target_chunk_size (assuming RAND_MAX + 1 is a power of 2.)
    //
    // The first iteration of the loop is unrolled to specially handle the data
    // layer.
    //
    // NB: The trick here is that we can look for a series of low 0 bits, and
    //     use that to both (a) check many bits in parallel, and (b)
    //     short-circuit the search when we find any non-zero bits.
    if (r % target_data_chunk_size == 0) {
      // Use a right shift to remove the used bits.
      r = r >> data_exp;

      for (result = 1; r % target_idx_chunk_size == 0 && result < layers;
           ++result) {
        // Use a right shift to remove the used bits.
        r = r >> index_exp;
      }
    }

    return result;
  }

  /// Checks two vectors to see if merging them is sensible. It's sensible if
  /// the sum of their sizes is under the merge threshold, or if b is totally
  /// empty.
  bool should_merge(index_t *a, index_t *b) {
    // Merges should never happen in skiplist simulation mode unless b is
    // totally empty. As INDEX_EXP is a templated value, this check gets
    // optimized out by the compiler.
    if (index_exp == 2)
      return b->v.get_size() == 0;

    const uint16_t idx_merge_threshold = merge_threshold * (1 << index_exp);
    return b->v.get_size() == 0 ||
           (a->v.get_size() + b->v.get_size() < idx_merge_threshold);
  }

  bool should_merge(data_t *a, data_t *b) {
    // Merges should never happen in skiplist simulation mode unless b is
    // totally empty. As DATA_EXP is a templated value, this check gets
    // optimized out by the compiler.
    if (data_exp == 2)
      return b->v.get_size() == 0;

    const uint16_t data_merge_threshold = merge_threshold * (1 << data_exp);
    return b->v.get_size() == 0 ||
           (a->v.get_size() + b->v.get_size() < data_merge_threshold);
  }

  /// Helper function that determines if a search should continue to the next
  /// chunk, and if so, it advances curr, repeatedly until no more advancing is
  /// necessary.
  ///
  ///  If parameter "cleanup" is set to true, this function will merge orphans
  ///  as necessary when they are found. If it is not, will only clean up empty
  ///  orphans. The cleanup flag is set to true by insert() and remove();
  ///  contains() sets it to false, to keep contains() fast.
  ///
  /// @returns true if successful, false on a seqlock verification failure
  template <typename T>
  bool check_next(T *&curr, uint64_t &curr_lock, const slkey_t &k, bool cleanup) {
    // The fastest way out of this loop is when next is nullptr or curr's last
    // element is >= k.  Finding these early avoids taking a hazard pointer on
    // next or reading its seqlock.  If the /while/ condition fails, we will
    // return true.
    T *next = curr->next;
    slkey_t last = k;
    while (next != nullptr && (!curr->v.last(last) || k > last)) {
      // Take a hazard pointer on next, then make sure curr hasn't changed
      hp_context->take(next);
      if (!curr->lock.confirm_read(curr_lock)) {
        hp_context->drop_next();
        return false;
      }

      // Read next's sequence lock.  If it's deleted, we need to retry
      uint64_t next_lock = next->lock.begin_read();
      if (sv_lock::is_dead(next_lock)) {
        hp_context->drop_next();
        return false;
      }

      // Check if /next/ needs to be removed (and possibly merged first)
      // - remove if it's an empty orphan
      // - merge+remove if it's an orphan and cleanup == should_merge() == true
      //
      // [mfs] The guard for this /if/ is the same as the guard for the
      //       do/while.  It's probably possible to refactor into a single
      //       /while/ loop
      if (sv_lock::is_orphan(next_lock) &&
          ((cleanup && should_merge(curr, next)) || next->v.get_size() == 0)) {

        // [mfs] This logic should be very infrequently needed.  I think we'd be
        //       better having it in a separate function, so that hopefully it
        //       doesn't get inlined

        // Get write lock on curr, since we'll modify curr->next
        if (!curr->lock.upgrade(curr_lock)) {
          hp_context->drop_next();
          return false;
        }

        // We unlink in a loop, since there may be multiple orphans
        bool changed = false;
        do {
          // Acquire next, so we can mark it deleted
          if (!next->lock.upgrade(next_lock)) {
            curr->lock.release_changed_if(changed); // may downgrade curr->lock
            hp_context->drop_next();
            return false;
          }

          // Mark next dead, unlink it, and mark it for reclamation.
          curr->merge();
          hp_context->drop_next();
          hp_context->reclaim(next);
          next = curr->next;
          changed = true;

          // We may need to keep looping.  Next==null is the easy exit case
          if (next == nullptr) {
            curr_lock = curr->lock.release();
            return true;
          }

          // NB: We don't have to check if next is dead or take a hazard pointer
          //     on it because we have its predecessor locked as a writer.  Even
          //     if it's not an orphan, it can't be deleted without holding a
          //     lock on its predecessor, and we have that lock.
          next_lock = next->lock.begin_read();
        } while (
            sv_lock::is_orphan(next_lock) &&
            ((cleanup && should_merge(curr, next)) || next->v.get_size() == 0));

        if (cleanup) {
          // If the cleanup flag is enabled, merging may have eliminated the
          // need to check next, so start again from the top.
          //
          // NB: curr->lock.release() still gives us a read lock on curr
          curr_lock = curr->lock.release();
          continue;
        } else {
          // Before we release the write lock on curr, we need to take a hazard
          // pointer on next, so that we can continue to access it safely after
          // the release.
          hp_context->take(next);
          curr_lock = curr->lock.release();
        }
      }

      // At this point we know that we have a nonempty next.
      if (k < next->v.first()) {
        // Next's first element is after k, so we have ruled out next.
        // Now we just need to check its sequence lock.
        // Return true if the check succeeds, false if it fails.
        bool result = next->lock.confirm_read(next_lock);
        hp_context->drop_next();
        return result;
      }

      // Next's first element is before (or equal to) the sought key,
      // so we to go to next and repeat from there. We're done with curr,
      // so we just need to confirm its sequence lock hasn't changed.
      if (!curr->lock.confirm_read(curr_lock)) {
        hp_context->drop_next();
        return false;
      }

      curr = next;
      curr_lock = next_lock;
      next = curr->next;
      hp_context->drop_curr();
    }

    // We ruled out next, so return true.
    return true;
  } //ends check_next method

  /// Given a node /curr/ that is read locked, give up that lock, and replace it
  /// with a read lock on new_node.  Also drops HP on curr, takes HP on new_node
  ///
  /// @returns true if successful, false if failed.
  template <typename T>
  bool reader_swap(index_t *curr, uint64_t &curr_lock, T *new_node) {
    // Take a hazard pointer on new_node, make sure curr hasn't changed
    hp_context->take(new_node);
    if (!curr->lock.confirm_read(curr_lock)) {
      hp_context->drop_next();
      return false;
    }

    // read-lock new_node, then check curr hasn't changed and new_lock not dead
    //
    // [mfs]: double-check on curr may be redundant?
    uint64_t new_lock = new_node->lock.begin_read();
    if (!curr->lock.confirm_read(curr_lock) || sv_lock::is_dead(new_lock)) {
      hp_context->drop_next();
      return false;
    }

    // finish the swap
    curr_lock = new_lock;
    hp_context->drop_curr();
    return true;
  }

  /// follow() is used by contains to find the correct down pointer from curr.
  /// follow() also swaps the lock on curr for a lock on the new down node
  template <typename T>
  bool follow(index_t *curr, uint64_t &curr_lock, const slkey_t &k, T *&down) {
    // if check_next() fails, start over
    if (!check_next<index_t>(curr, curr_lock, k, false)) {
      return false;
    }

    // Find down pointer in curr, confirm curr's sequence lock (and next's, if
    // next was read), and take a seqlock on down.
    void *down_void = nullptr;
    if (curr->v.find_lte(k, down_void)) {
      down = static_cast<T *>(down_void);
    }
    // [mfs] Could simplify to return reader_swap()...
    return reader_swap<T>(curr, curr_lock, down);
  }

  bool orphanize(const slkey_t &k) {
      init_context();
  top:

    //start at topmost index layer
    int layer = layers-1; //reminder: num layers doesn't include data layer
    index_t *curr = (index_head[layer]);
    uint64_t curr_lock = curr->lock.begin_read();
    hp_context->take_first(curr);
    data_t *curr_dl = data_head;

    //skip through index layers
    while(layer >= 0) {
      //check next, as it may need to be maintained or followed
      if (!check_next<index_t>(curr, curr_lock, k, true)) {
        hp_context->drop_all();
        goto top;
      }

      //now search the vector we arrived at for the right down pointer
      void *down = nullptr;
      index_t *down_idx = nullptr;
      slkey_t found_k = k;

      if(curr->v.find_lte(k, found_k, down)) {
        if (found_k == k) {
          //if we find the uppermost instance of k in the skipvector, lock
          //it and proceed to remove it

          //here we must confirm that this is the uppermost instance of k
          //if curr is not an orphan and k is the first element in this list
          //ththen this is not the uppermost instance of k so start over.
          //else, it's safe to proceed. This issue can happen if remove 
          //interleaves with insert on same k
          if(!sv_lock::is_orphan(curr_lock) && curr->v.first() == k) {
            hp_context->drop_all();
            goto top;
          }

          if(!curr->lock.upgrade(curr_lock)) {
            hp_context->drop_all();
            goto top;
          }

          //we have write lock on curr now so don't need hazard pointer now
          hp_context->drop_curr();
          break;
        }

        //else we found appropriate down pointer so follow it
        if(layer > 0) {
          down_idx = static_cast<index_t *>(down);
        } else { 
          curr_dl = static_cast<data_t *>(down);
        }
      } else {
        //no appropriate down pointer was found so start at next layer head
        if(layer > 0) {
          down_idx = (index_head[layer-1]);
        } else {
          curr_dl = data_head;
        }
      }

      //exchange curr's lock for down's lock
      if(layer > 0) {
        if (!reader_swap<index_t>(curr, curr_lock, down_idx)) {
          hp_context->drop_all();
          goto top;
        }
        curr = down_idx;
      } else {
        if (!reader_swap<data_t>(curr, curr_lock, curr_dl)) {
          hp_context->drop_all();
          goto top;
        }
      }

      --layer;
    }

    //if we made it to data layer, k wasn't in upper levels so something's odd
    if(layer == -1) {
      //std::cerr << "orphanize1\n";
      //check if we have to follow any next pointers
      bool check_next_success = check_next<data_t>(curr_dl, curr_lock, k, true);

      // Now, acquire curr_dl as a writer.
      if (!check_next_success || !curr_dl->lock.upgrade(curr_lock)) {
        hp_context->drop_all();
        goto top;
      }

      //edge case: like earlier, this call could interleave with insert
      //causing the node to not be an orphan. restart
      if (!sv_lock::is_orphan(curr_lock) && curr_dl->v.first() == k) {
        curr_dl->lock.release_unchanged();
        goto top;
      } 

      curr_dl->lock.release_unchanged();
      hp_context->drop_all();
      return true;
      
      /* else {
        std::cerr << "non-orphan was actually orphan\n";
        hp_context->drop_all();
        return true;
      } */
    }

    //remove starts being lazy here
    //lock all the way down
    //for each new node we access here, we have its parent locked as a 
    //writer so there is no need to take a hazard pointer on them

    for (int i = layer; i > 0; --i) {
      void *down_void = nullptr;
      curr->v.remove(k, down_void);
      index_t *down_idx = static_cast<index_t *>(down_void);
      down_idx->lock.acquire();

      //release first node normally, subsequent nodes as orphans
      if(i == layer) {
        curr->lock.release();
      } else {
        curr->lock.release_as_orphan();
      }

      curr = down_idx;
    }

    //once more for the last index layer
    void *down_void = nullptr;
    curr->v.remove(k, down_void);
    curr_dl = static_cast<data_t *>(down_void);
    curr_dl->lock.acquire();
    if(layer == 0) {
      curr->lock.release();
    } else {
      curr->lock.release_as_orphan();
    }

    //now just release the node as orphan
    curr_dl->lock.release_as_orphan();
    hp_context->drop_all();
    return true;
  } //ends orphanize method

  //fetch and decrement for concurrent extract min operation. be holding hazard pointer when call!
  int64_t next_minelem_ind() {
        return --minchunksz;
  }

public:
  /// insert() takes a reference to a key/val pair, so we expose the type here
  typedef std::pair<const slkey_t, val_t> value_type;

  //constructor inside SprayList test suite
  rsl(setup_t *cfg) : merge_threshold(2.0), layers(cfg->layers) {
      LAYERS = layers;
      // Make sure number of layers is valid.
      assert(layers > 0);
      assert(layers <= LAYERS);
      assert(layers < 100);

      // We use a single 64-bit random number on insert(), so make sure that's
      // enough for the chosen configuration.
      //assert(data_exp + (cfg->layers * index_exp) <= 64);

      for(int i = 0; i < layers; i++) {
        //index_t *n = new index_t();
        index_head[i] = new index_t();
      }

      minchunk = new data_t();
      minchunksz = 0;
      data_head = new data_t();
  }


/*  /// Sequential-only destructor
  ~rsl() {
      // First, free all index layer nodes BUT the leftmost ones.
      for (int i = 0; i < layers; ++i) {
          index_t *curr = index_head[i].next;
          while (curr != nullptr) {
              index_t *next = curr->next;
              delete curr;
              curr = next;
          }
      }

      // Free each node in data layer but the leftmost,
      // which was statically allocated
      data_t *data_curr = data_head->next;
      while (data_curr != nullptr) {
          data_t *next = data_curr->next;
          delete data_curr;
          data_curr = next;
      }

      // Go through all thread_hp_context_t instances and sweep each one, in
      // order to free all remaining nodes from this skipvector instance.
      // NB: Even if contexts_head changes immediately after this load(), the
      // added thread context can't contain any nodes from this skipvector
      // assuming this destructor was correctly called while the data structure
      // was quiescent.
      thread_hp_context_t *hp_head = contexts_head.load();

      // Edge case: Skipvector was simply constructed then destructed unused, and
      // no thread contexts were even initialized.
      if (hp_head != nullptr) {
      hp_head->sweep_hp_lists_recursive();
      }
  }

  // Sequential-only teardown method. Destroys ALL hazard pointer context,
  // for ALL threads, and for ALL instances of rsl<K,V>!
  // Teardown is IRREVOCABLE: NO instances of rsl will work after
  // this is called, as the thread_local pointers to their own contexts cannot
  // be nulled after the context objects are deleted!
  static void tear_down() {
      thread_hp_context_t *curr = contexts_head.exchange(nullptr);

      while (curr != nullptr) {
      // Destroy each thread's context.
      // Contexts' destructor will reclaim all remaining unlinked nodes.
      thread_hp_context_t *next = curr->get_next();
      delete curr;
      curr = next;
      }
  }
*/

  /// Search for a key in the skipvector
  /// Returns true if found, false if not found
  /// "val" parameter is used to pass back the value if found
  bool contains(const slkey_t &k, val_t &v) {
      //std::cerr << "contains " << k << std::endl;
      init_context();

    top:
      // Start from the head node (leftmost node in topmost layer.)
      int layer = layers - 1;
      index_t *curr = index_head[layer];

      // Read head node's sequence lock.
      // NB: Head node's dead bit won't ever be set, so skip that check.
      hp_context->take_first(curr);
      uint64_t curr_lock = curr->lock.begin_read();

      // Skip through all index layers but the last.
      for (; layer >= 1; --layer) {
          // If follow() doesn't find a suitable down pointer,
          // default to next index layer's head.
          index_t *down = index_head[layer - 1];
          if (!follow<index_t>(curr, curr_lock, k, down)) {
              // Sequence lock check failed
              hp_context->drop_all();
              goto top;
          }
          curr = down;
      }

      // Skip through the last index layer.
      data_t *curr_dl = data_head;
      if (!follow<data_t>(curr, curr_lock, k, curr_dl)) {
          // Sequence lock check failed
          hp_context->drop_all();
          goto top;
      }

      // Finally, read the data layer.
      if (!check_next<data_t>(curr_dl, curr_lock, k, false)) {
          hp_context->drop_all();
          goto top;
      }

      // Scan curr_dl for the sought value.
      // NB: There is a chance that this contains() will find a value,
      // but the final sequence lock checks will fail, making us start over,
      // and the element will be gone by the time we get back here.
      // This scenario would yield the somewhat undesirable result that this
      // method returns false but overwrites v with an outdated value.
      // To prevent this, we use a temporary intermediate variable, tmp.
      val_t tmp = v;
      bool result = curr_dl->v.contains(k, tmp);

      // Confirm curr's sequence lock.
      if (!curr_dl->lock.confirm_read(curr_lock)) {
      hp_context->drop_all();
      goto top;
      }

      hp_context->drop_all();

      if (result)
      v = tmp;

      return result;
  }

  /// Insert a new element into the RSL
  bool insert(const value_type &pair) {
    //std::cerr << "insert " << pair.first << std::endl;
    init_context();

    const slkey_t &k = pair.first;

    // Pre-generate a new height for the node
    int new_height = random_height();

    // Do a lookup as though doing a contains() operation, but save references
    // to index nodes we'll need later in an array.
    index_t *prev_nodes[LAYERS] = {nullptr};

  top:

    // Start at the topmost index layer.
    int layer = layers - 1;
    index_t *curr = (index_head[layer]);
    hp_context->take_first(curr);
    uint64_t curr_lock = curr->lock.begin_read();
    data_t *curr_dl = data_head;

    // checkpoint is a "safe node" that we know won't be deleted, because either
    // we hold the lock on its parent, or it is the head node of its layer.
    // If we have a sequence lock check fail during execution, and we have a
    // checkpoint, we can jump back to it rather than start all over.
    index_t *checkpoint = nullptr;
    data_t *checkpoint_dl = nullptr;
    bool load_checkpoint = false;

    // Skip through index layers.
    while (layer >= 0) {

      // Load the checkpoint if the appropriate flag is set.
      if (load_checkpoint) {
        hp_context->drop_all();
        load_checkpoint = false;
        if (checkpoint == nullptr) {
          // No checkpoint was set, so retry from start.
          goto top;
        }
        curr = checkpoint;

        // NB: We know the checkpoint won't be deleted, so we neither need to
        // check its sequence lock's dead bit nor double-check the hazard
        // pointer we take on it.
        hp_context->take_first(curr);
        curr_lock = curr->lock.begin_read();
      }

      // Check next, as it may need to be maintained or followed.
      if (!check_next<index_t>(curr, curr_lock, k, true)) {
        load_checkpoint = true;
        continue;
      }

      // If the inserted node is tall enough, lock this node and save it.
      if (layer < new_height) {
        if (!curr->lock.upgrade(curr_lock)) {
          load_checkpoint = true;
          continue;
        }

        prev_nodes[layer] = curr;
      }

      // Now, search the vector we arrived at for the right down pointer.
      void *down = nullptr;
      index_t *down_idx = nullptr;
      slkey_t found_k = k;

      if (curr->v.find_lte(k, found_k, down)) {
        if (found_k == k) {
          // If we find k in the index layer, stop and return false.
          if (layer < new_height) {
            // If we locked any nodes,
            // we must release them before returning.
            for (int i = layer; i < new_height; ++i) {
              prev_nodes[i]->lock.release_unchanged();
            }
          } else {
            // We don't have curr locked,
            // so we must validate its sequence lock before returning.
            if (!curr->lock.confirm_read(curr_lock)) {
              load_checkpoint = true;
              continue;
            }
          }
          hp_context->drop_all();
          return false;
        }

        // Otherwise, we found an appropriate down pointer, so follow it.
        if (layer > 0) {
          down_idx = static_cast<index_t *>(down);
        } else {
          curr_dl = static_cast<data_t *>(down);
        }
      } else {
        // No appropriate down pointer was found,
        // so start at the leftmost node at the next layer.
        if (layer > 0) {
          down_idx = (index_head[layer - 1]);
        } else {
          curr_dl = data_head;
        }
      }

      if (layer < new_height) {
        // If we have locked curr, then we can use down as a checkpoint.
        if (layer > 0) {
          hp_context->take(down_idx);
          hp_context->drop_curr();
          curr = down_idx;
          curr_lock = down_idx->lock.begin_read();
          checkpoint = down_idx;
        } else {
          hp_context->take(curr_dl);
          hp_context->drop_curr();
          curr_lock = curr_dl->lock.begin_read();
          checkpoint_dl = curr_dl;
        }
      } else {
        // If we have not locked curr,
        // we must safely exchange locks and hazard pointers.
        if (layer > 0) {
          if (!reader_swap<index_t>(curr, curr_lock, down_idx)) {
            load_checkpoint = true;
            continue;
          }
          curr = down_idx;
        } else {
          if (!reader_swap<data_t>(curr, curr_lock, curr_dl)) {
            load_checkpoint = true;
            continue;
          }
        }
      }

      --layer;
    }

  retry_dl:
    // At this point we should be at the data layer.

    // Check if we have to follow any next pointers.
    bool check_next_success = check_next<data_t>(curr_dl, curr_lock, k, true);

    // Now, acquire curr_dl as a writer.
    if (!check_next_success || !curr_dl->lock.upgrade(curr_lock)) {
      // Go back to checkpoint_dl if it exists, or start over from top.
      hp_context->drop_all();

      if (checkpoint_dl != nullptr) {
        curr_dl = checkpoint_dl;
        curr_lock = curr_dl->lock.begin_read();
        hp_context->take_first(curr_dl);
        goto retry_dl;
      } else {
        goto top;
      }
    }

    // Common case: generated height is 0,
    // so simply attempt to insert it into the data layer.
    if (new_height == 0) {
      bool result = curr_dl->insert(pair);
      curr_dl->lock.release_changed_if(result);
      hp_context->drop_all();
      return result;
    }

    // Generated height is at least 1, so we need to partition the data node.
    // First we must manually check if the key is present in the data node.
    if (curr_dl->v.contains(k)) {
      // If key is present, release everything and just return false.
      for (int i = 0; i < new_height; ++i) {
        prev_nodes[i]->lock.release_unchanged();
      }
      curr_dl->lock.release_unchanged();
      hp_context->drop_all();
      return false;
    }

    // Key isn't present, so do the partition.

    // Edge case: curr_dl may be full and the inserted key may be less than
    // its minimum. (This can only happen if it is leftmost.)
    // If this is the case, we must first partition curr_dl.
    if (curr_dl->v.get_size() == curr_dl->v.get_capacity() &&
        k < curr_dl->v.first()) {
      // NB: This newly created node is an orphan,
      // only reachable through curr_dl, which is locked by us.
      // Thus, there is no need for us to lock this new node.
      data_t *new_orphan = new data_t(curr_dl, true);
      new_orphan->v.steal_half(&(curr_dl->v));
    }

    // NB: As above, this new node is only reachable through curr_dl and its
    // parent, which we also have locked, so there is similarly no need for us
    // to lock it.
    data_t *new_data_node = new data_t(curr_dl, false);
    new_data_node->v.insert_and_steal_greater(&(curr_dl->v), pair);

    void *down_ptr = new_data_node;

    // Partition any index layers that need partitioning,
    // and insert down pointers.
    // NB: This loop's range excludes the top layer
    // because we do not partition at the top layer
    for (int i = 0; i + 1 < new_height; ++i) {
      index_t *victim = prev_nodes[i];

      // Same edge case as above, just for index layer nodes
      if (victim->v.get_size() == victim->v.get_capacity() &&
          k < victim->v.first()) {
        index_t *new_index_orphan = new index_t(victim, true);
        new_index_orphan->v.steal_half(&(victim->v));
      }

      index_t *new_index_node = new index_t(victim, false);
      new_index_node->v.insert_and_steal_greater(&(victim->v),
                                                 std::make_pair(k, down_ptr));
      down_ptr = new_index_node;
    }

    // Finally, at the pre-generated height,
    // simply insert the appropriate down pointer into the appropriate vector.
    prev_nodes[new_height - 1]->insert(std::make_pair(k, down_ptr));

    // Unlock everything and return!
    for (int i = 0; i < new_height; ++i) {
      prev_nodes[i]->lock.release();
    }

    curr_dl->lock.release();
    hp_context->drop_all();
    return true;
  } //ends insert method


  int simple_count = 0;

  //extracts min, places it into the k and v passed in
  bool extract_min_concur(slkey_t *k, val_t *v, int tid) {
    std::cout << "thread " << tid << " in exmin\n";
    init_context();
   acquire:
    data_t *curmin = minchunk;
    
    hp_context->take_first(curmin);
    if(curmin != minchunk && curmin->v.get_size() == 0)
      goto acquire;
    uint64_t lmin = curmin->lock.begin_read(); //n: maybe not needed
    int64_t ind = next_minelem_ind();
    
    if(ind == -1  ) { //get new minlist if needed
      std::cout << "need new minlist\n";
      hp_context->drop_all();
      if( !(*curmin).lock.upgrade(lmin) ) { //can't acquire lock, meaning someone else has it 
        std::cout << "can't acquire lock\n";
        minchunksz = 0;
        hp_context->drop_all();           
        return false;
      }

     top:

      //get head node
      if( !(*data_head).lock.acquire() ) {
        std::cout << "couldn't acquire data_head lock\n";
        minchunksz = 0;
        (*curmin).lock.release_unchanged();
        hp_context->drop_all();
        return false;
      }

      //get 1st DL node
      data_t *n1 = (*data_head).next;
      if(n1 == nullptr) { //usually means the structure is nearly empty
        (*data_head).lock.release();
        (*curmin).lock.release_unchanged();
        minchunksz = 0; //so another thread can try later
        hp_context->drop_all();
        return simple_extract_min(k,v);
      }

      hp_context->take(n1);
      while(n1 != (*data_head).next) {
        std::cout << "updating n1\n";
        n1 = (*data_head).next;
        hp_context->take(n1);
      }
      uint64_t l = n1->lock.begin_read();

      if(!n1->lock.is_orphan(l) ) {
        std::cout << "orphanize\n";
        slkey_t deprop = n1->v.first(); //this elem was propagated to upper levels
        (*data_head).lock.release();
        hp_context->drop_all();
        bool orphan = orphanize(deprop);  //to try: let orphanize itself find deprop?
        std::cout << "orphanize complete\n";
        goto top;
      }

      /*data_t *old_head = data_head;
      data_head = n1;

      std::cout << "finishing new minlist procedure, releasing locks..\n";
      
      (*curmin).lock.release();
      minchunk = old_head;*/
      std::cout << "releasing locks..\n";

      data_t *chunk = data_head;

      (*curmin).lock.release();
      minchunk = chunk;
      data_head = n1;

      minchunksz = (*minchunk).v.get_size();
      //(*minchunk).lock.release();

      hp_context->drop_all();
      std::cout << "about to give up after making new minlist\n";
      return extract_min_concur(k, v, tid);
    }
    
    //pop from min list
    bool minread = curmin->lock.confirm_read(lmin);
    if(!minread) {
      hp_context->drop_all();
      return false;
    }
    bool res = curmin->v.removeInd(ind, *k, *v); 
    if(!res)
      std::cout << "vector remove ind " << ind << " fails. vec size " << curmin->v.get_size() << std::endl;
    hp_context->drop_all();
    return res;
  }

  //data head->next is empty so remove first element of data head
  //without creating new minlist
  bool simple_extract_min(slkey_t *k, val_t *v) {
    //std::cout << "in simple\n";
    (*data_head).lock.acquire();
    if((*data_head).v.get_size() <= 0) {
      (*data_head).lock.release();
      *k = -1;
      *v = -1; //use -1 as a sign that the structure is empty
      return true;
    }
    *k = (*data_head).v.first();
    //data_head->v.last(*k);
    //std::cout << "about to remove, vec size is " << data_head->v.get_size() << std::endl;
    bool res = (*data_head).v.remove(*k, *v);
    //std::cout << "did remove, returning " << (int) res << std::endl;
    (*data_head).lock.release();
    return res;
  }

  // SEQUENTIAL-ONLY size function
  int get_size() {
    int result = 0;
    data_t *curr = data_head;

    while (curr != nullptr) {
      result += (int) curr->v.get_size();
      curr = curr->next;
    }
    result += (int) (*minchunk).v.get_size();
    //std::cout << result << std::endl;
    return result;
  }
};