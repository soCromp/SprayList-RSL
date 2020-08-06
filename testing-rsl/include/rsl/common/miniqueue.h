#pragma once

#include <atomic>

/// MiniQueue is a sequential queue that is appropriate for situations where the
/// maximum number of elements in the queue is known to have a reasonable limit
/// (e.g., because each thread can have at most one entry in the queue).  Given
/// such a constraint, MiniQueue uses a resizable array to store queue entries.
///
/// Beware: The interface for MiniQueue is specialized to our use case.
template <class T> class MiniQueue {
  /// The data stored in the queue
  T *data;

  /// Number of pops that have been done so far.
  uint64_t pops;

  /// Number of pushes that have been done so far
  uint64_t pushes;

  /// Maximum elements in the queue
  uint64_t capacity;

public:
  /// Create an empty queue with a default capacity of 16 items
  MiniQueue(int size = 16)
      : data(new T[size]), pops(0), pushes(0), capacity(size) {}

  /// initFrom() is like a copy constructor, except that it destroys an existing
  /// MiniQueue and replaces it with a copy of the contents of /q/
  void initFrom(MiniQueue &q) {
    delete (data);
    data = new T[q.capacity];
    memcpy(data, q.data, sizeof(T) * q.capacity);
    pops = q.pops;
    pushes = q.pushes;
    capacity = q.capacity;
  }

  /// Getter for # of pops, so that we can implement a poor-man's iterator
  uint64_t getPops() { return pops; }

  /// Getter for # of pushes, so that we can implement a poor-man's iterator
  uint64_t getPushes() { return pushes; }

  /// Logically remove the oldest element from the queue.  We do not return the
  /// element, because we do not expect the caller to care about its value (it
  /// would have already peeked)
  void dequeue() { ++pops; }

  /// Return a reference to the oldest element in the queue
  T *peek() { return (pops == pushes) ? nullptr : &data[pops % capacity]; }

  /// Return the number of items in the queue.  It is derived by looking at the
  /// difference between pushes and pops.  This design decision is essential,
  /// because it means that spinning on pops will always notify a thread when
  /// the queue state has changed
  uint64_t size() { return pushes - pops; }

  /// Insert an item at the tail of the queue.  If the queue is full, double its
  /// capacity to make space.
  uint64_t enqueue(T item) {
    // On overflow, create a new array and copy the elements into it
    if (pushes - pops == capacity) {
      uint64_t nc = capacity * 2;
      T *new_data = new T[nc];
      for (uint64_t i = pops; i < pushes; ++i) {
        new_data[i % nc] = data[i % capacity];
      }
      delete (data);
      data = new_data;
      capacity = nc;
    }

    // we return the index of this push, so that at() is possible
    uint64_t ret = pushes;
    data[pushes % capacity] = item;
    ++pushes;
    return ret;
  }

  /// Return a pointer to the element at position /idx/, so that the caller
  /// can update its fields.  If /idx/ has been popped, return nullptr
  T *at(uint64_t idx) { return (idx < pops) ? nullptr : &data[idx % capacity]; }
};
