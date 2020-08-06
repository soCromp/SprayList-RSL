#pragma once

#include <cassert>
#include <cstring>
#include <iostream>

/// A data structure which uses an unsorted vector to implement a set interface.
/// Terrible asymptotes (O(n)), but fast at small sizes.
/// Non-concurrent.
/// UF: Unsorted, Fixed capacity
template <typename K, int CAPACITY> class set_uf {

  /// List of keys
  K key_list[CAPACITY];

  /// Current number of elements in the list.
  int size = 0;

  /// Internal function used to find a key in the vector.
  /// If it exists, it returns true, and sets pos.
  /// Otherwise, it returns false.
  bool find(const K &k, int &pos) const {
    for (int i = 0; i < size; ++i) {
      if (key_list[i] == k) {
        pos = i;
        return true;
      }
    }
    return false;
  }

  /// As above, but we do not care about the index.
  bool find(const K &k) const {
    int _;
    return find(k, _);
  }

public:
  set_uf() {}
  ~set_uf() {}

  /// Remove all elements from this set.
  void clear() { size = 0; }

  /// Discard my elements, make myself into a copy of other,
  /// and finally delete all of other's elements.
  void clear_and_steal_all(set_uf<K, CAPACITY> *other) {
    std::memcpy(key_list, other->key_list, sizeof(K) * other->size);
    size = other->size;
    other->size = 0;
  }

  /// Insert a new element into the list.
  /// If already exists, do nothing and return false.
  bool insert(const K &k) {
    if (find(k)) {
      // Already exists
      return false;
    }

    // Prevent vector from becoming overfull
    assert(size < CAPACITY);

    // Insert new element
    key_list[size] = k;

    ++size;
    return true;
  }

  /// Remove an element from the list.
  /// Return true if successful, false if didn't exist.
  bool remove(const K &k) {
    int pos;

    if (!find(k, pos)) {
      // Didn't exist
      return false;
    }

    // Move the last element into the gap left by this removal
    // (unless removed element was the last element)
    --size;
    // NB: CAPACITY != 1 squelches a false compiler error and gets optimized out
    // by the compiler.
    if (pos != size && CAPACITY != 1) {
      key_list[pos] = key_list[size];
    }

    return true;
  }

  /// Find a given key in the list.
  /// Return false if not found.
  bool contains(const K &k) const { return find(k); }

  /// Copy all of another set's elements and add them to this set.
  /// This method assumes the sets are disjoint already.
  void copyall(set_uf<K, CAPACITY> *victim) {
    // Check for overfull
    assert(size + victim->size <= CAPACITY);

    std::memcpy(key_list + size, victim->key_list, victim->size * sizeof(K));
    size += victim->size;
  }

  /// Consume another set_uf, stealing all of its elements.
  /// This method assumes the sets are disjoint already.
  void merge(set_uf<K, CAPACITY> *victim) {
    copyall(victim);
    victim->size = 0;
  }

  bool verify() const {
    // Check for duplicated keys
    for (int i = 1; i < size; ++i) {
      for (int j = 0; j < i; ++j) {
        if (key_list[i] == key_list[j]) {
          std::cout << "Found duplicated key " << key_list[i] << " at indices "
                    << i << "," << j << std::endl;
          dump();
          return false;
        }
      }
    }

    return true;
  }

  /// Return the key at a given physical position in the vector.
  /// Assertion failure if index is out of range.
  K at(const int index) const {
    // Bounds check
    assert(index >= 0 && index < size);
    return key_list[index];
  }

  /// Return the key at a given physical position in the vector.
  /// Returns junk data if asked for invalid index.
  K at_unchecked(const int index) const {
    if (index < 0 || index >= CAPACITY) {
      return key_list[0];
    }
    return key_list[index];
  }

  void verbose_analysis() const {
    // Print size/CAPACITY
    std::cout << "(" << size << "/" << CAPACITY << ")" << std::endl;
  }

  void dump() const {
    std::cout << "[ ";
    for (int i = 0; i < size; ++i) {
      std::cout << +key_list[i] << " ";
    }
    std::cout << "]";
  }

  int get_capacity() const { return CAPACITY; }
  int get_size() const { return size; }
};
