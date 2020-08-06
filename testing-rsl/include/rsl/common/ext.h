#pragma once

#include <string>

// An extension of any templated type T with two additional values, BOTTOM and
// TOP. For all T t, BOTTOM < t < TOP.
// This class is not thread safe. Function calls may return nonsensical results
// if another thread is concurrently modifying involved objects of this class.
template <typename T> class ext {
  /// Constants representing the three states a ext<T> can be in.
  /// Values are chosen so that comparators may be used.
  static constexpr char TOP = 3;
  static constexpr char VALUE = 2;
  static constexpr char BOTTOM = 1;

  /// If this extended T is a VALUE, then this contains the value.
  /// Otherwise it contains junk data and should not be read.
  T t;

  /// Represents the state of this extended T: BOTTOM, VALUE, or TOP.
  char state;

  /// Private constructor for making instances of TOP and BOTTOM.
  ext(const char state) : t(), state(state) {}

public:
  /// "Blank" constructor for making arrays and such;
  /// produces an instance of BOTTOM as default value.
  ext() : t(), state(BOTTOM) {}

  /// Public constructor, creates a new ext<T> with the provided value for T.
  ext(const T t) : t(t), state(VALUE) {}

  /// Copy constructor
  ext(const ext<T> &other) : t(other.t), state(other.state) {}

  /// Create and return an instance of TOP.
  static ext<T> top() { return ext<T>(TOP); }

  /// Create and return an instance of BOTTOM.
  static ext<T> bottom() { return ext<T>(BOTTOM); }

  bool is_top() const { return state == TOP; }
  bool is_value() const { return state == VALUE; }
  bool is_bottom() const { return state == BOTTOM; }

  T value() const { return t; }

  /// Java-style compareTo() function.
  int compare_to(const ext<T> &other) const {
    if (state > other.state)
      return 1;
    if (state < other.state)
      return -1;
    if (state != VALUE)
      return 0;
    if (t > other.value())
      return 1;
    if (t < other.value())
      return -1;
    return 0;
  }

  /// Compare this ext<T> to a regular T, interpreting it as a VALUE.
  int compare_to(const T other) const {
    if (state == TOP)
      return 1;
    if (state == BOTTOM)
      return -1;
    if (t > other)
      return 1;
    if (t < other)
      return -1;
    return 0;
  }

  /// Assignment operator, where the rhs is another ext<T>.
  /// Not thread safe, but contrived to ensure this object doesn't end up in an
  /// invalid state, even if there are concurrent modifications going on.
  ext<T> &operator=(ext<T> rhs) {
    char rhs_state = rhs.state;

    // If going to the VALUE state, write value first, state second.
    if (rhs_state == VALUE) {
      t = rhs.t;
    }

    // Otherwise (if going to TOP or BOTTOM), just write the state.
    state = rhs_state;

    return *this;
  }

  /// Assignment operator, where the rhs is a T.
  /// Will put this ext<T> in the VALUE state with the value T.
  /// Not thread safe, but contrived to ensure this object doesn't end up in an
  /// invalid state, even if there are concurrent modifications going on.
  ext<T> &operator=(T rhs) {
    // Write value first, state second.
    t = rhs.t;
    state = VALUE;
    return *this;
  }

  // Print as a human-readable string.
  std::string to_string() const {
    if (state == TOP) {
      return "TOP";
    }
    if (state == BOTTOM) {
      return "BOTTOM";
    }
    return std::to_string(t);
  }
};

// Comparators for ext<T> against ext<T>.
template <typename T> inline bool operator>(const ext<T> &a, const ext<T> &b) {
  return a.compare_to(b) > 0;
}
template <typename T> inline bool operator<(const ext<T> &a, const ext<T> &b) {
  return a.compare_to(b) < 0;
}
template <typename T> inline bool operator>=(const ext<T> &a, const ext<T> &b) {
  return a.compare_to(b) >= 0;
}
template <typename T> inline bool operator<=(const ext<T> &a, const ext<T> &b) {
  return a.compare_to(b) <= 0;
}
template <typename T> inline bool operator==(const ext<T> &a, const ext<T> &b) {
  return a.compare_to(b) == 0;
}
template <typename T> inline bool operator!=(const ext<T> &a, const ext<T> &b) {
  return a.compare_to(b) != 0;
}

// Comparators for ext<T> against T.
template <typename T> inline bool operator>(const ext<T> &a, const T b) {
  return a.compare_to(b) > 0;
}
template <typename T> inline bool operator<(const ext<T> &a, const T b) {
  return a.compare_to(b) < 0;
}
template <typename T> inline bool operator>=(const ext<T> &a, const T b) {
  return a.compare_to(b) >= 0;
}
template <typename T> inline bool operator<=(const ext<T> &a, const T b) {
  return a.compare_to(b) <= 0;
}
template <typename T> inline bool operator==(const ext<T> &a, const T b) {
  return a.compare_to(b) == 0;
}
template <typename T> inline bool operator!=(const ext<T> &a, const T b) {
  return a.compare_to(b) != 0;
}

// Comparators for T against ext<T>.
template <typename T> inline bool operator>(const T a, const ext<T> &b) {
  return b.compare_to(a) < 0;
}
template <typename T> inline bool operator<(const T a, const ext<T> &b) {
  return b.compare_to(a) > 0;
}
template <typename T> inline bool operator>=(const T a, const ext<T> &b) {
  return b.compare_to(a) <= 0;
}
template <typename T> inline bool operator<=(const T a, const ext<T> &b) {
  return b.compare_to(a) >= 0;
}
template <typename T> inline bool operator==(const T a, const ext<T> &b) {
  return b.compare_to(a) == 0;
}
template <typename T> inline bool operator!=(const T a, const ext<T> &b) {
  return b.compare_to(a) != 0;
}

// Implement << stream operator for couts.
template <typename T>
std::ostream &operator<<(std::ostream &o, const ext<T> &t) {
  return o << t.to_string();
}
