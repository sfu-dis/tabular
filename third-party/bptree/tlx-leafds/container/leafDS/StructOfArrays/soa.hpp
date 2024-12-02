#pragma once

#include "multipointer.hpp"
#include <array>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <iterator>
#include <limits>
#include <tuple>
#include <utility>
#include <vector>

template <typename... Ts> class SOA {
public:
  using T = std::tuple<Ts...>;

private:
  static constexpr std::size_t num_types = sizeof...(Ts);
  static constexpr std::array<std::size_t, num_types> alignments = {
      std::alignment_of_v<Ts>...};

  template <int I> using NthType = typename std::tuple_element<I, T>::type;

  static constexpr std::array<std::size_t, num_types> sizes = {sizeof(Ts)...};

  size_t num_spots;
  void *base_array;

  template <size_t I>
  static NthType<I> *get_starting_pointer_to_type_static(void const *base_array,
                                                         size_t num_spots) {
    static_assert(I < num_types);
    uintptr_t offset = 0;
    for (size_t i = 0; i < I; i++) {
      offset += num_spots * sizes[i];
      if (offset % alignments[i + 1] != 0) {
        offset += alignments[i + 1] - (offset % alignments[i + 1]);
      }
    }
    return (NthType<I> *)((char *)base_array + offset);
  }

  template <size_t I> NthType<I> *get_starting_pointer_to_type() const {
    return get_starting_pointer_to_type_static<I>(base_array, num_spots);
  }

  template <std::size_t... Is>
  static void *resize_impl_static(
      void *old_base_array, size_t old_num_spots, size_t new_num_spots,
      [[maybe_unused]] std::integer_sequence<size_t, Is...> int_seq) {

    uintptr_t length_to_allocate = get_size_static(new_num_spots);
    void *new_base_array = std::malloc(length_to_allocate);

    size_t end = std::min(old_num_spots, new_num_spots);
    for (size_t i = 0; i < end; i++) {
      get_static<Is...>(new_base_array, new_num_spots, i) =
          get_static<Is...>(old_base_array, old_num_spots, i);
    }
    const T zero;
    for (size_t i = end; i < new_num_spots; i++) {
      get_static<Is...>(new_base_array, new_num_spots, i) = zero;
    }

    return new_base_array;
  }

  template <size_t I>
  static bool print_field_static(void *base_array, size_t num_spots) {
    for (size_t i = 0; i < num_spots; i++) {
      std::cout << std::get<0>(get_static<I>(base_array, num_spots, i)) << ", ";
    }
    std::cout << "\n";
    return true;
  }

  template <size_t I> bool print_field() const {
    return print_field_static<I>(base_array, num_spots);
  }

  template <size_t... Is>
  static void print_soa_impl_static(
      void *base_array, size_t num_spots,
      [[maybe_unused]] std::integer_sequence<size_t, Is...> int_seq) {
    auto x = {print_field_static<Is>(base_array, num_spots)...};
    (void)x;
  }

  template <size_t... Is>
  void print_soa_impl(
      [[maybe_unused]] std::integer_sequence<size_t, Is...> int_seq) const {
    print_soa_impl_static<Is...>(base_array, num_spots, int_seq);
  }

  template <size_t... Is>
  static auto get_impl_static(
      void const *base_array, size_t num_spots, size_t i,
      [[maybe_unused]] std::integer_sequence<size_t, Is...> int_seq) {
    return std::forward_as_tuple(
        get_starting_pointer_to_type_static<Is>(base_array, num_spots)[i]...);
  }

  template <size_t... Is>
  static MultiPointer<NthType<Is>...> get_ptr_impl_static(
      void const *base_array, size_t num_spots, size_t i,
      [[maybe_unused]] std::integer_sequence<size_t, Is...> int_seq) {
    return MultiPointer((
        get_starting_pointer_to_type_static<Is>(base_array, num_spots) + i)...);
  }

  template <size_t... Is>
  auto get_impl(
      size_t i,
      [[maybe_unused]] std::integer_sequence<size_t, Is...> int_seq) const {
    return get_impl_static<Is...>(base_array, num_spots, i, int_seq);
  }

public:
  static constexpr size_t get_size_static(size_t num_spots) {
    uintptr_t length_to_allocate = 0;
    for (size_t i = 0; i < num_types; i++) {
      length_to_allocate += num_spots * sizes[i];
      if (i < num_types - 1) {
        // get aligned for the next type
        if (length_to_allocate % alignments[i + 1] != 0) {
          length_to_allocate +=
              alignments[i + 1] - (length_to_allocate % alignments[i + 1]);
        }
      }
    }
    return length_to_allocate;
  }
  size_t get_size() const { return get_size_static(num_spots); }

  size_t size() const { return num_spots; }

  SOA(size_t n) : num_spots(n) {
    // set the total array to be 64 byte alignmed

    uintptr_t length_to_allocate = get_size();
    base_array = std::malloc(length_to_allocate);
  }

  SOA(void *array, size_t n) : num_spots(n), base_array(array) {}

  ~SOA() { free(base_array); }

  static void zero_static(void *base_array, size_t num_spots) {
    std::memset(base_array, 0, get_size_static(num_spots));
  }
  void zero() const { zero_static(base_array, num_spots); }

  template <size_t... Is>
  static auto get_static(void const *base_array, size_t num_spots, size_t i) {
    if constexpr (sizeof...(Is) > 0) {
      return get_impl_static<Is...>(base_array, num_spots, i, {});
    } else {
      return get_impl_static(base_array, num_spots, i,
                             std::make_index_sequence<num_types>{});
    }
  }

  template <size_t... Is>
  static auto get_static_ptr(void const *base_array, size_t num_spots,
                             size_t i) {
    if constexpr (sizeof...(Is) > 0) {
      if constexpr (sizeof...(Is) == 1) {
        return get_ptr_impl_static<Is...>(base_array, num_spots, i, {})
            .get_pointer();
      } else {
        return get_ptr_impl_static<Is...>(base_array, num_spots, i, {});
      }
    } else {
      return get_ptr_impl_static(base_array, num_spots, i,
                                 std::make_index_sequence<num_types>{});
    }
  }

  template <size_t... Is> auto get(size_t i) const {
    return get_static<Is...>(base_array, num_spots, i);
  }

  template <size_t... Is> auto get_ptr(size_t i) const {
    return get_static_ptr<Is...>(base_array, num_spots, i);
  }

  static void print_type_details() {
    std::cout << "num types are " << num_types << "\n";
    std::cout << "their alignments are ";
    for (const auto e : alignments) {
      std::cout << e << ", ";
    }
    std::cout << "\n";
    std::cout << "their sizes are ";
    for (const auto e : sizes) {
      std::cout << e << ", ";
    }
    std::cout << "\n";
  }

  template <size_t... Is>
  static void print_soa_static(void *base_array, size_t num_spots) {
    if constexpr (sizeof...(Is) > 0) {
      print_soa_impl_static<Is...>(base_array, num_spots, {});
    } else {
      print_soa_impl_static(base_array, num_spots,
                            std::make_index_sequence<num_types>{});
    }
  }
  template <size_t... Is> void print_soa() const {
    print_soa_static<Is...>(base_array, num_spots);
  }

  template <size_t... Is, class F>
  static void
  map_range_static(void const *base_array, size_t num_spots, F f,
                   size_t start = 0,
                   size_t end = std::numeric_limits<size_t>::max()) {
    if (end == std::numeric_limits<size_t>::max()) {
      end = num_spots;
    }
    for (size_t i = start; i < end; i++) {
      std::apply(f, get_static<Is...>(base_array, num_spots, i));
    }
  }

  template <size_t... Is, class F>
  void map_range(F f, size_t start = 0,
                 size_t end = std::numeric_limits<size_t>::max()) const {
    map_range_static<Is...>(base_array, num_spots, f, start, end);
  }

  template <size_t... Is, class F>
  static void
  map_range_with_index_static(void *base_array, size_t num_spots, F f,
                              size_t start = 0,
                              size_t end = std::numeric_limits<size_t>::max()) {
    if (end == std::numeric_limits<size_t>::max()) {
      end = num_spots;
    }
    for (size_t i = start; i < end; i++) {
      std::apply(f,
                 std::tuple_cat(std::make_tuple(i),
                                get_static<Is...>(base_array, num_spots, i)));
    }
  }

  template <size_t... Is, class F>
  void
  map_range_with_index(F f, size_t start = 0,
                       size_t end = std::numeric_limits<size_t>::max()) const {
    map_range_with_index_static<Is...>(base_array, num_spots, f, start, end);
  }

  template <size_t... Is>
  static void print_aos_static(void *base_array, size_t num_spots) {
    map_range_static<Is...>(base_array, num_spots, [](auto... args) {
      ((std::cout << args << ","), ...) << "\n";
    });
  }

  template <size_t... Is> void print_aos() const {
    print_aos_static(base_array, num_spots);
  }

  template <size_t... Is>
  static void print_aos_with_index_static(void *base_array, size_t num_spots) {
    map_range_with_index_static<Is...>(base_array, num_spots, [](auto... args) {
      ((std::cout << args << ","), ...) << "\n";
    });
  }

  template <size_t... Is> void print_aos_with_index() const {
    print_aos_with_index_static<Is...>(base_array, num_spots);
  }

  static void *resize_static(void *old_base_array, size_t old_num_spots,
                             size_t new_num_spots) {
    return resize_impl_static(old_base_array, old_num_spots, new_num_spots,
                              std::make_index_sequence<num_types>{});
  }

  SOA resize(size_t new_num_spots) const {
    return SOA(resize_static(base_array, num_spots, new_num_spots),
               new_num_spots);
  }
  template <size_t... Is>
  static void *pull_types_static(void *base_array, size_t num_spots) {

    uintptr_t length_to_allocate =
        SOA<NthType<Is>...>::get_size_static(num_spots);
    void *new_base_array = std::malloc(length_to_allocate);

    for (size_t i = 0; i < num_spots; i++) {
      SOA<NthType<Is>...>::get_static(new_base_array, num_spots, i) =
          get_static<Is...>(base_array, num_spots, i);
    }
    return new_base_array;
  }
  template <size_t... Is> SOA<NthType<Is>...> pull_types() const {
    SOA<NthType<Is>...> soa(pull_types_static<Is...>(base_array, num_spots),
                            num_spots);
    return soa;
  }

  class Iterator {

  public:
    using difference_type = uint64_t;
    using value_type = T;
    using iterator_category = std::random_access_iterator_tag;

    struct reference {
      void const *_array;
      size_t _spots;
      uint64_t _index;

      reference &operator=(reference &&v) {
        get_static(_array, _spots, _index) =
            get_static(v._array, v._spots, v._index);
        return *this;
      }
      reference &operator=(const value_type &v) {
        get_static(_array, _spots, _index) = v;
        return *this;
      }

      operator value_type() const { return get_static(_array, _spots, _index); }

      template <size_t... Is> auto get() {
        return get_static<Is...>(_array, _spots, _index);
      }

      friend void swap(const reference &l, const reference &r) {
        T temp = get_static(l._array, l._spots, l._index);
        get_static(l._array, l._spots, l._index) =
            get_static(r._array, r._spots, r._index);
        get_static(r._array, r._spots, r._index) = temp;
      }

      auto operator<(const reference &b) const {
        return value_type() < value_type(b);
      }
      auto operator<(const T &b) const { return value_type() < b; }
      friend auto operator<(const T &b, const reference &a) {
        return b < value_type(a);
      }

      reference(void const *array, uint64_t spots, uint64_t index)
          : _array(array), _spots(spots), _index(index) {}
    };

    Iterator(void const *array, uint64_t spots, uint64_t index)
        : _array(array), _spots(spots), _index(index) {}

    inline Iterator &operator+=(difference_type rhs) {
      _index += rhs;
      return *this;
    }
    inline Iterator &operator-=(difference_type rhs) {
      _index -= rhs;
      return *this;
    }

    inline reference operator*() const {
      return reference(_array, _spots, _index);
    }
    inline reference operator[](difference_type rhs) const {
      return reference(_array, _spots, _index + rhs);
    }

    inline Iterator &operator++() {
      ++_index;
      return *this;
    }
    inline Iterator &operator--() {
      --_index;
      return *this;
    }
    inline Iterator operator++(int) {
      Iterator tmp(*this);
      ++_index;
      return tmp;
    }
    inline Iterator operator--(int) {
      Iterator tmp(*this);
      --_index;
      return tmp;
    }

    inline difference_type operator-(const Iterator &rhs) const {
      return _index - rhs._index;
    }
    inline Iterator operator+(difference_type rhs) const {
      return Iterator(_array, _spots, _index + rhs);
    }
    inline Iterator operator-(difference_type rhs) const {
      return Iterator(_array, _spots, _index - rhs);
    }
    friend inline Iterator operator+(difference_type lhs, const Iterator &rhs) {
      return Iterator(rhs._array, rhs._spots, lhs + rhs._index);
    }
    friend inline Iterator operator-(difference_type lhs, const Iterator &rhs) {
      return Iterator(rhs._array, rhs._spots, lhs - rhs._index);
    }

    inline bool operator==(const Iterator &rhs) const {
      return _index == rhs._index;
    }
    inline bool operator!=(const Iterator &rhs) const {
      return _index != rhs._index;
    }
    inline bool operator>(const Iterator &rhs) const {
      return _index > rhs._index;
    }
    inline bool operator<(const Iterator &rhs) const {
      return _index < rhs._index;
    }
    inline bool operator>=(const Iterator &rhs) const {
      return _index >= rhs._index;
    }
    inline bool operator<=(const Iterator &rhs) const {
      return _index <= rhs._index;
    }

  private:
    void const *_array;
    size_t _spots;
    uint64_t _index = 0;
  };

  static Iterator begin_static(void const *base_array, size_t num_spots) {
    return Iterator(base_array, num_spots, 0);
  }
  auto begin() const { return begin_static(base_array, num_spots); }

  static Iterator end_static(void const *base_array, size_t num_spots) {
    return Iterator(base_array, num_spots, num_spots);
  }
  auto end() const { return end_static(base_array, num_spots); }
};
