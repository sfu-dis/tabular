#pragma once
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <tuple>

template <typename... Ts> class AOS {
public:
  using T = std::tuple<Ts...>;
private:
  static constexpr std::size_t num_types = sizeof...(Ts);
  static constexpr size_t element_size = sizeof(T);

  static constexpr std::array<std::size_t, num_types> alignments = {
      std::alignment_of_v<Ts>...};

  template <int I> using NthType = typename std::tuple_element<I, T>::type;

  static constexpr std::array<std::size_t, num_types> sizes = {sizeof(Ts)...};
  size_t num_spots;
  T *base_array;

  template <size_t... Is>
  static auto get_impl_static(T *base_array, size_t i,
                [[maybe_unused]] std::integer_sequence<size_t, Is...> int_seq) {
    return std::forward_as_tuple(std::get<Is>(base_array[i])...);
  }

  template <size_t... Is>
  static auto get_impl_static(T const *base_array, size_t i,
                [[maybe_unused]] std::integer_sequence<size_t, Is...> int_seq) {
    return std::forward_as_tuple(std::get<Is>(base_array[i])...);
  }
  template <size_t... Is>
  auto get_impl(size_t i,
                [[maybe_unused]] std::integer_sequence<size_t, Is...> int_seq) {
    return std::forward_as_tuple(std::get<Is>(base_array[i])...);
  }

public:
  static constexpr size_t get_size_static(size_t num_spots) {
    return num_spots * element_size;
  }
  size_t get_size() { return num_spots * element_size; }

  AOS(size_t n) : num_spots(n) {
    // set the total array to be 64 byte alignmed

    uintptr_t length_to_allocate = get_size();
    base_array = static_cast<T *>(std::malloc(length_to_allocate));
    std::cout << "allocated size " << length_to_allocate << "\n";
  }
  ~AOS() { free(base_array); }
  void zero() { std::memset(base_array, 0, get_size()); }

  template <size_t... Is>
  static auto get_static(void const * base_array, size_t num_spots, size_t i) {
    if constexpr (sizeof...(Is) > 0) {
      return get_impl_static<Is...>((T const *)base_array, i, {});
    } else {
      return get_impl_static((T const *)base_array, i,
                             std::make_index_sequence<num_types>{});
    }
  }
  template <size_t... Is>
  static auto get_static(void* base_array, size_t num_spots, size_t i) {
    if constexpr (sizeof...(Is) > 0) {
      return get_impl_static<Is...>((T *)base_array, i, {});
    } else {
      return get_impl_static((T *)base_array, i,
                             std::make_index_sequence<num_types>{});
    }
  }

  template <size_t... Is> auto get(size_t i) {
    if constexpr (sizeof...(Is) > 0) {
      return get_impl<Is...>(i, {});
    } else {
      return get_impl(i, std::make_index_sequence<num_types>{});
    }
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

  template <size_t... Is, class F>
  void map_range(F f, size_t start = 0,
                 size_t end = std::numeric_limits<size_t>::max()) {
    if (end == std::numeric_limits<size_t>::max()) {
      end = num_spots;
    }
    for (size_t i = start; i < end; i++) {
      std::apply(f, get<Is...>(i));
    }
  }

  template <size_t... Is, class F>
  void map_range_with_index(F f, size_t start = 0,
                            size_t end = std::numeric_limits<size_t>::max()) {
    if (end == std::numeric_limits<size_t>::max()) {
      end = num_spots;
    }
    for (size_t i = start; i < end; i++) {
      std::apply(f, std::tuple_cat(std::make_tuple(i), get<Is...>(i)));
    }
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
      std::apply(f, std::tuple_cat(std::make_tuple(i), get_static<Is...>(base_array, num_spots, i)));
    }
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
  static void
  map_range_static(void *base_array, size_t num_spots, F f,
                              size_t start = 0,
                              size_t end = std::numeric_limits<size_t>::max()) {
    if (end == std::numeric_limits<size_t>::max()) {
      end = num_spots;
    }
    for (size_t i = start; i < end; i++) {
      std::apply(f, get_static<Is...>(base_array, num_spots, i));
    }
  }
  
  class Iterator {

  public:
    using difference_type = uint64_t;
    using value_type = T;
    using iterator_category = std::random_access_iterator_tag;
    using reference = T&;

    Iterator(void *array, uint64_t spots, uint64_t index)
        : _array((T*)array), _spots(spots), _index(index) {}

    inline Iterator &operator+=(difference_type rhs) {
      _index += rhs;
      return *this;
    }
    inline Iterator &operator-=(difference_type rhs) {
      _index -= rhs;
      return *this;
    }

    inline reference operator*() {
      return _array[_index];
    }
    inline reference operator[](difference_type rhs) {
      return _array[_index + rhs];
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
    T* _array;
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
