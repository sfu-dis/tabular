#pragma once

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <string>
#include <type_traits>

template <size_t I> class __attribute__((__packed__)) sized_uint {
  static_assert(I <= 8);
  std::array<uint8_t, I> data = {0};

  constexpr auto get() const {
    if constexpr (I == 1) {
      uint8_t x = 0;
      std::memcpy(&x, data.data(), I);
      return x;
    } else if constexpr (I == 2) {
      uint16_t x = 0;
      std::memcpy(&x, data.data(), I);
      return x;
    } else if constexpr (I <= 4) {
      uint32_t x = 0;
      std::memcpy(&x, data.data(), I);
      return x;
    } else {
      uint64_t x = 0;
      std::memcpy(&x, data.data(), I);
      return x;
    }
  }

public:
  constexpr sized_uint(uint64_t e) { std::memcpy(data.data(), &e, I); }
  template <size_t J> constexpr sized_uint(const sized_uint<J> &e) {
    auto el = e.get();
    std::memcpy(data.data(), &el, I);
  }
  constexpr sized_uint() { data.fill(0); }
  operator auto() const { return get(); }
  static std::string name() {
    return std::string("sized_uint<") + std::to_string(I) + ">";
  }
};
