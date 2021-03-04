// Copied from GCC libstdc++v3
// Ref: https://github.com/gcc-mirror/gcc/commit/3dfd5493cf9798d46dd24ac32becc54d5074271e
//
// Construct from days since 1970/01/01.
// Proposition 6.3 of Neri and Schneider,
// "Euclidean Affine Functions and Applications to Calendar Algorithms".
// https://arxiv.org/abs/2102.06959

#include <cstdio>

static void
write_day_since_epoch_as_yyyy_mm_dd (int32_t days, char* out)
{
    constexpr auto __z2    = static_cast<uint32_t>(-1468000);
    constexpr auto __r2_e3 = static_cast<uint32_t>(536895458);

    const auto __r0 = days + __r2_e3;

    const auto __n1 = 4 * __r0 + 3;
    const auto __q1 = __n1 / 146097;
    const auto __r1 = __n1 % 146097 / 4;

    constexpr auto __p32 = static_cast<uint64_t>(1) << 32;
    const auto __n2 = 4 * __r1 + 3;
    const auto __u2 = static_cast<uint64_t>(2939745) * __n2;
    const auto __q2 = static_cast<uint32_t>(__u2 / __p32);
    const auto __r2 = static_cast<uint32_t>(__u2 % __p32) / 2939745 / 4;

    constexpr auto __p16 = static_cast<uint32_t>(1) << 16;
    const auto __n3 = 2141 * __r2 + 197913;
    const auto __q3 = __n3 / __p16;
    const auto __r3 = __n3 % __p16 / 2141;

    const auto __y0 = 100 * __q1 + __q2;
    const auto __m0 = __q3;
    const auto __d0 = __r3;

    const auto __j  = __r2 >= 306;
    const auto __y1 = __y0 + __j;
    const auto __m1 = __j ? __m0 - 12 : __m0;
    const auto __d1 = __d0 + 1;

    std::snprintf(out, 11, "%04d-%02d-%02d", __y1 + __z2, __m1, __d1);
}
