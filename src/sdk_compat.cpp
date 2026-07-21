#include "mdl_api_types.h"

#if !defined(L2MOCK_BUILDING_STANDALONE_SDK_COMPAT)
#error "sdk_compat.cpp is only for the L2Flow::l2mock_standalone target"
#endif

#include <algorithm>
#include <chrono>
#include <cstring>
#include <ctime>

#if defined(_MSC_VER)
#include <intrin.h>
#endif

namespace datayes {

// These symbols intentionally have the same C linkage as the SDK helpers.
// Keep this translation unit isolated in the explicit standalone target: if
// its definitions enter an executable that also loads libmdl_api.so, ELF
// executable symbol preemption can redirect the real feed through these mock
// implementations.
extern "C" int DTAPIDLLCALL DllInterlockedIncrement(volatile int* value) {
#if defined(_MSC_VER)
    return static_cast<int>(_InterlockedIncrement(
        reinterpret_cast<volatile long*>(value)));
#else
    return __atomic_add_fetch(value, 1, __ATOMIC_ACQ_REL);
#endif
}

extern "C" int DTAPIDLLCALL DllInterlockedDecrement(volatile int* value) {
#if defined(_MSC_VER)
    return static_cast<int>(_InterlockedDecrement(
        reinterpret_cast<volatile long*>(value)));
#else
    return __atomic_sub_fetch(value, 1, __ATOMIC_ACQ_REL);
#endif
}

namespace mdl {
namespace {

uint32_t CopyEncodingBytes(const char* input,
                           uint32_t input_size,
                           char* output,
                           uint32_t output_size) {
    if (input != nullptr && output != nullptr && output_size != 0) {
        std::memcpy(
            output, input, std::min(input_size, output_size));
    }
    // Linux DataYes strings used by the mock are ASCII/UTF-8 compatible.  The
    // return contract mirrors the SDK conversion helpers: report required
    // bytes even when the caller-provided output is smaller.
    return input_size;
}

} // namespace

extern "C" uint32_t DTAPIDLLCALL DllConvertUTF8ToAnsi(
    const char* utf8,
    uint32_t utf8_size,
    char* ansi,
    uint32_t ansi_size) {
    return CopyEncodingBytes(utf8, utf8_size, ansi, ansi_size);
}

extern "C" uint32_t DTAPIDLLCALL DllConvertAnsiToUTF8(
    const char* ansi,
    uint32_t ansi_size,
    char* utf8,
    uint32_t utf8_size) {
    return CopyEncodingBytes(ansi, ansi_size, utf8, utf8_size);
}

} // namespace mdl
} // namespace datayes
