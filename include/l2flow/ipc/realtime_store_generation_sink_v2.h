#pragma once

#include <memory>

namespace l2flow::market {

class IntradayInstrumentStoreGenerationV1;

}  // namespace l2flow::market

namespace l2flow::ipc {

// Required publication boundary for an application that exports immutable
// Store generations through Wire V2. The Pipeline invokes this synchronously
// after History has release-published the exact Store generation and before
// Factor calculation begins. A true return means that this exact generation is
// already discoverable by the sink's readers. Returning false fails the
// Pipeline closed.
//
// The shared_ptr is the exact History-owned generation handle and permits a
// sink to retain its immutable Store arena while publishing reader-owned
// resources. Implementations must not wait for Journal durability.
class RealtimeStoreGenerationSinkV2 {
public:
    virtual ~RealtimeStoreGenerationSinkV2() = default;

    [[nodiscard]] virtual bool PublishStoreGeneration(
        const std::shared_ptr<
            const l2flow::market::IntradayInstrumentStoreGenerationV1>&
            generation) noexcept = 0;
};

}  // namespace l2flow::ipc
