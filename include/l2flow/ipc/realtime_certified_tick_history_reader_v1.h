#pragma once

#include "l2flow/ipc/certified_tick_journal_v1.h"
#include "l2flow/ipc/realtime_certified_reader_v1.h"

#include <cstdint>
#include <memory>
#include <span>

namespace l2flow::ipc {

// Production UDS adapter for the independent append-only CERTIFIED Tick
// journal. The low-level journal reader owns the read-only mmap; this wrapper
// performs same-UID control authentication and descriptor/session binding.
class RealtimeCertifiedTickHistoryReaderV1 final {
public:
    RealtimeCertifiedTickHistoryReaderV1(
        const RealtimeCertifiedTickHistoryReaderV1&) = delete;
    RealtimeCertifiedTickHistoryReaderV1& operator=(
        const RealtimeCertifiedTickHistoryReaderV1&) = delete;
    RealtimeCertifiedTickHistoryReaderV1(
        RealtimeCertifiedTickHistoryReaderV1&&) = delete;
    RealtimeCertifiedTickHistoryReaderV1& operator=(
        RealtimeCertifiedTickHistoryReaderV1&&) = delete;
    ~RealtimeCertifiedTickHistoryReaderV1();

    [[nodiscard]] static RealtimeCertifiedReaderOpenErrorV1 Open(
        RealtimeCertifiedReaderOpenOptionsV1 options,
        std::unique_ptr<RealtimeCertifiedTickHistoryReaderV1>* output,
        int* system_error_number = nullptr) noexcept;

    // Test seam. descriptor is borrowed. The exact capacity/mapping values
    // normally come from the authenticated kGetTickHistory response.
    [[nodiscard]] static RealtimeCertifiedReaderOpenErrorV1
    OpenDescriptorForTest(
        int descriptor,
        const RealtimeCertifiedExpectedSessionV1& expected_session,
        std::uint64_t tick_capacity,
        std::uint64_t total_mapping_bytes,
        std::unique_ptr<RealtimeCertifiedTickHistoryReaderV1>* output,
        int* system_error_number = nullptr) noexcept;

    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadStatus(
        CertifiedTickJournalStatusV1* output) const noexcept;
    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadOne(
        std::uint64_t canonical_apply_sequence,
        RealtimeCertifiedTickEnvelopeV1* output) const noexcept;
    [[nodiscard]] CertifiedTickJournalReadResultV1 Read(
        std::uint64_t first_canonical_apply_sequence,
        std::span<RealtimeCertifiedTickEnvelopeV1> output,
        CertifiedTickJournalReadBatchV1* result) const noexcept;
    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadSlots(
        std::uint64_t first_canonical_apply_sequence,
        std::span<RealtimeCertifiedTickSlotV1> output,
        CertifiedTickJournalReadBatchV1* result) const noexcept;
    [[nodiscard]] CertifiedTickJournalReadResultV1 ReadSlotBytes(
        std::uint64_t first_canonical_apply_sequence,
        std::span<std::byte> output,
        CertifiedTickJournalReadBatchV1* result) const noexcept;

    [[nodiscard]] const CertifiedTickJournalSessionV1& session()
        const noexcept;

private:
    explicit RealtimeCertifiedTickHistoryReaderV1(
        std::unique_ptr<CertifiedTickJournalReaderV1> journal)
        noexcept;
    std::unique_ptr<CertifiedTickJournalReaderV1> journal_;
};

}  // namespace l2flow::ipc
