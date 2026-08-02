#include "l2flow/ipc/realtime_shm_reader_c_v2.h"
#include "l2flow/ipc/realtime_wire_v2.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <limits>
#include <span>
#include <string_view>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

namespace {

namespace ipc = l2flow::ipc;

static_assert(sizeof(l2flow_shm_session_info_v2) == 240U);
static_assert(sizeof(l2flow_selection_envelope_v2) == 144U);
static_assert(sizeof(l2flow_shm_health_v2) == 32U);
static_assert(sizeof(l2flow_kline_coverage_info_v2) == 32U);

constexpr std::uint32_t kCapacity = 4U;
constexpr std::uint32_t kTradeDate = 20260729U;
constexpr std::uint64_t kWindowDurationNs = 60'000'000'000ULL;
// 2026-07-29 10:02:17 at the fixed UTC+8 market-calendar offset.
constexpr std::uint64_t kProcessStartCoverageUnixNs =
    1'785'290'537'000'000'000ULL;
constexpr std::size_t kKeyArenaBytes = 256U;
constexpr std::size_t kRingCapacity = 4U;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

template <typename Integer, typename Value>
void AtomicStore(Integer& target, Value value) noexcept {
    std::atomic_ref<Integer>(target).store(
        static_cast<Integer>(value), std::memory_order_release);
}

std::uint64_t AlignUp(
    std::uint64_t value,
    std::uint64_t alignment) noexcept {
    const std::uint64_t remainder = value % alignment;
    return remainder == 0U ? value : value + alignment - remainder;
}

class UniqueFd final {
public:
    explicit UniqueFd(int fd = -1) noexcept : fd_(fd) {}
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    ~UniqueFd() { Reset(); }

    [[nodiscard]] int get() const noexcept { return fd_; }

    void Reset(int fd = -1) noexcept {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        fd_ = fd;
    }

private:
    int fd_ = -1;
};

class ReaderHandle final {
public:
    ReaderHandle() = default;
    ReaderHandle(const ReaderHandle&) = delete;
    ReaderHandle& operator=(const ReaderHandle&) = delete;
    ~ReaderHandle() { Reset(); }

    [[nodiscard]] l2flow_shm_reader_v2* get() const noexcept {
        return reader_;
    }

    [[nodiscard]] l2flow_shm_reader_v2** output() noexcept {
        Reset();
        return &reader_;
    }

    void Reset() noexcept {
        l2flow_shm_reader_close_v2(reader_);
        reader_ = nullptr;
    }

private:
    l2flow_shm_reader_v2* reader_ = nullptr;
};

struct Layout final {
    std::array<ipc::RealtimeWireRegionDescriptorV2, 9U> regions{};
    std::uint64_t mapping_bytes = 0U;
};

void SetRegion(
    ipc::RealtimeWireRegionDescriptorV2* region,
    ipc::RealtimeRegionKindV2 kind,
    std::uint64_t offset,
    std::uint64_t stride,
    std::uint64_t count,
    std::uint32_t alignment) {
    *region = {};
    region->kind = static_cast<std::uint32_t>(kind);
    region->schema_major = ipc::kRealtimeWireMajorV2;
    region->schema_minor = ipc::kRealtimeWireMinorV2;
    region->offset = offset;
    region->length = stride * count;
    region->element_stride = stride;
    region->element_count = count;
    region->capacity = count;
    region->alignment = alignment;
}

Layout MakeLayout() {
    Layout result{};
    std::uint64_t cursor = sizeof(ipc::RealtimeWireHeaderV2);
    const auto add = [&result, &cursor](
                         std::size_t index,
                         ipc::RealtimeRegionKindV2 kind,
                         std::uint64_t stride,
                         std::uint64_t count,
                         std::uint32_t alignment) {
        cursor = AlignUp(cursor, alignment);
        SetRegion(
            &result.regions[index],
            kind,
            cursor,
            stride,
            count,
            alignment);
        cursor += stride * count;
    };
    add(
        0U,
        ipc::RealtimeRegionKindV2::kInstrumentRows,
        sizeof(ipc::RealtimeWireInstrumentV2),
        kCapacity,
        alignof(ipc::RealtimeWireInstrumentV2));
    add(
        1U,
        ipc::RealtimeRegionKindV2::kInstrumentKeyArena,
        1U,
        kKeyArenaBytes,
        1U);
    add(
        2U,
        ipc::RealtimeRegionKindV2::kKLineWindows,
        sizeof(ipc::RealtimeWireKLineWindowV2),
        1U,
        alignof(ipc::RealtimeWireKLineWindowV2));
    add(
        3U,
        ipc::RealtimeRegionKindV2::kLatestSnapshots,
        sizeof(ipc::RealtimeWireSnapshotSlotV2),
        kCapacity,
        alignof(ipc::RealtimeWireSnapshotSlotV2));
    add(
        4U,
        ipc::RealtimeRegionKindV2::kLatestTicks,
        sizeof(ipc::RealtimeWireTickSlotV2),
        kCapacity,
        alignof(ipc::RealtimeWireTickSlotV2));
    add(
        5U,
        ipc::RealtimeRegionKindV2::kLatestKLines,
        sizeof(ipc::RealtimeWireKLineSlotV2),
        2U * kCapacity,
        alignof(ipc::RealtimeWireKLineSlotV2));
    add(
        6U,
        ipc::RealtimeRegionKindV2::kTickRingSlots,
        sizeof(ipc::RealtimeWireTickSlotV2),
        kRingCapacity,
        alignof(ipc::RealtimeWireTickSlotV2));
    result.mapping_bytes = cursor;
    SetRegion(
        &result.regions[7U],
        ipc::RealtimeRegionKindV2::kReserved8,
        cursor,
        0U,
        0U,
        1U);
    SetRegion(
        &result.regions[8U],
        ipc::RealtimeRegionKindV2::kReserved9,
        cursor,
        0U,
        0U,
        1U);
    return result;
}

template <typename Slot, typename Payload>
void PublishSlot(
    Slot* slot,
    const Payload& payload,
    std::uint64_t publish_tag = 2U) {
    std::memcpy(
        slot->payload_words.data(), &payload, sizeof(payload));
    AtomicStore(slot->publish_tag, publish_tag);
}

class MappedFixture final {
public:
    MappedFixture() = default;
    MappedFixture(const MappedFixture&) = delete;
    MappedFixture& operator=(const MappedFixture&) = delete;

    ~MappedFixture() {
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(mapping_, mapping_bytes_));
        }
    }

    [[nodiscard]] bool Create(bool add_seals = true) {
        const Layout layout = MakeLayout();
        rw_fd_.Reset(::memfd_create(
            "l2flow-reader-v2-test",
            MFD_CLOEXEC | MFD_ALLOW_SEALING));
        if (rw_fd_.get() < 0 ||
            ::ftruncate(
                rw_fd_.get(),
                static_cast<off_t>(layout.mapping_bytes)) != 0) {
            return false;
        }
        mapping_bytes_ =
            static_cast<std::size_t>(layout.mapping_bytes);
        mapping_ = ::mmap(
            nullptr,
            mapping_bytes_,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            rw_fd_.get(),
            0);
        if (mapping_ == MAP_FAILED) {
            return false;
        }
        std::memset(mapping_, 0, mapping_bytes_);
        header_ =
            static_cast<ipc::RealtimeWireHeaderV2*>(mapping_);
        *header_ = {};
        header_->magic = ipc::kRealtimeShmMagicV2;
        header_->abi_major = ipc::kRealtimeWireMajorV2;
        header_->abi_minor = ipc::kRealtimeWireMinorV2;
        header_->header_bytes =
            sizeof(ipc::RealtimeWireHeaderV2);
        header_->endian_marker =
            ipc::kRealtimeLittleEndianMarkerV2;
        header_->total_mapping_bytes = layout.mapping_bytes;
        for (std::size_t index = 0U;
             index < header_->run_id.size();
             ++index) {
            header_->run_id[index] =
                static_cast<std::uint8_t>(index + 1U);
        }
        header_->session_epoch = 17U;
        header_->trade_date = kTradeDate;
        header_->flags =
            ipc::kRealtimeHeaderKLineEnabledV2 |
            ipc::kRealtimeHeaderCoverageFromOpenV2;
        header_->capacity = kCapacity;
        header_->window_count = 1U;
        header_->catalog_scope =
            static_cast<std::uint32_t>(
                ipc::RealtimeCatalogScopeV2::
                    kDeclaredDailyAShare);
        header_->coverage_complete = 1U;
        header_->catalog_trade_date = kTradeDate;
        header_->catalog_version = 19U;
        header_->layout_digest.words = {
            0x0102030405060708ULL,
            0x1112131415161718ULL,
            0x2122232425262728ULL,
            0x3132333435363738ULL};
        header_->status_publish_tag = 2U;
        header_->catalog_generation = 1U;
        header_->data_state_generation = 4U;
        header_->catalog_digest.words = {
            0x4142434445464748ULL,
            0x5152535455565758ULL,
            0x6162636465666768ULL,
            0x7172737475767778ULL};
        header_->bound_count = kCapacity;
        header_->available_count = 2U;
        header_->snapshot_available_count = 1U;
        header_->tick_available_count = 1U;
        header_->factor_eligible_count = 1U;
        header_->accepted_sequence = 100U;
        header_->applied_sequence = 99U;
        header_->heartbeat_monotonic_ns = 777U;
        header_->tick_highest_published_sequence = 1U;
        header_->tick_contiguous_published_sequence = 1U;
        header_->kline_generation = 1U;
        header_->published_records = 3U;
        header_->region_count = layout.regions.size();
        header_->region_descriptor_bytes =
            sizeof(ipc::RealtimeWireRegionDescriptorV2);
        header_->regions = layout.regions;
        header_->server_state = static_cast<std::uint32_t>(
            ipc::RealtimeServerStateV2::kActive);

        auto* const base = static_cast<std::byte*>(mapping_);
        instruments_ =
            reinterpret_cast<ipc::RealtimeWireInstrumentV2*>(
                base + layout.regions[0U].offset);
        key_arena_ = base + layout.regions[1U].offset;
        windows_ =
            reinterpret_cast<ipc::RealtimeWireKLineWindowV2*>(
                base + layout.regions[2U].offset);
        snapshots_ =
            reinterpret_cast<ipc::RealtimeWireSnapshotSlotV2*>(
                base + layout.regions[3U].offset);
        latest_ticks_ =
            reinterpret_cast<ipc::RealtimeWireTickSlotV2*>(
                base + layout.regions[4U].offset);
        klines_ =
            reinterpret_cast<ipc::RealtimeWireKLineSlotV2*>(
                base + layout.regions[5U].offset);
        tick_ring_ =
            reinterpret_cast<ipc::RealtimeWireTickSlotV2*>(
                base + layout.regions[6U].offset);

        windows_[0U].window_id = 1U;
        windows_[0U].duration_ns = kWindowDurationNs;
        BindInitialRows();
        PublishInitialPayloads();

        if (add_seals) {
            constexpr int seals =
                F_SEAL_GROW | F_SEAL_SHRINK |
                F_SEAL_FUTURE_WRITE | F_SEAL_SEAL;
            if (::fcntl(rw_fd_.get(), F_ADD_SEALS, seals) != 0) {
                return false;
            }
        }
        char descriptor_path[64]{};
        const int path_bytes = std::snprintf(
            descriptor_path,
            sizeof(descriptor_path),
            "/proc/self/fd/%d",
            rw_fd_.get());
        if (path_bytes <= 0 ||
            static_cast<std::size_t>(path_bytes) >=
                sizeof(descriptor_path)) {
            return false;
        }
        ro_fd_.Reset(
            ::open(descriptor_path, O_RDONLY | O_CLOEXEC));
        return ro_fd_.get() >= 0;
    }

    [[nodiscard]] int reader_fd() const noexcept {
        return ro_fd_.get();
    }

    [[nodiscard]] ipc::RealtimeWireHeaderV2* header() noexcept {
        return header_;
    }

    [[nodiscard]] ipc::RealtimeWireInstrumentV2* instruments()
        noexcept {
        return instruments_;
    }

    void PublishKLine(
        const ipc::RealtimeWireKLinePayloadV2& payload) noexcept {
        // Generation one selects table one. This fixture has one window, so
        // the second table begins after exactly capacity slots.
        PublishSlot(&klines_[kCapacity], payload);
    }

    void SetStatusTag(std::uint64_t tag) noexcept {
        AtomicStore(header_->status_publish_tag, tag);
    }

    void BindFourth(bool duplicate_key, bool snapshot_available) {
        AtomicStore(header_->status_publish_tag, 3U);
        ipc::RealtimeWireInstrumentV2& row = instruments_[3U];
        AtomicStore(row.publish_tag, 1U);
        row.instrument_id = 4U;
        row.ordinal = 3U;
        row.binding_state = static_cast<std::uint32_t>(
            snapshot_available
                ? ipc::RealtimeInstrumentBindingStateV2::kAvailable
                : ipc::RealtimeInstrumentBindingStateV2::
                      kBoundNoData);
        row.availability_flags =
            snapshot_available
                ? ipc::kRealtimeInstrumentHasSnapshotV2
                : 0U;
        row.market = duplicate_key ? 2U : 1U;
        row.quantity_unit = 1U;
        row.security_type = 1U;
        row.asset_scope = 1U;
        const std::string_view source =
            duplicate_key ? std::string_view{} : "N";
        const std::string_view security_id =
            duplicate_key ? "ZZ" : "CC";
        StoreKey(source, security_id, &row);
        if (snapshot_available) {
            row.first_ingress_sequence = 20U;
            row.last_ingress_sequence = 20U;
            PublishSnapshot(3U, 4U, 20U, row.market);
        }
        AtomicStore(row.publish_tag, 2U);
        header_->catalog_generation = 4U;
        header_->data_state_generation =
            snapshot_available ? 5U : 4U;
        header_->catalog_digest.words[0U] ^= 0x55U;
        header_->bound_count = 4U;
        if (snapshot_available) {
            header_->available_count = 3U;
            header_->snapshot_available_count = 2U;
        }
        AtomicStore(header_->status_publish_tag, 4U);
    }

private:
    void StoreKey(
        std::string_view source,
        std::string_view security_id,
        ipc::RealtimeWireInstrumentV2* row) {
        row->security_id_source_offset = key_cursor_;
        row->security_id_source_length =
            static_cast<std::uint32_t>(source.size());
        if (!source.empty()) {
            std::memcpy(
                key_arena_ + key_cursor_,
                source.data(),
                source.size());
        }
        key_cursor_ += source.size();
        row->security_id_offset = key_cursor_;
        row->security_id_length =
            static_cast<std::uint32_t>(security_id.size());
        std::memcpy(
            key_arena_ + key_cursor_,
            security_id.data(),
            security_id.size());
        key_cursor_ += security_id.size();
    }

    void BindRow(
        std::size_t ordinal,
        std::uint8_t market,
        std::string_view source,
        std::string_view security_id,
        ipc::RealtimeInstrumentBindingStateV2 state,
        std::uint32_t availability_flags,
        std::uint64_t first_ingress,
        std::uint64_t last_ingress) {
        ipc::RealtimeWireInstrumentV2& row =
            instruments_[ordinal];
        row.publish_tag = 2U;
        row.instrument_id =
            static_cast<std::uint32_t>(ordinal + 1U);
        row.ordinal = static_cast<std::uint32_t>(ordinal);
        row.binding_state =
            static_cast<std::uint32_t>(state);
        row.availability_flags = availability_flags;
        row.market = market;
        row.quantity_unit = 1U;
        row.security_type = 1U;
        row.asset_scope = 1U;
        StoreKey(source, security_id, &row);
        row.first_ingress_sequence = first_ingress;
        row.last_ingress_sequence = last_ingress;
    }

    void BindInitialRows() {
        BindRow(
            0U,
            2U,
            "",
            "ZZ",
            ipc::RealtimeInstrumentBindingStateV2::kAvailable,
            ipc::kRealtimeInstrumentHasSnapshotV2 |
                ipc::kRealtimeInstrumentHasKLineV2 |
                ipc::kRealtimeInstrumentFactorEligibleV2,
            11U,
            11U);
        BindRow(
            1U,
            1U,
            "X",
            "BB",
            ipc::RealtimeInstrumentBindingStateV2::kBoundNoData,
            0U,
            0U,
            0U);
        BindRow(
            2U,
            1U,
            "A",
            "AA",
            ipc::RealtimeInstrumentBindingStateV2::kAvailable,
            ipc::kRealtimeInstrumentHasTickV2,
            13U,
            13U);
        BindRow(
            3U,
            1U,
            "N",
            "CC",
            ipc::RealtimeInstrumentBindingStateV2::kBoundNoData,
            0U,
            0U,
            0U);
    }

    ipc::RealtimeWireCommonRecordV2 Common(
        std::uint32_t instrument_id,
        std::uint32_t ordinal,
        std::uint64_t ingress,
        std::uint8_t market,
        bool tick) {
        ipc::RealtimeWireCommonRecordV2 common{};
        common.record_schema_version = 2U;
        common.instrument_id = instrument_id;
        common.ordinal = ordinal;
        common.source_sequence = 1U;
        common.ingress_sequence = ingress;
        common.tick_stream_sequence = tick ? 1U : 0U;
        common.source_stream_id =
            tick ? 12U : (market == 1U ? 11U : 13U);
        common.trade_date = kTradeDate;
        common.source_slot =
            tick ? 1U : (market == 1U ? 0U : 2U);
        common.event_kind =
            tick ? 2U : (market == 1U ? 1U : 3U);
        common.market = market;
        common.quantity_unit = 1U;
        common.security_type = 1U;
        common.asset_scope = 1U;
        return common;
    }

    void PublishSnapshot(
        std::size_t ordinal,
        std::uint32_t instrument_id,
        std::uint64_t ingress,
        std::uint8_t market) {
        ipc::RealtimeWireSnapshotPayloadV2 snapshot{};
        snapshot.common = Common(
            instrument_id,
            static_cast<std::uint32_t>(ordinal),
            ingress,
            market,
            false);
        snapshot.common.record_bytes = sizeof(snapshot);
        PublishSlot(&snapshots_[ordinal], snapshot);
    }

    void PublishInitialPayloads() {
        PublishSnapshot(0U, 1U, 11U, 2U);

        ipc::RealtimeWireTickPayloadV2 tick{};
        tick.common = Common(3U, 2U, 13U, 1U, true);
        tick.common.record_bytes = sizeof(tick);
        PublishSlot(&latest_ticks_[2U], tick);
        PublishSlot(&tick_ring_[0U], tick);

        ipc::RealtimeWireKLinePayloadV2 kline{};
        kline.generation = 1U;
        kline.trade_date = kTradeDate;
        kline.instrument_id = 1U;
        kline.window_id = 1U;
        kline.window_duration_ns = kWindowDurationNs;
        kline.present = 1U;
        // Generation one selects table one. Each table has capacity slots
        // because this fixture has one configured KLine window.
        PublishSlot(&klines_[kCapacity], kline);
    }

    UniqueFd rw_fd_;
    UniqueFd ro_fd_;
    void* mapping_ = MAP_FAILED;
    std::size_t mapping_bytes_ = 0U;
    ipc::RealtimeWireHeaderV2* header_ = nullptr;
    ipc::RealtimeWireInstrumentV2* instruments_ = nullptr;
    std::byte* key_arena_ = nullptr;
    ipc::RealtimeWireKLineWindowV2* windows_ = nullptr;
    ipc::RealtimeWireSnapshotSlotV2* snapshots_ = nullptr;
    ipc::RealtimeWireTickSlotV2* latest_ticks_ = nullptr;
    ipc::RealtimeWireKLineSlotV2* klines_ = nullptr;
    ipc::RealtimeWireTickSlotV2* tick_ring_ = nullptr;
    std::size_t key_cursor_ = 0U;
};

bool OpenFixture(
    MappedFixture* fixture,
    ReaderHandle* reader) {
    return Expect(
               fixture->Create(),
               "create sealed Wire V2 fixture") &&
           Expect(
               l2flow_shm_reader_open_fd_v2(
                   fixture->reader_fd(), reader->output()) ==
                   L2FLOW_SHM_READER_OK_V2,
               "open sealed Wire V2 fixture");
}

bool TestSessionAndPointStates() {
    MappedFixture fixture;
    ReaderHandle reader;
    if (!OpenFixture(&fixture, &reader)) {
        return false;
    }
    l2flow_shm_session_info_v2 session{};
    bool ok = true;
    ok &= Expect(
        l2flow_shm_reader_session_v2(
            reader.get(), &session) ==
            L2FLOW_SHM_READER_OK_V2,
        "read coherent V2 session envelope");
    ok &= Expect(
        session.session_epoch == 17U &&
            session.capacity == kCapacity &&
            session.catalog_scope ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeCatalogScopeV2::
                        kDeclaredDailyAShare) &&
            session.coverage_complete == 1U &&
            session.catalog_trade_date == kTradeDate &&
            session.catalog_version == 19U &&
            session.catalog_generation == 1U &&
            session.data_state_generation == 4U &&
            session.bound_count == kCapacity &&
            session.available_count == 2U &&
            session.snapshot_available_count == 1U &&
            session.tick_available_count == 1U &&
            session.factor_eligible_count == 1U &&
            session.accepted_sequence == 100U &&
            session.applied_sequence == 99U &&
            session.processing_lag_records == 1U &&
            session.tick_ring_capacity == kRingCapacity &&
            session.tick_highest_published_sequence == 1U &&
            session.tick_contiguous_published_sequence == 1U &&
            session.kline_generation == 1U &&
            session.heartbeat_monotonic_ns == 777U,
        "session reports its exact processing lag");
    ok &= Expect(
        std::any_of(
            std::begin(session.layout_digest),
            std::end(session.layout_digest),
            [](std::uint8_t byte) { return byte != 0U; }) &&
            std::any_of(
                std::begin(session.catalog_digest),
                std::end(session.catalog_digest),
            [](std::uint8_t byte) { return byte != 0U; }),
        "session returns immutable layout and daily catalog digests");

    l2flow_kline_coverage_info_v2 kline_coverage{};
    ok &= Expect(
        l2flow_shm_reader_kline_coverage_v2(
            reader.get(), &kline_coverage) ==
                L2FLOW_SHM_READER_OK_V2 &&
            kline_coverage.session_epoch == 17U &&
            kline_coverage.coverage_start_unix_ns == 0U &&
            kline_coverage.coverage_kind ==
                L2FLOW_KLINE_COVERAGE_FROM_OPEN_V2 &&
            kline_coverage.reserved0 == 0U &&
            kline_coverage.reserved[0U] == 0U,
        "KLine coverage getter reports a from-open contract without a process-start boundary");

    l2flow_shm_health_v2 health{};
    ok &= Expect(
        l2flow_shm_reader_health_v2(reader.get(), &health) ==
                L2FLOW_SHM_READER_OK_V2 &&
            health.session_epoch == 17U &&
            health.heartbeat_monotonic_ns == 777U &&
            health.server_state ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeServerStateV2::kActive) &&
            health.flags ==
                (ipc::kRealtimeHeaderKLineEnabledV2 |
                 ipc::kRealtimeHeaderCoverageFromOpenV2) &&
            std::all_of(
                std::begin(health.reserved),
                std::end(health.reserved),
                [](std::uint32_t value) { return value == 0U; }),
        "health read returns only stable epoch, heartbeat, state, and flags");

    ipc::RealtimeWireInstrumentV2 row{};
    std::array<std::uint8_t, 8U> source{};
    std::array<std::uint8_t, 8U> security_id{};
    std::size_t source_written = 99U;
    std::size_t id_written = 99U;
    std::uint8_t status = 0xffU;
    ok &= Expect(
        l2flow_shm_reader_instrument_v2(
            reader.get(),
            1U,
            &row,
            sizeof(row),
            source.data(),
            source.size(),
            &source_written,
            security_id.data(),
            security_id.size(),
            &id_written,
            &status) == L2FLOW_SHM_READER_OK_V2 &&
            status == L2FLOW_INSTRUMENT_AVAILABLE_V2 &&
            row.instrument_id == 1U && row.ordinal == 0U &&
            source_written == 0U && id_written == 2U &&
            std::memcmp(security_id.data(), "ZZ", 2U) == 0,
        "point lookup directly resolves available ordinal zero");

    ok &= Expect(
        l2flow_shm_reader_instrument_v2(
            reader.get(),
            2U,
            &row,
            sizeof(row),
            source.data(),
            source.size(),
            &source_written,
            security_id.data(),
            security_id.size(),
            &id_written,
            &status) == L2FLOW_SHM_READER_OK_V2 &&
            status == L2FLOW_INSTRUMENT_BOUND_NO_DATA_V2 &&
            row.instrument_id == 2U && row.ordinal == 1U &&
            source_written == 1U && id_written == 2U,
        "point lookup distinguishes BOUND_NO_DATA");

    row.instrument_id = 99U;
    ok &= Expect(
        l2flow_shm_reader_instrument_v2(
            reader.get(),
            4U,
            &row,
            sizeof(row),
            source.data(),
            source.size(),
            &source_written,
            security_id.data(),
            security_id.size(),
            &id_written,
            &status) == L2FLOW_SHM_READER_OK_V2 &&
            status == L2FLOW_INSTRUMENT_BOUND_NO_DATA_V2 &&
            source_written == 1U && id_written == 2U &&
            row.instrument_id == 4U && row.ordinal == 3U,
        "point lookup returns prepublished daily identity with no data");

    ok &= Expect(
        l2flow_shm_reader_instrument_v2(
            reader.get(),
            5U,
            &row,
            sizeof(row),
            source.data(),
            source.size(),
            &source_written,
            security_id.data(),
            security_id.size(),
            &id_written,
            &status) == L2FLOW_SHM_READER_OK_V2 &&
            status == L2FLOW_INSTRUMENT_INVALID_ID_V2,
        "point lookup distinguishes ID beyond session capacity");
    return ok;
}

bool TestLatestStatesAndStreams() {
    MappedFixture fixture;
    ReaderHandle reader;
    if (!OpenFixture(&fixture, &reader)) {
        return false;
    }
    constexpr std::array<std::uint32_t, 6U> ids{
        1U, 2U, 3U, 4U, 5U, 0U};
    std::array<ipc::RealtimeWireSnapshotPayloadV2, ids.size()>
        snapshots{};
    std::array<std::uint8_t, ids.size()> statuses{};
    bool ok = true;
    ok &= Expect(
        l2flow_shm_reader_latest_snapshots_v2(
            reader.get(),
            ids.data(),
            ids.size(),
            snapshots.data(),
            sizeof(snapshots[0U]),
            statuses.data()) == L2FLOW_SHM_READER_OK_V2 &&
            statuses ==
                std::array<std::uint8_t, ids.size()>{
                    L2FLOW_LATEST_AVAILABLE_V2,
                    L2FLOW_LATEST_BOUND_NO_DATA_V2,
                    L2FLOW_LATEST_TYPE_UNAVAILABLE_V2,
                    L2FLOW_LATEST_BOUND_NO_DATA_V2,
                    L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V2,
                    L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V2} &&
            snapshots[0U].common.instrument_id == 1U &&
            snapshots[0U].common.ordinal == 0U,
        "snapshot latest separates no-data, type, and invalid states");

    std::array<ipc::RealtimeWireTickPayloadV2, ids.size()> ticks{};
    ok &= Expect(
        l2flow_shm_reader_latest_ticks_v2(
            reader.get(),
            ids.data(),
            ids.size(),
            ticks.data(),
            sizeof(ticks[0U]),
            statuses.data()) == L2FLOW_SHM_READER_OK_V2 &&
            statuses ==
                std::array<std::uint8_t, ids.size()>{
                    L2FLOW_LATEST_TYPE_UNAVAILABLE_V2,
                    L2FLOW_LATEST_BOUND_NO_DATA_V2,
                    L2FLOW_LATEST_AVAILABLE_V2,
                    L2FLOW_LATEST_BOUND_NO_DATA_V2,
                    L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V2,
                    L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V2} &&
            ticks[2U].common.instrument_id == 3U &&
            ticks[2U].common.ordinal == 2U,
        "tick latest uses O(1) ordinal identity and type availability");

    constexpr std::array<std::uint32_t, 5U> kline_ids{
        1U, 2U, 3U, 4U, 5U};
    constexpr std::array<std::uint32_t, 5U> window_ids{
        1U, 1U, 1U, 1U, 1U};
    std::array<ipc::RealtimeWireKLinePayloadV2, kline_ids.size()>
        klines{};
    std::array<std::uint8_t, kline_ids.size()> kline_statuses{};
    ok &= Expect(
        l2flow_shm_reader_latest_klines_v2(
            reader.get(),
            kline_ids.data(),
            window_ids.data(),
            kline_ids.size(),
            klines.data(),
            sizeof(klines[0U]),
            kline_statuses.data()) ==
                L2FLOW_SHM_READER_OK_V2 &&
            kline_statuses ==
                std::array<std::uint8_t, kline_ids.size()>{
                    L2FLOW_LATEST_AVAILABLE_V2,
                    L2FLOW_LATEST_BOUND_NO_DATA_V2,
                    L2FLOW_LATEST_TYPE_UNAVAILABLE_V2,
                    L2FLOW_LATEST_BOUND_NO_DATA_V2,
                    L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V2} &&
            klines[0U].instrument_id == 1U,
        "KLine latest preserves daily-catalog data states");

    std::array<ipc::RealtimeWireTickPayloadV2, 2U> ring_ticks{};
    std::size_t written = 0U;
    std::uint64_t next = 0U;
    std::uint64_t observed = 0U;
    ok &= Expect(
        l2flow_shm_reader_ticks_v2(
            reader.get(),
            1U,
            ring_ticks.data(),
            sizeof(ring_ticks[0U]),
            ring_ticks.size(),
            &written,
            &next,
            &observed) == L2FLOW_SHM_READER_OK_V2 &&
            written == 1U && next == 2U && observed == 0U &&
            ring_ticks[0U].common.instrument_id == 3U &&
            ring_ticks[0U].common.ordinal == 2U,
        "mixed tick ring validates session-local ID/ordinal identity");

    AtomicStore(
        fixture.header()->tick_highest_published_sequence, 5U);
    AtomicStore(
        fixture.header()->tick_contiguous_published_sequence, 5U);
    ok &= Expect(
        l2flow_shm_reader_ticks_v2(
            reader.get(),
            1U,
            ring_ticks.data(),
            sizeof(ring_ticks[0U]),
            ring_ticks.size(),
            &written,
            &next,
            &observed) == L2FLOW_SHM_READER_OVERRUN_V2 &&
            written == 0U && next == 1U && observed == 2U,
        "tick ring reports the exact oldest candidate on overrun");
    return ok;
}

bool TestResolveRefreshAndSelections() {
    MappedFixture fixture;
    ReaderHandle reader;
    if (!OpenFixture(&fixture, &reader)) {
        return false;
    }
    // Explicit arrays avoid taking addresses of temporary string_view sizes.
    constexpr std::array<std::uint8_t, 4U> markets{2U, 1U, 1U, 9U};
    constexpr std::array<std::string_view, 4U> sources{
        "", "A", "X", ""};
    constexpr std::array<std::string_view, 4U> ids{
        "ZZ", "AA", "missing", "ZZ"};
    std::array<const std::uint8_t*, 4U> source_ptrs{};
    std::array<const std::uint8_t*, 4U> id_ptrs{};
    std::array<std::size_t, 4U> source_lengths{};
    std::array<std::size_t, 4U> id_lengths{};
    for (std::size_t index = 0U; index < markets.size(); ++index) {
        source_ptrs[index] =
            sources[index].empty()
                ? nullptr
                : reinterpret_cast<const std::uint8_t*>(
                      sources[index].data());
        id_ptrs[index] =
            reinterpret_cast<const std::uint8_t*>(
                ids[index].data());
        source_lengths[index] = sources[index].size();
        id_lengths[index] = ids[index].size();
    }
    std::array<std::uint32_t, 4U> resolved{};
    std::array<std::uint8_t, 4U> lookup_statuses{};
    bool ok = true;
    const int initial_resolve_error =
        l2flow_shm_reader_resolve_instruments_v2(
            reader.get(),
            markets.data(),
            source_ptrs.data(),
            source_lengths.data(),
            id_ptrs.data(),
            id_lengths.data(),
            markets.size(),
            resolved.data(),
            lookup_statuses.data());
    ok &= Expect(
        initial_resolve_error == L2FLOW_SHM_READER_OK_V2 &&
            resolved ==
                std::array<std::uint32_t, 4U>{1U, 3U, 0U, 0U} &&
            lookup_statuses ==
                std::array<std::uint8_t, 4U>{
                    L2FLOW_INSTRUMENT_LOOKUP_FOUND_V2,
                    L2FLOW_INSTRUMENT_LOOKUP_FOUND_V2,
                    L2FLOW_INSTRUMENT_LOOKUP_UNKNOWN_V2,
                    L2FLOW_INSTRUMENT_LOOKUP_INVALID_MARKET_V2},
        "lazy key index sorts exact opaque keys independently of ordinal");
    if (initial_resolve_error != L2FLOW_SHM_READER_OK_V2 ||
        resolved !=
            std::array<std::uint32_t, 4U>{1U, 3U, 0U, 0U}) {
        std::cerr << "resolve error=" << initial_resolve_error
                  << " ids=" << resolved[0U] << ','
                  << resolved[1U] << ',' << resolved[2U] << ','
                  << resolved[3U] << " status="
                  << static_cast<unsigned int>(lookup_statuses[0U])
                  << ','
                  << static_cast<unsigned int>(lookup_statuses[1U])
                  << ','
                  << static_cast<unsigned int>(lookup_statuses[2U])
                  << ','
                  << static_cast<unsigned int>(lookup_statuses[3U])
                  << '\n';
    }

    std::array<std::uint32_t, kCapacity> selected{};
    std::size_t required = 0U;
    l2flow_selection_envelope_v2 envelope{};
    ok &= Expect(
        l2flow_shm_reader_select_instruments_v2(
            reader.get(),
            L2FLOW_SELECTION_BOUND_V2,
            selected.data(),
            selected.size(),
            &required,
            &envelope) == L2FLOW_SHM_READER_OK_V2 &&
            required == 4U &&
            selected ==
                std::array<std::uint32_t, 4U>{1U, 2U, 3U, 4U} &&
            envelope.returned_row_count == 4U &&
            envelope.bound_count == 4U &&
            envelope.catalog_generation == 1U &&
            envelope.data_state_generation == 4U &&
            envelope.accepted_sequence == 100U &&
            envelope.applied_sequence == 99U &&
            envelope.processing_lag_records == 1U,
        "CATALOG_ALL selection returns every dense ID in one envelope");

    selected.fill(99U);
    ok &= Expect(
        l2flow_shm_reader_select_instruments_v2(
            reader.get(),
            L2FLOW_SELECTION_OBSERVED_ANY_V2,
            selected.data(),
            selected.size(),
            &required,
            &envelope) == L2FLOW_SHM_READER_OK_V2 &&
            required == 2U && selected[0U] == 1U &&
            selected[1U] == 3U &&
            envelope.returned_row_count ==
                envelope.available_count,
        "AVAILABLE_ANY selection count equals available_count");

    selected.fill(77U);
    ok &= Expect(
        l2flow_shm_reader_select_instruments_v2(
            reader.get(),
            L2FLOW_SELECTION_TICK_AVAILABLE_V2,
            selected.data(),
            selected.size(),
            &required,
            &envelope) == L2FLOW_SHM_READER_OK_V2 &&
            required == 1U && selected[0U] == 3U &&
            envelope.returned_row_count ==
                envelope.tick_available_count,
        "TICK_AVAILABLE selection count equals tick_available_count");

    ok &= Expect(
        l2flow_shm_reader_select_instruments_v2(
            reader.get(),
            L2FLOW_SELECTION_FACTOR_ELIGIBLE_V2,
            selected.data(),
            selected.size(),
            &required,
            &envelope) == L2FLOW_SHM_READER_OK_V2 &&
            required == 1U && selected[0U] == 1U &&
            envelope.returned_row_count ==
                envelope.factor_eligible_count,
        "FACTOR_ELIGIBLE selection count equals eligible count");

    selected.fill(77U);
    ok &= Expect(
        l2flow_shm_reader_select_instruments_v2(
            reader.get(),
            L2FLOW_SELECTION_SNAPSHOT_AVAILABLE_V2,
            nullptr,
            0U,
            &required,
            &envelope) ==
                L2FLOW_SHM_READER_BUFFER_TOO_SMALL_V2 &&
            required == 1U && envelope.returned_row_count == 1U &&
            envelope.snapshot_available_count == 1U &&
            selected[0U] == 77U,
        "selection reports required count and envelope without partial IDs");

    std::uint32_t fourth = 0U;
    std::uint8_t fourth_status = 0xffU;
    constexpr std::uint8_t market = 1U;
    constexpr std::string_view fourth_source = "N";
    constexpr std::string_view fourth_id = "CC";
    const std::uint8_t* source_pointer =
        reinterpret_cast<const std::uint8_t*>(
            fourth_source.data());
    const std::uint8_t* id_pointer =
        reinterpret_cast<const std::uint8_t*>(
            fourth_id.data());
    const std::size_t source_length = fourth_source.size();
    const std::size_t id_length = fourth_id.size();
    const int fourth_resolve_error =
        l2flow_shm_reader_resolve_instruments_v2(
            reader.get(),
            &market,
            &source_pointer,
            &source_length,
            &id_pointer,
            &id_length,
            1U,
            &fourth,
            &fourth_status);
    ok &= Expect(
        fourth_resolve_error == L2FLOW_SHM_READER_OK_V2 &&
            fourth == 4U &&
            fourth_status ==
                L2FLOW_INSTRUMENT_LOOKUP_FOUND_V2,
        "resolve includes a catalog instrument with no data");

    ok &= Expect(
        l2flow_shm_reader_select_instruments_v2(
            reader.get(),
            L2FLOW_SELECTION_BOUND_V2,
            selected.data(),
            selected.size(),
            &required,
            &envelope) == L2FLOW_SHM_READER_OK_V2 &&
            required == 4U &&
            envelope.returned_row_count == 4U &&
            envelope.catalog_generation == 1U,
        "selection retains one frozen catalog cut");

    fixture.SetStatusTag(5U);
    ok &= Expect(
        l2flow_shm_reader_select_instruments_v2(
            reader.get(),
            L2FLOW_SELECTION_BOUND_V2,
            selected.data(),
            selected.size(),
            &required,
            &envelope) ==
            L2FLOW_SHM_READER_INCONSISTENT_READ_V2,
        "selection explicitly rejects a persistently unstable status tag");
    fixture.SetStatusTag(6U);
    return ok;
}

bool TestLatestNeverRefreshesKeyIndex() {
    MappedFixture fixture;
    ReaderHandle reader;
    if (!OpenFixture(&fixture, &reader)) {
        return false;
    }
    // Publish a fourth point-readable row whose key duplicates row one.
    // Latest-by-ID must remain independent of the key index, while resolve
    // must detect the duplicate during its generation-triggered rebuild.
    fixture.BindFourth(true, true);
    // Simulate a concurrent progress/catalog-envelope publication. Point
    // latest must use only the fixed-ordinal path, so an unrelated persistently
    // odd status tag cannot delay it or trigger a key-index refresh.
    fixture.SetStatusTag(5U);
    constexpr std::uint32_t id = 4U;
    ipc::RealtimeWireSnapshotPayloadV2 snapshot{};
    std::uint8_t latest_status = 0xffU;
    bool ok = true;
    ok &= Expect(
        l2flow_shm_reader_latest_snapshots_v2(
            reader.get(),
            &id,
            1U,
            &snapshot,
            sizeof(snapshot),
            &latest_status) == L2FLOW_SHM_READER_OK_V2 &&
            latest_status == L2FLOW_LATEST_AVAILABLE_V2 &&
            snapshot.common.instrument_id == 4U,
        "latest-by-ID ignores status seqcount and does not rebuild key index");
    fixture.SetStatusTag(6U);

    constexpr std::uint8_t market = 2U;
    constexpr std::string_view security_id = "ZZ";
    const std::uint8_t* null_source = nullptr;
    const std::uint8_t* id_pointer =
        reinterpret_cast<const std::uint8_t*>(
            security_id.data());
    constexpr std::size_t source_length = 0U;
    const std::size_t id_length = security_id.size();
    std::uint32_t resolved = 0U;
    std::uint8_t lookup_status = 0xffU;
    ok &= Expect(
        l2flow_shm_reader_resolve_instruments_v2(
            reader.get(),
            &market,
            &null_source,
            &source_length,
            &id_pointer,
            &id_length,
            1U,
            &resolved,
            &lookup_status) ==
            L2FLOW_SHM_READER_LAYOUT_INVALID_V2,
        "key resolve alone rebuilds the index and rejects duplicate keys");
    return ok;
}

bool TestAvailableLatestIgnoresBusyInstrumentRow() {
    MappedFixture fixture;
    ReaderHandle reader;
    if (!OpenFixture(&fixture, &reader)) {
        return false;
    }

    // A live writer changes the instrument row's last_ingress_sequence for
    // every event. Hold its seqcount in the writer-owned odd state to model
    // sustained same-instrument row contention while the latest snapshot slot
    // itself remains stable and fully published.
    AtomicStore(fixture.instruments()[0U].publish_tag, 3U);
    constexpr std::uint32_t instrument_id = 1U;
    ipc::RealtimeWireSnapshotPayloadV2 snapshot{};
    std::uint8_t status = 0xffU;
    bool ok = Expect(
        l2flow_shm_reader_latest_snapshots_v2(
            reader.get(),
            &instrument_id,
            1U,
            &snapshot,
            sizeof(snapshot),
            &status) == L2FLOW_SHM_READER_OK_V2 &&
            status == L2FLOW_LATEST_AVAILABLE_V2 &&
            snapshot.common.instrument_id == instrument_id &&
            snapshot.common.ordinal == 0U &&
            snapshot.common.ingress_sequence == 11U,
        "available latest reads only its stable slot while row seqcount is busy");

    constexpr std::uint32_t window_id = 1U;
    ipc::RealtimeWireKLinePayloadV2 kline{};
    status = 0xffU;
    ok &= Expect(
        l2flow_shm_reader_latest_klines_v2(
            reader.get(),
            &instrument_id,
            &window_id,
            1U,
            &kline,
            sizeof(kline),
            &status) == L2FLOW_SHM_READER_OK_V2 &&
            status == L2FLOW_LATEST_AVAILABLE_V2 &&
            kline.present == 1U &&
            kline.generation == 1U &&
            kline.instrument_id == instrument_id &&
            kline.window_id == window_id,
        "available KLine reads its completed-generation slot while row seqcount is busy");
    return ok;
}

bool TestHardLayoutAndSealRejection() {
    bool ok = true;
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(),
            "create fixture for ACTIVE coverage rejection");
        fixture.header()->flags =
            ipc::kRealtimeHeaderKLineEnabledV2;
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_LAYOUT_INVALID_V2,
            "reader rejects impossible ACTIVE without coverage_from_open");
    }
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(),
            "create fixture for unprepared LIVE_PARTIAL state validation");
        fixture.header()->server_state = static_cast<std::uint32_t>(
            ipc::RealtimeServerStateV2::kLivePartial);
        fixture.header()->flags =
            ipc::kRealtimeHeaderKLineEnabledV2;
        fixture.header()->kline_generation = 0U;
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_OK_V2,
            "reader accepts an unprepared provisional partial-KLine mapping");
        l2flow_kline_coverage_info_v2 coverage{};
        ok &= Expect(
            l2flow_shm_reader_kline_coverage_v2(
                reader.get(), &coverage) ==
                L2FLOW_SHM_READER_UNAVAILABLE_V2,
            "partial KLine coverage stays unavailable until its process-start boundary is published");
    }
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(),
            "create fixture for prepared LIVE_PARTIAL state validation");
        fixture.header()->server_state = static_cast<std::uint32_t>(
            ipc::RealtimeServerStateV2::kLivePartial);
        fixture.header()->flags =
            ipc::kRealtimeHeaderKLineEnabledV2;
        fixture.header()->kline_generation = 0U;
        fixture.header()->kline_coverage_start_unix_ns =
            kProcessStartCoverageUnixNs;
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_OK_V2,
            "reader accepts a prepared process-start partial-KLine contract");
        l2flow_kline_coverage_info_v2 coverage{};
        ok &= Expect(
            l2flow_shm_reader_kline_coverage_v2(
                reader.get(), &coverage) ==
                    L2FLOW_SHM_READER_OK_V2 &&
                coverage.session_epoch == 17U &&
                coverage.coverage_start_unix_ns ==
                    kProcessStartCoverageUnixNs &&
                coverage.coverage_kind ==
                    L2FLOW_KLINE_COVERAGE_PROCESS_START_PARTIAL_V2 &&
                coverage.reserved0 == 0U &&
                coverage.reserved[0U] == 0U,
            "coverage getter exposes the exact prepared process-start boundary");
    }
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(),
            "create fixture for missing partial-KLine boundary rejection");
        fixture.header()->server_state = static_cast<std::uint32_t>(
            ipc::RealtimeServerStateV2::kLivePartial);
        fixture.header()->flags =
            ipc::kRealtimeHeaderKLineEnabledV2;
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_LAYOUT_INVALID_V2,
            "reader rejects a published partial KLine generation without its coverage boundary");
    }
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(),
            "create fixture for LIVE_PARTIAL strong-flag rejection");
        fixture.header()->server_state = static_cast<std::uint32_t>(
            ipc::RealtimeServerStateV2::kLivePartial);
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_LAYOUT_INVALID_V2,
            "reader rejects LIVE_PARTIAL carrying coverage_from_open");
    }
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(false),
            "create otherwise-valid unsealed fixture");
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_SYSTEM_ERROR_V2,
            "V2 reader rejects a descriptor without required seals");
    }
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(), "create fixture for magic rejection");
        fixture.header()->magic =
            std::array<std::uint8_t, 8U>{
                'L', '2', 'F', 'S', 'H', 'M', '1', '\0'};
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_LAYOUT_INVALID_V2,
            "V2 reader rejects the V1 magic instead of adapting it");
    }
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(), "create fixture for row rejection");
        fixture.instruments()[0U].availability_flags =
            ipc::kRealtimeInstrumentFactorEligibleV2;
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_OK_V2,
            "open is independent of mutable availability state");
        ipc::RealtimeWireInstrumentV2 row{};
        std::array<std::uint8_t, 8U> source{};
        std::array<std::uint8_t, 8U> security_id{};
        std::size_t source_written = 0U;
        std::size_t id_written = 0U;
        std::uint8_t item_status = 0U;
        ok &= Expect(
            l2flow_shm_reader_instrument_v2(
                reader.get(),
                1U,
                &row,
                sizeof(row),
                source.data(),
                source.size(),
                &source_written,
                security_id.data(),
                security_id.size(),
                &id_written,
                &item_status) ==
                L2FLOW_SHM_READER_LAYOUT_INVALID_V2,
            "point read rejects a stable row violating availability invariants");
    }
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(),
            "create fixture for immutable identity rejection");
        fixture.instruments()[0U].instrument_id = 2U;
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_LAYOUT_INVALID_V2,
            "open rejects malformed identities in the published prefix");
    }
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(),
            "create fixture for progress invariant rejection");
        fixture.header()->applied_sequence =
            fixture.header()->accepted_sequence + 1U;
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_OK_V2,
            "open does not wait for or copy processing progress");
        l2flow_shm_session_info_v2 session{};
        ok &= Expect(
            l2flow_shm_reader_session_v2(
                reader.get(), &session) ==
                L2FLOW_SHM_READER_LAYOUT_INVALID_V2,
            "session read rejects applied_sequence beyond accepted_sequence");
    }
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(),
            "create fixture for exact count rejection");
        fixture.header()->snapshot_available_count = 2U;
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_OK_V2,
            "open does not scan mutable availability counts");
        std::array<std::uint32_t, 4U> selected{};
        std::size_t required = 0U;
        l2flow_selection_envelope_v2 envelope{};
        ok &= Expect(
            l2flow_shm_reader_select_instruments_v2(
                reader.get(),
                L2FLOW_SELECTION_SNAPSHOT_AVAILABLE_V2,
                selected.data(),
                selected.size(),
                &required,
                &envelope) ==
                L2FLOW_SHM_READER_LAYOUT_INVALID_V2,
            "global selection rejects header counts that disagree with rows");
    }
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(),
            "create fixture for status seqcount rejection");
        fixture.SetStatusTag(3U);
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_OK_V2,
            "open is independent of a changing global status cut");
        l2flow_shm_session_info_v2 session{};
        ok &= Expect(
            l2flow_shm_reader_session_v2(
                reader.get(), &session) ==
                L2FLOW_SHM_READER_INCONSISTENT_READ_V2,
            "session read rejects a persistently odd status tag");
    }
    return ok;
}

bool TestKLineCoveragePayloadValidation() {
    constexpr std::int64_t window_start_unix_ns =
        static_cast<std::int64_t>(
            kProcessStartCoverageUnixNs - 17'000'000'000ULL);
    constexpr std::int64_t window_end_unix_ns =
        window_start_unix_ns +
        static_cast<std::int64_t>(kWindowDurationNs);
    const auto configure_partial = [](
                                       MappedFixture* fixture,
                                       std::uint32_t coverage_flags) {
        fixture->header()->server_state =
            static_cast<std::uint32_t>(
                ipc::RealtimeServerStateV2::kLivePartial);
        fixture->header()->flags =
            ipc::kRealtimeHeaderKLineEnabledV2;
        fixture->header()->kline_coverage_start_unix_ns =
            kProcessStartCoverageUnixNs;
        ipc::RealtimeWireKLinePayloadV2 payload{};
        payload.generation = 1U;
        payload.trade_date = kTradeDate;
        payload.instrument_id = 1U;
        payload.window_id = 1U;
        payload.coverage_flags = coverage_flags;
        payload.window_duration_ns = kWindowDurationNs;
        payload.window_start_unix_ns = window_start_unix_ns;
        payload.window_end_unix_ns = window_end_unix_ns;
        payload.present = 1U;
        fixture->PublishKLine(payload);
    };

    bool ok = true;
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(),
            "create fixture for canonical partial-KLine payload");
        configure_partial(
            &fixture,
            ipc::kRealtimeWireKLineProcessStartPartialV2 |
                ipc::kRealtimeWireKLineNaturalWindowLeftTruncatedV2);
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_OK_V2,
            "open canonical partial-KLine mapping");
        constexpr std::uint32_t instrument_id = 1U;
        constexpr std::uint32_t window_id = 1U;
        ipc::RealtimeWireKLinePayloadV2 payload{};
        std::uint8_t status = 0xffU;
        ok &= Expect(
            l2flow_shm_reader_latest_klines_v2(
                reader.get(),
                &instrument_id,
                &window_id,
                1U,
                &payload,
                sizeof(payload),
                &status) == L2FLOW_SHM_READER_OK_V2 &&
                status == L2FLOW_LATEST_AVAILABLE_V2 &&
                payload.coverage_flags ==
                    (ipc::kRealtimeWireKLineProcessStartPartialV2 |
                     ipc::
                         kRealtimeWireKLineNaturalWindowLeftTruncatedV2),
            "reader accepts exact process-start and left-truncated flags for the natural window containing the boundary");
    }
    {
        MappedFixture fixture;
        ok &= Expect(
            fixture.Create(),
            "create fixture for understated partial-KLine payload");
        configure_partial(
            &fixture,
            ipc::kRealtimeWireKLineProcessStartPartialV2);
        ReaderHandle reader;
        ok &= Expect(
            l2flow_shm_reader_open_fd_v2(
                fixture.reader_fd(), reader.output()) ==
                L2FLOW_SHM_READER_OK_V2,
            "open partial mapping before per-payload validation");
        constexpr std::uint32_t instrument_id = 1U;
        constexpr std::uint32_t window_id = 1U;
        ipc::RealtimeWireKLinePayloadV2 payload{};
        std::uint8_t status = 0xffU;
        ok &= Expect(
            l2flow_shm_reader_latest_klines_v2(
                reader.get(),
                &instrument_id,
                &window_id,
                1U,
                &payload,
                sizeof(payload),
                &status) == L2FLOW_SHM_READER_LAYOUT_INVALID_V2,
            "reader rejects a natural window containing the boundary when its left-truncated flag is missing");
    }
    return ok;
}

}  // namespace

int main() {
    bool ok = true;
    ok &= TestSessionAndPointStates();
    ok &= TestLatestStatesAndStreams();
    ok &= TestResolveRefreshAndSelections();
    ok &= TestLatestNeverRefreshesKeyIndex();
    ok &= TestAvailableLatestIgnoresBusyInstrumentRow();
    ok &= TestHardLayoutAndSealRejection();
    ok &= TestKLineCoveragePayloadValidation();
    if (!ok) {
        return 1;
    }
    std::cout << "realtime mapped reader V2 tests passed\n";
    return 0;
}
