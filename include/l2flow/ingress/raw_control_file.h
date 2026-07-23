#pragma once

#include "l2flow/ingress/raw_control_page.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <cstdint>
#include <memory>
#include <string>

namespace l2flow::ingress {

inline constexpr char kRawControlFilename[] = "control.page";

class RawControlFileWriter final {
public:
    ~RawControlFileWriter();

    RawControlFileWriter(const RawControlFileWriter&) = delete;
    RawControlFileWriter& operator=(const RawControlFileWriter&) = delete;
    RawControlFileWriter(RawControlFileWriter&&) = delete;
    RawControlFileWriter& operator=(RawControlFileWriter&&) = delete;

    [[nodiscard]] bool Publish(
        const RawControlSnapshot& snapshot) noexcept;
    [[nodiscard]] int descriptor() const noexcept {
        return file_fd_;
    }
    [[nodiscard]] const l2flow::common::Identity128&
    writer_instance() const noexcept {
        return writer_instance_;
    }

private:
    friend std::unique_ptr<RawControlFileWriter>
    CreateRawControlFile(
        const RawWriterLease&,
        const RawControlSnapshot&,
        std::string*) noexcept;

    RawControlFileWriter(
        int file_fd,
        void* mapping,
        RawControlPageV1* page,
        l2flow::common::Identity128 writer_instance,
        l2flow::common::Identity128 stream_day_id,
        std::uint32_t source_stream_id,
        std::uint32_t capture_date,
        RawControlSnapshot initial_snapshot) noexcept;

    int file_fd_ = -1;
    void* mapping_ = nullptr;
    RawControlPageV1* page_ = nullptr;
    l2flow::common::Identity128 writer_instance_{};
    l2flow::common::Identity128 stream_day_id_{};
    std::uint32_t source_stream_id_ = 0U;
    std::uint32_t capture_date_ = 0U;
    RawControlSnapshot last_snapshot_{};
};

// Creates a new fixed-size inode under a writer-instance-derived typed tmp,
// constructs/publishes the first even page, atomically replaces control.page,
// fsyncs the stream directory, and retains the same inode/mapping.
[[nodiscard]] std::unique_ptr<RawControlFileWriter>
CreateRawControlFile(
    const RawWriterLease& lease,
    const RawControlSnapshot& initial,
    std::string* error = nullptr) noexcept;

class RawControlFileReader final {
public:
    ~RawControlFileReader();

    RawControlFileReader(const RawControlFileReader&) = delete;
    RawControlFileReader& operator=(const RawControlFileReader&) = delete;
    RawControlFileReader(RawControlFileReader&&) = delete;
    RawControlFileReader& operator=(RawControlFileReader&&) = delete;

    [[nodiscard]] bool Read(
        RawControlSnapshot* snapshot,
        std::uint64_t* generation = nullptr) const noexcept;
    // A writer may be descheduled while the shared page generation is odd.
    // Transient contention is retried against a bounded monotonic deadline;
    // a permanently busy or invalid page still fails closed.
    // False means the final pathname now names a replacement inode (or is
    // unavailable); the caller must unmap and execute the full attach gate.
    [[nodiscard]] bool PathStillNamesMapping() const noexcept;
    [[nodiscard]] int descriptor() const noexcept {
        return file_fd_;
    }

private:
    friend std::unique_ptr<RawControlFileReader>
    OpenRawControlFile(
        int,
        std::uint32_t,
        std::uint32_t,
        std::string*) noexcept;

    RawControlFileReader(
        int directory_fd,
        int file_fd,
        void* mapping) noexcept;

    int directory_fd_ = -1;
    int file_fd_ = -1;
    void* mapping_ = nullptr;
};

// Securely opens and maps the current control.page relative to an already
// retained stream-day directory. The mapping is accepted only when its first
// coherent publication matches the expected namespace.
[[nodiscard]] std::unique_ptr<RawControlFileReader>
OpenRawControlFile(
    int stream_directory_fd,
    std::uint32_t expected_source_stream_id,
    std::uint32_t expected_capture_date,
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
