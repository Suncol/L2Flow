#include "partial_event_stable_broker_v2.h"

#include "l2flow/ipc/partial_order_event_reader_v2.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <condition_variable>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <string>
#include <thread>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#ifndef F_SEAL_FUTURE_WRITE
#define F_SEAL_FUTURE_WRITE 0x0010
#endif

namespace l2flow::apps {
namespace {

namespace ipc = l2flow::ipc;
using ipc::PartialEventBrokerRequestV2;
using ipc::PartialEventBrokerResponseV2;
using ipc::PartialEventBrokerResultV2;
using ipc::PartialEventBrokerStateV2;
using ipc::PartialEventHandoffRequestV2;
using ipc::PartialEventHandoffResponseV2;
using ipc::kPartialEventBrokerProtocolMajorV2;
using ipc::kPartialEventBrokerProtocolMinorV2;
using ipc::kPartialEventBrokerRequestMagicV2;
using ipc::kPartialEventBrokerResponseMagicV2;
using ipc::kPartialEventHandoffRequestMagicV2;

class UniqueFd final {
public:
    UniqueFd() noexcept = default;
    explicit UniqueFd(int descriptor) noexcept
        : descriptor_(descriptor) {}
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    UniqueFd(UniqueFd&& other) noexcept
        : descriptor_(std::exchange(other.descriptor_, -1)) {}
    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            Reset(std::exchange(other.descriptor_, -1));
        }
        return *this;
    }
    ~UniqueFd() {
        Reset();
    }
    [[nodiscard]] int get() const noexcept {
        return descriptor_;
    }
    [[nodiscard]] int Release() noexcept {
        return std::exchange(descriptor_, -1);
    }
    void Reset(int descriptor = -1) noexcept {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        descriptor_ = descriptor;
    }

private:
    int descriptor_ = -1;
};

template <typename Value, std::size_t Size>
[[nodiscard]] bool AllZero(
    const std::array<Value, Size>& values) noexcept {
    return std::all_of(
        values.begin(), values.end(), [](Value value) noexcept {
            return value == Value{};
        });
}

[[nodiscard]] std::array<std::uint8_t, 16U> WireIdentity(
    const common::Identity128& identity) noexcept {
    std::array<std::uint8_t, 16U> result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] = std::to_integer<std::uint8_t>(identity[index]);
    }
    return result;
}

[[nodiscard]] common::Identity128 NativeIdentity(
    const std::array<std::uint8_t, 16U>& identity) noexcept {
    common::Identity128 result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] = static_cast<std::byte>(identity[index]);
    }
    return result;
}

[[nodiscard]] bool ValidTradeDate(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10'000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || year > 2200U || month == 0U ||
        month > 12U || day == 0U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> days{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum = days[month - 1U];
    const bool leap =
        (year % 4U == 0U && year % 100U != 0U) ||
        year % 400U == 0U;
    if (month == 2U && leap) {
        maximum = 29U;
    }
    return day <= maximum;
}

[[nodiscard]] bool ValidTimeout(
    std::chrono::milliseconds timeout) noexcept {
    return timeout > std::chrono::milliseconds::zero() &&
           timeout <= std::chrono::seconds(60);
}

[[nodiscard]] bool ValidLifecycleTiming(
    std::chrono::milliseconds interval,
    std::chrono::milliseconds timeout) noexcept {
    return interval >= std::chrono::milliseconds(1) &&
           interval <= std::chrono::seconds(10) &&
           timeout >= interval * 3 && timeout <= std::chrono::minutes(5);
}

template <typename Value>
[[nodiscard]] std::atomic_ref<Value> Atomic(Value& value) noexcept {
    return std::atomic_ref<Value>(value);
}

static_assert(std::atomic_ref<std::uint32_t>::is_always_lock_free);
static_assert(std::atomic_ref<std::uint64_t>::is_always_lock_free);
static_assert(std::atomic_ref<std::int64_t>::is_always_lock_free);

[[nodiscard]] bool ReadMonotonicNs(std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    timespec value{};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0 ||
        value.tv_nsec >= 1'000'000'000L) {
        return false;
    }
    const auto seconds = static_cast<std::uint64_t>(value.tv_sec);
    const auto nanoseconds = static_cast<std::uint64_t>(value.tv_nsec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() - nanoseconds) /
            1'000'000'000ULL) {
        return false;
    }
    *output = seconds * 1'000'000'000ULL + nanoseconds;
    return true;
}

[[nodiscard]] bool SocketAddress(
    const std::filesystem::path& path,
    sockaddr_un* output,
    socklen_t* output_size) {
    if (output == nullptr || output_size == nullptr ||
        path.empty() || !path.is_absolute()) {
        return false;
    }
    const std::string text = path.string();
    if (text.empty() || text.size() >= sizeof(output->sun_path)) {
        return false;
    }
    *output = {};
    output->sun_family = AF_UNIX;
    std::memcpy(output->sun_path, text.c_str(), text.size() + 1U);
    const std::size_t size =
        offsetof(sockaddr_un, sun_path) + text.size() + 1U;
    if (size >
        static_cast<std::size_t>(
            std::numeric_limits<socklen_t>::max())) {
        return false;
    }
    *output_size = static_cast<socklen_t>(size);
    return true;
}

struct BoundSocket final {
    UniqueFd descriptor;
    std::filesystem::path path;
    dev_t device = 0;
    ino_t inode = 0;

    BoundSocket() noexcept = default;
    BoundSocket(const BoundSocket&) = delete;
    BoundSocket& operator=(const BoundSocket&) = delete;
    BoundSocket(BoundSocket&&) noexcept = default;
    BoundSocket& operator=(BoundSocket&&) noexcept = default;
    ~BoundSocket() {
        CleanupPath();
    }

    void CleanupPath() noexcept {
        descriptor.Reset();
        if (path.empty() || inode == 0) {
            return;
        }
        struct stat status {};
        if (::lstat(path.c_str(), &status) == 0 &&
            S_ISSOCK(status.st_mode) && status.st_dev == device &&
            status.st_ino == inode) {
            static_cast<void>(::unlink(path.c_str()));
        }
        path.clear();
        device = 0;
        inode = 0;
    }
};

[[nodiscard]] bool CreateListener(
    const std::filesystem::path& path,
    std::uint32_t backlog,
    BoundSocket* output,
    int* system_error_number) {
    if (output == nullptr) {
        return false;
    }
    sockaddr_un address{};
    socklen_t address_size = 0;
    if (!SocketAddress(path, &address, &address_size)) {
        if (system_error_number != nullptr) {
            *system_error_number = EINVAL;
        }
        return false;
    }
    UniqueFd descriptor(
        ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0));
    if (descriptor.get() < 0) {
        if (system_error_number != nullptr) {
            *system_error_number = errno;
        }
        return false;
    }
    if (::bind(
            descriptor.get(),
            reinterpret_cast<const sockaddr*>(&address),
            address_size) != 0) {
        if (system_error_number != nullptr) {
            *system_error_number = errno;
        }
        return false;
    }
    bool remove_path = true;
    const auto cleanup = [&]() noexcept {
        if (remove_path) {
            static_cast<void>(::unlink(path.c_str()));
        }
    };
    if (::chmod(path.c_str(), S_IRUSR | S_IWUSR) != 0 ||
        backlog >
            static_cast<std::uint32_t>(
                std::numeric_limits<int>::max()) ||
        ::listen(descriptor.get(), static_cast<int>(backlog)) != 0) {
        if (system_error_number != nullptr) {
            *system_error_number = errno != 0 ? errno : EINVAL;
        }
        cleanup();
        return false;
    }
    struct stat status {};
    if (::lstat(path.c_str(), &status) != 0 ||
        !S_ISSOCK(status.st_mode)) {
        if (system_error_number != nullptr) {
            *system_error_number = errno != 0 ? errno : EINVAL;
        }
        cleanup();
        return false;
    }
    output->descriptor = std::move(descriptor);
    output->path = path;
    output->device = status.st_dev;
    output->inode = status.st_ino;
    remove_path = false;
    return true;
}

void SetSocketTimeout(
    int descriptor,
    std::chrono::milliseconds timeout) noexcept {
    const auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(timeout);
    const auto remainder = timeout - seconds;
    timeval value{};
    value.tv_sec = static_cast<time_t>(seconds.count());
    value.tv_usec = static_cast<suseconds_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(remainder)
            .count());
    static_cast<void>(::setsockopt(
        descriptor, SOL_SOCKET, SO_RCVTIMEO, &value, sizeof(value)));
    static_cast<void>(::setsockopt(
        descriptor, SOL_SOCKET, SO_SNDTIMEO, &value, sizeof(value)));
}

[[nodiscard]] bool PeerCredentials(
    int descriptor,
    ucred* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    *output = {};
    socklen_t size = sizeof(*output);
    return ::getsockopt(
               descriptor, SOL_SOCKET, SO_PEERCRED, output, &size) == 0 &&
           size == sizeof(*output) && output->pid > 0;
}

template <typename Packet>
[[nodiscard]] bool SendPacket(
    int socket_descriptor,
    const Packet& packet,
    int passed_descriptor = -1,
    int second_passed_descriptor = -1) noexcept {
    iovec vector{};
    vector.iov_base = const_cast<Packet*>(&packet);
    vector.iov_len = sizeof(packet);
    std::array<std::byte, CMSG_SPACE(sizeof(int) * 2U)> control{};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    std::array<int, 2U> descriptors{};
    std::size_t descriptor_count = 0U;
    if (passed_descriptor >= 0) {
        descriptors[descriptor_count++] = passed_descriptor;
    }
    if (second_passed_descriptor >= 0) {
        descriptors[descriptor_count++] = second_passed_descriptor;
    }
    if (descriptor_count != 0U) {
        message.msg_control = control.data();
        message.msg_controllen = control.size();
        cmsghdr* header = CMSG_FIRSTHDR(&message);
        if (header == nullptr) {
            return false;
        }
        header->cmsg_level = SOL_SOCKET;
        header->cmsg_type = SCM_RIGHTS;
        header->cmsg_len = CMSG_LEN(sizeof(int) * descriptor_count);
        std::memcpy(
            CMSG_DATA(header),
            descriptors.data(),
            sizeof(int) * descriptor_count);
    }
    const ssize_t sent = ::sendmsg(socket_descriptor, &message, MSG_NOSIGNAL);
    return sent == static_cast<ssize_t>(sizeof(packet));
}

template <typename Packet>
[[nodiscard]] bool ReceivePacket(
    int socket_descriptor,
    Packet* packet,
    int expected_descriptor_count,
    UniqueFd* received_descriptor) noexcept {
    if (packet == nullptr || received_descriptor == nullptr) {
        return false;
    }
    *packet = {};
    received_descriptor->Reset();
    iovec vector{};
    vector.iov_base = packet;
    vector.iov_len = sizeof(*packet);
    std::array<std::byte, CMSG_SPACE(sizeof(int) * 2U)> control{};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    message.msg_control = control.data();
    message.msg_controllen = control.size();
    const ssize_t received =
        ::recvmsg(socket_descriptor, &message, MSG_CMSG_CLOEXEC);
    const bool payload_valid =
        received == static_cast<ssize_t>(sizeof(*packet)) &&
        (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) == 0;
    bool ancillary_valid = true;
    std::size_t descriptor_count = 0U;
    for (cmsghdr* header = CMSG_FIRSTHDR(&message);
         header != nullptr;
         header = CMSG_NXTHDR(&message, header)) {
        if (header->cmsg_level != SOL_SOCKET ||
            header->cmsg_type != SCM_RIGHTS) {
            ancillary_valid = false;
            continue;
        }
        const std::size_t header_bytes = CMSG_LEN(0U);
        if (header->cmsg_len < header_bytes) {
            ancillary_valid = false;
            continue;
        }
        const std::size_t payload_bytes =
            header->cmsg_len - header_bytes;
        if (payload_bytes == 0U ||
            payload_bytes % sizeof(int) != 0U) {
            ancillary_valid = false;
            continue;
        }
        const std::size_t count = payload_bytes / sizeof(int);
        for (std::size_t index = 0U; index < count; ++index) {
            int descriptor = -1;
            std::memcpy(
                &descriptor,
                CMSG_DATA(header) + index * sizeof(int),
                sizeof(descriptor));
            ++descriptor_count;
            if (descriptor_count == 1U) {
                received_descriptor->Reset(descriptor);
            } else if (descriptor >= 0) {
                static_cast<void>(::close(descriptor));
            }
        }
    }
    if (!payload_valid || !ancillary_valid) {
        return false;
    }
    return expected_descriptor_count < 0
               ? descriptor_count <= 1U
               : descriptor_count ==
                     static_cast<std::size_t>(
                         expected_descriptor_count);
}

[[nodiscard]] bool PublicRequestCanonical(
    const PartialEventBrokerRequestV2& request) noexcept {
    return request.magic == kPartialEventBrokerRequestMagicV2 &&
           request.protocol_major ==
               kPartialEventBrokerProtocolMajorV2 &&
           request.protocol_minor ==
               kPartialEventBrokerProtocolMinorV2 &&
           request.request_bytes == sizeof(request) &&
           request.reserved0 == 0U && AllZero(request.reserved);
}

[[nodiscard]] bool HandoffRequestCanonical(
    const PartialEventHandoffRequestV2& request) noexcept {
    return request.magic == kPartialEventHandoffRequestMagicV2 &&
           request.protocol_major ==
               kPartialEventBrokerProtocolMajorV2 &&
           request.protocol_minor ==
               kPartialEventBrokerProtocolMinorV2 &&
           request.request_bytes == sizeof(request) &&
           request.reserved0 == 0U && AllZero(request.reserved);
}

[[nodiscard]] ipc::PartialOrderEventJournalSessionV2 HandoffSession(
    const PartialEventHandoffRequestV2& request) noexcept {
    ipc::PartialOrderEventJournalSessionV2 result{};
    result.run_id = NativeIdentity(request.run_id);
    result.session_epoch = request.session_epoch;
    result.trade_date = request.trade_date;
    result.publication_generation = request.publication_generation;
    result.correction_epoch = request.correction_epoch;
    result.coverage_start_unix_ns = request.coverage_start_unix_ns;
    result.ordering_quality = request.ordering_quality;
    result.event_capacity = request.event_capacity;
    result.affected_channel_capacity =
        request.affected_channel_capacity;
    result.order_state_capacity = request.order_state_capacity;
    result.total_mapping_bytes = request.total_mapping_bytes;
    return result;
}

class LifecyclePagePublisher final {
public:
    LifecyclePagePublisher() noexcept = default;
    LifecyclePagePublisher(const LifecyclePagePublisher&) = delete;
    LifecyclePagePublisher& operator=(const LifecyclePagePublisher&) = delete;

    ~LifecyclePagePublisher() {
        if (mapping_ != MAP_FAILED) {
            static_cast<void>(::munmap(
                mapping_, ipc::kPartialEventBrokerLifecycleBytesV2));
        }
    }

    [[nodiscard]] static bool Create(
        const common::Identity128& run_id,
        std::uint64_t session_epoch,
        std::uint32_t trade_date,
        std::chrono::milliseconds heartbeat_timeout,
        std::unique_ptr<LifecyclePagePublisher>* output,
        int* system_error_number) {
        if (output == nullptr) {
            if (system_error_number != nullptr) {
                *system_error_number = EINVAL;
            }
            return false;
        }
        output->reset();
        auto publisher = std::make_unique<LifecyclePagePublisher>();
        if (!publisher->Initialize(
                run_id,
                session_epoch,
                trade_date,
                heartbeat_timeout,
                system_error_number)) {
            return false;
        }
        *output = std::move(publisher);
        return true;
    }

    [[nodiscard]] bool Publish(
        PartialEventBrokerStateV2 state,
        std::uint64_t lifecycle_epoch,
        std::uint64_t publication_generation,
        std::uint64_t correction_epoch,
        pid_t expected_worker) noexcept {
        if (page_ == nullptr || expected_worker < -1 ||
            expected_worker == 0 ||
            ((publication_generation == 0U) !=
             (correction_epoch == 0U))) {
            return false;
        }
        std::uint64_t heartbeat = 0U;
        if (!ReadMonotonicNs(&heartbeat)) {
            return false;
        }
        auto tag = Atomic(page_->commit_tag);
        std::uint64_t stable = tag.load(std::memory_order_acquire);
        if ((stable & 1U) != 0U ||
            stable > std::numeric_limits<std::uint64_t>::max() - 2U ||
            !tag.compare_exchange_strong(
                stable,
                stable + 1U,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return false;
        }
        Atomic(page_->heartbeat_monotonic_ns)
            .store(heartbeat, std::memory_order_relaxed);
        Atomic(page_->lifecycle_epoch)
            .store(lifecycle_epoch, std::memory_order_relaxed);
        Atomic(page_->publication_generation)
            .store(publication_generation, std::memory_order_relaxed);
        Atomic(page_->correction_epoch)
            .store(correction_epoch, std::memory_order_relaxed);
        Atomic(page_->expected_worker)
            .store(
                static_cast<std::int64_t>(expected_worker),
                std::memory_order_relaxed);
        Atomic(page_->broker_state).store(state, std::memory_order_relaxed);
        const bool stale =
            state != PartialEventBrokerStateV2::kReady &&
            state != PartialEventBrokerStateV2::kStoppedClean;
        Atomic(page_->broker_stale)
            .store(stale ? 1U : 0U, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_release);
        tag.store(stable + 2U, std::memory_order_release);
        return true;
    }

    void SignalFailure(
        pid_t worker,
        std::uint64_t lease_epoch) noexcept {
        if (page_ == nullptr || worker <= 0 || lease_epoch == 0U) {
            return;
        }
        const std::uint64_t published_epoch =
            Atomic(page_->lifecycle_epoch)
                .load(std::memory_order_acquire);
        const std::int64_t published_worker =
            Atomic(page_->expected_worker)
                .load(std::memory_order_acquire);
        if (lease_epoch != published_epoch ||
            static_cast<std::int64_t>(worker) != published_worker) {
            return;
        }
        auto failed = Atomic(page_->asynchronously_failed_lease_epoch);
        std::uint64_t observed = failed.load(std::memory_order_relaxed);
        while (observed < lease_epoch &&
               !failed.compare_exchange_weak(
                   observed,
                   lease_epoch,
                   std::memory_order_release,
                   std::memory_order_relaxed)) {
        }
    }

    [[nodiscard]] std::uint64_t failed_lease_epoch() const noexcept {
        return page_ == nullptr
                   ? 0U
                   : Atomic(page_->asynchronously_failed_lease_epoch)
                         .load(std::memory_order_acquire);
    }

    [[nodiscard]] int read_only_descriptor() const noexcept {
        return read_only_fd_.get();
    }

private:
    [[nodiscard]] bool Initialize(
        const common::Identity128& run_id,
        std::uint64_t session_epoch,
        std::uint32_t trade_date,
        std::chrono::milliseconds heartbeat_timeout,
        int* system_error_number) {
        writable_fd_.Reset(::memfd_create(
            "l2flow-partial-event-broker-lifecycle-v2",
            MFD_CLOEXEC | MFD_ALLOW_SEALING));
        if (writable_fd_.get() < 0 ||
            ::ftruncate(
                writable_fd_.get(),
                static_cast<off_t>(
                    ipc::kPartialEventBrokerLifecycleBytesV2)) != 0) {
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return false;
        }
        mapping_ = ::mmap(
            nullptr,
            ipc::kPartialEventBrokerLifecycleBytesV2,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            writable_fd_.get(),
            0);
        if (mapping_ == MAP_FAILED ||
            ::madvise(
                mapping_,
                ipc::kPartialEventBrokerLifecycleBytesV2,
                MADV_DONTFORK) != 0) {
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return false;
        }
        page_ = static_cast<ipc::PartialEventBrokerLifecyclePageV2*>(mapping_);
        *page_ = {};
        page_->run_id = WireIdentity(run_id);
        page_->session_epoch = session_epoch;
        page_->trade_date = trade_date;
        page_->broker_pid = static_cast<std::int64_t>(::getpid());
        const auto timeout_count = heartbeat_timeout.count();
        if (timeout_count <= 0 ||
            static_cast<std::uint64_t>(timeout_count) >
                std::numeric_limits<std::uint64_t>::max() / 1'000'000ULL) {
            if (system_error_number != nullptr) {
                *system_error_number = EOVERFLOW;
            }
            return false;
        }
        page_->heartbeat_timeout_ns =
            static_cast<std::uint64_t>(timeout_count) * 1'000'000ULL;

        constexpr char prefix[] = "/proc/self/fd/";
        std::array<
            char,
            sizeof(prefix) +
                static_cast<std::size_t>(
                    std::numeric_limits<int>::digits10) +
                2U>
            descriptor_path{};
        std::memcpy(
            descriptor_path.data(), prefix, sizeof(prefix) - 1U);
        char* const number_begin =
            descriptor_path.data() + sizeof(prefix) - 1U;
        const auto conversion = std::to_chars(
            number_begin,
            descriptor_path.data() + descriptor_path.size() - 1U,
            writable_fd_.get());
        if (conversion.ec != std::errc{}) {
            if (system_error_number != nullptr) {
                *system_error_number = EOVERFLOW;
            }
            return false;
        }
        *conversion.ptr = '\0';
        read_only_fd_.Reset(
            ::open(descriptor_path.data(), O_RDONLY | O_CLOEXEC));
        if (read_only_fd_.get() < 0) {
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return false;
        }
        constexpr int seals =
            F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_FUTURE_WRITE |
            F_SEAL_SEAL;
        if (::fcntl(writable_fd_.get(), F_ADD_SEALS, seals) != 0) {
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return false;
        }
        return Publish(
            PartialEventBrokerStateV2::kUnavailable,
            0U,
            0U,
            0U,
            -1);
    }

    UniqueFd writable_fd_;
    UniqueFd read_only_fd_;
    void* mapping_ = MAP_FAILED;
    ipc::PartialEventBrokerLifecyclePageV2* page_ = nullptr;
};

}  // namespace

class PartialEventStableBrokerV2::Impl final {
public:
    struct Generation final {
        UniqueFd descriptor;
        ipc::PartialOrderEventJournalSessionV2 session{};
        ipc::PartialOrderEventStatusSnapshotV2 adopted_status{};
        std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    };

    Impl(
        PartialEventStableBrokerConfigV2 config,
        std::unique_ptr<LifecyclePagePublisher> lifecycle,
        BoundSocket public_socket,
        BoundSocket handoff_socket,
        UniqueFd public_wake,
        UniqueFd handoff_wake)
        : config_(std::move(config)),
          lifecycle_(std::move(lifecycle)),
          public_socket_(std::move(public_socket)),
          handoff_socket_(std::move(handoff_socket)),
          public_wake_(std::move(public_wake)),
          handoff_wake_(std::move(handoff_wake)) {}

    ~Impl() {
        Stop();
    }

    [[nodiscard]] bool Start() noexcept {
        try {
            public_thread_ = std::thread([this]() { PublicLoop(); });
            if (handoff_socket_.descriptor.get() >= 0) {
                handoff_thread_ =
                    std::thread([this]() { HandoffLoop(); });
            }
            heartbeat_thread_ =
                std::thread([this]() { HeartbeatLoop(); });
            return true;
        } catch (...) {
            Stop();
            return false;
        }
    }

    void Stop() noexcept {
        if (stopping_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        {
            std::lock_guard lock(mutex_);
            if (state_ != PartialEventBrokerStateV2::kStoppedClean) {
                state_ = generation_ == nullptr
                             ? PartialEventBrokerStateV2::kUnavailable
                             : PartialEventBrokerStateV2::kStale;
                expected_worker_ = -1;
            }
            static_cast<void>(PublishLifecycleLocked());
        }
        heartbeat_cv_.notify_all();
        constexpr std::uint64_t one = 1U;
        if (public_wake_.get() >= 0) {
            const ssize_t wake_result =
                ::write(public_wake_.get(), &one, sizeof(one));
            static_cast<void>(wake_result);
        }
        if (handoff_wake_.get() >= 0) {
            const ssize_t wake_result =
                ::write(handoff_wake_.get(), &one, sizeof(one));
            static_cast<void>(wake_result);
        }
        if (public_thread_.joinable()) {
            public_thread_.join();
        }
        if (handoff_thread_.joinable()) {
            handoff_thread_.join();
        }
        if (heartbeat_thread_.joinable()) {
            heartbeat_thread_.join();
        }
        public_socket_.CleanupPath();
        handoff_socket_.CleanupPath();
    }

    [[nodiscard]] bool MarkWorkerRestarting(
        pid_t worker,
        std::uint64_t* lease_epoch) noexcept {
        if (lease_epoch != nullptr) {
            *lease_epoch = 0U;
        }
        if (worker <= 0) {
            return false;
        }
        std::lock_guard lock(mutex_);
        if (stopping_.load(std::memory_order_acquire) ||
            lifecycle_epoch_ ==
            std::numeric_limits<std::uint64_t>::max()) {
            return false;
        }
        ++lifecycle_epoch_;
        expected_worker_ = worker;
        state_ = PartialEventBrokerStateV2::kRestarting;
        if (!PublishLifecycleLocked()) {
            return false;
        }
        if (lease_epoch != nullptr) {
            *lease_epoch = lifecycle_epoch_;
        }
        return true;
    }

    void SignalWorkerFailed(
        pid_t worker,
        std::uint64_t lease_epoch) noexcept {
        if (!stopping_.load(std::memory_order_acquire) && worker > 0 &&
            lease_epoch != 0U && lifecycle_ != nullptr) {
            lifecycle_->SignalFailure(worker, lease_epoch);
        }
    }

    void MarkWorkerFailed(
        pid_t worker,
        std::uint64_t lease_epoch) noexcept {
        std::lock_guard lock(mutex_);
        if (stopping_.load(std::memory_order_acquire) || worker <= 0 ||
            worker != expected_worker_ ||
            lease_epoch == 0U || lease_epoch != lifecycle_epoch_) {
            return;
        }
        if (lifecycle_epoch_ !=
            std::numeric_limits<std::uint64_t>::max()) {
            ++lifecycle_epoch_;
        }
        expected_worker_ = -1;
        state_ = generation_ == nullptr
                     ? PartialEventBrokerStateV2::kUnavailable
                     : PartialEventBrokerStateV2::kStale;
        static_cast<void>(PublishLifecycleLocked());
    }

    void MarkWorkerStoppedClean(
        pid_t worker,
        std::uint64_t lease_epoch) noexcept {
        std::lock_guard lock(mutex_);
        if (stopping_.load(std::memory_order_acquire) || worker <= 0 ||
            worker != expected_worker_ ||
            lease_epoch == 0U || lease_epoch != lifecycle_epoch_) {
            return;
        }
        if (lifecycle_epoch_ !=
            std::numeric_limits<std::uint64_t>::max()) {
            ++lifecycle_epoch_;
        }
        expected_worker_ = -1;
        if (lifecycle_ != nullptr &&
            lifecycle_->failed_lease_epoch() == lease_epoch) {
            state_ = generation_ == nullptr
                         ? PartialEventBrokerStateV2::kUnavailable
                         : PartialEventBrokerStateV2::kStale;
            static_cast<void>(PublishLifecycleLocked());
            return;
        }
        if (generation_ != nullptr) {
            ipc::PartialOrderEventStatusSnapshotV2 status{};
            if (generation_->reader->ReadStatus(&status) ==
                    ipc::PartialOrderEventReadResultV2::kOk &&
                status.cut.state ==
                    ipc::PartialOrderEventServiceStateV2::
                        kStoppedClean) {
                state_ = PartialEventBrokerStateV2::kStoppedClean;
                static_cast<void>(PublishLifecycleLocked());
                return;
            }
        }
        state_ = generation_ == nullptr
                     ? PartialEventBrokerStateV2::kUnavailable
                     : PartialEventBrokerStateV2::kStale;
        static_cast<void>(PublishLifecycleLocked());
    }

    [[nodiscard]] PartialEventStableBrokerSnapshotV2 Snapshot()
        const noexcept {
        std::lock_guard lock(mutex_);
        PartialEventStableBrokerSnapshotV2 result{};
        result.state = EffectiveStateLocked();
        result.expected_worker = expected_worker_;
        result.lifecycle_epoch = lifecycle_epoch_;
        if (generation_ != nullptr) {
            result.has_generation = true;
            result.publication_generation =
                generation_->session.publication_generation;
            result.correction_epoch =
                generation_->session.correction_epoch;
            result.adopted_commit_sequence =
                generation_->adopted_status.cut.commit_sequence;
            result.adopted_canonical_frontier =
                generation_->adopted_status.cut
                    .canonical_apply_frontier;
            result.adopted_event_frontier =
                generation_->adopted_status.cut
                    .event_published_frontier;
        }
        return result;
    }

    [[nodiscard]] PartialEventBrokerResultV2 AdoptDuplicate(
        int descriptor,
        const ipc::PartialOrderEventJournalSessionV2& session,
        bool correction_promotion) noexcept {
        if (descriptor < 0) {
            return PartialEventBrokerResultV2::kDescriptorRejected;
        }
        std::uint64_t lifecycle_epoch = 0U;
        pid_t expected_worker = -1;
        {
            std::lock_guard lock(mutex_);
            if (stopping_.load(std::memory_order_acquire)) {
                return PartialEventBrokerResultV2::kGenerationRejected;
            }
            lifecycle_epoch = lifecycle_epoch_;
            expected_worker = expected_worker_;
        }
        const int duplicate = ::fcntl(descriptor, F_DUPFD_CLOEXEC, 0);
        if (duplicate < 0) {
            return PartialEventBrokerResultV2::kDescriptorRejected;
        }
        return ValidateAndAdopt(
            UniqueFd(duplicate),
            session,
            lifecycle_epoch,
            expected_worker,
            correction_promotion);
    }

private:
    [[nodiscard]] bool PublishLifecycleLocked() noexcept {
        if (lifecycle_ == nullptr || lifecycle_publication_failed_) {
            return false;
        }
        const std::uint64_t publication_generation =
            generation_ == nullptr
                ? 0U
                : generation_->session.publication_generation;
        const std::uint64_t correction_epoch =
            generation_ == nullptr
                ? 0U
                : generation_->session.correction_epoch;
        if (!lifecycle_->Publish(
                EffectiveStateLocked(),
                lifecycle_epoch_,
                publication_generation,
                correction_epoch,
                expected_worker_)) {
            lifecycle_publication_failed_ = true;
            return false;
        }
        return true;
    }

    [[nodiscard]] PartialEventBrokerStateV2 EffectiveStateLocked()
        const noexcept {
        if (lifecycle_publication_failed_) {
            return generation_ == nullptr
                       ? PartialEventBrokerStateV2::kUnavailable
                       : PartialEventBrokerStateV2::kStale;
        }
        const std::uint64_t failed_lease_epoch =
            lifecycle_ == nullptr
                ? lifecycle_epoch_
                : lifecycle_->failed_lease_epoch();
        if (expected_worker_ > 0 && lifecycle_epoch_ != 0U &&
            failed_lease_epoch == lifecycle_epoch_) {
            return generation_ == nullptr
                       ? PartialEventBrokerStateV2::kUnavailable
                       : PartialEventBrokerStateV2::kStale;
        }
        return state_;
    }

    [[nodiscard]] PartialEventBrokerResultV2 ValidateAndAdopt(
        UniqueFd descriptor,
        const ipc::PartialOrderEventJournalSessionV2& session,
        std::uint64_t lifecycle_epoch,
        pid_t expected_worker,
        bool correction_promotion) noexcept {
        try {
            if (descriptor.get() < 0 ||
                session.run_id != config_.expected_run_id ||
                session.session_epoch !=
                    config_.expected_session_epoch ||
                session.trade_date != config_.expected_trade_date) {
                return PartialEventBrokerResultV2::kSessionMismatch;
            }
            const int descriptor_flags =
                ::fcntl(descriptor.get(), F_GETFL);
            if (descriptor_flags < 0 ||
                (descriptor_flags & O_ACCMODE) != O_RDONLY) {
                return PartialEventBrokerResultV2::kDescriptorRejected;
            }
            struct stat descriptor_status {};
            if (::fstat(descriptor.get(), &descriptor_status) != 0 ||
                !S_ISREG(descriptor_status.st_mode) ||
                descriptor_status.st_size <= 0 ||
                static_cast<std::uint64_t>(
                    descriptor_status.st_size) !=
                    session.total_mapping_bytes ||
                session.total_mapping_bytes >
                    config_.maximum_mapping_bytes) {
                return PartialEventBrokerResultV2::kDescriptorRejected;
            }

            ipc::PartialOrderEventExpectedSessionV2 expected{};
            expected.run_id = session.run_id;
            expected.session_epoch = session.session_epoch;
            expected.trade_date = session.trade_date;
            expected.publication_generation =
                session.publication_generation;
            expected.correction_epoch = session.correction_epoch;
            std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
            int system_error = 0;
            if (ipc::PartialOrderEventReaderV2::OpenDescriptor(
                    descriptor.get(), expected, &reader, &system_error) !=
                    ipc::PartialOrderEventReaderOpenErrorV2::kNone ||
                reader == nullptr) {
                return PartialEventBrokerResultV2::kDescriptorRejected;
            }
            ipc::PartialOrderEventStatusSnapshotV2 status{};
            if (reader->ReadStatus(&status) !=
                ipc::PartialOrderEventReadResultV2::kOk) {
                return PartialEventBrokerResultV2::kDescriptorRejected;
            }
            if (NativeIdentity(status.run_id) != session.run_id ||
                status.session_epoch != session.session_epoch ||
                status.trade_date != session.trade_date ||
                status.publication_generation !=
                    session.publication_generation ||
                status.correction_epoch != session.correction_epoch ||
                status.coverage_start_unix_ns !=
                    session.coverage_start_unix_ns ||
                status.ordering_quality != session.ordering_quality ||
                status.event_capacity != session.event_capacity ||
                status.channel_capacity !=
                    session.affected_channel_capacity ||
                status.order_state_capacity !=
                    session.order_state_capacity) {
                return PartialEventBrokerResultV2::kSessionMismatch;
            }

            auto candidate = std::make_shared<Generation>();
            candidate->descriptor = std::move(descriptor);
            candidate->session = session;
            candidate->adopted_status = status;
            candidate->reader = std::move(reader);

            std::shared_ptr<Generation> previous;
            {
                std::lock_guard lock(mutex_);
                if (stopping_.load(std::memory_order_acquire) ||
                    lifecycle_epoch_ != lifecycle_epoch ||
                    (expected_worker > 0 &&
                     (expected_worker_ != expected_worker ||
                      (lifecycle_ != nullptr &&
                       lifecycle_->failed_lease_epoch() ==
                           lifecycle_epoch)))) {
                    return PartialEventBrokerResultV2::
                        kGenerationRejected;
                }
                previous = generation_;
            }
            if (previous == nullptr) {
                if (correction_promotion) {
                    return PartialEventBrokerResultV2::kGenerationRejected;
                }
            } else {
                const auto& old_session = previous->session;
                ipc::PartialOrderEventStatusSnapshotV2 old_status{};
                if (previous->reader->ReadStatus(&old_status) !=
                    ipc::PartialOrderEventReadResultV2::kOk) {
                    return PartialEventBrokerResultV2::
                        kContinuityRejected;
                }
                const auto& old_cut = old_status.cut;
                const bool same_correction_epoch =
                    session.correction_epoch ==
                    old_session.correction_epoch;
                const bool next_correction_epoch =
                    old_session.correction_epoch !=
                        std::numeric_limits<std::uint64_t>::max() &&
                    session.correction_epoch ==
                        old_session.correction_epoch + 1U;
                if (session.publication_generation <=
                        old_session.publication_generation ||
                    session.coverage_start_unix_ns !=
                        old_session.coverage_start_unix_ns ||
                    session.ordering_quality !=
                        old_session.ordering_quality ||
                    (!same_correction_epoch &&
                     !next_correction_epoch)) {
                    return PartialEventBrokerResultV2::
                        kGenerationRejected;
                }
                // Only an in-epoch publication is an append-only successor.
                // A correction epoch is a full replacement: corrected order
                // state can change the number of derived rows, and this ABI
                // carries no manifest proving that any of these process-local
                // frontiers form an input superset of the prior generation.
                if (same_correction_epoch) {
                    if (correction_promotion ||
                        status.cut.captured_source_frontier <
                            old_cut.captured_source_frontier ||
                        status.cut.canonical_apply_frontier <
                            old_cut.canonical_apply_frontier ||
                        status.cut.event_published_frontier <
                            old_cut.event_published_frontier) {
                        return PartialEventBrokerResultV2::
                            kGenerationRejected;
                    }
                    if (!SamePublishedPrefix(
                            *previous,
                            *candidate,
                            old_cut.event_published_frontier)) {
                        return PartialEventBrokerResultV2::
                            kContinuityRejected;
                    }
                } else {
                    const bool promotion_cut_ready =
                        status.cut.stale == 0U &&
                        status.cut.last_error ==
                            ipc::PartialOrderEventLastErrorV2::kNone &&
                        status.cut.pending_count == 0U &&
                        status.cut.affected_channel_count == 0U &&
                        (status.cut.state ==
                             ipc::PartialOrderEventServiceStateV2::
                                 kContiguous ||
                         status.cut.state ==
                             ipc::PartialOrderEventServiceStateV2::
                                 kStoppedClean);
                    if (!correction_promotion || !promotion_cut_ready) {
                        return PartialEventBrokerResultV2::
                            kGenerationRejected;
                    }
                }
            }

            {
                std::lock_guard lock(mutex_);
                // Handoffs are serialized by one listener thread. The check
                // protects same-process AdoptGeneration racing that listener.
                if (stopping_.load(std::memory_order_acquire) ||
                    generation_ != previous ||
                    lifecycle_epoch_ != lifecycle_epoch ||
                    (expected_worker > 0 &&
                     (expected_worker_ != expected_worker ||
                      (lifecycle_ != nullptr &&
                       lifecycle_->failed_lease_epoch() ==
                           lifecycle_epoch)))) {
                    return PartialEventBrokerResultV2::
                        kGenerationRejected;
                }
                const PartialEventBrokerStateV2 previous_state = state_;
                generation_ = std::move(candidate);
                // Broker state reports worker/mapping lifecycle only. Native
                // reorder, gap, and correction quality remains authoritative
                // in the journal cut returned through the descriptor.
                state_ = status.cut.state ==
                                 ipc::PartialOrderEventServiceStateV2::
                                     kStoppedClean
                             ? PartialEventBrokerStateV2::kStoppedClean
                             : PartialEventBrokerStateV2::kReady;
                if (!PublishLifecycleLocked()) {
                    generation_ = previous;
                    state_ = previous_state;
                    return PartialEventBrokerResultV2::kInternalError;
                }
            }
            return PartialEventBrokerResultV2::kOk;
        } catch (const std::bad_alloc&) {
            return PartialEventBrokerResultV2::kInternalError;
        } catch (...) {
            return PartialEventBrokerResultV2::kInternalError;
        }
    }

    [[nodiscard]] static bool SamePublishedPrefix(
        const Generation& previous,
        const Generation& candidate,
        std::uint64_t frontier) {
        constexpr std::size_t batch_size = 256U;
        std::vector<ipc::PartialOrderEventEnvelopeV2> old_rows(
            batch_size);
        std::vector<ipc::PartialOrderEventEnvelopeV2> new_rows(
            batch_size);
        std::uint64_t next = 1U;
        while (next <= frontier) {
            const std::uint64_t remaining = frontier - next + 1U;
            const std::size_t count =
                remaining < batch_size
                    ? static_cast<std::size_t>(remaining)
                    : batch_size;
            ipc::PartialOrderEventReadBatchResultV2 old_result{};
            ipc::PartialOrderEventReadBatchResultV2 new_result{};
            if (previous.reader->ReadEvents(
                    next,
                    std::span(old_rows).first(count),
                    &old_result) !=
                    ipc::PartialOrderEventReadResultV2::kOk ||
                candidate.reader->ReadEvents(
                    next,
                    std::span(new_rows).first(count),
                    &new_result) !=
                    ipc::PartialOrderEventReadResultV2::kOk ||
                old_result.rows_read != count ||
                new_result.rows_read != count ||
                old_result.next_event_sequence !=
                    next + static_cast<std::uint64_t>(count) ||
                new_result.next_event_sequence !=
                    next + static_cast<std::uint64_t>(count)) {
                return false;
            }
            for (std::size_t index = 0U; index < count; ++index) {
                if (old_rows[index].canonical_apply_sequence !=
                        new_rows[index].canonical_apply_sequence ||
                    std::memcmp(
                        &old_rows[index].event,
                        &new_rows[index].event,
                        sizeof(old_rows[index].event)) != 0) {
                    return false;
                }
            }
            next += static_cast<std::uint64_t>(count);
        }
        return true;
    }

    void PublicLoop() noexcept {
        ServeLoop(
            public_socket_.descriptor.get(),
            public_wake_.get(),
            [this](int client) { ServePublic(client); });
    }

    void HeartbeatLoop() noexcept {
        std::unique_lock heartbeat_lock(heartbeat_mutex_);
        while (!stopping_.load(std::memory_order_acquire)) {
            if (heartbeat_cv_.wait_for(
                    heartbeat_lock,
                    config_.lifecycle_heartbeat_interval,
                    [this]() noexcept {
                        return stopping_.load(std::memory_order_acquire);
                    })) {
                return;
            }
            heartbeat_lock.unlock();
            {
                std::lock_guard lock(mutex_);
                static_cast<void>(PublishLifecycleLocked());
            }
            heartbeat_lock.lock();
        }
    }

    void HandoffLoop() noexcept {
        ServeLoop(
            handoff_socket_.descriptor.get(),
            handoff_wake_.get(),
            [this](int client) { ServeHandoff(client); });
    }

    template <typename Handler>
    void ServeLoop(
        int listener,
        int wake,
        Handler handler) noexcept {
        while (!stopping_.load(std::memory_order_acquire)) {
            std::array<pollfd, 2U> descriptors{{
                {.fd = listener, .events = POLLIN, .revents = 0},
                {.fd = wake, .events = POLLIN, .revents = 0},
            }};
            int result = -1;
            do {
                result = ::poll(
                    descriptors.data(), descriptors.size(), -1);
            } while (result < 0 && errno == EINTR);
            if (result <= 0 || descriptors[1U].revents != 0 ||
                stopping_.load(std::memory_order_acquire)) {
                return;
            }
            if ((descriptors[0U].revents & POLLIN) == 0) {
                continue;
            }
            UniqueFd client(::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC));
            if (client.get() < 0) {
                continue;
            }
            SetSocketTimeout(client.get(), config_.request_timeout);
            handler(client.get());
        }
    }

    void ServePublic(int client) noexcept {
        PartialEventBrokerResponseV2 response{};
        ucred credentials{};
        if (!PeerCredentials(client, &credentials) ||
            credentials.uid != config_.allowed_uid) {
            response.result = PartialEventBrokerResultV2::kPeerRejected;
            static_cast<void>(SendPacket(client, response));
            return;
        }
        PartialEventBrokerRequestV2 request{};
        UniqueFd unexpected_descriptor;
        if (!ReceivePacket(
                client, &request, 0, &unexpected_descriptor) ||
            !PublicRequestCanonical(request)) {
            response.result = PartialEventBrokerResultV2::kProtocolError;
            static_cast<void>(SendPacket(client, response));
            return;
        }
        if (NativeIdentity(request.run_id) !=
                config_.expected_run_id ||
            request.session_epoch != config_.expected_session_epoch ||
            request.trade_date != config_.expected_trade_date) {
            response.result = PartialEventBrokerResultV2::kSessionMismatch;
            static_cast<void>(SendPacket(client, response));
            return;
        }

        std::shared_ptr<Generation> generation;
        {
            std::lock_guard lock(mutex_);
            generation = generation_;
            response.broker_state = EffectiveStateLocked();
            response.broker_stale =
                response.broker_state ==
                            PartialEventBrokerStateV2::kReady ||
                        response.broker_state ==
                            PartialEventBrokerStateV2::kStoppedClean
                    ? 0U
                    : 1U;
        }
        if (generation == nullptr ||
            generation->session.publication_generation <
                request.minimum_publication_generation) {
            response.result = PartialEventBrokerResultV2::kUnavailable;
            static_cast<void>(SendPacket(client, response));
            return;
        }
        response.result = PartialEventBrokerResultV2::kOk;
        response.descriptor_count = 2U;
        response.run_id = WireIdentity(generation->session.run_id);
        response.session_epoch = generation->session.session_epoch;
        response.publication_generation =
            generation->session.publication_generation;
        response.correction_epoch =
            generation->session.correction_epoch;
        response.adopted_commit_sequence =
            generation->adopted_status.cut.commit_sequence;
        response.adopted_canonical_frontier =
            generation->adopted_status.cut.canonical_apply_frontier;
        response.adopted_event_frontier =
            generation->adopted_status.cut.event_published_frontier;
        response.trade_date = generation->session.trade_date;
        response.ordering_quality = generation->session.ordering_quality;
        static_cast<void>(SendPacket(
            client,
            response,
            generation->descriptor.get(),
            lifecycle_ == nullptr
                ? -1
                : lifecycle_->read_only_descriptor()));
    }

    void ServeHandoff(int client) noexcept {
        PartialEventHandoffResponseV2 response{};
        ucred credentials{};
        pid_t expected_worker = -1;
        std::uint64_t lifecycle_epoch = 0U;
        {
            std::lock_guard lock(mutex_);
            expected_worker = expected_worker_;
            lifecycle_epoch = lifecycle_epoch_;
        }
        if (!PeerCredentials(client, &credentials) ||
            credentials.uid != config_.allowed_uid ||
            credentials.pid != expected_worker) {
            response.result = PartialEventBrokerResultV2::kPeerRejected;
            static_cast<void>(SendPacket(client, response));
            return;
        }
        PartialEventHandoffRequestV2 request{};
        UniqueFd descriptor;
        if (!ReceivePacket(client, &request, 1, &descriptor) ||
            !HandoffRequestCanonical(request)) {
            response.result = PartialEventBrokerResultV2::kProtocolError;
            static_cast<void>(SendPacket(client, response));
            return;
        }
        response.result = ValidateAndAdopt(
            std::move(descriptor),
            HandoffSession(request),
            lifecycle_epoch,
            credentials.pid,
            false);
        if (response.result == PartialEventBrokerResultV2::kOk) {
            const PartialEventStableBrokerSnapshotV2 snapshot = Snapshot();
            response.accepted_publication_generation =
                snapshot.publication_generation;
            response.accepted_correction_epoch =
                snapshot.correction_epoch;
        }
        static_cast<void>(SendPacket(client, response));
    }

    PartialEventStableBrokerConfigV2 config_;
    std::unique_ptr<LifecyclePagePublisher> lifecycle_;
    BoundSocket public_socket_;
    BoundSocket handoff_socket_;
    UniqueFd public_wake_;
    UniqueFd handoff_wake_;
    std::atomic<bool> stopping_{false};
    std::thread public_thread_;
    std::thread handoff_thread_;
    std::thread heartbeat_thread_;
    std::mutex heartbeat_mutex_;
    std::condition_variable heartbeat_cv_;
    mutable std::mutex mutex_;
    std::shared_ptr<Generation> generation_;
    PartialEventBrokerStateV2 state_ =
        PartialEventBrokerStateV2::kUnavailable;
    pid_t expected_worker_ = -1;
    std::uint64_t lifecycle_epoch_ = 0U;
    bool lifecycle_publication_failed_ = false;
};

std::string_view PartialEventStableBrokerCreateErrorNameV2(
    PartialEventStableBrokerCreateErrorV2 error) noexcept {
    switch (error) {
        case PartialEventStableBrokerCreateErrorV2::kNone:
            return "none";
        case PartialEventStableBrokerCreateErrorV2::kNullOutput:
            return "null_output";
        case PartialEventStableBrokerCreateErrorV2::
            kInvalidConfiguration:
            return "invalid_configuration";
        case PartialEventStableBrokerCreateErrorV2::
            kLifecyclePageCreateFailed:
            return "lifecycle_page_create_failed";
        case PartialEventStableBrokerCreateErrorV2::
            kPublicSocketCreateFailed:
            return "public_socket_create_failed";
        case PartialEventStableBrokerCreateErrorV2::
            kHandoffSocketCreateFailed:
            return "handoff_socket_create_failed";
        case PartialEventStableBrokerCreateErrorV2::kThreadCreateFailed:
            return "thread_create_failed";
        case PartialEventStableBrokerCreateErrorV2::kResourceExhausted:
            return "resource_exhausted";
        case PartialEventStableBrokerCreateErrorV2::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "unknown";
}

PartialEventStableBrokerV2::PartialEventStableBrokerV2(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

PartialEventStableBrokerV2::~PartialEventStableBrokerV2() = default;

PartialEventStableBrokerCreateErrorV2
PartialEventStableBrokerV2::Create(
    PartialEventStableBrokerConfigV2 config,
    std::unique_ptr<PartialEventStableBrokerV2>* output,
    int* system_error_number) noexcept {
    if (output == nullptr) {
        return PartialEventStableBrokerCreateErrorV2::kNullOutput;
    }
    output->reset();
    if (system_error_number != nullptr) {
        *system_error_number = 0;
    }
    try {
        sockaddr_un public_address{};
        sockaddr_un handoff_address{};
        socklen_t public_size = 0;
        socklen_t handoff_size = 0;
        const bool handoff_enabled =
            !config.worker_handoff_socket_path.empty();
        if ((handoff_enabled &&
             config.public_socket_path ==
                 config.worker_handoff_socket_path) ||
            !SocketAddress(
                config.public_socket_path,
                &public_address,
                &public_size) ||
            (handoff_enabled &&
             !SocketAddress(
                 config.worker_handoff_socket_path,
                 &handoff_address,
                 &handoff_size)) ||
            common::IsZeroIdentity(config.expected_run_id) ||
            config.expected_session_epoch == 0U ||
            !ValidTradeDate(config.expected_trade_date) ||
            config.maximum_mapping_bytes <
                ipc::kPartialOrderEventHeaderBytesV2 ||
            config.allowed_uid == static_cast<uid_t>(-1) ||
            config.listen_backlog == 0U ||
            !ValidTimeout(config.request_timeout) ||
            !ValidLifecycleTiming(
                config.lifecycle_heartbeat_interval,
                config.lifecycle_heartbeat_timeout)) {
            return PartialEventStableBrokerCreateErrorV2::
                kInvalidConfiguration;
        }
        std::unique_ptr<LifecyclePagePublisher> lifecycle;
        if (!LifecyclePagePublisher::Create(
                config.expected_run_id,
                config.expected_session_epoch,
                config.expected_trade_date,
                config.lifecycle_heartbeat_timeout,
                &lifecycle,
                system_error_number) ||
            lifecycle == nullptr) {
            return PartialEventStableBrokerCreateErrorV2::
                kLifecyclePageCreateFailed;
        }
        BoundSocket public_socket;
        if (!CreateListener(
                config.public_socket_path,
                config.listen_backlog,
                &public_socket,
                system_error_number)) {
            return PartialEventStableBrokerCreateErrorV2::
                kPublicSocketCreateFailed;
        }
        BoundSocket handoff_socket;
        if (handoff_enabled &&
            !CreateListener(
                config.worker_handoff_socket_path,
                config.listen_backlog,
                &handoff_socket,
                system_error_number)) {
            return PartialEventStableBrokerCreateErrorV2::
                kHandoffSocketCreateFailed;
        }
        UniqueFd public_wake(::eventfd(0U, EFD_CLOEXEC | EFD_NONBLOCK));
        UniqueFd handoff_wake(
            handoff_enabled
                ? ::eventfd(0U, EFD_CLOEXEC | EFD_NONBLOCK)
                : -1);
        if (public_wake.get() < 0 ||
            (handoff_enabled && handoff_wake.get() < 0)) {
            if (system_error_number != nullptr) {
                *system_error_number = errno;
            }
            return PartialEventStableBrokerCreateErrorV2::
                kThreadCreateFailed;
        }
        auto impl = std::make_unique<Impl>(
            std::move(config),
            std::move(lifecycle),
            std::move(public_socket),
            std::move(handoff_socket),
            std::move(public_wake),
            std::move(handoff_wake));
        if (!impl->Start()) {
            return PartialEventStableBrokerCreateErrorV2::
                kThreadCreateFailed;
        }
        *output = std::unique_ptr<PartialEventStableBrokerV2>(
            new PartialEventStableBrokerV2(std::move(impl)));
        return PartialEventStableBrokerCreateErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return PartialEventStableBrokerCreateErrorV2::kResourceExhausted;
    } catch (...) {
        return PartialEventStableBrokerCreateErrorV2::kUnexpectedFailure;
    }
}

PartialEventBrokerResultV2 PartialEventStableBrokerV2::AdoptGeneration(
    int descriptor,
    const ipc::PartialOrderEventJournalSessionV2& session) noexcept {
    return impl_ == nullptr
               ? PartialEventBrokerResultV2::kInternalError
               : impl_->AdoptDuplicate(descriptor, session, false);
}

PartialEventBrokerResultV2
PartialEventStableBrokerV2::PromoteCorrectedGeneration(
    int descriptor,
    const ipc::PartialOrderEventJournalSessionV2& session) noexcept {
    return impl_ == nullptr
               ? PartialEventBrokerResultV2::kInternalError
               : impl_->AdoptDuplicate(descriptor, session, true);
}

bool PartialEventStableBrokerV2::MarkWorkerRestarting(
    pid_t expected_worker,
    std::uint64_t* lease_epoch) noexcept {
    return impl_ != nullptr &&
           impl_->MarkWorkerRestarting(expected_worker, lease_epoch);
}

void PartialEventStableBrokerV2::SignalWorkerFailed(
    pid_t worker,
    std::uint64_t lease_epoch) noexcept {
    if (impl_ != nullptr) {
        impl_->SignalWorkerFailed(worker, lease_epoch);
    }
}

void PartialEventStableBrokerV2::MarkWorkerFailed(
    pid_t worker,
    std::uint64_t lease_epoch) noexcept {
    if (impl_ != nullptr) {
        impl_->MarkWorkerFailed(worker, lease_epoch);
    }
}

void PartialEventStableBrokerV2::MarkWorkerStoppedClean(
    pid_t worker,
    std::uint64_t lease_epoch) noexcept {
    if (impl_ != nullptr) {
        impl_->MarkWorkerStoppedClean(worker, lease_epoch);
    }
}

PartialEventBrokerStateV2 PartialEventStableBrokerV2::state()
    const noexcept {
    return Snapshot().state;
}

std::uint64_t PartialEventStableBrokerV2::publication_generation()
    const noexcept {
    return Snapshot().publication_generation;
}

std::uint64_t PartialEventStableBrokerV2::correction_epoch()
    const noexcept {
    return Snapshot().correction_epoch;
}

PartialEventStableBrokerSnapshotV2 PartialEventStableBrokerV2::Snapshot()
    const noexcept {
    return impl_ == nullptr ? PartialEventStableBrokerSnapshotV2{}
                            : impl_->Snapshot();
}

void PartialEventStableBrokerV2::Stop() noexcept {
    if (impl_ != nullptr) {
        impl_->Stop();
    }
}

}  // namespace l2flow::apps
