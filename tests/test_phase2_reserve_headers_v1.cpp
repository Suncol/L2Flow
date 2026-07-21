#include "l2flow/common/crc32c.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/reserve_headers_v1.h"
#include "l2flow/ingress/reserve_state_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace common = l2flow::common;
namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(
        bool condition,
        std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    int failures = 0;
};

static_assert(ingress::kReserveHeadersV1Version == 1U);
static_assert(
    ingress::kReserveHeadersV1LittleEndian == 1U);
static_assert(ingress::kReserveHeaderV1Bytes == 4096U);
static_assert(
    ingress::kReserveInodeV1MaxCount == 100000000U);
static_assert(
    ingress::kReserveInodeV1MaxCount ==
    ingress::kReserveStateV1MaxInodeReserveCount);
static_assert(
    ingress::kReserveInodeFilenameV1Bytes == 55U);
static_assert(
    ingress::reserve_headers_v1_offset::common::
        kReserveStateUuid == 16U);
static_assert(
    ingress::reserve_headers_v1_offset::common::
        kDeviceId == 32U);
static_assert(
    ingress::reserve_headers_v1_offset::common::
        kQuotaIdentitySha256 == 40U);
static_assert(
    ingress::reserve_headers_v1_offset::common::
        kMountIdentitySha256 == 72U);
static_assert(
    ingress::reserve_headers_v1_offset::file::
        kDeclaredBytes == 104U);
static_assert(
    ingress::reserve_headers_v1_offset::file::
        kHeaderCrc32c == 112U);
static_assert(
    ingress::reserve_headers_v1_offset::file::
        kReservedTail == 116U);
static_assert(
    ingress::reserve_headers_v1_offset::inode::kIndex ==
    104U);
static_assert(
    ingress::reserve_headers_v1_offset::inode::kCount ==
    108U);
static_assert(
    ingress::reserve_headers_v1_offset::inode::
        kHeaderCrc32c == 112U);

ingress::ReserveAllocationPoolIdentityV1 GoldenPool() {
    ingress::ReserveAllocationPoolIdentityV1 pool{};
    for (std::size_t index = 0U;
         index < pool.reserve_state_uuid.size();
         ++index) {
        pool.reserve_state_uuid[index] =
            static_cast<std::byte>(index + 1U);
    }
    pool.device_id = 0x0102030405060708ULL;
    for (std::size_t index = 0U;
         index < pool.quota_identity_sha256.size();
         ++index) {
        pool.quota_identity_sha256[index] =
            static_cast<std::byte>(0x20U + index);
        pool.mount_identity_sha256[index] =
            static_cast<std::byte>(0x40U + index);
    }
    return pool;
}

ingress::ReserveFileHeaderV1 GoldenFileHeader() {
    ingress::ReserveFileHeaderV1 header{};
    header.pool = GoldenPool();
    header.declared_bytes =
        0x0102030405061000ULL;
    return header;
}

ingress::ReserveInodeHeaderV1 GoldenInodeHeader() {
    ingress::ReserveInodeHeaderV1 header{};
    header.pool = GoldenPool();
    header.inode_index = 42U;
    header.declared_inode_reserve_count = 1000U;
    return header;
}

std::uint32_t LoadU32Le(
    const ingress::ReserveHeaderWireV1& wire,
    std::size_t offset) {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        value |=
            std::to_integer<std::uint32_t>(
                wire[offset + index])
            << static_cast<unsigned int>(index * 8U);
    }
    return value;
}

void StoreU16Le(
    std::uint16_t value,
    ingress::ReserveHeaderWireV1* wire,
    std::size_t offset) {
    (*wire)[offset] =
        static_cast<std::byte>(value & 0xffU);
    (*wire)[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32Le(
    std::uint32_t value,
    ingress::ReserveHeaderWireV1* wire,
    std::size_t offset) {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        (*wire)[offset + index] =
            static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(index * 8U)) &
                0xffU);
    }
}

void StoreU64Le(
    std::uint64_t value,
    ingress::ReserveHeaderWireV1* wire,
    std::size_t offset) {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        (*wire)[offset + index] =
            static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(index * 8U)) &
                0xffU);
    }
}

void RecomputeCrc(
    ingress::ReserveHeaderWireV1* wire) {
    constexpr std::size_t crc_offset =
        ingress::reserve_headers_v1_offset::file::
            kHeaderCrc32c;
    std::fill_n(
        wire->begin() + crc_offset,
        sizeof(std::uint32_t),
        std::byte{0});
    StoreU32Le(
        common::ComputeCrc32c(*wire),
        wire,
        crc_offset);
}

void TestSchema(
    const std::filesystem::path& schema,
    TestContext* test) {
    std::error_code size_error;
    const std::uintmax_t size =
        std::filesystem::file_size(
            schema,
            size_error);
    test->Expect(
        !size_error &&
            size ==
                ingress::
                    kReserveHeadersV1SchemaBytes,
        "reserve header schema byte length is frozen");

    common::Sha256Digest actual{};
    std::string hash_error;
    test->Expect(
        common::ComputeFileSha256(
            schema,
            &actual,
            &hash_error),
        "reserve header schema exact file can be hashed");
    test->Expect(
        actual ==
            ingress::
                kReserveHeadersV1SchemaSha256,
        "reserve header schema binary digest is frozen");
    test->Expect(
        common::Sha256Hex(actual) ==
            ingress::
                kReserveHeadersV1SchemaSha256Hex,
        "reserve header schema hex digest is frozen");
    test->Expect(
        &ingress::
             ReserveHeadersV1SchemaSha256Digest() ==
            &ingress::
                kReserveHeadersV1SchemaSha256,
        "schema digest API returns the frozen object");
    test->Expect(
        ingress::
                ReserveHeadersV1SchemaSha256Hex() ==
            ingress::
                kReserveHeadersV1SchemaSha256Hex,
        "schema hex API returns the frozen value");
    std::string verify_error;
    test->Expect(
        ingress::
            VerifyReserveHeadersV1SchemaFile(
                schema,
                &verify_error) &&
            verify_error.empty(),
        "secure schema verifier accepts exact bytes");
}

void TestFileHeader(
    TestContext* test,
    ingress::ReserveHeaderWireV1* golden_wire) {
    const ingress::ReserveFileHeaderV1 header =
        GoldenFileHeader();
    test->Expect(
        ingress::EncodeReserveFileHeaderV1(
            header,
            golden_wire) ==
            ingress::ReserveHeaderV1Error::kNone,
        "reserve data header encodes");
    test->Expect(
        std::equal(
            ingress::kReserveFileHeaderV1Magic.begin(),
            ingress::kReserveFileHeaderV1Magic.end(),
            golden_wire->begin()),
        "reserve data header magic is exact");
    test->Expect(
        (*golden_wire)[8U] == std::byte{1U} &&
            (*golden_wire)[9U] == std::byte{0U} &&
            (*golden_wire)[10U] == std::byte{1U} &&
            (*golden_wire)[12U] == std::byte{0U} &&
            (*golden_wire)[13U] == std::byte{0x10U} &&
            (*golden_wire)[14U] == std::byte{0U} &&
            (*golden_wire)[15U] == std::byte{0U},
        "version/endian/header size are explicit little-endian");
    test->Expect(
        (*golden_wire)[32U] == std::byte{0x08U} &&
            (*golden_wire)[39U] == std::byte{0x01U},
        "device identity is explicit little-endian");
    test->Expect(
        (*golden_wire)[104U] == std::byte{0x00U} &&
            (*golden_wire)[105U] == std::byte{0x10U} &&
            (*golden_wire)[111U] == std::byte{0x01U},
        "declared reserve bytes are explicit little-endian");

    ingress::ReserveHeaderWireV1 zeroed = *golden_wire;
    std::fill_n(
        zeroed.begin() +
            ingress::reserve_headers_v1_offset::file::
                kHeaderCrc32c,
        sizeof(std::uint32_t),
        std::byte{0});
    test->Expect(
        LoadU32Le(
            *golden_wire,
            ingress::reserve_headers_v1_offset::file::
                kHeaderCrc32c) ==
            common::ComputeCrc32c(zeroed),
        "reserve data header CRC covers all 4096 bytes");

    const std::string digest =
        common::Sha256Hex(
            common::ComputeSha256(*golden_wire));
    if (digest !=
        ingress::
            kReserveFileHeaderV1GoldenSha256Hex) {
        std::cerr
            << "INFO: reserve data header golden SHA-256: "
            << digest << '\n';
    }
    test->Expect(
        digest ==
            ingress::
                kReserveFileHeaderV1GoldenSha256Hex,
        "reserve data header complete-byte golden is frozen");

    ingress::ReserveFileHeaderV1 decoded{};
    test->Expect(
        ingress::DecodeReserveFileHeaderV1(
            *golden_wire,
            &decoded) ==
                ingress::ReserveHeaderV1Error::kNone &&
            decoded == header,
        "reserve data header round-trips");
    test->Expect(
        ingress::ValidateReserveFileHeaderV1(
            *golden_wire) ==
            ingress::ReserveHeaderV1Error::kNone,
        "reserve data header strict validator accepts golden");
    test->Expect(
        ingress::
            ValidateReserveFileHeaderBindingV1(
                decoded,
                header.pool,
                header.declared_bytes) ==
            ingress::ReserveHeaderV1Error::kNone,
        "reserve data header binds exact pool and st_size");

    std::array<
        ingress::ReserveAllocationPoolIdentityV1,
        4U>
        wrong_pools{
            header.pool,
            header.pool,
            header.pool,
            header.pool};
    wrong_pools[0U].reserve_state_uuid[0U] ^=
        std::byte{1U};
    wrong_pools[1U].device_id ^= 1U;
    wrong_pools[2U].quota_identity_sha256[0U] ^=
        std::byte{1U};
    wrong_pools[3U].mount_identity_sha256[0U] ^=
        std::byte{1U};
    bool every_pool_mismatch_rejected = true;
    for (const auto& wrong_pool : wrong_pools) {
        every_pool_mismatch_rejected =
            every_pool_mismatch_rejected &&
            ingress::
                ValidateReserveFileHeaderBindingV1(
                    decoded,
                    wrong_pool,
                    header.declared_bytes) ==
                ingress::ReserveHeaderV1Error::
                    kIdentityMismatch;
    }
    test->Expect(
        every_pool_mismatch_rejected,
        "reserve data header rejects UUID/device/quota/mount mismatches");
    test->Expect(
        ingress::
            ValidateReserveFileHeaderBindingV1(
                decoded,
                header.pool,
                header.declared_bytes + 1U) ==
            ingress::ReserveHeaderV1Error::
                kDeclaredBytesMismatch,
        "reserve data header rejects st_size mismatch");

    ingress::ReserveFileHeaderV1 sentinel =
        header;
    sentinel.declared_bytes += 1U;
    const ingress::ReserveFileHeaderV1 before = sentinel;
    test->Expect(
        ingress::DecodeReserveFileHeaderV1(
            std::span<const std::byte>(
                golden_wire->data(),
                golden_wire->size() - 1U),
            &sentinel) ==
                ingress::ReserveHeaderV1Error::
                    kInvalidWireSize &&
            sentinel == before,
        "failed reserve data decode leaves output unchanged");
}

void TestInodeHeader(
    TestContext* test,
    ingress::ReserveHeaderWireV1* golden_wire) {
    const ingress::ReserveInodeHeaderV1 header =
        GoldenInodeHeader();
    test->Expect(
        ingress::EncodeReserveInodeHeaderV1(
            header,
            golden_wire) ==
            ingress::ReserveHeaderV1Error::kNone,
        "reserve inode header encodes");
    test->Expect(
        std::equal(
            ingress::kReserveInodeHeaderV1Magic.begin(),
            ingress::kReserveInodeHeaderV1Magic.end(),
            golden_wire->begin()),
        "reserve inode header magic is exact");
    test->Expect(
        (*golden_wire)[104U] == std::byte{42U} &&
            (*golden_wire)[105U] == std::byte{0U} &&
            (*golden_wire)[108U] == std::byte{0xe8U} &&
            (*golden_wire)[109U] == std::byte{0x03U},
        "inode index/count are explicit little-endian");

    const std::string digest =
        common::Sha256Hex(
            common::ComputeSha256(*golden_wire));
    if (digest !=
        ingress::
            kReserveInodeHeaderV1GoldenSha256Hex) {
        std::cerr
            << "INFO: reserve inode header golden SHA-256: "
            << digest << '\n';
    }
    test->Expect(
        digest ==
            ingress::
                kReserveInodeHeaderV1GoldenSha256Hex,
        "reserve inode header complete-byte golden is frozen");

    ingress::ReserveInodeHeaderV1 decoded{};
    test->Expect(
        ingress::DecodeReserveInodeHeaderV1(
            *golden_wire,
            &decoded) ==
                ingress::ReserveHeaderV1Error::kNone &&
            decoded == header,
        "reserve inode header round-trips");
    test->Expect(
        ingress::ValidateReserveInodeHeaderV1(
            *golden_wire) ==
            ingress::ReserveHeaderV1Error::kNone,
        "reserve inode strict validator accepts golden");

    std::string filename;
    test->Expect(
        ingress::FormatReserveInodeFilenameV1(
            header.pool.reserve_state_uuid,
            header.inode_index,
            &filename) ==
                ingress::ReserveHeaderV1Error::kNone &&
            filename ==
                "inode-0102030405060708090a0b0c0d0e0f10-"
                "00000042.reserve",
        "reserve inode canonical filename is exact");
    test->Expect(
        ingress::
            ValidateReserveInodeHeaderBindingV1(
                decoded,
                header.pool,
                header.declared_inode_reserve_count,
                filename) ==
            ingress::ReserveHeaderV1Error::kNone,
        "reserve inode binds exact pool/count/name");
    test->Expect(
        ingress::
            ValidateReserveInodeHeaderBindingV1(
                decoded,
                header.pool,
                header.declared_inode_reserve_count + 1U,
                filename) ==
            ingress::ReserveHeaderV1Error::
                kInodeCountMismatch,
        "reserve inode rejects immutable inventory count mismatch");
    std::string wrong_filename = filename;
    wrong_filename[45U] = '3';
    test->Expect(
        ingress::
            ValidateReserveInodeHeaderBindingV1(
                decoded,
                header.pool,
                header.declared_inode_reserve_count,
                wrong_filename) ==
            ingress::ReserveHeaderV1Error::
                kFilenameMismatch,
        "reserve inode rejects header/name index mismatch");
}

void TestLogicalRejections(TestContext* test) {
    ingress::ReserveHeaderWireV1 unused{};

    ingress::ReserveFileHeaderV1 file =
        GoldenFileHeader();
    file.pool.reserve_state_uuid.fill(std::byte{0});
    test->Expect(
        ingress::EncodeReserveFileHeaderV1(
            file,
            &unused) ==
            ingress::ReserveHeaderV1Error::
                kInvalidReserveUuid,
        "zero reserve UUID is rejected");
    file = GoldenFileHeader();
    file.pool.device_id = 0U;
    test->Expect(
        ingress::EncodeReserveFileHeaderV1(
            file,
            &unused) ==
            ingress::ReserveHeaderV1Error::
                kInvalidDeviceIdentity,
        "zero device identity is rejected");
    file = GoldenFileHeader();
    file.pool.quota_identity_sha256.fill(std::byte{0});
    test->Expect(
        ingress::EncodeReserveFileHeaderV1(
            file,
            &unused) ==
            ingress::ReserveHeaderV1Error::
                kInvalidQuotaIdentity,
        "zero quota identity is rejected");
    file = GoldenFileHeader();
    file.pool.mount_identity_sha256.fill(std::byte{0});
    test->Expect(
        ingress::EncodeReserveFileHeaderV1(
            file,
            &unused) ==
            ingress::ReserveHeaderV1Error::
                kInvalidMountIdentity,
        "zero mount identity is rejected");
    file = GoldenFileHeader();
    file.declared_bytes =
        ingress::kReserveHeaderV1Bytes - 1U;
    test->Expect(
        ingress::EncodeReserveFileHeaderV1(
            file,
            &unused) ==
            ingress::ReserveHeaderV1Error::
                kInvalidDeclaredBytes,
        "declared bytes smaller than the header are rejected");

    ingress::ReserveInodeHeaderV1 inode =
        GoldenInodeHeader();
    inode.declared_inode_reserve_count = 0U;
    test->Expect(
        ingress::EncodeReserveInodeHeaderV1(
            inode,
            &unused) ==
            ingress::ReserveHeaderV1Error::
                kInvalidInodeCount,
        "zero inode count is rejected");
    inode = GoldenInodeHeader();
    inode.declared_inode_reserve_count =
        ingress::kReserveInodeV1MaxCount + 1U;
    test->Expect(
        ingress::EncodeReserveInodeHeaderV1(
            inode,
            &unused) ==
            ingress::ReserveHeaderV1Error::
                kInvalidInodeCount,
        "inode count above 100000000 is rejected");
    inode = GoldenInodeHeader();
    inode.inode_index =
        inode.declared_inode_reserve_count;
    test->Expect(
        ingress::EncodeReserveInodeHeaderV1(
            inode,
            &unused) ==
            ingress::ReserveHeaderV1Error::
                kInvalidInodeIndex,
        "inode index equal to count is rejected");

    inode = GoldenInodeHeader();
    inode.declared_inode_reserve_count =
        ingress::kReserveInodeV1MaxCount;
    inode.inode_index =
        ingress::kReserveInodeV1MaxCount - 1U;
    test->Expect(
        ingress::EncodeReserveInodeHeaderV1(
            inode,
            &unused) ==
            ingress::ReserveHeaderV1Error::kNone,
        "maximum count and terminal index are accepted");
}

void TestCrcConsistentRejections(
    const ingress::ReserveHeaderWireV1& file_golden,
    const ingress::ReserveHeaderWireV1& inode_golden,
    TestContext* test) {
    ingress::ReserveHeaderWireV1 corrupted = file_golden;
    corrupted[
        ingress::reserve_headers_v1_offset::common::
            kReserved0] = std::byte{1U};
    RecomputeCrc(&corrupted);
    test->Expect(
        ingress::ValidateReserveFileHeaderV1(
            corrupted) ==
            ingress::ReserveHeaderV1Error::
                kNonzeroReserved,
        "CRC-consistent common reserved byte is rejected");

    corrupted = file_golden;
    corrupted[
        ingress::reserve_headers_v1_offset::file::
            kReservedTail] = std::byte{1U};
    RecomputeCrc(&corrupted);
    test->Expect(
        ingress::ValidateReserveFileHeaderV1(
            corrupted) ==
            ingress::ReserveHeaderV1Error::
                kNonzeroReserved,
        "CRC-consistent data reserved tail is rejected");

    corrupted = file_golden;
    std::fill_n(
        corrupted.begin() +
            ingress::reserve_headers_v1_offset::common::
                kReserveStateUuid,
        16U,
        std::byte{0});
    RecomputeCrc(&corrupted);
    test->Expect(
        ingress::ValidateReserveFileHeaderV1(
            corrupted) ==
            ingress::ReserveHeaderV1Error::
                kInvalidReserveUuid,
        "CRC-consistent zero reserve UUID is rejected");

    corrupted = file_golden;
    StoreU64Le(
        ingress::kReserveHeaderV1Bytes - 1U,
        &corrupted,
        ingress::reserve_headers_v1_offset::file::
            kDeclaredBytes);
    RecomputeCrc(&corrupted);
    test->Expect(
        ingress::ValidateReserveFileHeaderV1(
            corrupted) ==
            ingress::ReserveHeaderV1Error::
                kInvalidDeclaredBytes,
        "CRC-consistent undersized declaration is rejected");

    corrupted = file_golden;
    StoreU16Le(
        2U,
        &corrupted,
        ingress::reserve_headers_v1_offset::common::
            kVersion);
    RecomputeCrc(&corrupted);
    test->Expect(
        ingress::ValidateReserveFileHeaderV1(
            corrupted) ==
            ingress::ReserveHeaderV1Error::
                kUnsupportedVersion,
        "CRC-consistent unknown version is rejected");

    corrupted = file_golden;
    corrupted[
        ingress::reserve_headers_v1_offset::common::
            kEndian] = std::byte{2U};
    RecomputeCrc(&corrupted);
    test->Expect(
        ingress::ValidateReserveFileHeaderV1(
            corrupted) ==
            ingress::ReserveHeaderV1Error::
                kInvalidEndian,
        "CRC-consistent unknown endian is rejected");

    corrupted = inode_golden;
    corrupted[
        ingress::reserve_headers_v1_offset::inode::
            kReservedTail] = std::byte{1U};
    RecomputeCrc(&corrupted);
    test->Expect(
        ingress::ValidateReserveInodeHeaderV1(
            corrupted) ==
            ingress::ReserveHeaderV1Error::
                kNonzeroReserved,
        "CRC-consistent inode reserved tail is rejected");

    corrupted = inode_golden;
    StoreU32Le(
        0U,
        &corrupted,
        ingress::reserve_headers_v1_offset::inode::
            kCount);
    RecomputeCrc(&corrupted);
    test->Expect(
        ingress::ValidateReserveInodeHeaderV1(
            corrupted) ==
            ingress::ReserveHeaderV1Error::
                kInvalidInodeCount,
        "CRC-consistent zero inode count is rejected");

    corrupted = inode_golden;
    StoreU32Le(
        1000U,
        &corrupted,
        ingress::reserve_headers_v1_offset::inode::
            kIndex);
    RecomputeCrc(&corrupted);
    test->Expect(
        ingress::ValidateReserveInodeHeaderV1(
            corrupted) ==
            ingress::ReserveHeaderV1Error::
                kInvalidInodeIndex,
        "CRC-consistent index equal to count is rejected");

    test->Expect(
        ingress::ValidateReserveInodeHeaderV1(
            file_golden) ==
            ingress::ReserveHeaderV1Error::
                kInvalidMagic &&
            ingress::ValidateReserveFileHeaderV1(
                inode_golden) ==
                ingress::ReserveHeaderV1Error::
                    kInvalidMagic,
        "data and inode header types cannot be confused");
}

template <typename Validator>
void TestEverySingleBitFlip(
    const ingress::ReserveHeaderWireV1& golden,
    Validator validator,
    std::string_view label,
    TestContext* test) {
    ingress::ReserveHeaderWireV1 corrupted = golden;
    bool all_rejected = true;
    for (std::size_t byte_index = 0U;
         byte_index < corrupted.size() && all_rejected;
         ++byte_index) {
        for (unsigned int bit = 0U;
             bit < 8U;
             ++bit) {
            corrupted[byte_index] ^=
                static_cast<std::byte>(1U << bit);
            if (validator(corrupted) ==
                ingress::ReserveHeaderV1Error::kNone) {
                all_rejected = false;
            }
            corrupted[byte_index] ^=
                static_cast<std::byte>(1U << bit);
        }
    }
    test->Expect(all_rejected, label);
}

void TestFilenames(TestContext* test) {
    const auto uuid = GoldenPool().reserve_state_uuid;
    std::vector<std::string> names;
    for (const std::uint32_t index :
         std::array<std::uint32_t, 4U>{
             0U, 9U, 10U, 99999999U}) {
        std::string name;
        test->Expect(
            ingress::FormatReserveInodeFilenameV1(
                uuid,
                index,
                &name) ==
                ingress::ReserveHeaderV1Error::kNone,
            "canonical inode filename formats");
        names.push_back(std::move(name));
    }
    test->Expect(
        std::is_sorted(names.begin(), names.end()),
        "fixed-width filename lexical order is index order");

    common::Identity128 parsed_uuid{};
    std::uint32_t parsed_index = 0U;
    test->Expect(
        ingress::ParseReserveInodeFilenameV1(
            names[2U],
            &parsed_uuid,
            &parsed_index) ==
                ingress::ReserveHeaderV1Error::kNone &&
            parsed_uuid == uuid &&
            parsed_index == 10U,
        "canonical inode filename round-trips");

    std::vector<std::string> invalid{
        names[2U].substr(1U),
        names[2U] + "x",
        "Inode-0102030405060708090a0b0c0d0e0f10-"
        "00000010.reserve",
        "inode-0102030405060708090A0b0c0d0e0f10-"
        "00000010.reserve",
        "inode-00000000000000000000000000000000-"
        "00000010.reserve",
        "inode-0102030405060708090a0b0c0d0e0f10-"
        "0000001a.reserve",
        "inode-0102030405060708090a0b0c0d0e0f10-"
        "-0000010.reserve",
        "inode-0102030405060708090a0b0c0d0e0f10-"
        "0000010.reserve",
        "inode-0102030405060708090a0b0c0d0e0f10-"
        "00000010.RESERVE",
        "inode-0102030405060708090a0b0c0d0e0f10-"
        "100000000.reserve"};
    for (const std::string& name : invalid) {
        const common::Identity128 before_uuid =
            parsed_uuid;
        const std::uint32_t before_index =
            parsed_index;
        test->Expect(
            ingress::ParseReserveInodeFilenameV1(
                name,
                &parsed_uuid,
                &parsed_index) ==
                    ingress::ReserveHeaderV1Error::
                        kFilenameMismatch &&
                parsed_uuid == before_uuid &&
                parsed_index == before_index,
            "noncanonical inode filename is rejected without output mutation");
    }

    common::Identity128 zero_uuid{};
    std::string unchanged = "unchanged";
    test->Expect(
        ingress::FormatReserveInodeFilenameV1(
            zero_uuid,
            0U,
            &unchanged) ==
                ingress::ReserveHeaderV1Error::
                    kInvalidReserveUuid &&
            unchanged == "unchanged",
        "zero UUID cannot format a reserve inode name");
    test->Expect(
        ingress::FormatReserveInodeFilenameV1(
            uuid,
            ingress::kReserveInodeV1MaxCount,
            &unchanged) ==
                ingress::ReserveHeaderV1Error::
                    kInvalidInodeIndex &&
            unchanged == "unchanged",
        "nine-digit inode index cannot format");
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr
            << "usage: test_phase2_reserve_headers_v1 "
               "<schema>\n";
        return 2;
    }

    TestContext test;
    TestSchema(argv[1], &test);

    ingress::ReserveHeaderWireV1 file_golden{};
    ingress::ReserveHeaderWireV1 inode_golden{};
    TestFileHeader(&test, &file_golden);
    TestInodeHeader(&test, &inode_golden);
    TestLogicalRejections(&test);
    TestCrcConsistentRejections(
        file_golden,
        inode_golden,
        &test);
    TestEverySingleBitFlip(
        file_golden,
        [](std::span<const std::byte> wire) {
            return ingress::
                ValidateReserveFileHeaderV1(wire);
        },
        "every single-bit data-header corruption is rejected",
        &test);
    TestEverySingleBitFlip(
        inode_golden,
        [](std::span<const std::byte> wire) {
            return ingress::
                ValidateReserveInodeHeaderV1(wire);
        },
        "every single-bit inode-header corruption is rejected",
        &test);
    TestFilenames(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " reserve header test(s) failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 reserve header codec tests passed\n";
    return 0;
}
