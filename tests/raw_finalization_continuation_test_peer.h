#pragma once

#include "l2flow/ingress/raw_finalization_continuation_posix.h"

namespace l2flow::ingress {

class RawFinalizationContinuationTestPeerV1 final {
public:
    static void MutateKey(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            ++receipt->key_.action_id;
        }
    }

    static void MutateActionKind(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            receipt->key_.action_kind =
                FinalizationActionKindV1::kIndex;
        }
    }

    static void MutatePlan(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            ++receipt->plan_.range_start;
        }
    }

    static void MutateImmutableGrantHash(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            receipt->immutable_grant_sha256_[0U] ^=
                std::byte{0x01U};
        }
    }

    static void MutateByteCap(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            ++receipt->byte_cap_;
        }
    }

    static void MutateInodeCap(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            ++receipt->inode_cap_;
        }
    }

    static void MutateExecutor(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            receipt->executor_instance_[0U] ^=
                std::byte{0x01U};
        }
    }

    static void MutateDebitGeneration(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            ++receipt->debit_generation_;
        }
    }

    static void MutateFrozenRecordCount(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            ++receipt->frozen_record_count_;
        }
    }

    static void MutateFrozenFramedBytes(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            ++receipt->frozen_framed_wal_bytes_;
        }
    }

    static void MutateFinalCursor(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            ++receipt->final_cursor_.ingress_sequence;
        }
    }

    static void ExceedObservedByteCap(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            receipt->allocation_observation_
                .conservative_positive_allocation_delta =
                    receipt->byte_cap_ + 1U;
        }
    }

    static void MutateAuthorizedInodeCap(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            ++receipt->allocation_observation_
                  .authorized_inode_cap;
        }
    }

    static void MutateRetainedWriterSnapshot(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            ++receipt->retained_writer_snapshot_
                  .append.global_wal_pos;
        }
    }

    static void DropRetainedWriterLease(
        RawFinalizationContinuationReceiptV1* receipt) noexcept {
        if (receipt != nullptr) {
            receipt->retained_writer_.reset();
        }
    }
};

class RawFinalizationContinuationCoordinatorTestPeerV1 final {
public:
    static void DropLease(
        RawReserveRegistryCoordinatorV1* coordinator) noexcept {
        if (coordinator != nullptr) {
            coordinator->lease_.reset();
        }
    }
};

}  // namespace l2flow::ingress
