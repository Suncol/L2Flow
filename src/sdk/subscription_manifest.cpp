#include "l2flow/sdk/subscription_manifest.h"

#include "mdl_shl2_msg.h"
#include "mdl_szl2_msg.h"

#include <algorithm>
#include <array>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>
#include <tuple>

namespace l2flow::sdk {
namespace {

namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sz = datayes::mdl::mdl_szl2_msg;

// Approved SDK 2.13.234 contract values are intentionally literal here.
// Phase-0 ABI probes independently compare the vendor C++ type constants to
// these values, so an SDK header drift cannot silently rewrite both sides of
// the comparison.
constexpr MessageKey kShSnapshot{4U, 101U, 4U};
constexpr MessageKey kShIndex{4U, 101U, 6U};
constexpr MessageKey kShTick{4U, 101U, 24U};
constexpr MessageKey kSzSnapshot{6U, 101U, 28U};
constexpr MessageKey kSzIndex{6U, 101U, 29U};
constexpr MessageKey kSzOrder{6U, 101U, 33U};
constexpr MessageKey kSzTransaction{6U, 101U, 36U};
constexpr MessageKey kSzCombinedTick{6U, 101U, 53U};

const std::vector<IngressSpec>& Specs() {
    static const std::vector<IngressSpec> specs = {
        {
            IngressKind::ShSnapshot,
            "mdl-ingress-sh-snapshot",
            1001U,
            static_cast<std::uint8_t>(datayes::mdl::MDLSID_MDL_SHL2),
            2,
            1,
            {kShSnapshot},
            {kShIndex},
            {},
        },
        {
            IngressKind::ShTick,
            "mdl-ingress-sh-tick",
            1002U,
            static_cast<std::uint8_t>(datayes::mdl::MDLSID_MDL_SHL2),
            4,
            1,
            {kShTick},
            {},
            {},
        },
        {
            IngressKind::SzSnapshot,
            "mdl-ingress-sz-snapshot",
            2001U,
            static_cast<std::uint8_t>(datayes::mdl::MDLSID_MDL_SZL2),
            2,
            1,
            {kSzSnapshot},
            {kSzIndex},
            {kSzCombinedTick},
        },
        {
            IngressKind::SzTick,
            "mdl-ingress-sz-tick",
            2002U,
            static_cast<std::uint8_t>(datayes::mdl::MDLSID_MDL_SZL2),
            4,
            1,
            {
                kSzOrder,
                kSzTransaction,
            },
            {},
            {kSzCombinedTick},
        },
    };
    return specs;
}

using SortKey = std::tuple<std::uint8_t, std::uint16_t, std::uint16_t>;

SortKey AsSortKey(const MessageKey& key) {
    return {key.service_id, key.service_version, key.message_id};
}

}  // namespace

const IngressSpec& GetIngressSpec(IngressKind kind) {
    const auto& specs = Specs();
    const auto found = std::find_if(
        specs.begin(), specs.end(),
        [kind](const IngressSpec& spec) { return spec.kind == kind; });
    // IngressKind is a closed enum owned by this library. Returning the first
    // spec for a corrupted value would silently subscribe the wrong stream.
    if (found == specs.end()) {
        throw std::invalid_argument("unknown ingress kind");
    }
    return *found;
}

const std::vector<IngressSpec>& AllIngressSpecs() {
    return Specs();
}

std::string ValidateIngressSpecs() {
    const auto& specs = Specs();
    if (specs.size() != 4U) {
        return "the production manifest must define exactly four ingress streams";
    }

    std::set<std::uint32_t> stream_ids;
    std::set<IngressKind> kinds;
    for (const IngressSpec& spec : specs) {
        if (spec.source_stream_id == 0U ||
            !stream_ids.insert(spec.source_stream_id).second) {
            return "source_stream_id values must be non-zero and unique";
        }
        if (!kinds.insert(spec.kind).second) {
            return "ingress kinds must be unique";
        }
        if (spec.default_work_threads <= 0 ||
            spec.default_io_threads <= 0) {
            return "SDK thread counts must be positive";
        }
        if (spec.required.empty()) {
            return "every ingress stream must have a required subscription";
        }
        if (spec.required.size() >
            std::numeric_limits<std::uint64_t>::digits) {
            return "required subscription masks support at most 64 messages";
        }

        std::set<SortKey> enabled;
        for (const MessageKey& key : spec.required) {
            if (key.service_id != spec.market_service_id ||
                !enabled.insert(AsSortKey(key)).second) {
                return "invalid or duplicate required subscription";
            }
        }
        for (const MessageKey& key : spec.optional) {
            if (key.service_id != spec.market_service_id ||
                !enabled.insert(AsSortKey(key)).second) {
                return "invalid or duplicate optional subscription";
            }
        }
        for (const MessageKey& key : spec.forbidden) {
            if (enabled.count(AsSortKey(key)) != 0U) {
                return "a forbidden subscription is enabled";
            }
        }
    }

    const IngressSpec& sz_tick = GetIngressSpec(IngressKind::SzTick);
    const std::set<SortKey> expected_sz_tick = {
        AsSortKey(kSzOrder),
        AsSortKey(kSzTransaction),
    };
    std::set<SortKey> actual_sz_tick;
    for (const MessageKey& key : sz_tick.required) {
        actual_sz_tick.insert(AsSortKey(key));
    }
    if (actual_sz_tick != expected_sz_tick) {
        return "SZ 6.33 and 6.36 must be the only required SZ tick messages";
    }

    const MessageKey combined = kSzCombinedTick;
    for (const IngressSpec& spec : specs) {
        const auto enabled_combined =
            [combined](const MessageKey& key) { return key == combined; };
        if (std::any_of(spec.required.begin(), spec.required.end(),
                        enabled_combined) ||
            std::any_of(spec.optional.begin(), spec.optional.end(),
                        enabled_combined)) {
            return "SZ 6.53 CombinedTick must not be a core subscription";
        }
    }
    return {};
}

void AddConfiguredSubscriptions(datayes::mdl::Subscriber& subscriber,
                                const IngressSpec& spec,
                                bool include_optional_indices) {
    for (const MessageKey& key : spec.required) {
        subscriber.AddSubscription(
            key.service_id, key.service_version, key.message_id);
    }
    if (include_optional_indices) {
        for (const MessageKey& key : spec.optional) {
            subscriber.AddSubscription(
                key.service_id, key.service_version, key.message_id);
        }
    }
}

std::string_view ToString(IngressKind kind) {
    return GetIngressSpec(kind).service_name;
}

bool ParseIngressKind(std::string_view text, IngressKind* result) {
    if (result == nullptr) {
        return false;
    }
    static constexpr std::array<std::pair<std::string_view, IngressKind>, 8>
        names = {{
            {"sh-snapshot", IngressKind::ShSnapshot},
            {"mdl-ingress-sh-snapshot", IngressKind::ShSnapshot},
            {"sh-tick", IngressKind::ShTick},
            {"mdl-ingress-sh-tick", IngressKind::ShTick},
            {"sz-snapshot", IngressKind::SzSnapshot},
            {"mdl-ingress-sz-snapshot", IngressKind::SzSnapshot},
            {"sz-tick", IngressKind::SzTick},
            {"mdl-ingress-sz-tick", IngressKind::SzTick},
        }};
    const auto found = std::find_if(
        names.begin(), names.end(),
        [text](const auto& entry) { return entry.first == text; });
    if (found == names.end()) {
        return false;
    }
    *result = found->second;
    return true;
}

std::optional<std::size_t>
RequiredMessageFixedBodyBytes(const MessageKey& key) noexcept {
    if (key == kShSnapshot) {
        return sizeof(sh::SHL2MarketData);
    }
    if (key == kShTick) {
        return sizeof(sh::NGTSTick);
    }
    if (key == kSzSnapshot) {
        return sizeof(sz::Snapshot300111_v2);
    }
    if (key == kSzOrder) {
        return sizeof(sz::Order300192_v2);
    }
    if (key == kSzTransaction) {
        return sizeof(sz::Transaction300191_v2);
    }
    return std::nullopt;
}

}  // namespace l2flow::sdk
