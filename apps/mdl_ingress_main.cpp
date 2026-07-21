#include "l2flow/apps/ingress_service.h"

#include <cstdint>

#ifndef L2FLOW_INGRESS_KIND
#error "L2FLOW_INGRESS_KIND must select an ingress kind from 0 through 3"
#endif

#if L2FLOW_INGRESS_KIND < 0 || L2FLOW_INGRESS_KIND > 3
#error "L2FLOW_INGRESS_KIND must select an ingress kind from 0 through 3"
#endif

namespace {

constexpr auto kIngressKind =
    static_cast<l2flow::sdk::IngressKind>(
        static_cast<std::uint8_t>(L2FLOW_INGRESS_KIND));

}  // namespace

int main(int argc, char* argv[]) {
    return l2flow::apps::RunIngressService(
        kIngressKind, argc, argv);
}
