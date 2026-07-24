#pragma once

#include <filesystem>
#include <memory>
#include <string>

namespace l2flow::sdk {

class SdkFactory;

// Loads the vendor SDK selected by the operator and returns the physical
// factory exported by that DSO.  This path performs no archive, baseline,
// digest, ELF, compiled-ABI or lifecycle approval.  The only pre-dlopen
// validation is that the path is non-empty and contains no embedded NUL.
//
// On success the returned factory creates one physical IOManager per Create()
// call. The production composition creates one physical Subscriber from it.
//
// The successfully loaded vendor DSO is retained until process exit.  The
// vendor owns process-global dispatcher/logging state and its unload
// finalizers are not a supported in-process shutdown boundary.  IOManager and
// Subscriber objects must still be shut down and released explicitly through
// the narrow SdkFactory/SdkManager/SdkSubscriber contract.
[[nodiscard]] std::shared_ptr<SdkFactory> LoadSdkFactoryFromPath(
    const std::filesystem::path& shared_library,
    std::string* error) noexcept;

}  // namespace l2flow::sdk
