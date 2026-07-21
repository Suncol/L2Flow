#include "l2mock/l2_mock.h"

#include "mdl_cffexl2_msg.h"

#include <array>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <iostream>
#include <limits>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace mdl = datayes::mdl;
namespace mock = datayes::mdl::mock;
namespace cffex = datayes::mdl::mdl_cffexl2_msg;

namespace {

struct TestContext {
    void Expect(bool condition, const std::string& description) {
        if (condition) {
            return;
        }
        ++failures;
        std::cerr << "FAIL: " << description << '\n';
    }

    void ExpectValid(const mock::Config& config,
                     const std::string& description) {
        const std::string error = mock::ValidateConfig(config);
        Expect(error.empty(),
               description +
                   (error.empty() ? std::string()
                                  : std::string(" (unexpected error: ") +
                                        error + ")"));
    }

    void ExpectInvalid(const mock::Config& config,
                       const std::string& expected_text,
                       const std::string& description) {
        const std::string error = mock::ValidateConfig(config);
        Expect(!error.empty(), description + " should be rejected");
        if (!error.empty() && !expected_text.empty()) {
            Expect(error.find(expected_text) != std::string::npos,
                   description + " reported the wrong validation error: " +
                       error);
        }
    }

    int failures = 0;
};

mock::Config MinimalSyntheticConfig() {
    mock::Config config;
    config.messages_per_second = 1000;
    config.shanghai_instruments = 1;
    config.shenzhen_instruments = 0;
    config.derivatives_per_market = 0;
    config.include_derivatives = false;
    config.clock_mode = mock::ClockMode::SimulatedTradingDay;
    config.simulated_start_date = 20260105;
    config.simulated_start_time = 93000000;
    return config;
}

bool IsCashService(std::uint8_t service_id) {
    return service_id == mdl::MDLSID_MDL_SHL2 ||
           service_id == mdl::MDLSID_MDL_SZL2;
}

mock::Instrument ValidInstrument(std::uint8_t service_id,
                                 const std::string& security_id) {
    mock::Instrument instrument;
    instrument.service_id = service_id;
    instrument.security_id = security_id;
    instrument.reference_price_milli =
        IsCashService(service_id) ? 100000 : 500000;
    instrument.tick_size_milli = IsCashService(service_id) ? 10 : 100;
    instrument.lot_size = IsCashService(service_id) ? 100U : 1U;
    instrument.option = false;
    return instrument;
}

mock::Config ConfigForInstrument(const mock::Instrument& instrument) {
    mock::Config config = MinimalSyntheticConfig();
    config.instruments.push_back(instrument);
    return config;
}

bool StatisticsAreZero(const mock::Statistics& statistics) {
    return statistics.generated == 0 && statistics.delivered == 0 &&
           statistics.filtered == 0 && statistics.dropped == 0 &&
           statistics.callback_errors == 0 &&
           statistics.queue_high_watermark == 0 &&
           statistics.active_subscribers == 0;
}

void CheckScalarConfigBoundaries(TestContext* test) {
    const mock::Config base = MinimalSyntheticConfig();
    test->ExpectValid(mock::Config(), "the documented default config");
    test->ExpectValid(base, "a minimal synthetic config");

    {
        mock::Config config = base;
        config.book_depth = 1;
        test->ExpectValid(config, "book_depth lower bound");
        config.book_depth = 50;
        test->ExpectValid(config, "book_depth upper bound");
        config.book_depth = 0;
        test->ExpectInvalid(config, "book_depth", "book_depth below range");
        config.book_depth = 51;
        test->ExpectInvalid(config, "book_depth", "book_depth above range");
    }
    {
        mock::Config config = base;
        config.orders_per_level = 1;
        test->ExpectValid(config, "orders_per_level lower bound");
        config.orders_per_level = 100;
        test->ExpectValid(config, "orders_per_level upper bound");
        config.orders_per_level = 0;
        test->ExpectInvalid(
            config, "orders_per_level", "orders_per_level below range");
        config.orders_per_level = 101;
        test->ExpectInvalid(
            config, "orders_per_level", "orders_per_level above range");
    }
    {
        mock::Config config = base;
        config.callback_threads = 1;
        test->ExpectValid(config, "callback_threads lower bound");
        config.callback_threads = 256;
        test->ExpectValid(config, "callback_threads upper bound");
        config.callback_threads = 0;
        test->ExpectInvalid(
            config, "callback_threads", "callback_threads below range");
        config.callback_threads = 257;
        test->ExpectInvalid(
            config, "callback_threads", "callback_threads above range");
    }
    {
        mock::Config config = base;
        config.callback_queue_capacity = 1;
        test->ExpectValid(config, "callback queue lower bound");
        config.callback_queue_capacity = 10000000;
        test->ExpectValid(config, "callback queue upper bound");
        config.callback_queue_capacity = 0;
        test->ExpectInvalid(
            config, "callback_queue_capacity", "callback queue below range");
        config.callback_queue_capacity = 10000001;
        test->ExpectInvalid(
            config, "callback_queue_capacity", "callback queue above range");
    }
    {
        mock::Config config = base;
        config.backpressure =
            static_cast<mock::BackpressurePolicy>(999);
        test->ExpectInvalid(
            config, "BackpressurePolicy",
            "unknown backpressure enum value");
        config = base;
        config.clock_mode = static_cast<mock::ClockMode>(999);
        test->ExpectInvalid(
            config, "ClockMode", "unknown clock mode enum value");
    }
    {
        mock::Config config = base;
        config.max_realtime_lag_ms = 0;
        test->ExpectValid(config, "zero realtime lag");
        config.max_realtime_lag_ms = 86399999;
        test->ExpectValid(config, "realtime lag just below one day");
        config.max_realtime_lag_ms = 86400000;
        test->ExpectInvalid(
            config, "less than one day", "a full day of realtime lag");
    }
    {
        mock::Config config = base;
        config.simulated_min_step_ms = 7;
        config.simulated_max_step_ms = 7;
        test->ExpectValid(config, "equal simulated clock step bounds");
        config.simulated_min_step_ms = 8;
        config.simulated_max_step_ms = 7;
        test->ExpectInvalid(
            config, "must not exceed", "reversed simulated clock bounds");
    }
}

void CheckDateAndTimeBoundaries(TestContext* test) {
    const mock::Config base = MinimalSyntheticConfig();

    const std::array<std::uint32_t, 3> valid_dates = {
        20000229U, 20240229U, 20260717U};
    for (std::uint32_t date : valid_dates) {
        mock::Config config = base;
        config.simulated_start_date = date;
        test->ExpectValid(config,
                          "valid Gregorian weekday " + std::to_string(date));
    }

    const std::array<std::uint32_t, 7> invalid_dates = {
        0U, 20230001U, 20231301U, 20230431U,
        20230229U, 19000229U, 100000101U};
    for (std::uint32_t date : invalid_dates) {
        mock::Config config = base;
        config.simulated_start_date = date;
        test->ExpectInvalid(
            config, "Gregorian", "invalid Gregorian date " +
                                      std::to_string(date));
    }
    {
        mock::Config config = base;
        config.simulated_start_date = 99991231U;
        test->ExpectInvalid(
            config, "before year 9999",
            "the maximum MDLDate year cannot support simulated rollover");
    }

    const std::array<std::uint32_t, 2> weekend_dates = {
        20260718U, 20260719U};
    for (std::uint32_t date : weekend_dates) {
        mock::Config config = base;
        config.simulated_start_date = date;
        test->ExpectInvalid(
            config, "Monday-Friday",
            "weekend simulated date " + std::to_string(date));
    }

    const std::array<std::uint32_t, 6> valid_times = {
        93000000U, 93000001U, 112959999U,
        113000000U, 130000000U, 150000000U};
    for (std::uint32_t time : valid_times) {
        mock::Config config = base;
        config.simulated_start_time = time;
        test->ExpectValid(
            config, "valid session time " + std::to_string(time));
    }

    const std::array<std::uint32_t, 10> invalid_times = {
        0U,         92959999U,  113000001U, 120000000U, 125959999U,
        150000001U, 240000000U, 96000000U,  93060000U,  1000000000U};
    for (std::uint32_t time : invalid_times) {
        mock::Config config = base;
        config.simulated_start_time = time;
        test->ExpectInvalid(
            config, "09:30-11:30",
            "invalid or out-of-session time " + std::to_string(time));
    }
}

void CheckStringBoundaries(TestContext* test) {
    const mock::Config base = MinimalSyntheticConfig();

    {
        mock::Config config = base;
        config.trading_phase_code.clear();
        test->ExpectValid(config, "empty mock trading phase token");
        config.trading_phase_code = "T-A1";
        test->ExpectValid(config, "7-bit ASCII trading phase token");
        config.trading_phase_code.assign(1, '\0');
        test->ExpectInvalid(
            config, "7-bit ASCII", "NUL in trading phase token");
        config.trading_phase_code.assign(
            1, static_cast<char>(static_cast<unsigned char>(0x80U)));
        test->ExpectInvalid(
            config, "7-bit ASCII", "non-ASCII trading phase token");
    }
    {
        mock::Config config = base;
        config.trading_phase_code.assign(
            std::numeric_limits<std::uint16_t>::max(), 'P');
        test->ExpectValid(config, "maximum-length trading phase token");
        config.trading_phase_code.push_back('P');
        test->ExpectInvalid(
            config, "7-bit ASCII", "overlong trading phase token");
    }

    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_MDL_SZL2, "");
        mock::Config config = ConfigForInstrument(instrument);
        test->ExpectInvalid(config, "security_id", "empty security ID");

        config.instruments[0].security_id.assign(1, '\0');
        test->ExpectInvalid(config, "security_id", "NUL in security ID");

        config.instruments[0].security_id.assign(
            1, static_cast<char>(static_cast<unsigned char>(0xffU)));
        test->ExpectInvalid(config, "security_id", "non-ASCII security ID");

        config.instruments[0].security_id.assign(
            std::numeric_limits<std::uint16_t>::max(), 'S');
        test->ExpectValid(config, "maximum-length security ID");
        config.instruments[0].security_id.push_back('S');
        test->ExpectInvalid(config, "security_id", "overlong security ID");
    }
}

void CheckSyntheticCountBoundaries(TestContext* test) {
    mock::Config config = MinimalSyntheticConfig();
    config.shanghai_instruments = 100000;
    config.shenzhen_instruments = 100000;
    config.derivatives_per_market = 10000;
    config.include_derivatives = true;
    test->ExpectValid(config, "all synthetic count upper bounds");

    config = MinimalSyntheticConfig();
    config.shanghai_instruments = 100001;
    test->ExpectInvalid(
        config, "cash instrument counts", "Shanghai count above limit");

    config = MinimalSyntheticConfig();
    config.shenzhen_instruments = 100001;
    test->ExpectInvalid(
        config, "cash instrument counts", "Shenzhen count above limit");

    config = MinimalSyntheticConfig();
    config.derivatives_per_market = 10001;
    config.include_derivatives = true;
    test->ExpectInvalid(
        config, "derivatives_per_market", "derivative count above limit");

    config = MinimalSyntheticConfig();
    config.derivatives_per_market =
        std::numeric_limits<std::uint32_t>::max();
    config.include_derivatives = false;
    test->ExpectValid(
        config,
        "disabled derivatives ignore the derivative count");

    config = MinimalSyntheticConfig();
    config.shanghai_instruments = 0;
    config.shenzhen_instruments = 0;
    config.derivatives_per_market = 0;
    config.include_derivatives = false;
    test->ExpectInvalid(
        config, "at least one instrument", "empty synthetic universe");

    config.include_derivatives = true;
    config.derivatives_per_market = 1;
    test->ExpectValid(config, "derivative-only synthetic universe");

    config = MinimalSyntheticConfig();
    config.shanghai_instruments = 0;
    config.derivatives_per_market = 10000;
    config.include_derivatives = false;
    test->ExpectInvalid(
        config, "at least one instrument",
        "ignored derivative count cannot make a non-derivative universe");
}

void CheckCustomInstrumentBoundaries(TestContext* test) {
    const std::array<std::uint8_t, 7> service_ids = {
        mdl::MDLSID_MDL_SHL2,   mdl::MDLSID_MDL_SZL2,
        mdl::MDLSID_MDL_CFFEXL2, mdl::MDLSID_MDL_SHFEL2,
        mdl::MDLSID_MDL_CZCEL2,  mdl::MDLSID_MDL_DCEL2,
        mdl::MDLSID_MDL_GFEXL2};

    mock::Config all_services = MinimalSyntheticConfig();
    all_services.instruments.clear();
    for (std::size_t index = 0; index < service_ids.size(); ++index) {
        all_services.instruments.push_back(
            ValidInstrument(service_ids[index],
                            "VALID" + std::to_string(index)));
    }
    test->ExpectValid(all_services, "one custom instrument for every L2 SID");

    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_UNDEFINED, "BADSID");
        mock::Config config = ConfigForInstrument(instrument);
        test->ExpectInvalid(config, "seven L2 services", "unsupported SID");
    }
    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_MDL_SZL2, "REF");
        instrument.reference_price_milli = 0;
        mock::Config config = ConfigForInstrument(instrument);
        test->ExpectInvalid(config, "must be positive", "zero reference price");
        config.instruments[0].reference_price_milli = -10;
        test->ExpectInvalid(
            config, "must be positive", "negative reference price");
        config.instruments[0].reference_price_milli =
            std::numeric_limits<std::int64_t>::max() / 10000 + 1;
        test->ExpectInvalid(
            config, "too large", "overflow-prone reference price");
    }
    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_MDL_SZL2, "TICK");
        instrument.tick_size_milli = 0;
        mock::Config config = ConfigForInstrument(instrument);
        test->ExpectInvalid(config, "must be positive", "zero tick size");
        config.instruments[0].tick_size_milli = -1;
        test->ExpectInvalid(config, "must be positive", "negative tick size");
        config.instruments[0].tick_size_milli =
            std::numeric_limits<std::int64_t>::max();
        test->ExpectInvalid(config, "tick_size_milli is too large",
                            "overflow-prone tick size");
    }
    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_MDL_SZL2, "ALIGN");
        instrument.reference_price_milli = 100001;
        mock::Config config = ConfigForInstrument(instrument);
        test->ExpectInvalid(
            config, "align to tick_size_milli", "misaligned reference price");
    }
    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_MDL_SZL2, "LOT");
        instrument.lot_size = 1;
        mock::Config config = ConfigForInstrument(instrument);
        test->ExpectValid(config, "lot size lower bound");
        config.instruments[0].lot_size = 1000000;
        test->ExpectValid(config, "lot size upper bound");
        config.instruments[0].lot_size = 0;
        test->ExpectInvalid(config, "lot_size", "lot size below range");
        config.instruments[0].lot_size = 1000001;
        test->ExpectInvalid(config, "lot_size", "lot size above range");
    }
    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_MDL_SHL2, "SHPRICE");
        instrument.tick_size_milli = 1;
        instrument.reference_price_milli =
            static_cast<std::int64_t>(INT_MAX / 2) - 1;
        mock::Config config = ConfigForInstrument(instrument);
        test->ExpectValid(config, "Shanghai price just below SDK guard");
        config.instruments[0].reference_price_milli =
            static_cast<std::int64_t>(INT_MAX / 2);
        test->ExpectInvalid(
            config, "Shanghai prices", "Shanghai price at SDK guard");
    }
    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_MDL_SHL2, "SHMONEY");
        instrument.reference_price_milli = 1000000000;
        instrument.tick_size_milli = 1;
        instrument.lot_size = 1000000;
        mock::Config config = ConfigForInstrument(instrument);
        test->ExpectInvalid(
            config, "TradeMoney",
            "Shanghai price/lot combination that overflows scale-5 money");
    }
    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_MDL_SZL2, "SZMONEY");
        instrument.reference_price_milli = INT64_C(900000000000000);
        instrument.tick_size_milli = 1000;
        instrument.lot_size = 1000000;
        mock::Config config = ConfigForInstrument(instrument);
        test->ExpectInvalid(
            config, "Shenzhen lot",
            "Shenzhen price/lot combination without cumulative room");
        config.instruments[0].reference_price_milli =
            INT64_C(800000000000);
        test->ExpectValid(
            config,
            "Shenzhen price/lot combination with one-lot cumulative room");
    }
    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_MDL_DCEL2, "DCEMONEY");
        instrument.reference_price_milli = INT64_C(3900000000000);
        instrument.tick_size_milli = 1000000;
        instrument.lot_size = 1000000;
        mock::Config config = ConfigForInstrument(instrument);
        test->ExpectInvalid(
            config, "derivative lot",
            "derivative price/lot combination without cumulative room");
        config.instruments[0].reference_price_milli =
            INT64_C(3800000000000);
        test->ExpectValid(
            config,
            "derivative price/lot combination with one-lot cumulative room");
    }
    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_MDL_CFFEXL2, "CFVOL");
        instrument.lot_size = 1000000;
        mock::Config config = ConfigForInstrument(instrument);
        config.orders_per_level = 100;
        test->ExpectInvalid(
            config, "int32 book volume",
            "CFFEX lot/order combination that exceeds int32 book volume");
        config.instruments[0].lot_size = 429496;
        test->ExpectValid(
            config, "largest tested CFFEX lot/order product below int32");
    }
    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_MDL_SZL2, "ROOM");
        instrument.reference_price_milli = 1200;
        instrument.tick_size_milli = 10;
        mock::Config config = ConfigForInstrument(instrument);
        test->ExpectInvalid(config, "too small", "cash depth room equality");
        config.instruments[0].reference_price_milli = 1210;
        test->ExpectValid(config, "cash depth room just above requirement");
    }
    {
        mock::Instrument instrument =
            ValidInstrument(mdl::MDLSID_MDL_DCEL2, "DROOM");
        instrument.reference_price_milli = 600;
        instrument.tick_size_milli = 10;
        mock::Config config = ConfigForInstrument(instrument);
        test->ExpectInvalid(
            config, "too small", "derivative depth room equality");
        config.instruments[0].reference_price_milli = 610;
        test->ExpectValid(
            config, "derivative depth room just above requirement");
    }
    {
        mock::Config config = MinimalSyntheticConfig();
        config.instruments.push_back(
            ValidInstrument(mdl::MDLSID_MDL_SZL2, "DUP"));
        config.instruments.push_back(
            ValidInstrument(mdl::MDLSID_MDL_SZL2, "DUP"));
        test->ExpectInvalid(
            config, "duplicate", "duplicate service/security key");

        config.instruments[1] =
            ValidInstrument(mdl::MDLSID_MDL_SHL2, "DUP");
        test->ExpectValid(
            config, "the same security ID on different services");
    }

    {
        mock::Config config = MinimalSyntheticConfig();
        config.instruments.reserve(250001);
        for (std::size_t index = 0; index < 250000; ++index) {
            config.instruments.push_back(ValidInstrument(
                mdl::MDLSID_MDL_SZL2, "U" + std::to_string(index)));
        }
        test->ExpectValid(config, "custom instrument count upper bound");
        config.instruments.push_back(
            ValidInstrument(mdl::MDLSID_MDL_SZL2, "U250000"));
        test->ExpectInvalid(
            config, "must not exceed 250000",
            "custom instrument count above upper bound");
    }
}

bool SameInstrument(const mock::Instrument& left,
                    const mock::Instrument& right) {
    return left.service_id == right.service_id &&
           left.security_id == right.security_id &&
           left.reference_price_milli == right.reference_price_milli &&
           left.tick_size_milli == right.tick_size_milli &&
           left.lot_size == right.lot_size &&
           left.option == right.option;
}

void CheckSyntheticUniverseFactory(TestContext* test) {
    const std::vector<mock::Instrument> universe =
        mock::MakeSyntheticUniverse(3, 2, 3, true);
    test->Expect(universe.size() == 20,
                 "synthetic universe has cash plus five derivative markets");
    test->Expect(
        universe.size() >= 5 && universe[0].security_id == "600000" &&
            universe[2].security_id == "600002" &&
            universe[3].security_id == "000001" &&
            universe[4].security_id == "000002",
        "synthetic cash identifiers are deterministic six-digit tokens");

    std::set<std::pair<std::uint8_t, std::string> > unique;
    std::array<std::vector<bool>, 7> options_by_service;
    const std::array<std::uint8_t, 7> service_ids = {
        mdl::MDLSID_MDL_SHL2,   mdl::MDLSID_MDL_SZL2,
        mdl::MDLSID_MDL_CFFEXL2, mdl::MDLSID_MDL_SHFEL2,
        mdl::MDLSID_MDL_CZCEL2,  mdl::MDLSID_MDL_DCEL2,
        mdl::MDLSID_MDL_GFEXL2};

    for (const mock::Instrument& instrument : universe) {
        test->Expect(instrument.reference_price_milli > 0,
                     "synthetic reference prices are positive");
        test->Expect(instrument.tick_size_milli > 0,
                     "synthetic tick sizes are positive");
        test->Expect(instrument.reference_price_milli %
                             instrument.tick_size_milli ==
                         0,
                     "synthetic reference prices align to ticks");
        test->Expect(
            unique.insert(
                      std::make_pair(
                          instrument.service_id, instrument.security_id))
                .second,
            "synthetic service/security keys are unique");

        std::size_t service_index = service_ids.size();
        for (std::size_t index = 0; index < service_ids.size(); ++index) {
            if (instrument.service_id == service_ids[index]) {
                service_index = index;
                break;
            }
        }
        test->Expect(service_index < service_ids.size(),
                     "synthetic universe uses only supported services");
        if (service_index >= service_ids.size()) {
            continue;
        }

        if (IsCashService(instrument.service_id)) {
            test->Expect(instrument.security_id.size() == 6,
                         "synthetic cash IDs are six characters");
            test->Expect(instrument.lot_size == 100,
                         "synthetic cash lot size is 100");
            test->Expect(!instrument.option,
                         "synthetic cash instruments are not options");
        } else {
            test->Expect(instrument.security_id.size() == 8,
                         "synthetic derivative IDs are eight characters");
            test->Expect(instrument.lot_size == 1,
                         "synthetic derivative lot size is one");
            options_by_service[service_index].push_back(instrument.option);
        }
    }

    for (std::size_t index = 2; index < service_ids.size(); ++index) {
        const std::vector<bool>& options = options_by_service[index];
        test->Expect(options.size() == 3,
                     "each derivative service receives the requested count");
        if (options.size() == 3) {
            test->Expect(!options[0] && options[1] && !options[2],
                         "derivative future/option flags alternate");
        }
    }

    const std::vector<mock::Instrument> repeat =
        mock::MakeSyntheticUniverse(3, 2, 3, true);
    test->Expect(repeat.size() == universe.size(),
                 "repeated synthetic generation preserves size");
    if (repeat.size() == universe.size()) {
        for (std::size_t index = 0; index < universe.size(); ++index) {
            test->Expect(
                SameInstrument(repeat[index], universe[index]),
                "repeated synthetic generation is deterministic at index " +
                    std::to_string(index));
        }
    }

    mock::Config config = MinimalSyntheticConfig();
    config.instruments = universe;
    test->ExpectValid(config,
                      "MakeSyntheticUniverse output passes config validation");

    const std::vector<mock::Instrument> cash_only =
        mock::MakeSyntheticUniverse(1, 1, 99, false);
    test->Expect(cash_only.size() == 2,
                 "include_derivatives=false ignores derivative count");
    test->Expect(
        cash_only.size() == 2 &&
            cash_only[0].service_id == mdl::MDLSID_MDL_SHL2 &&
            cash_only[1].service_id == mdl::MDLSID_MDL_SZL2,
        "cash-only synthetic universe contains both requested cash markets");

    test->Expect(mock::MakeSyntheticUniverse(0, 0, 0, false).empty(),
                 "the standalone universe helper permits an empty result");

    const auto expect_factory_invalid =
        [test](std::uint32_t shanghai,
               std::uint32_t shenzhen,
               std::uint32_t derivatives,
               bool include_derivatives,
               const std::string& description) {
            bool rejected = false;
            try {
                (void)mock::MakeSyntheticUniverse(
                    shanghai,
                    shenzhen,
                    derivatives,
                    include_derivatives);
            } catch (const std::invalid_argument&) {
                rejected = true;
            } catch (...) {
                test->Expect(
                    false,
                    description + " threw the wrong exception type");
                return;
            }
            test->Expect(rejected, description + " is rejected");
        };
    expect_factory_invalid(
        100001, 0, 0, false, "standalone Shanghai count above limit");
    expect_factory_invalid(
        0, 100001, 0, false, "standalone Shenzhen count above limit");
    expect_factory_invalid(
        0, 0, 10001, true, "standalone derivative count above limit");

    const std::vector<mock::Instrument> ignored_large_derivative_count =
        mock::MakeSyntheticUniverse(
            1,
            0,
            std::numeric_limits<std::uint32_t>::max(),
            false);
    test->Expect(
        ignored_large_derivative_count.size() == 1,
        "standalone disabled derivatives ignore their count");
}

struct RolloverObservation {
    bool connected = false;
    bool received = false;
    std::uint32_t action_day = 0;
    std::uint32_t trading_day = 0;
    std::uint32_t update_time = 0;
    std::uint32_t local_time = 0;
};

class RolloverHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*, const mdl::MDLMessage* message) override {
        if (message == NULL || message->GetHead() == NULL ||
            message->GetBody() == NULL) {
            return;
        }
        const mdl::MDLMessageHead* const head = message->GetHead();
        if (head->ServiceID != cffex::Future::ServiceID ||
            head->ServiceVersion != cffex::Future::ServiceVer ||
            head->MessageID != cffex::Future::MessageID ||
            head->MessageSize < head->HeadSize ||
            static_cast<std::size_t>(head->MessageSize - head->HeadSize) <
                sizeof(cffex::Future)) {
            return;
        }
        const cffex::Future* const body =
            reinterpret_cast<const cffex::Future*>(message->GetBody());
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (received_) {
                return;
            }
            received_ = true;
            action_day_ = body->ActionDay.m_Value;
            trading_day_ = body->TradDay.m_Value;
            update_time_ = body->UpdateTime.m_Value;
            local_time_ = head->LocalTime.m_Value;
        }
        condition_.notify_one();
    }

    RolloverObservation Wait(bool connected) {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait_for(
            lock, std::chrono::seconds(2), [this] { return received_; });
        RolloverObservation result;
        result.connected = connected;
        result.received = received_;
        result.action_day = action_day_;
        result.trading_day = trading_day_;
        result.update_time = update_time_;
        result.local_time = local_time_;
        return result;
    }

private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool received_ = false;
    std::uint32_t action_day_ = 0;
    std::uint32_t trading_day_ = 0;
    std::uint32_t update_time_ = 0;
    std::uint32_t local_time_ = 0;
};

RolloverObservation ObserveSimulatedRollover(
    std::uint32_t start_date) {
    mock::Config config = MinimalSyntheticConfig();
    config.messages_per_second = 0;
    config.book_depth = 1;
    config.orders_per_level = 1;
    config.callback_threads = 1;
    config.callback_queue_capacity = 1;
    config.simulated_start_date = start_date;
    config.simulated_start_time = 150000000;
    config.simulated_min_step_ms = 1;
    config.simulated_max_step_ms = 1;
    config.instruments.push_back(
        ValidInstrument(mdl::MDLSID_MDL_CFFEXL2, "CLOCK"));

    RolloverHandler handler;
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    mdl::SubscriberPtr subscriber =
        manager->CreateSubscriber(&handler, false);
    subscriber->SubcribeMessage<cffex::Future>();
    const char* const connect_result = subscriber->Connect();
    const bool connected =
        connect_result != NULL && std::string(connect_result).empty();
    const RolloverObservation observation = handler.Wait(connected);
    manager->Shutdown();
    return observation;
}

void CheckSimulatedRollover(TestContext* test) {
    const RolloverObservation leap =
        ObserveSimulatedRollover(20240229U);
    test->Expect(leap.connected,
                 "leap-day rollover subscriber connects");
    test->Expect(leap.received,
                 "leap-day rollover emits a derivative snapshot");
    test->Expect(
        leap.action_day == 20240301U &&
            leap.trading_day == 20240301U,
        "2024 leap-day close rolls ActionDay/TradDay to 2024-03-01");
    test->Expect(
        leap.update_time == 93000000U &&
            leap.local_time == 93000000U,
        "leap-day close rolls event/local time to 09:30:00.000");

    const RolloverObservation friday =
        ObserveSimulatedRollover(20260109U);
    test->Expect(friday.connected,
                 "Friday rollover subscriber connects");
    test->Expect(friday.received,
                 "Friday rollover emits a derivative snapshot");
    test->Expect(
        friday.action_day == 20260112U &&
            friday.trading_day == 20260112U,
        "Friday close skips the weekend and rolls to Monday");
    test->Expect(
        friday.update_time == 93000000U &&
            friday.local_time == 93000000U,
        "Friday close rolls event/local time to 09:30:00.000");
}

class NullHandler final : public mdl::MessageHandlerBase {
public:
    void OnMessage(mdl::Subscriber*, const mdl::MDLMessage*) override {}
};

class ForeignIOManager final : public mdl::IOManager {
public:
    void AddRef() override {}
    int ReleaseRef() override {
        return 1;
    }

    mdl::AutoSubscriberPtr CreateAutoSubscriber(
        mdl::MessageHandlerBase*, bool) override {
        return mdl::AutoSubscriberPtr();
    }
    bool RegisterMessage(std::uint8_t, std::uint16_t, std::uint16_t) override {
        return false;
    }
    void AsyncResponse(
        mdl::Publisher*, datayes::RefCounted*, const mdl::MDLMessage*) override {}
    void AsyncPublish(const mdl::MDLMessage*) override {}
    void SyncPublish(const mdl::MDLMessage*) override {}
    void EnableLog(const char*, bool) override {}
    void Shutdown() override {}
    void SetWorkersProxy(mdl::IOManager*) override {}

protected:
    mdl::Publisher* _CreatePublisher(mdl::PublisherType) override {
        return NULL;
    }
    mdl::Publisher* _GetPublisherByType(mdl::PublisherType) override {
        return NULL;
    }
    mdl::Subscriber* _CreateSubscriber(
        mdl::MessageHandlerBase*, bool) override {
        return NULL;
    }
};

void ExpectFactoryRejects(TestContext* test,
                          const mock::Config& config,
                          const std::string& description) {
    bool invalid_argument = false;
    bool wrong_exception = false;
    try {
        mdl::IOManagerPtr manager = mock::CreateIOManager(config);
        manager->Shutdown();
    } catch (const std::invalid_argument&) {
        invalid_argument = true;
    } catch (...) {
        wrong_exception = true;
    }
    test->Expect(invalid_argument && !wrong_exception,
                 description + " must throw std::invalid_argument");
}

mdl::IOManagerPtr CreateManagerFromLocalConfig() {
    mock::Config config = MinimalSyntheticConfig();
    config.messages_per_second = 1000;
    config.book_depth = 1;
    config.orders_per_level = 1;
    config.callback_threads = 1;
    config.callback_queue_capacity = 4;
    config.simulated_min_step_ms = 1;
    config.simulated_max_step_ms = 1;
    config.instruments.push_back(
        ValidInstrument(mdl::MDLSID_MDL_CFFEXL2, "OWNEDCFG"));
    return mock::CreateIOManager(config);
}

void CheckManagerFactoryAndLifecycle(TestContext* test) {
    test->Expect(!mock::IsMockIOManager(NULL),
                 "a null manager is not identified as the mock");
    test->Expect(StatisticsAreZero(mock::GetStatistics(NULL)),
                 "null manager statistics are zero initialized");

    ForeignIOManager foreign;
    test->Expect(!mock::IsMockIOManager(&foreign),
                 "a foreign IOManager is not identified as the mock");
    test->Expect(StatisticsAreZero(mock::GetStatistics(&foreign)),
                 "foreign manager statistics are zero initialized");

    {
        mdl::IOManagerPtr manager = mock::CreateIOManager();
        test->Expect(!manager.IsNull(), "default factory returns a manager");
        test->Expect(mock::IsMockIOManager(manager.Get()),
                     "default factory result is identified as the mock");
        test->Expect(StatisticsAreZero(mock::GetStatistics(manager.Get())),
                     "an idle default manager starts with zero statistics");
        manager->Shutdown();
        manager->Shutdown();
    }

    {
        // Generation starts only after the factory's local Config has gone out
        // of scope.  This catches Runtime/Generator references to caller-owned
        // configuration storage under ASan's stack-use-after-return mode.
        RolloverHandler handler;
        mdl::IOManagerPtr owned_config_manager =
            CreateManagerFromLocalConfig();
        mdl::SubscriberPtr subscriber =
            owned_config_manager->CreateSubscriber(&handler, false);
        subscriber->SubcribeMessage<cffex::Future>();
        const char* const result = subscriber->Connect();
        const bool connected =
            result != NULL && std::string(result).empty();
        const RolloverObservation observation = handler.Wait(connected);
        owned_config_manager->Shutdown();
        test->Expect(
            observation.connected && observation.received,
            "manager owns Config state after factory caller scope ends");
    }

    mock::Config config = MinimalSyntheticConfig();
    mdl::IOManagerPtr manager = mock::CreateIOManager(config);
    test->Expect(!manager.IsNull(), "minimal factory returns a manager");
    test->Expect(mock::IsMockIOManager(manager.Get()),
                 "minimal factory result is identified as the mock");

    for (const mock::SupportedMessage& message : mock::SupportedMessages()) {
        test->Expect(
            manager->RegisterMessage(
                message.key.service_id,
                message.key.service_version,
                message.key.message_id),
            "RegisterMessage accepts supported key " +
                std::to_string(message.key.service_id) + "/" +
                std::to_string(message.key.service_version) + "/" +
                std::to_string(message.key.message_id));
    }
    test->Expect(!manager->RegisterMessage(mdl::MDLSID_UNDEFINED, 0, 0),
                 "RegisterMessage rejects an undefined key");
    if (!mock::SupportedMessages().empty()) {
        const mock::MessageKey key = mock::SupportedMessages()[0].key;
        test->Expect(
            !manager->RegisterMessage(
                key.service_id,
                static_cast<std::uint16_t>(key.service_version + 1U),
                key.message_id),
            "RegisterMessage rejects the wrong service version");
        test->Expect(
            !manager->RegisterMessage(
                key.service_id, key.service_version,
                std::numeric_limits<std::uint16_t>::max()),
            "RegisterMessage rejects an unsupported message ID");
    }

    {
        mdl::PublisherPtr publisher =
            manager->CreatePublisher(mdl::PUBLISH_TYPE_TCP);
        test->Expect(!publisher.IsNull(),
                     "CreatePublisher returns a mock publisher");
        if (!publisher.IsNull()) {
            test->Expect(publisher->GetType() == mdl::PUBLISH_TYPE_TCP,
                         "publisher retains its requested type");
            publisher->SetListenAddress("mock://127.0.0.1:12345");
            test->Expect(
                std::string(publisher->GetListenAddress()) ==
                    "mock://127.0.0.1:12345",
                "publisher retains its listen address");
            test->Expect(std::string(publisher->Listen()).empty(),
                         "mock publisher Listen succeeds");
            publisher->SetListenAddress(NULL);
            test->Expect(std::string(publisher->GetListenAddress()).empty(),
                         "null publisher address maps to an empty string");
        }

        mdl::PublisherPtr by_type =
            manager->GetPublisherByType(mdl::PUBLISH_TYPE_REDIS);
        test->Expect(!by_type.IsNull(),
                     "GetPublisherByType returns a mock publisher");
        if (!by_type.IsNull()) {
            test->Expect(by_type->GetType() == mdl::PUBLISH_TYPE_REDIS,
                         "GetPublisherByType retains the requested type");
        }
    }

    {
        mdl::AutoSubscriberPtr invalid =
            manager->CreateAutoSubscriber(NULL, false);
        test->Expect(!invalid.IsNull(),
                     "CreateAutoSubscriber accepts deferred handler checking");
        if (!invalid.IsNull()) {
            invalid->SetURL(NULL);
            invalid->SetToken(NULL);
            invalid->SetOptionsJson(NULL);
            invalid->SetDailyCheckTime(0);
            invalid->SetQueryTimeoutMs(0);
            const char* result = invalid->Start();
            test->Expect(result != NULL && !std::string(result).empty(),
                         "AutoSubscriber Start rejects a null handler");
            invalid->Stop();
            invalid->Stop();
        }
    }

    {
        NullHandler handler;
        mdl::AutoSubscriberPtr automatic =
            manager->CreateAutoSubscriber(&handler, false);
        test->Expect(!automatic.IsNull(),
                     "CreateAutoSubscriber returns a usable object");
        if (!automatic.IsNull()) {
            automatic->SetURL("mock://catalog");
            automatic->SetToken("synthetic-token");
            automatic->SetOptionsJson("{\"mock\":true}");
            automatic->SetDailyCheckTime(91500);
            automatic->SetQueryTimeoutMs(250);
            const char* first = automatic->Start();
            test->Expect(first != NULL && std::string(first).empty(),
                         "AutoSubscriber first Start succeeds");
            const char* second = automatic->Start();
            test->Expect(second != NULL && std::string(second).empty(),
                         "AutoSubscriber repeated Start is idempotent");
            automatic->Stop();
            automatic->Stop();
            const char* restarted = automatic->Start();
            test->Expect(restarted != NULL && std::string(restarted).empty(),
                         "AutoSubscriber can restart after Stop");
            automatic->Stop();
            test->Expect(
                mock::GetStatistics(manager.Get()).active_subscribers == 0,
                "AutoSubscriber Stop synchronously clears active count");
        }
    }

    mock::SubscribeAll(NULL);
    manager->EnableLog("test_config", false);
    manager->SetWorkersProxy(NULL);
    manager->Shutdown();
    manager->Shutdown();

    mock::Config invalid = MinimalSyntheticConfig();
    invalid.book_depth = 0;
    ExpectFactoryRejects(test, invalid, "invalid mock factory config");

    invalid = MinimalSyntheticConfig();
    invalid.simulated_start_date = 20260718;
    ExpectFactoryRejects(test, invalid, "weekend mock factory config");
}

} // namespace

int main() {
    TestContext test;
    CheckScalarConfigBoundaries(&test);
    CheckDateAndTimeBoundaries(&test);
    CheckStringBoundaries(&test);
    CheckSyntheticCountBoundaries(&test);
    CheckCustomInstrumentBoundaries(&test);
    CheckSyntheticUniverseFactory(&test);
    CheckSimulatedRollover(&test);
    CheckManagerFactoryAndLifecycle(&test);

    if (test.failures != 0) {
        std::cerr << test.failures << " config/lifecycle checks failed\n";
        return 1;
    }
    std::cout << "config/lifecycle contract checks passed\n";
    return 0;
}
