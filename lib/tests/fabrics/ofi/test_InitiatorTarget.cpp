// SPDX-FileCopyrightText: 2026 Contributors to the Media eXchange Layer project.
//
// SPDX-License-Identifier: Apache-2.0

#include <cstddef>
#include <cstdint>
#include <memory>
#include <catch2/catch_test_macros.hpp>
#include "Completion.hpp"
#include "Endpoint.hpp"
#include "Protocol.hpp"
#include "RCInitiator.hpp"
#include "RDMInitiator.hpp"
#include "SliceRange.hpp"
#include "TargetInfo.hpp"
#include "Util.hpp"

using namespace mxl::lib::fabrics::ofi;

namespace
{
    // Records the protocol it was told to continue from.
    struct FakeProtocol final : EgressProtocol
    {
        EgressProtocol const* continuedFrom = nullptr;

        void registerMemory(std::shared_ptr<Domain>) override
        {}

        void transferGrain(Endpoint const&, std::uint64_t, std::uint64_t, std::uint32_t, SliceRange const&, ::fi_addr_t) override
        {}

        void transferSamples(Endpoint const&, std::uint64_t, std::size_t, ::fi_addr_t) override
        {}

        void processCompletion(Endpoint const&, Completion::Data const&, ::fi_addr_t) override
        {}

        void continueFrom(EgressProtocol const& previous) noexcept override
        {
            continuedFrom = &previous;
        }

        [[nodiscard]]
        bool hasPendingWork() const override
        {
            return false;
        }

        std::size_t reset() override
        {
            return 0;
        }
    };
}

TEST_CASE("ofi: A target's new protocol continues from the one it replaces", "[ofi][Initiator]")
{
    auto previous = std::make_unique<FakeProtocol>();
    auto const* previousPtr = previous.get();
    auto next = std::make_unique<FakeProtocol>();
    auto const* nextPtr = next.get();

    SECTION("RC")
    {
        auto endpoint = RCInitiatorEndpoint{Endpoint::create(getDomain()), std::move(previous), TargetInfo{}};
        endpoint.setProtocol(std::move(next));
        REQUIRE(nextPtr->continuedFrom == previousPtr);
    }

    SECTION("RDM")
    {
        auto target = RDMInitiatorTarget{std::move(previous), TargetInfo{}};
        target.setProtocol(std::move(next));
        REQUIRE(nextPtr->continuedFrom == previousPtr);
    }
}
