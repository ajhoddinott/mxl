// SPDX-FileCopyrightText: 2026 Contributors to the Media eXchange Layer project.
//
// SPDX-License-Identifier: Apache-2.0

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <random>
#include <utility>
#include <vector>
#include <uuid.h>
#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>
#include <picojson/wrapper.h>
#include <rdma/fabric.h>
#include <mxl/fabrics.h>
#include "mxl/flow.h"
#include "mxl/mxl.h"
#include "../Utils.hpp"

#ifdef MXL_FABRICS_OFI
// clang-format off
    #include "AudioBounceBuffer.hpp"
    #include "TargetInfo.hpp"
// clang-format on
#else
#endif

namespace
{
    constexpr auto POLL_TIMEOUT = std::chrono::seconds(5);
    constexpr auto BLOCKING_WAIT = std::chrono::milliseconds(20);

    template<typename ProgressFn>
    void requireProgressStatus(ProgressFn&& progressFn, char const* actor)
    {
        auto status = progressFn();
        if (status != MXL_STATUS_OK && status != MXL_ERR_NOT_READY)
        {
            FAIL(std::string{"Something went wrong in the "} + actor + ": " + std::to_string(status));
        }
    }

    template<typename StepFn>
    void pollUntilSuccess(StepFn&& stepFn, std::chrono::steady_clock::duration timeout, char const* timeoutMessage)
    {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        do
        {
            if (stepFn())
            {
                return;
            }
        }
        while (std::chrono::steady_clock::now() < deadline);

        FAIL(timeoutMessage);
    }

    template<typename TargetProgressFn, typename InitiatorProgressFn>
    void waitForConnection(TargetProgressFn&& targetProgressFn, InitiatorProgressFn&& initiatorProgressFn)
    {
        pollUntilSuccess(
            [&]()
            {
                targetProgressFn();

                auto status = initiatorProgressFn();
                if (status != MXL_STATUS_OK && status != MXL_ERR_NOT_READY)
                {
                    FAIL("Something went wrong in the initiator: " + std::to_string(status));
                }

                return status == MXL_STATUS_OK;
            },
            POLL_TIMEOUT,
            "Failed to connect in 5 seconds");
    }

    template<typename Targets, typename TargetProgressFn, typename InitiatorProgressFn>
    void waitForAllConnections(Targets const& targets, TargetProgressFn&& targetProgressFn, InitiatorProgressFn&& initiatorProgressFn)
    {
        waitForConnection(
            [&]()
            {
                for (auto& target : targets)
                {
                    targetProgressFn(target);
                }
            },
            std::forward<InitiatorProgressFn>(initiatorProgressFn));
    }

    template<typename TargetProgressFn, typename InitiatorProgressFn, typename TransferFn>
    void waitForTransferStart(TargetProgressFn&& targetProgressFn, InitiatorProgressFn&& initiatorProgressFn, TransferFn&& transferFn)
    {
        pollUntilSuccess(
            [&]()
            {
                targetProgressFn();
                initiatorProgressFn();
                return transferFn() == MXL_STATUS_OK;
            },
            POLL_TIMEOUT,
            "Failed to start transfer in 5 seconds");
    }

    template<typename Targets, typename TargetProgressFn, typename InitiatorProgressFn, typename TransferFn>
    void waitForAllTransfersStart(Targets const& targets, TargetProgressFn&& targetProgressFn, InitiatorProgressFn&& initiatorProgressFn,
        TransferFn&& transferFn)
    {
        waitForTransferStart(
            [&]()
            {
                for (auto& target : targets)
                {
                    targetProgressFn(target);
                }
            },
            std::forward<InitiatorProgressFn>(initiatorProgressFn),
            std::forward<TransferFn>(transferFn));
    }

    template<typename InitiatorProgressFn, typename CompletionFn>
    void waitForTransferCompletion(InitiatorProgressFn&& initiatorProgressFn, CompletionFn&& completionFn)
    {
        pollUntilSuccess(
            [&]()
            {
                initiatorProgressFn();

                auto status = completionFn();
                if (status == MXL_ERR_INTERRUPTED)
                {
                    FAIL("Peer disconnected before the transfer completed");
                }

                return status == MXL_STATUS_OK;
            },
            POLL_TIMEOUT,
            "Transfer did not complete in 5 seconds");
    }

    template<typename Targets, typename InitiatorProgressFn, typename CompletionFn>
    void waitForAllTransfersCompletion(Targets const& targets, InitiatorProgressFn&& initiatorProgressFn, CompletionFn&& completionFn)
    {
        auto transfer_complete = std::vector<bool>(targets.size(), false);

        pollUntilSuccess(
            [&]()
            {
                initiatorProgressFn();

                for (size_t i = 0; i < targets.size(); i++)
                {
                    auto status = completionFn(targets[i]);
                    if (status == MXL_ERR_INTERRUPTED)
                    {
                        FAIL("Peer disconnected before the transfer completed");
                    }
                    if (status == MXL_STATUS_OK)
                    {
                        transfer_complete[i] = true;
                    }
                }

                return std::ranges::all_of(transfer_complete, [](bool v) { return v; });
            },
            POLL_TIMEOUT,
            "Transfer did not complete in 5 seconds");
    }
}

TEST_CASE("Fabrics basic creation/destroy", "[fabrics][basics]")
{
    auto instance = mxlCreateInstance("/dev/shm/", "");

    mxlFabricsInstance fabrics;
    SECTION("instance creation/destruction")
    {
        REQUIRE(mxlFabricsCreateInstance(instance, nullptr, &fabrics) == MXL_STATUS_OK);

        SECTION("target creation/destruction")
        {
            mxlFabricsTarget target;
            REQUIRE(mxlFabricsCreateTarget(fabrics, &target) == MXL_STATUS_OK);
            REQUIRE(mxlFabricsDestroyTarget(fabrics, target) == MXL_STATUS_OK);
        }

        SECTION("initiator creation/destruction")
        {
            mxlFabricsInitiator initiator;
            REQUIRE(mxlFabricsCreateInitiator(fabrics, &initiator) == MXL_STATUS_OK);
            REQUIRE(mxlFabricsDestroyInitiator(fabrics, initiator) == MXL_STATUS_OK);
        }

        REQUIRE(mxlFabricsDestroyInstance(fabrics) == MXL_STATUS_OK);
    }
}

TEST_CASE_PERSISTENT_FIXTURE(mxl::tests::mxlDomainFixture, "Fabrics connection oriented activation tests", "[fabrics][connected][activation]")
{
    auto instance = mxlCreateInstance(domain.c_str(), "");

    auto flowDef = mxl::tests::readFile("../data/v210_flow.json");
    auto const flowId = "5fbec3b1-1b0f-417d-9059-8b94a47197ed";

    // A flow writer and reader provide the memory regions for the target and initiator respectively.
    mxlFlowWriter writer;
    REQUIRE(mxlCreateFlowWriter(instance, flowDef.c_str(), nullptr, &writer, nullptr, nullptr) == MXL_STATUS_OK);

    mxlFlowReader reader;
    REQUIRE(mxlCreateFlowReader(instance, flowId, "", &reader) == MXL_STATUS_OK);

    mxlFabricsInstance fabrics;
    REQUIRE(mxlFabricsCreateInstance(instance, nullptr, &fabrics) == MXL_STATUS_OK);

    mxlFabricsTarget target;
    REQUIRE(mxlFabricsCreateTarget(fabrics, &target) == MXL_STATUS_OK);

    mxlFabricsInitiator initiator;
    REQUIRE(mxlFabricsCreateInitiator(fabrics, &initiator) == MXL_STATUS_OK);

    SECTION("target/initiator setup")
    {
        auto targetConfig = mxlFabricsTargetConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_TCP,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "127.0.0.1", .service = nullptr},
                          .attr = nullptr},
            .writer = writer,
        };
        mxlFabricsTargetInfo targetInfo;
        REQUIRE(mxlFabricsTargetSetup(target, &targetConfig, nullptr, &targetInfo) == MXL_STATUS_OK);

        auto initiatorConfig = mxlFabricsInitiatorConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_TCP,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "127.0.0.1", .service = nullptr},
                          .attr = nullptr},
            .reader = reader,
        };
        REQUIRE(mxlFabricsInitiatorSetup(initiator, &initiatorConfig, nullptr) == MXL_STATUS_OK);

        SECTION("initiator add/remove target")
        {
            REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo) == MXL_STATUS_OK);

            SECTION("non-blocking")
            {
                std::uint64_t dummyIndex;
                waitForConnection([&]() { mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); },
                    [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });
            }

            SECTION("blocking")
            {
                std::uint64_t dummyIndex;
                waitForConnection([&]() { mxlFabricsTargetReadGrain(target, BLOCKING_WAIT.count(), &dummyIndex); },
                    [&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); });
            }

            REQUIRE(mxlFabricsInitiatorRemoveTarget(initiator, targetInfo) == MXL_STATUS_OK);
        }

        REQUIRE(mxlFabricsFreeTargetInfo(targetInfo) == MXL_STATUS_OK);
    }

    REQUIRE(mxlFabricsDestroyInitiator(fabrics, initiator) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyTarget(fabrics, target) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInstance(fabrics) == MXL_STATUS_OK);
    REQUIRE(mxlReleaseFlowReader(instance, reader) == MXL_STATUS_OK);
    REQUIRE(mxlReleaseFlowWriter(instance, writer) == MXL_STATUS_OK);
    REQUIRE(mxlDestroyInstance(instance) == MXL_STATUS_OK);
}

TEST_CASE_PERSISTENT_FIXTURE(mxl::tests::mxlDomainFixture, "Fabrics connectionless activation tests", "[fabrics][connectionless][activation]")
{
    auto instance = mxlCreateInstance(domain.c_str(), "");

    auto flowDef = mxl::tests::readFile("../data/v210_flow.json");
    auto const flowId = "5fbec3b1-1b0f-417d-9059-8b94a47197ed";

    // A flow writer and reader provide the memory regions for the target and initiator respectively.
    mxlFlowWriter writer;
    REQUIRE(mxlCreateFlowWriter(instance, flowDef.c_str(), nullptr, &writer, nullptr, nullptr) == MXL_STATUS_OK);

    mxlFlowReader reader;
    REQUIRE(mxlCreateFlowReader(instance, flowId, "", &reader) == MXL_STATUS_OK);

    mxlFabricsInstance fabrics;
    REQUIRE(mxlFabricsCreateInstance(instance, nullptr, &fabrics) == MXL_STATUS_OK);

    mxlFabricsTarget target;
    REQUIRE(mxlFabricsCreateTarget(fabrics, &target) == MXL_STATUS_OK);

    mxlFabricsInitiator initiator;
    REQUIRE(mxlFabricsCreateInitiator(fabrics, &initiator) == MXL_STATUS_OK);

    auto targetConfig = mxlFabricsTargetConfig{
        .version = MXL_FABRICS_API_VERSION,
        .interface = {.version = MXL_FABRICS_API_VERSION,
                      .provider = MXL_FABRICS_PROVIDER_SHM,
                      .caps =
                {
                    .version = MXL_FABRICS_API_VERSION,
                    .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                    .maxMessageSize = 0,
                }, .address = {.node = "target", .service = "activation"},
                      .attr = nullptr},
        .writer = writer,
    };
    mxlFabricsTargetInfo targetInfo;
    REQUIRE(mxlFabricsTargetSetup(target, &targetConfig, nullptr, &targetInfo) == MXL_STATUS_OK);

    auto initiatorConfig = mxlFabricsInitiatorConfig{
        .version = MXL_FABRICS_API_VERSION,
        .interface = {.version = MXL_FABRICS_API_VERSION,
                      .provider = MXL_FABRICS_PROVIDER_SHM,
                      .caps =
                {
                    .version = MXL_FABRICS_API_VERSION,
                    .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                    .maxMessageSize = 0,
                }, .address = {.node = "initiator", .service = "activation"},
                      .attr = nullptr},
        .reader = reader,
    };
    REQUIRE(mxlFabricsInitiatorSetup(initiator, &initiatorConfig, nullptr) == MXL_STATUS_OK);

    SECTION("initiator add/remove target")
    {
        REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo) == MXL_STATUS_OK);
        REQUIRE(mxlFabricsInitiatorRemoveTarget(initiator, targetInfo) == MXL_STATUS_OK);
    }
    REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo) == MXL_STATUS_OK);

    SECTION("non-blocking")
    {
        std::uint64_t dummyIndex;
        waitForConnection([&]() { mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); },
            [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });
    }

    SECTION("blocking")
    {
        std::uint64_t dummyIndex;
        waitForConnection([&]() { mxlFabricsTargetReadGrain(target, BLOCKING_WAIT.count(), &dummyIndex); },
            [&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); });
    }

    REQUIRE(mxlFabricsDestroyInitiator(fabrics, initiator) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsFreeTargetInfo(targetInfo) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyTarget(fabrics, target) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInstance(fabrics) == MXL_STATUS_OK);
    REQUIRE(mxlReleaseFlowReader(instance, reader) == MXL_STATUS_OK);
    REQUIRE(mxlReleaseFlowWriter(instance, writer) == MXL_STATUS_OK);
    REQUIRE(mxlDestroyInstance(instance) == MXL_STATUS_OK);
}

TEST_CASE_PERSISTENT_FIXTURE(mxl::tests::mxlDomainFixture, "Fabrics: Transfer Grain with flows", "[Fabrics][Transfer][Flows][Grain]")
{
    auto instance = mxlCreateInstance(domain.c_str(), "");

    mxlFabricsInstance fabrics;
    REQUIRE(mxlFabricsCreateInstance(instance, nullptr, &fabrics) == MXL_STATUS_OK);

    mxlFabricsTarget target;
    REQUIRE(mxlFabricsCreateTarget(fabrics, &target) == MXL_STATUS_OK);

    mxlFabricsInitiator initiator;
    REQUIRE(mxlFabricsCreateInitiator(fabrics, &initiator) == MXL_STATUS_OK);

    auto flowDef = mxl::tests::readFile("../data/v210_flow.json");
    auto const flowId = "5fbec3b1-1b0f-417d-9059-8b94a47197ed";

    mxlFlowWriter writer;
    REQUIRE(mxlCreateFlowWriter(instance, flowDef.c_str(), nullptr, &writer, nullptr, nullptr) == MXL_STATUS_OK);

    // Initiator
    mxlFlowReader reader;
    REQUIRE(mxlCreateFlowReader(instance, flowId, "", &reader) == MXL_STATUS_OK);

    SECTION("RC")
    {
        auto targetConfig = mxlFabricsTargetConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_TCP,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "127.0.0.1", .service = nullptr},
                          .attr = nullptr},
            .writer = writer,
        };
        mxlFabricsTargetInfo targetInfo;
        REQUIRE(mxlFabricsTargetSetup(target, &targetConfig, nullptr, &targetInfo) == MXL_STATUS_OK);

        auto initiatorConfig = mxlFabricsInitiatorConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_TCP,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "127.0.0.1", .service = nullptr},
                          .attr = nullptr},
            .reader = reader,
        };
        REQUIRE(mxlFabricsInitiatorSetup(initiator, &initiatorConfig, nullptr) == MXL_STATUS_OK);
        REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo) == MXL_STATUS_OK);

        std::uint64_t dummyIndex;
        waitForConnection([&]() { mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); },
            [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });

        SECTION("non-blocking")
        {
            waitForTransferStart([&]() { mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); },
                [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferGrain(initiator, 0, 0, 1); });

            waitForTransferCompletion([&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&]() { return mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); });
        }

        SECTION("blocking")
        {
            waitForTransferStart([&]() { mxlFabricsTargetReadGrain(target, BLOCKING_WAIT.count(), &dummyIndex); },
                [&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferGrain(initiator, 0, 0, 1); });

            waitForTransferCompletion([&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&]() { return mxlFabricsTargetReadGrain(target, BLOCKING_WAIT.count(), &dummyIndex); });
        }

        mxlFabricsFreeTargetInfo(targetInfo);
    }

    SECTION("RDM")
    {
        auto targetConfig = mxlFabricsTargetConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_SHM,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "target", .service = "test"},
                          .attr = nullptr},
            .writer = writer,
        };
        mxlFabricsTargetInfo targetInfo;
        REQUIRE(mxlFabricsTargetSetup(target, &targetConfig, nullptr, &targetInfo) == MXL_STATUS_OK);

        auto initiatorConfig = mxlFabricsInitiatorConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_SHM,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "initiator", .service = "test"},
                          .attr = nullptr},
            .reader = reader,
        };
        REQUIRE(mxlFabricsInitiatorSetup(initiator, &initiatorConfig, nullptr) == MXL_STATUS_OK);
        REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo) == MXL_STATUS_OK);

        std::uint64_t dummyIndex;
        waitForConnection([&]() { mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); },
            [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });

        SECTION("non-blocking")
        {
            waitForTransferStart([&]() { mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); },
                [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferGrain(initiator, 0, 0, 1); });

            waitForTransferCompletion([&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&]() { return mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); });
        }

        SECTION("blocking")
        {
            waitForTransferStart([&]() { mxlFabricsTargetReadGrain(target, BLOCKING_WAIT.count(), &dummyIndex); },
                [&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferGrain(initiator, 0, 0, 1); });

            waitForTransferCompletion([&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&]() { return mxlFabricsTargetReadGrain(target, BLOCKING_WAIT.count(), &dummyIndex); });
        }

        mxlFabricsFreeTargetInfo(targetInfo);
    }

    REQUIRE(mxlReleaseFlowReader(instance, reader) == MXL_STATUS_OK);
    REQUIRE(mxlReleaseFlowWriter(instance, writer) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInitiator(fabrics, initiator) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyTarget(fabrics, target) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInstance(fabrics) == MXL_STATUS_OK);
    REQUIRE(mxlDestroyInstance(instance) == MXL_STATUS_OK);
}

TEST_CASE_PERSISTENT_FIXTURE(mxl::tests::mxlDomainFixture, "Fabrics: Transfer Sample with flows", "[Fabrics][Transfer][Flows][Sample]")
{
    auto instance = mxlCreateInstance(domain.c_str(), "");

    mxlFabricsInstance fabrics;
    REQUIRE(mxlFabricsCreateInstance(instance, nullptr, &fabrics) == MXL_STATUS_OK);

    mxlFabricsTarget target;
    REQUIRE(mxlFabricsCreateTarget(fabrics, &target) == MXL_STATUS_OK);

    mxlFabricsInitiator initiator;
    REQUIRE(mxlFabricsCreateInitiator(fabrics, &initiator) == MXL_STATUS_OK);

    auto flowDef = mxl::tests::readFile("../data/audio_flow.json");
    auto const flowId = "b3bb5be7-9fe9-4324-a5bb-4c70e1084449";

    mxlFlowWriter writer;
    REQUIRE(mxlCreateFlowWriter(instance, flowDef.c_str(), nullptr, &writer, nullptr, nullptr) == MXL_STATUS_OK);

    // Initiator
    mxlFlowReader reader;
    REQUIRE(mxlCreateFlowReader(instance, flowId, "", &reader) == MXL_STATUS_OK);

    SECTION("RC")
    {
        auto targetConfig = mxlFabricsTargetConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_TCP,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "127.0.0.1", .service = nullptr},
                          .attr = nullptr},
            .writer = writer,
        };
        mxlFabricsTargetInfo targetInfo;
        REQUIRE(mxlFabricsTargetSetup(target, &targetConfig, nullptr, &targetInfo) == MXL_STATUS_OK);

        auto initiatorConfig = mxlFabricsInitiatorConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_TCP,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "127.0.0.1", .service = nullptr},
                          .attr = nullptr},
            .reader = reader,
        };
        REQUIRE(mxlFabricsInitiatorSetup(initiator, &initiatorConfig, nullptr) == MXL_STATUS_OK);
        REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo) == MXL_STATUS_OK);

        std::uint64_t dummyIndex;
        std::size_t dummyCount;
        waitForConnection([&]() { mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); },
            [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });

        SECTION("non-blocking")
        {
            waitForTransferStart([&]() { mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); },
                [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferSamples(initiator, 0, 480); });

            waitForTransferCompletion([&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&]() { return mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); });
        }

        SECTION("blocking")
        {
            waitForTransferStart([&]() { mxlFabricsTargetReadSamples(target, BLOCKING_WAIT.count(), &dummyIndex, &dummyCount); },
                [&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferSamples(initiator, 0, 480); });

            waitForTransferCompletion([&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&]() { return mxlFabricsTargetReadSamples(target, BLOCKING_WAIT.count(), &dummyIndex, &dummyCount); });
        }

        mxlFabricsFreeTargetInfo(targetInfo);
    }

    SECTION("RDM")
    {
        auto targetConfig = mxlFabricsTargetConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_SHM,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "target", .service = "test"},
                          .attr = nullptr},
            .writer = writer,
        };
        mxlFabricsTargetInfo targetInfo;
        REQUIRE(mxlFabricsTargetSetup(target, &targetConfig, nullptr, &targetInfo) == MXL_STATUS_OK);

        auto initiatorConfig = mxlFabricsInitiatorConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_SHM,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "initiator", .service = "test"},
                          .attr = nullptr},
            .reader = reader,
        };
        REQUIRE(mxlFabricsInitiatorSetup(initiator, &initiatorConfig, nullptr) == MXL_STATUS_OK);
        REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo) == MXL_STATUS_OK);

        std::uint64_t dummyIndex;
        std::size_t dummyCount;
        waitForConnection([&]() { mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); },
            [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });

        SECTION("non-blocking")
        {
            waitForTransferStart([&]() { mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); },
                [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferSamples(initiator, 0, 480); });

            waitForTransferCompletion([&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&]() { return mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); });
        }

        SECTION("blocking")
        {
            waitForTransferStart([&]() { mxlFabricsTargetReadSamples(target, BLOCKING_WAIT.count(), &dummyIndex, &dummyCount); },
                [&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferSamples(initiator, 0, 480); });

            waitForTransferCompletion([&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&]() { return mxlFabricsTargetReadSamples(target, BLOCKING_WAIT.count(), &dummyIndex, &dummyCount); });
        }

        mxlFabricsFreeTargetInfo(targetInfo);
    }

    REQUIRE(mxlReleaseFlowReader(instance, reader) == MXL_STATUS_OK);
    REQUIRE(mxlReleaseFlowWriter(instance, writer) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInitiator(fabrics, initiator) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyTarget(fabrics, target) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInstance(fabrics) == MXL_STATUS_OK);
    REQUIRE(mxlDestroyInstance(instance) == MXL_STATUS_OK);
}

TEST_CASE_PERSISTENT_FIXTURE(mxl::tests::mxlDomainFixture, "Fabrics: Transfer Samples larger than the target bounce buffer entry",
    "[Fabrics][Transfer][Flows][Sample]")
{
    auto instance = mxlCreateInstance(domain.c_str(), "");

    mxlFabricsInstance fabrics;
    REQUIRE(mxlFabricsCreateInstance(instance, nullptr, &fabrics) == MXL_STATUS_OK);

    mxlFabricsTarget target;
    REQUIRE(mxlFabricsCreateTarget(fabrics, &target) == MXL_STATUS_OK);

    mxlFabricsInitiator initiator;
    REQUIRE(mxlFabricsCreateInitiator(fabrics, &initiator) == MXL_STATUS_OK);

    // The default sync batch size hint (480 samples at 48 kHz) sizes the target's bounce buffer entries.
    auto flowDef = mxl::tests::readFile("../data/audio_flow.json");
    auto const flowId = "b3bb5be7-9fe9-4324-a5bb-4c70e1084449";

    mxlFlowWriter writer;
    mxlFlowConfigInfo configInfo;
    REQUIRE(mxlCreateFlowWriter(instance, flowDef.c_str(), nullptr, &writer, &configInfo, nullptr) == MXL_STATUS_OK);
    REQUIRE(configInfo.common.maxSyncBatchSizeHint == 480);

    mxlFlowReader reader;
    REQUIRE(mxlCreateFlowReader(instance, flowId, "", &reader) == MXL_STATUS_OK);

    auto const iface = mxlFabricsInterfaceConfig{
        .version = MXL_FABRICS_API_VERSION,
        .provider = MXL_FABRICS_PROVIDER_TCP,
        .caps =
            {
                   .version = MXL_FABRICS_API_VERSION,
                   .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                   .maxMessageSize = 0,
                   },
        .address = {.node = "127.0.0.1",                                  .service = nullptr                                                                                                          },
        .attr = nullptr
    };

    auto targetConfig = mxlFabricsTargetConfig{.version = MXL_FABRICS_API_VERSION, .interface = iface, .writer = writer};
    mxlFabricsTargetInfo targetInfo;
    REQUIRE(mxlFabricsTargetSetup(target, &targetConfig, nullptr, &targetInfo) == MXL_STATUS_OK);

    auto initiatorConfig = mxlFabricsInitiatorConfig{.version = MXL_FABRICS_API_VERSION, .interface = iface, .reader = reader};
    REQUIRE(mxlFabricsInitiatorSetup(initiator, &initiatorConfig, nullptr) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo) == MXL_STATUS_OK);

    std::uint64_t dummyIndex;
    std::size_t dummyCount;
    waitForConnection([&]() { mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); },
        [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });

    // Returns the (head index, count) pairs the target reports, in order.
    auto transfer = [&](std::uint64_t headIndex, std::size_t count)
    {
        REQUIRE(mxlFabricsInitiatorTransferSamples(initiator, headIndex, count) == MXL_STATUS_OK);

        auto received = std::vector<std::pair<std::uint64_t, std::size_t>>{};
        auto receivedCount = std::size_t{0};
        pollUntilSuccess(
            [&]()
            {
                requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator");

                auto index = std::uint64_t{0};
                auto n = std::size_t{0};
                if (mxlFabricsTargetReadSamples(target, BLOCKING_WAIT.count(), &index, &n) == MXL_STATUS_OK)
                {
                    received.emplace_back(index, n);
                    receivedCount += n;
                }
                return receivedCount >= count;
            },
            POLL_TIMEOUT,
            "Transfer did not complete in 5 seconds");
        return received;
    };

    SECTION("801 samples arrive as 480 then 321")
    {
        auto const received = transfer(801, 801);
        REQUIRE(received == std::vector<std::pair<std::uint64_t, std::size_t>>{
                                {480, 480},
                                {801, 321}
        });
    }

    SECTION("800 samples arrive as 480 then 320")
    {
        auto const received = transfer(800, 800);
        REQUIRE(received == std::vector<std::pair<std::uint64_t, std::size_t>>{
                                {480, 480},
                                {800, 320}
        });
    }

    SECTION("801 samples across the end of the ring arrive as 480 then 321")
    {
        // The second part covers samples 19435 to 19755, across the end of the 19456-sample ring.
        REQUIRE(configInfo.continuous.bufferLength == 19456);
        auto const received = transfer(19756, 801);
        REQUIRE(received == std::vector<std::pair<std::uint64_t, std::size_t>>{
                                {19435, 480},
                                {19756, 321}
        });
    }

    SECTION("a split transfer that would start before index 0 is rejected")
    {
        // The first part would end at index -321, which has no ring position.
        REQUIRE(mxlFabricsInitiatorTransferSamples(initiator, 0, 801) == MXL_ERR_INVALID_ARG);
        REQUIRE(mxlFabricsInitiatorMakeProgressNonBlocking(initiator) == MXL_STATUS_OK);
    }

    SECTION("a transfer larger than the source ring is rejected")
    {
        auto const count = std::size_t{configInfo.continuous.bufferLength} + 1;
        REQUIRE(mxlFabricsInitiatorTransferSamples(initiator, 100000, count) == MXL_ERR_INVALID_ARG);
        REQUIRE(mxlFabricsInitiatorMakeProgressNonBlocking(initiator) == MXL_STATUS_OK);
    }

    mxlFabricsFreeTargetInfo(targetInfo);
    REQUIRE(mxlReleaseFlowReader(instance, reader) == MXL_STATUS_OK);
    REQUIRE(mxlReleaseFlowWriter(instance, writer) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInitiator(fabrics, initiator) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyTarget(fabrics, target) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInstance(fabrics) == MXL_STATUS_OK);
    REQUIRE(mxlDestroyInstance(instance) == MXL_STATUS_OK);
}

TEST_CASE_PERSISTENT_FIXTURE(mxl::tests::mxlDomainFixture, "Fabrics: Initiator transfers from a recreated flow", "[Fabrics][Transfer][Flows][Sample]")
{
    auto instance = mxlCreateInstance(domain.c_str(), "");
    // The source flow's writer has its own instance, as it would be in another process.
    auto sourceInstance = mxlCreateInstance(domain.c_str(), "");
    // The new reader comes from another instance, so it can be opened while the old one is still mapped.
    auto newReaderInstance = mxlCreateInstance(domain.c_str(), "");

    mxlFabricsInstance fabrics;
    REQUIRE(mxlFabricsCreateInstance(instance, nullptr, &fabrics) == MXL_STATUS_OK);

    mxlFabricsTarget target;
    REQUIRE(mxlFabricsCreateTarget(fabrics, &target) == MXL_STATUS_OK);

    mxlFabricsInitiator initiator;
    REQUIRE(mxlFabricsCreateInitiator(fabrics, &initiator) == MXL_STATUS_OK);

    // The target writes to its own flow, with the same layout, so that recreating the source flow does not affect it.
    auto const sourceDef = mxl::tests::readFile("../data/audio_flow.json");
    auto const sourceId = std::string{"b3bb5be7-9fe9-4324-a5bb-4c70e1084449"};
    auto const targetId = std::string{"3e4f0d6a-7c55-4b8e-9a51-2f7c86d1b0e3"};
    auto targetDef = sourceDef;
    targetDef.replace(targetDef.find(sourceId), sourceId.size(), targetId);

    mxlFlowWriter sourceWriter;
    REQUIRE(mxlCreateFlowWriter(sourceInstance, sourceDef.c_str(), nullptr, &sourceWriter, nullptr, nullptr) == MXL_STATUS_OK);
    mxlFlowReader reader;
    REQUIRE(mxlCreateFlowReader(instance, sourceId.c_str(), "", &reader) == MXL_STATUS_OK);

    mxlFlowWriter targetWriter;
    REQUIRE(mxlCreateFlowWriter(instance, targetDef.c_str(), nullptr, &targetWriter, nullptr, nullptr) == MXL_STATUS_OK);
    mxlFlowReader targetReader;
    REQUIRE(mxlCreateFlowReader(instance, targetId.c_str(), "", &targetReader) == MXL_STATUS_OK);

    // Writes count samples up to head index into every channel, all bytes set to value.
    auto writeSamples = [](mxlFlowWriter writer, std::uint64_t index, std::size_t count, std::uint8_t value)
    {
        mxlMutableWrappedMultiBufferSlice slices;
        REQUIRE(mxlFlowWriterOpenSamples(writer, index, count, &slices) == MXL_STATUS_OK);
        for (auto channel = std::size_t{0}; channel < slices.count; ++channel)
        {
            for (auto const& fragment : slices.base.fragments)
            {
                std::memset(static_cast<std::uint8_t*>(fragment.pointer) + (channel * slices.stride), value, fragment.size);
            }
        }
        REQUIRE(mxlFlowWriterCommitSamples(writer) == MXL_STATUS_OK);
    };

    // Transfers count samples up to head index, then checks that every byte of them arrived in the target's flow as value.
    auto transferAndCheck = [&](std::uint64_t index, std::size_t count, std::uint8_t value)
    {
        std::uint64_t dummyIndex;
        std::size_t dummyCount;
        waitForTransferStart([&]() { mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); },
            [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
            [&]() { return mxlFabricsInitiatorTransferSamples(initiator, index, count); });
        pollUntilSuccess(
            [&]()
            {
                requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator");
                auto headIndex = std::uint64_t{0};
                auto n = std::size_t{0};
                return (mxlFabricsTargetReadSamples(target, BLOCKING_WAIT.count(), &headIndex, &n) == MXL_STATUS_OK) && (headIndex == index);
            },
            POLL_TIMEOUT,
            "Transfer did not complete in 5 seconds");
        pollUntilSuccess([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator) == MXL_STATUS_OK; },
            POLL_TIMEOUT,
            "Initiator did not complete in 5 seconds");

        // The samples are in the target's flow; committing makes them readable, as a target application does.
        mxlMutableWrappedMultiBufferSlice dummy;
        REQUIRE(mxlFlowWriterOpenSamples(targetWriter, index, count, &dummy) == MXL_STATUS_OK);
        REQUIRE(mxlFlowWriterCommitSamples(targetWriter) == MXL_STATUS_OK);

        mxlWrappedMultiBufferSlice slices;
        REQUIRE(mxlFlowReaderGetSamplesNonBlocking(targetReader, index, count, &slices) == MXL_STATUS_OK);
        for (auto channel = std::size_t{0}; channel < slices.count; ++channel)
        {
            for (auto const& fragment : slices.base.fragments)
            {
                auto const* bytes = static_cast<std::uint8_t const*>(fragment.pointer) + (channel * slices.stride);
                REQUIRE(std::all_of(bytes, bytes + fragment.size, [&](std::uint8_t b) { return b == value; }));
            }
        }
    };

    // Recreates the source flow and switches the initiator to a reader of the new one.
    auto recreateSource = [&]()
    {
        // Releasing the last writer deletes the flow; recreating it gives new memory at the same path.
        REQUIRE(mxlReleaseFlowWriter(sourceInstance, sourceWriter) == MXL_STATUS_OK);
        REQUIRE(mxlCreateFlowWriter(sourceInstance, sourceDef.c_str(), nullptr, &sourceWriter, nullptr, nullptr) == MXL_STATUS_OK);
        mxlWrappedMultiBufferSlice dummySlices;
        REQUIRE(mxlFlowReaderGetSamplesNonBlocking(reader, 1100, 100, &dummySlices) == MXL_ERR_FLOW_INVALID);

        // Opened before the old reader is released, so the new flow cannot be mapped at the old one's address.
        mxlFlowReader newReader;
        REQUIRE(mxlCreateFlowReader(newReaderInstance, sourceId.c_str(), "", &newReader) == MXL_STATUS_OK);
        REQUIRE(mxlFabricsInitiatorSetReader(initiator, newReader) == MXL_STATUS_OK);
        REQUIRE(mxlReleaseFlowReader(instance, reader) == MXL_STATUS_OK);
        reader = newReader;
    };

    // Transfers went to bounce buffer entries 0 and 1 before the change; the next one must use entry 2, not overwrite entry 0, which the
    // target may not have read yet. The target runs in this process, so its entry headers can be read where the remote addresses are virtual
    // addresses (SHM, not TCP).
#ifdef MXL_FABRICS_OFI
    auto checkBounceEntries = [&](mxlFabricsTargetInfo targetInfo)
    {
        auto const& entries = mxl::lib::fabrics::ofi::TargetInfo::fromAPI(targetInfo)->remoteRegions;
        auto const entryHeadIndex = [&](std::size_t entry)
        {
            // NOLINTNEXTLINE(performance-no-int-to-ptr): the address is the target's, in this process.
            return reinterpret_cast<mxl::lib::fabrics::ofi::AudioEntryHeader const*>(entries[entry].addr)->headIndex;
        };
        if (entries[0].addr != 0)
        {
            REQUIRE(entryHeadIndex(0) == 1000);
            REQUIRE(entryHeadIndex(2) == 1100);
        }
    };
#endif

    // A flow with a different layout is rejected, and so is any change while a removed target may still have writes in flight.
    auto checkRejections = [&](mxlFabricsTargetInfo targetInfo)
    {
        auto const videoDef = mxl::tests::readFile("../data/v210_flow.json");
        mxlFlowWriter videoWriter;
        REQUIRE(mxlCreateFlowWriter(sourceInstance, videoDef.c_str(), nullptr, &videoWriter, nullptr, nullptr) == MXL_STATUS_OK);
        mxlFlowReader videoReader;
        REQUIRE(mxlCreateFlowReader(instance, "5fbec3b1-1b0f-417d-9059-8b94a47197ed", "", &videoReader) == MXL_STATUS_OK);
        REQUIRE(mxlFabricsInitiatorSetReader(initiator, videoReader) == MXL_ERR_INVALID_ARG);
        REQUIRE(mxlReleaseFlowReader(instance, videoReader) == MXL_STATUS_OK);
        REQUIRE(mxlReleaseFlowWriter(sourceInstance, videoWriter) == MXL_STATUS_OK);

        std::uint64_t dummyIndex;
        std::size_t dummyCount;
        waitForTransferStart([&]() { mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); },
            [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
            [&]() { return mxlFabricsInitiatorTransferSamples(initiator, 1100, 100); });
        REQUIRE(mxlFabricsInitiatorRemoveTarget(initiator, targetInfo) == MXL_STATUS_OK);
        REQUIRE(mxlFabricsInitiatorSetReader(initiator, reader) == MXL_ERR_NOT_READY);
        // Once progress reports nothing pending, the change succeeds. Progress comes first, so it cannot report that before the write completes.
        pollUntilSuccess(
            [&]()
            {
                auto const status = mxlFabricsInitiatorMakeProgressNonBlocking(initiator);
                mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount);
                return status == MXL_STATUS_OK;
            },
            POLL_TIMEOUT,
            "The removed target's writes did not complete in 5 seconds");
        REQUIRE(mxlFabricsInitiatorSetReader(initiator, reader) == MXL_STATUS_OK);
    };

    auto run = [&](mxlFabricsInterfaceConfig const& targetIface, mxlFabricsInterfaceConfig const& initiatorIface)
    {
        auto targetConfig = mxlFabricsTargetConfig{.version = MXL_FABRICS_API_VERSION, .interface = targetIface, .writer = targetWriter};
        mxlFabricsTargetInfo targetInfo;
        REQUIRE(mxlFabricsTargetSetup(target, &targetConfig, nullptr, &targetInfo) == MXL_STATUS_OK);

        auto initiatorConfig = mxlFabricsInitiatorConfig{.version = MXL_FABRICS_API_VERSION, .interface = initiatorIface, .reader = reader};
        REQUIRE(mxlFabricsInitiatorSetup(initiator, &initiatorConfig, nullptr) == MXL_STATUS_OK);
        REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo) == MXL_STATUS_OK);

        std::uint64_t dummyIndex;
        std::size_t dummyCount;
        waitForConnection([&]() { mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); },
            [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });

        writeSamples(sourceWriter, 1000, 100, 0x11);
        transferAndCheck(1000, 100, 0x11);

        // The reader cannot be changed while a transfer is pending.
        waitForTransferStart([&]() { mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); },
            [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
            [&]() { return mxlFabricsInitiatorTransferSamples(initiator, 1000, 100); });
        REQUIRE(mxlFabricsInitiatorSetReader(initiator, reader) == MXL_ERR_NOT_READY);
        waitForConnection([&]() { mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); },
            [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });

        recreateSource();
        writeSamples(sourceWriter, 1100, 100, 0x22);
        transferAndCheck(1100, 100, 0x22);
#ifdef MXL_FABRICS_OFI
        checkBounceEntries(targetInfo);
#endif
        checkRejections(targetInfo);

        mxlFabricsFreeTargetInfo(targetInfo);
    };

    auto iface = [](mxlFabricsProvider provider, char const* node, char const* service)
    {
        return mxlFabricsInterfaceConfig{
            .version = MXL_FABRICS_API_VERSION,
            .provider = provider,
            .caps = {.version = MXL_FABRICS_API_VERSION,
                     .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                     .maxMessageSize = 0},
            .address = {.node = node, .service = service},
            .attr = nullptr,
        };
    };

    SECTION("RC")
    {
        run(iface(MXL_FABRICS_PROVIDER_TCP, "127.0.0.1", nullptr), iface(MXL_FABRICS_PROVIDER_TCP, "127.0.0.1", nullptr));
    }

    SECTION("RDM")
    {
        run(iface(MXL_FABRICS_PROVIDER_SHM, "target", "test"), iface(MXL_FABRICS_PROVIDER_SHM, "initiator", "test"));
    }

    REQUIRE(mxlReleaseFlowReader(newReaderInstance, reader) == MXL_STATUS_OK);
    REQUIRE(mxlReleaseFlowReader(instance, targetReader) == MXL_STATUS_OK);
    REQUIRE(mxlReleaseFlowWriter(instance, targetWriter) == MXL_STATUS_OK);
    REQUIRE(mxlReleaseFlowWriter(sourceInstance, sourceWriter) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInitiator(fabrics, initiator) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyTarget(fabrics, target) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInstance(fabrics) == MXL_STATUS_OK);
    REQUIRE(mxlDestroyInstance(newReaderInstance) == MXL_STATUS_OK);
    REQUIRE(mxlDestroyInstance(sourceInstance) == MXL_STATUS_OK);
    REQUIRE(mxlDestroyInstance(instance) == MXL_STATUS_OK);
}

TEST_CASE_PERSISTENT_FIXTURE(mxl::tests::mxlDomainFixture, "Fabrics: Transfer Grain with flows multi target",
    "[Fabrics][Transfer][Flows][Grain][Multi-targets]")
{
    auto flowDef = mxl::tests::readFile("../data/v210_flow.json");
    auto jsonValue = picojson::value{};
    REQUIRE(picojson::parse(jsonValue, flowDef).empty());
    REQUIRE(jsonValue.is<picojson::object>());
    auto root = jsonValue.get<picojson::object>();

    mxlFabricsInstance fabrics;
    auto instance = mxlCreateInstance(domain.c_str(), "");
    REQUIRE(mxlFabricsCreateInstance(instance, nullptr, &fabrics) == MXL_STATUS_OK);

    constexpr auto nbTargets = 2;
    std::array<mxlFabricsTarget, nbTargets> targets;
    std::array<std::string, nbTargets> flowIds;
    std::array<std::string, nbTargets> flowDefs;
    std::array<mxlFlowConfigInfo, nbTargets> configInfo;
    std::array<mxlFlowWriter, nbTargets> writer;
    auto engine = std::mt19937{std::random_device{}()};
    auto uuidGen = uuids::uuid_random_generator{engine};
    for (size_t i = 0; i < nbTargets; i++)
    {
        REQUIRE(mxlFabricsCreateTarget(fabrics, &targets[i]) == MXL_STATUS_OK);
        flowIds[i] = uuids::to_string(uuidGen());
        root.at("id") = picojson::value{flowIds[i]};
        flowDefs[i] = picojson::value{root}.serialize();
        REQUIRE(mxlCreateFlowWriter(instance, flowDefs[i].c_str(), nullptr, &writer[i], &configInfo[i], nullptr) == MXL_STATUS_OK);
    }

    // Initiator
    mxlFlowReader reader;
    REQUIRE(mxlCreateFlowReader(instance, root.at("id").get<std::string>().c_str(), "", &reader) == MXL_STATUS_OK);

    mxlFabricsInitiator initiator;
    REQUIRE(mxlFabricsCreateInitiator(fabrics, &initiator) == MXL_STATUS_OK);

    SECTION("RC")
    {
        auto initiatorConfig = mxlFabricsInitiatorConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_TCP,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "127.0.0.1", .service = nullptr},
                          .attr = nullptr},
            .reader = reader,
        };

        REQUIRE(mxlFabricsInitiatorSetup(initiator, &initiatorConfig, nullptr) == MXL_STATUS_OK);

        std::array<mxlFabricsTargetConfig, nbTargets> targetConfig;
        std::array<mxlFabricsTargetInfo, nbTargets> targetInfo;
        for (size_t i = 0; i < nbTargets; i++)
        {
            targetConfig[i] = mxlFabricsTargetConfig{
                .version = MXL_FABRICS_API_VERSION,
                .interface = {.version = MXL_FABRICS_API_VERSION,
                              .provider = MXL_FABRICS_PROVIDER_TCP,
                              .caps =
                        {
                            .version = MXL_FABRICS_API_VERSION,
                            .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                            .maxMessageSize = 0,
                        }, .address = {.node = "127.0.0.1", .service = nullptr},
                              .attr = nullptr},
                .writer = writer[i],
            };
            REQUIRE(mxlFabricsTargetSetup(targets[i], &targetConfig[i], nullptr, &targetInfo[i]) == MXL_STATUS_OK);

            REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo[i]) == MXL_STATUS_OK);
        }

        std::uint64_t dummyIndex;
        waitForAllConnections(
            targets,
            [&](auto& target) { requireProgressStatus([&]() { return mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); }, "target"); },
            [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });

        SECTION("non-blocking")
        {
            waitForAllTransfersStart(
                targets,
                [&](auto& target) { mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); },
                [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferGrain(initiator, 0, 0, 1); });

            waitForAllTransfersCompletion(
                targets,
                [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&](auto const& target) { return mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); });
        }

        SECTION("blocking")
        {
            waitForAllTransfersStart(
                targets,
                [&](auto& target) { mxlFabricsTargetReadGrain(target, BLOCKING_WAIT.count(), &dummyIndex); },
                [&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferGrain(initiator, 0, 0, 1); });

            waitForAllTransfersCompletion(
                targets,
                [&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&](auto const& target) { return mxlFabricsTargetReadGrain(target, BLOCKING_WAIT.count(), &dummyIndex); });
        }

        for (size_t i = 0; i < nbTargets; i++)
        {
            mxlFabricsFreeTargetInfo(targetInfo[i]);
        }
    }

    SECTION("RDM")
    {
        auto initiatorConfig = mxlFabricsInitiatorConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_SHM,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "initiator", .service = "test"},
                          .attr = nullptr},
            .reader = reader,
        };
        REQUIRE(mxlFabricsInitiatorSetup(initiator, &initiatorConfig, nullptr) == MXL_STATUS_OK);

        std::array<mxlFabricsTargetConfig, nbTargets> targetConfig;
        std::array<mxlFabricsTargetInfo, nbTargets> targetInfo;
        for (size_t i = 0; i < nbTargets; i++)
        {
            targetConfig[i] = mxlFabricsTargetConfig{
                .version = MXL_FABRICS_API_VERSION,
                .interface = {.version = MXL_FABRICS_API_VERSION,
                              .provider = MXL_FABRICS_PROVIDER_SHM,
                              .caps =
                        {
                            .version = MXL_FABRICS_API_VERSION,
                            .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                            .maxMessageSize = 0,
                        }, .address = {.node = "target", .service = "test"},
                              .attr = nullptr},
                .writer = writer[i],
            };
            REQUIRE(mxlFabricsTargetSetup(targets[i], &targetConfig[i], nullptr, &targetInfo[i]) == MXL_STATUS_OK);

            REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo[i]) == MXL_STATUS_OK);
        }

        std::uint64_t dummyIndex;
        waitForAllConnections(
            targets,
            [&](auto& target) { requireProgressStatus([&]() { return mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); }, "target"); },
            [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });

        SECTION("non-blocking")
        {
            waitForAllTransfersStart(
                targets,
                [&](auto& target) { mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); },
                [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferGrain(initiator, 0, 0, 1); });

            waitForAllTransfersCompletion(
                targets,
                [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&](auto const& target) { return mxlFabricsTargetReadGrainNonBlocking(target, &dummyIndex); });
        }

        SECTION("blocking")
        {
            waitForAllTransfersStart(
                targets,
                [&](auto& target) { mxlFabricsTargetReadGrain(target, BLOCKING_WAIT.count(), &dummyIndex); },
                [&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferGrain(initiator, 0, 0, 1); });

            waitForAllTransfersCompletion(
                targets,
                [&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&](auto const& target) { return mxlFabricsTargetReadGrain(target, BLOCKING_WAIT.count(), &dummyIndex); });
        }

        for (size_t i = 0; i < nbTargets; i++)
        {
            mxlFabricsFreeTargetInfo(targetInfo[i]);
        }
    }

    REQUIRE(mxlReleaseFlowReader(instance, reader) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInitiator(fabrics, initiator) == MXL_STATUS_OK);
    for (size_t i = 0; i < nbTargets; i++)
    {
        REQUIRE(mxlReleaseFlowWriter(instance, writer[i]) == MXL_STATUS_OK);
        REQUIRE(mxlFabricsDestroyTarget(fabrics, targets[i]) == MXL_STATUS_OK);
    }
    REQUIRE(mxlFabricsDestroyInstance(fabrics) == MXL_STATUS_OK);
    REQUIRE(mxlDestroyInstance(instance) == MXL_STATUS_OK);
}

TEST_CASE_PERSISTENT_FIXTURE(mxl::tests::mxlDomainFixture, "Fabrics: Transfer Samples with flows multi target",
    "[Fabrics][Transfer][Flows][Samples][Multi-targets]")
{
    auto flowDef = mxl::tests::readFile("../data/audio_flow.json");
    auto jsonValue = picojson::value{};
    REQUIRE(picojson::parse(jsonValue, flowDef).empty());
    REQUIRE(jsonValue.is<picojson::object>());
    auto root = jsonValue.get<picojson::object>();

    mxlFabricsInstance fabrics;
    auto instance = mxlCreateInstance(domain.c_str(), "");
    REQUIRE(mxlFabricsCreateInstance(instance, nullptr, &fabrics) == MXL_STATUS_OK);

    constexpr auto nbTargets = 2;
    std::array<mxlFabricsTarget, nbTargets> targets;
    std::array<std::string, nbTargets> flowIds;
    std::array<std::string, nbTargets> flowDefs;
    std::array<mxlFlowConfigInfo, nbTargets> configInfo;
    std::array<mxlFlowWriter, nbTargets> writer;
    auto engine = std::mt19937{std::random_device{}()};
    auto uuidGen = uuids::uuid_random_generator{engine};
    for (size_t i = 0; i < nbTargets; i++)
    {
        REQUIRE(mxlFabricsCreateTarget(fabrics, &targets[i]) == MXL_STATUS_OK);
        flowIds[i] = uuids::to_string(uuidGen());
        root.at("id") = picojson::value{flowIds[i]};
        flowDefs[i] = picojson::value{root}.serialize();
        REQUIRE(mxlCreateFlowWriter(instance, flowDefs[i].c_str(), nullptr, &writer[i], &configInfo[i], nullptr) == MXL_STATUS_OK);
    }

    // Initiator
    mxlFlowReader reader;
    REQUIRE(mxlCreateFlowReader(instance, root.at("id").get<std::string>().c_str(), "", &reader) == MXL_STATUS_OK);

    mxlFabricsInitiator initiator;
    REQUIRE(mxlFabricsCreateInitiator(fabrics, &initiator) == MXL_STATUS_OK);

    SECTION("RC")
    {
        auto initiatorConfig = mxlFabricsInitiatorConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_TCP,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "127.0.0.1", .service = nullptr},
                          .attr = nullptr},
            .reader = reader,
        };

        REQUIRE(mxlFabricsInitiatorSetup(initiator, &initiatorConfig, nullptr) == MXL_STATUS_OK);

        std::array<mxlFabricsTargetConfig, nbTargets> targetConfig;
        std::array<mxlFabricsTargetInfo, nbTargets> targetInfo;
        for (size_t i = 0; i < nbTargets; i++)
        {
            targetConfig[i] = mxlFabricsTargetConfig{
                .version = MXL_FABRICS_API_VERSION,
                .interface = {.version = MXL_FABRICS_API_VERSION,
                              .provider = MXL_FABRICS_PROVIDER_TCP,
                              .caps =
                        {
                            .version = MXL_FABRICS_API_VERSION,
                            .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                            .maxMessageSize = 0,
                        }, .address = {.node = "127.0.0.1", .service = nullptr},
                              .attr = nullptr},
                .writer = writer[i],
            };
            REQUIRE(mxlFabricsTargetSetup(targets[i], &targetConfig[i], nullptr, &targetInfo[i]) == MXL_STATUS_OK);

            REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo[i]) == MXL_STATUS_OK);
        }

        std::uint64_t dummyIndex;
        std::size_t dummyCount;
        waitForAllConnections(
            targets,
            [&](auto& target)
            { requireProgressStatus([&]() { return mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); }, "target"); },
            [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });

        SECTION("non-blocking")
        {
            waitForAllTransfersStart(
                targets,
                [&](auto& target) { mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); },
                [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferSamples(initiator, 0, 480); });

            waitForAllTransfersCompletion(
                targets,
                [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&](auto const& target) { return mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); });
        }

        SECTION("blocking")
        {
            waitForAllTransfersStart(
                targets,
                [&](auto& target) { mxlFabricsTargetReadSamples(target, BLOCKING_WAIT.count(), &dummyIndex, &dummyCount); },
                [&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferSamples(initiator, 0, 480); });

            waitForAllTransfersCompletion(
                targets,
                [&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&](auto const& target) { return mxlFabricsTargetReadSamples(target, BLOCKING_WAIT.count(), &dummyIndex, &dummyCount); });
        }

        for (size_t i = 0; i < nbTargets; i++)
        {
            mxlFabricsFreeTargetInfo(targetInfo[i]);
        }
    }

    SECTION("RDM")
    {
        auto initiatorConfig = mxlFabricsInitiatorConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_SHM,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "initiator", .service = "test"},
                          .attr = nullptr},
            .reader = reader,
        };
        REQUIRE(mxlFabricsInitiatorSetup(initiator, &initiatorConfig, nullptr) == MXL_STATUS_OK);

        std::array<mxlFabricsTargetConfig, nbTargets> targetConfig;
        std::array<mxlFabricsTargetInfo, nbTargets> targetInfo;
        for (size_t i = 0; i < nbTargets; i++)
        {
            targetConfig[i] = mxlFabricsTargetConfig{
                .version = MXL_FABRICS_API_VERSION,
                .interface = {.version = MXL_FABRICS_API_VERSION,
                              .provider = MXL_FABRICS_PROVIDER_SHM,
                              .caps =
                        {
                            .version = MXL_FABRICS_API_VERSION,
                            .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                            .maxMessageSize = 0,
                        }, .address = {.node = "target", .service = "test"},
                              .attr = nullptr},
                .writer = writer[i],
            };
            REQUIRE(mxlFabricsTargetSetup(targets[i], &targetConfig[i], nullptr, &targetInfo[i]) == MXL_STATUS_OK);

            REQUIRE(mxlFabricsInitiatorAddTarget(initiator, targetInfo[i]) == MXL_STATUS_OK);
        }

        std::uint64_t dummyIndex;
        std::size_t dummyCount;
        waitForAllConnections(
            targets,
            [&](auto& target)
            { requireProgressStatus([&]() { return mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); }, "target"); },
            [&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); });

        SECTION("non-blocking")
        {
            waitForAllTransfersStart(
                targets,
                [&](auto& target) { mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); },
                [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferSamples(initiator, 0, 480); });

            waitForAllTransfersCompletion(
                targets,
                [&]() { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressNonBlocking(initiator); }, "initiator"); },
                [&](auto const& target) { return mxlFabricsTargetReadSamplesNonBlocking(target, &dummyIndex, &dummyCount); });
        }

        SECTION("blocking")
        {
            waitForAllTransfersStart(
                targets,
                [&](auto& target) { mxlFabricsTargetReadSamples(target, BLOCKING_WAIT.count(), &dummyIndex, &dummyCount); },
                [&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&]() { return mxlFabricsInitiatorTransferSamples(initiator, 0, 480); });

            waitForAllTransfersCompletion(
                targets,
                [&]()
                { requireProgressStatus([&]() { return mxlFabricsInitiatorMakeProgressBlocking(initiator, BLOCKING_WAIT.count()); }, "initiator"); },
                [&](auto const& target) { return mxlFabricsTargetReadSamples(target, BLOCKING_WAIT.count(), &dummyIndex, &dummyCount); });
        }

        for (size_t i = 0; i < nbTargets; i++)
        {
            mxlFabricsFreeTargetInfo(targetInfo[i]);
        }
    }

    REQUIRE(mxlReleaseFlowReader(instance, reader) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInitiator(fabrics, initiator) == MXL_STATUS_OK);
    for (size_t i = 0; i < nbTargets; i++)
    {
        REQUIRE(mxlReleaseFlowWriter(instance, writer[i]) == MXL_STATUS_OK);
        REQUIRE(mxlFabricsDestroyTarget(fabrics, targets[i]) == MXL_STATUS_OK);
    }
    REQUIRE(mxlFabricsDestroyInstance(fabrics) == MXL_STATUS_OK);
    REQUIRE(mxlDestroyInstance(instance) == MXL_STATUS_OK);
}

#ifdef MXL_FABRICS_OFI
TEST_CASE_PERSISTENT_FIXTURE(mxl::tests::mxlDomainFixture, "Fabrics: TargetInfo serialize/deserialize", "[fabrics][ofi][target-info]")
{
    namespace ofi = mxl::lib::fabrics::ofi;

    auto instance = mxlCreateInstance(domain.c_str(), "");
    mxlFabricsInstance fabrics;
    mxlFabricsTarget target;
    mxlFabricsTargetInfo targetInfo;

    REQUIRE(mxlFabricsCreateInstance(instance, nullptr, &fabrics) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsCreateTarget(fabrics, &target) == MXL_STATUS_OK);

    auto flowDef = mxl::tests::readFile("../data/v210_flow.json");
    mxlFlowWriter writer;
    REQUIRE(mxlCreateFlowWriter(instance, flowDef.c_str(), nullptr, &writer, nullptr, nullptr) == MXL_STATUS_OK);

    auto config = mxlFabricsTargetConfig{
        .version = MXL_FABRICS_API_VERSION,
        .interface = {.version = MXL_FABRICS_API_VERSION,
                      .provider = MXL_FABRICS_PROVIDER_TCP,
                      .caps =
                {
                    .version = MXL_FABRICS_API_VERSION,
                    .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                    .maxMessageSize = 0,
                }, .address = {.node = "127.0.0.1", .service = nullptr},
                      .attr = nullptr},
        .writer = writer,
    };

    // Retrieve the target info from the target setup
    REQUIRE(mxlFabricsTargetSetup(target, &config, nullptr, &targetInfo) == MXL_STATUS_OK);

    // Serialize the target info to a string
    size_t targetInfoStrSize = 0;
    REQUIRE(mxlFabricsTargetInfoToString(targetInfo, nullptr, &targetInfoStrSize) == MXL_STATUS_OK);
    auto targetInfoStr = std::string{};
    targetInfoStr.resize(targetInfoStrSize);
    REQUIRE(mxlFabricsTargetInfoToString(targetInfo, targetInfoStr.data(), &targetInfoStrSize) == MXL_STATUS_OK);

    // Deserialize the target info from the string
    mxlFabricsTargetInfo deserializedTargetInfo;
    REQUIRE(mxlFabricsTargetInfoFromString(targetInfoStr.c_str(), &deserializedTargetInfo) == MXL_STATUS_OK);

    // Now compare that the original and deserialized target info are the same
    auto targetInfoIn = ofi::TargetInfo::fromAPI(targetInfo);
    auto targetInfoOut = ofi::TargetInfo::fromAPI(deserializedTargetInfo);
    REQUIRE(*targetInfoIn == *targetInfoOut);

    // Cleanup
    REQUIRE(mxlFabricsDestroyTarget(fabrics, target) == MXL_STATUS_OK);
    REQUIRE(mxlFabricsDestroyInstance(fabrics) == MXL_STATUS_OK);
    REQUIRE(mxlReleaseFlowWriter(instance, writer) == MXL_STATUS_OK);
    REQUIRE(mxlDestroyInstance(instance) == MXL_STATUS_OK);
}

TEST_CASE_PERSISTENT_FIXTURE(mxl::tests::mxlDomainFixture, "Fabrics: Target setup honors cqDepth option", "[fabrics][cqdepth]")
{
    auto instance = mxlCreateInstance(domain.c_str(), "");
    mxlFabricsInstance fabrics;
    REQUIRE(mxlFabricsCreateInstance(instance, nullptr, &fabrics) == MXL_STATUS_OK);

    auto flowDef = mxl::tests::readFile("../data/v210_flow.json");
    mxlFlowWriter writer;
    REQUIRE(mxlCreateFlowWriter(instance, flowDef.c_str(), nullptr, &writer, nullptr, nullptr) == MXL_STATUS_OK);

    auto makeConfig = [&]()
    {
        return mxlFabricsTargetConfig{
            .version = MXL_FABRICS_API_VERSION,
            .interface = {.version = MXL_FABRICS_API_VERSION,
                          .provider = MXL_FABRICS_PROVIDER_TCP,
                          .caps =
                    {
                        .version = MXL_FABRICS_API_VERSION,
                        .flags = MXL_FABRICS_IFACE_CAP_BLOCKING_OPERATIONS | MXL_FABRICS_IFACE_CAP_REMOTE_WRITE,
                        .maxMessageSize = 0,
                    }, .address = {.node = "127.0.0.1", .service = "0"},
                          .attr = nullptr},
            .writer = writer,
        };
    };

    auto setupWithOptions = [&](char const* options, mxlFabricsTargetInfo* outInfo)
    {
        mxlFabricsTarget target;
        REQUIRE(mxlFabricsCreateTarget(fabrics, &target) == MXL_STATUS_OK);
        auto config = makeConfig();
        auto const status = mxlFabricsTargetSetup(target, &config, options, outInfo);
        REQUIRE(mxlFabricsDestroyTarget(fabrics, target) == MXL_STATUS_OK);
        return status;
    };

    SECTION("a valid cqDepth option is accepted")
    {
        mxlFabricsTargetInfo targetInfo;
        REQUIRE(setupWithOptions(R"({"cqDepth": 64})", &targetInfo) == MXL_STATUS_OK);
    }

    SECTION("no options selects the default and is accepted")
    {
        mxlFabricsTargetInfo targetInfo;
        REQUIRE(setupWithOptions(nullptr, &targetInfo) == MXL_STATUS_OK);
        REQUIRE(setupWithOptions("", &targetInfo) == MXL_STATUS_OK);
    }

    SECTION("an invalid cqDepth option is rejected")
    {
        mxlFabricsTargetInfo targetInfo;
        // Zero / negative depth.
        REQUIRE(setupWithOptions(R"({"cqDepth": 0})", &targetInfo) == MXL_ERR_INVALID_ARG);
        // Wrong type.
        REQUIRE(setupWithOptions(R"({"cqDepth": "big"})", &targetInfo) == MXL_ERR_INVALID_ARG);
        // Malformed JSON.
        REQUIRE(setupWithOptions("{not json", &targetInfo) == MXL_ERR_INVALID_ARG);
    }

    REQUIRE(mxlFabricsDestroyInstance(fabrics) == MXL_STATUS_OK);
    REQUIRE(mxlReleaseFlowWriter(instance, writer) == MXL_STATUS_OK);
    REQUIRE(mxlDestroyInstance(instance) == MXL_STATUS_OK);
}
#endif
