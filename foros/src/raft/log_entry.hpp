/*
 * Copyright (c) 2021 42dot All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AKIT_FAILOVER_FOROS_RAFT_LOG_ENTRY_HPP_
#define AKIT_FAILOVER_FOROS_RAFT_LOG_ENTRY_HPP_

#include <rclcpp/macros.hpp>
#include <variant>
#include <vector>

#include "akit/failover/foros/command.hpp"
#include "raft/cluster_config.hpp"

namespace akit {
namespace failover {
namespace foros {
namespace raft {

enum class LogEntryType : uint8_t {
  kCommand = 0,
  kClusterConfig = 1,
};

class LogEntry {
 public:
  RCLCPP_SMART_PTR_DEFINITIONS(LogEntry)

  LogEntry(uint64_t id, uint64_t term, Command::SharedPtr command)
      : id_(id), term_(term), payload_(command) {}

  LogEntry(uint64_t id, uint64_t term, ClusterConfig config)
      : id_(id), term_(term), payload_(std::move(config)) {}

  LogEntryType type() const {
    return std::holds_alternative<Command::SharedPtr>(payload_)
               ? LogEntryType::kCommand
               : LogEntryType::kClusterConfig;
  }

  const Command::SharedPtr command() const {
    return std::get<Command::SharedPtr>(payload_);
  }

  const ClusterConfig& cluster_config() const {
    return std::get<ClusterConfig>(payload_);
  }

  const uint64_t id_;
  const uint64_t term_;

 private:
  std::variant<Command::SharedPtr, ClusterConfig> payload_;
};

}  // namespace raft
}  // namespace foros
}  // namespace failover
}  // namespace akit

#endif  // AKIT_FAILOVER_FOROS_RAFT_LOG_ENTRY_HPP_
