#pragma once

#include <memory>

#include "raft/context.hpp"
#include "raft/event.hpp"
#include "raft/state.hpp"

namespace akit {
namespace failover {
namespace foros {
namespace raft {

class Learner final : public State {
 public:
  explicit Learner(std::shared_ptr<Context> context, rclcpp::Logger& logger)
      : State(
            StateType::kLearner,
            {
                {Event::kStarted, StateType::kStay},
                {Event::kTerminated, StateType::kStandby},
                {Event::kPromotedToMember, StateType::kFollower},
            },
            context,
            logger
        ) {}

  void on_started() override;
  void on_timedout() override;
  void on_promoted_to_member() override;
  void on_broadcast_timedout() override;
  void on_leader_discovered() override;
  void on_new_term_received() override;
  void on_elected() override;
  void on_terminated() override;

  void entry() override;
  void exit() override;
};

}  // namespace raft
}  // namespace foros
}  // namespace failover
}  // namespace akit
