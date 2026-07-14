#pragma once

#include <foros_msgs/msg/raft_state.hpp>
#include <foros_msgs/srv/init_raft.hpp>
#include <memory>
#include <rclcpp/rclcpp.hpp>
#include <string>

#include "akit/failover/foros/cluster_node_options.hpp"
#include "cluster_node_impl.hpp"
#include "foros_msgs/msg/raft_state.hpp"
#include "foros_msgs/srv/init_raft.hpp"
#include "raft/state_type.hpp"

namespace akit {
namespace failover {
namespace foros {

/// A standalone node to interface with raft
class RaftInterfaceNode : public rclcpp::Node {
 public:
  /// Create a new raft interface for the given cluster
  RaftInterfaceNode();

  ~RaftInterfaceNode() = default;

 private:
  /// Callback for InitRaft
  void on_init(
      const foros_msgs::srv::InitRaft::Request::SharedPtr request,
      foros_msgs::srv::InitRaft::Response::SharedPtr response
  );

  /// Publishes the current cached raft state
  void publish_state();

 private:
  std::string cluster_name_;
  uint32_t node_id_;
  ClusterNodeOptions options_{};

  raft::StateType state_;
  uint32_t leader_id_;

  std::unique_ptr<ClusterNodeImpl> impl_;

  rclcpp::Publisher<foros_msgs::msg::RaftState>::SharedPtr state_pub_;
  rclcpp::Service<foros_msgs::srv::InitRaft>::SharedPtr init_srv_;
};

}  // namespace foros
}  // namespace failover
}  // namespace akit