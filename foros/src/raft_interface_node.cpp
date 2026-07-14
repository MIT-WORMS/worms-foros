#include "akit/failover/foros/raft_interface_node.hpp"

#include <cstdint>
#include <memory>
#include <stdexcept>

#include "akit/failover/foros/cluster_node.hpp"
#include "akit/failover/foros/cluster_node_options.hpp"
#include "foros_msgs/msg/raft_state.hpp"
#include "foros_msgs/srv/init_raft.hpp"
#include "raft/state_type.hpp"

namespace akit {
namespace failover {
namespace foros {

RaftInterfaceNode::RaftInterfaceNode() : rclcpp::Node("raft_interface") {
  // Parameters
  declare_parameter<std::string>("cluster_name");
  declare_parameter<int64_t>("node_id");
  declare_parameter("eviction_timeout_ms", 20000);

  std::string cluster_name;
  int64_t node_id_64;
  unsigned int eviction_timeout_ms;

  if (!get_parameter("cluster_name", cluster_name))
    throw std::runtime_error("cluster_name is required");

  if (!get_parameter("node_id", node_id_64))
    throw std::runtime_error("node_id is required");

  get_parameter("eviction_timeout_ms", eviction_timeout_ms);

  cluster_name_ = cluster_name;
  node_id_ = static_cast<uint32_t>(node_id_64);
  options_.eviction_timeout(eviction_timeout_ms);

  // Raft state publisher
  state_pub_ = create_publisher<foros_msgs::msg::RaftState>(
      "~/state", rclcpp::QoS(1).transient_local()
  );

  // Initialization service
  init_srv_ = create_service<foros_msgs::srv::InitRaft>(
      "~/init_node_" + std::to_string(node_id_),
      std::bind(
          &RaftInterfaceNode::on_init,
          this,
          std::placeholders::_1,
          std::placeholders::_2
      )
  );

  // Publish the initial standby state
  state_ = raft::StateType::kStandby;
  leader_id_ = foros_msgs::msg::RaftState::UNKNOWN_LEADER;
  publish_state();
}

void RaftInterfaceNode::on_init(
    const foros_msgs::srv::InitRaft::Request::SharedPtr request,
    foros_msgs::srv::InitRaft::Response::SharedPtr response
) {
  // Already started
  if (impl_) {
    response->success = false;
    return;
  }

  std::vector<uint32_t> ids(request->member_ids.begin(), request->member_ids.end());

  impl_ = std::make_unique<ClusterNodeImpl>(
      cluster_name_,
      node_id_,
      ids,
      get_node_base_interface(),
      get_node_graph_interface(),
      get_node_logging_interface(),
      get_node_services_interface(),
      get_node_topics_interface(),
      get_node_timers_interface(),
      get_node_clock_interface(),
      options_
  );

  // Setup the state callback
  impl_->register_on_raft_state([this](const raft::StateType state) {
    state_ = state;
    publish_state();
  });

  // Setup the leader ID callback
  impl_->register_on_leader_discovered([this](uint32_t leader_id) {
    leader_id_ = leader_id;
    publish_state();
  });

  // Publish current state to make sure it is captured (race condition)
  state_ = impl_->get_raft_state();
  leader_id_ = impl_->get_leader_id();
  publish_state();

  response->success = true;
  if (ids.empty()) {
    RCLCPP_DEBUG(get_logger(), "Raft join process initiated");
  } else {
    RCLCPP_DEBUG(get_logger(), "Raft initialized with %zu member(s)", ids.size());
  }
}

void RaftInterfaceNode::publish_state() {
  auto msg = std::make_unique<foros_msgs::msg::RaftState>();
  msg->state = static_cast<uint8_t>(state_);
  msg->leader_id = leader_id_;
  state_pub_->publish(std::move(msg));
}

}  // namespace foros
}  // namespace failover
}  // namespace akit