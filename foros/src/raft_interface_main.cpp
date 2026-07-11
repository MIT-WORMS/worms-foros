#include <rclcpp/rclcpp.hpp>

#include "akit/failover/foros/raft_interface_node.hpp"

int main(int argc, char* argv[]) {
  rclcpp::init(argc, argv);
  auto node = std::make_shared<akit::failover::foros::RaftInterfaceNode>();
  rclcpp::spin(node);
  rclcpp::shutdown();
  return 0;
}