#include "raft/state/learner.hpp"

namespace akit {
namespace failover {
namespace foros {
namespace raft {

void Learner::on_started() { context_->send_join_request_async(); }

void Learner::on_timedout() {}

void Learner::on_promoted_to_member() {}

void Learner::on_broadcast_timedout() {}

void Learner::on_leader_discovered() {}

void Learner::on_new_term_received() {}

void Learner::on_elected() {}

void Learner::on_terminated() {}

void Learner::entry() {}

void Learner::exit() {}

}  // namespace raft
}  // namespace foros
}  // namespace failover
}  // namespace akit
