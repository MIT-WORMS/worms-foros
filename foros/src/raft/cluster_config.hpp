#pragma once
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>

namespace akit {
namespace failover {
namespace foros {
namespace raft {

struct ClusterConfig {
  std::vector<uint32_t> member_ids;

  uint32_t quorum_size() const {
    return static_cast<uint32_t>(member_ids.size() / 2 + 1);
  }

  bool is_member(uint32_t node_id) const {
    return std::find(member_ids.begin(), member_ids.end(), node_id) != member_ids.end();
  }

  std::vector<uint8_t> serialize() const {
    std::vector<uint8_t> out;
    uint32_t count = member_ids.size();

    // Stored as [count][id0][id1]...
    out.resize(sizeof(count) + count * sizeof(uint32_t));

    uint8_t* ptr = out.data();

    memcpy(ptr, &count, sizeof(count));
    ptr += sizeof(count);
    memcpy(ptr, member_ids.data(), count * sizeof(uint32_t));

    return out;
  }

  static ClusterConfig deserialize(const std::vector<uint8_t>& bytes) {
    return deserialize(reinterpret_cast<const char*>(bytes.data()));
  }

  static ClusterConfig deserialize(const char* bytes) {
    ClusterConfig cfg;

    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(bytes);

    uint32_t count;

    // Get count first
    memcpy(&count, ptr, sizeof(count));
    ptr += sizeof(count);

    // Remainder is member IDs
    cfg.member_ids.resize(count);
    memcpy(cfg.member_ids.data(), ptr, count * sizeof(uint32_t));

    return cfg;
  }
};

}  // namespace raft
}  // namespace foros
}  // namespace failover
}  // namespace akit