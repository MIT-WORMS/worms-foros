uint32 leader_id
uint8 state

uint8 UNKNOWN = 0
uint8 STANDBY = 1
uint8 FOLLOWER = 2
uint8 CANDIDATE = 3
uint8 LEADER = 4
uint8 LEARNER = 5

uint32 UNKNOWN_LEADER = 0xFFFFFFFF