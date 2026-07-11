builtin_interfaces/Time stamp   # timestamp
string cluster_name             # cluster name
uint32 cluster_size             # cluster size
uint32 id                       # id
foros_msgs/RaftState raft       # current state
uint64 term                     # current term
uint64 data_size                # data size
uint32 voted_for                # id voted by this node

string TOPIC_NAME = "/foros/inspector"
