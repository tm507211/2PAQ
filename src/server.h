#include "rpc/server.h"
#include "key_value.h"
#include "circular_buffer.h"
#include <string>

template <class T>
class server {
  rpc::client* parent_; /* parent */
  rpc::client* left_;   /* left child */
  rpc::client* right_;  /* right child */

  rpc::server self_;    /* self */
  KeyValueStore<CircularBuffer<T>> kv_; /* self's key value storage */

  void register_funcs(){
    srv.bind("get", [&kv](string key){ return kv_.get(key); });
    srv.bind("put", [&kv](string key, T){ kv_.put(key, val); });
    srv.bind("remove", [&kv](string key){ kv_.remove(key); });
  }

  T get(const std::string& key){
    CircularBuffer<T>& vals = kv_[key];
    if (vals.size() <= 1){
    }
  }
  
 public:
  server(size_t port=8080) : parent_(NULL), left_(NULL), right_(NULL), self_(port) {
    register_funcs();
  }

  void set_parent(const std::string& ip, size_t port=8080){
    parent_ = new rpc::client(ip, port);
  }

  void set_left(const std::string& ip, size_t port=8080){
    left_ = new rpc::client(ip, port);
  }

  void set_right(const std::string& ip, size_t port=8080){
    right_ = new rpc::client(ip, port);
  }

  void run(){
    self_.run();
  }
};
