/*************************************************************************************
    Author: Charlie Murphy
    Email:  tcm3@cs.princeton.edu

    Date:   April 22, 2017

    Description: A simple implementation for Two Phase Commit Servers
 *************************************************************************************/

#include "rpc/server.h"
#include "rpc/client.h"
#include "key_value.h"
#include "hash_table.h"
#include <vector>
#include <string>
#include <iostream>

#ifndef CM_SERVER_2PC
#define CM_SERVER_2PC

#define PUT 0
#define REMOVE 1

template <class T>
class Server {
  rpc::server self_;                                     /* self */
  std::vector<rpc::client*> others_;                     /* others[0] == Leader */
  bool leader_;                                          /* Am I the Leader? */
  
  KeyValueStore<std::string, T> kv_;                     /* self's key value storage */

  typedef char Action;

  struct Query{
    Query() {}
    Query(const std::string& k, const T& val, Action act, size_t a = 0) : key(k), val(val), action(act), acks(a) {}
    std::string key;
    T val;
    Action action;
    size_t acks;    /* If acks == 0 then ready to commit */
  };

  HashTable<size_t, Query> queries_;
  size_t next_query_;

  void register_funcs(){
    self_.bind("get", [this](std::string key){ return this->get(key); });
    self_.bind("put", [this](std::string key, T val){ this->put(key, val); });
    self_.bind("remove", [this](std::string key){ this->remove(key); });
    self_.bind("acknowledge", [this](size_t query){ this->acknowledge(query); });
    self_.bind("join", [this](std::string address, size_t port = 8080){ this->join(address, port); });
    self_.bind("stage", [this](std::string key, T val, Action act, size_t query){ this->stage(key, val, act, query); });
    self_.bind("commit", [this](size_t query){ this->commit(query); });
  }

  T get(const std::string& key){
    if (leader_)
      return kv_.get(key);
    return others_[0]->call("get", key).template as<T>();
  }

  void put(const std::string& key, const T& val){
    if (leader_){
      stage(key, val, PUT, next_query_++);
    } else {
      others_[0]->send("put", key, val); /* All calls must be redirected to leader */
    }
  }

  void remove(const std::string& key){
    if (leader_){
      stage(key, T(), REMOVE, next_query_++);
    } else {
      others_[0]->send("remove", key);
    }
  }

  void acknowledge(size_t query){
    --queries_[query].acks;
    if (queries_[query].acks == 0){
      commit(query);
    }
  }

  void join(const std::string& addr, const size_t port){
    others_.push_back(new rpc::client(address, port));
  }

  void stage(const std::string& key, const T& val, Action act, size_t query){
    if (leader_){
      if (others_.size() == 0){
        switch(act){
          case PUT:
    	    kv_.put(key, val);
  	  break;
          case REMOVE:
	    kv_.remove(key);
	  break;
        }
	return;
      }
      std::cout << "OTHERS" << std::endl;
      queries_.insert(query, Query(key, val, act, others_.size()));
      for (size_t i = 0; i < others_.size(); ++i){
        others_[i]->send("stage", key, val, act, query);
      }
    }
    else {
      queries_.insert(query, Query(key, val, act));
      others_[0]->send("acknowledge", query);
    }
  }

  void commit(size_t query){
    Query q = queries_[query];
    queries_.remove(query);
        switch (q.action){
      case PUT:
	kv_.put(q.key, q.val);
        break;
      case REMOVE:
	kv_.remove(q.key);
        break;
    }
    if (leader_){
      for (size_t i = 0; i < others_.size(); ++i){
	others_[i]->send("commit", query);
      }
    }
  }
  
 public:
  Server(size_t port=8080) : self_(port), leader_(false), next_query_(0) {
    register_funcs();
  }

  ~Server(){
    for (size_t i = 0; i < others_.size(); ++i){
      delete others_[i];
    }
  }

  void run(std::string self_addr, size_t self_port, std::string address, size_t port){
    rpc::client client(address, port);
    std::pair<std::string, size_t> leader = client.call("leader", self_addr, self_port).as<std::pair<std::string, size_t>>();
    self_.async_run();
    if (leader == std::make_pair(self_addr, self_port)){
      leader_ = true;
    } else {
      others_.push_back(new rpc::client(leader.first, leader.second));
      while (others_[0]->get_connection_state() != rpc::client::connection_state::connected);
      others_[0]->send("join", self_addr, self_port);
    }
  }
};

#endif
