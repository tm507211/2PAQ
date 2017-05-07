/*************************************************************************************
    Author: Charlie Murphy
    Email:  tcm3@cs.princeton.edu

    Date:   April 22, 2017

    Description: A simple implementation for Two Phase Commit Servers with AQ.
    Commits all queries (doesn't ditch older queries after newer queries are committed)
 *************************************************************************************/

#include "rpc/server.h"
#include "rpc/client.h"
#include "key_value.h"
#include "hash_table.h"
#include "circular_buffer.h"
#include <vector>
#include <string>

#ifndef CM_SERVER_2PC
#define CM_SERVER_2PC

#define PUT 0
#define REMOVE 1

template <class T>
class Server {
  rpc::server self_;                                     /* self */
  std::vector<rpc::client*> others_;     /* others[0] == Leader */
  bool leader_;                                          /* Am I the Leader? */
  
  KeyValueStore<std::string, std::pair< std::pair<T,size_t>,CircularBuffer<size_t> >> kv_;   		/*Latest committed value and list of pending queries (new versions) */
  
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
    self_.bind("join", [this](std::string address, size_t port = 8080){ this->others_.push_back( new rpc::client(address, port)); });
    self_.bind("version", [this](std::string key){ return this->get_version(key);});			/*Leader gives version number*/
   
    self_.bind("stage", [this](std::string key, T val, Action act, size_t query){ this->stage(key, val, act, query); });
    self_.bind("commit", [this](size_t query){ this->commit(query); });
  }

//Check if clean else ask leader for version number
  T get(const std::string& key){
    if (leader_)
      return ((kv_.get(key)).first).first;
    if(isclean(key))
      return ((kv_.get(key)).first).first;  
    return get_val(others_[0]->call("version", key).template as<size_t>()); //Asks leader for version number
  }

  void put(const std::string& key, const T& val){
    if (leader_){
      stage(key, val, PUT, next_query_++);
    } else {
      others_[0]->send("put", key, val); /* All calls must be redirected to leader */
    }
  }

/* Checks if value is unique */
bool isclean(const std::string& key){
    CircularBuffer<size_t> tmp_ver((kv_.get(key)).second); 
    if(tmp_ver.size()==0){
       return true;
    }
    return false;
  }

/* Add new version to the circualr buffer */
void add_version(const std::string& key, size_t query){
   CircularBuffer<size_t> new_ver((kv_.get(key)).second);
   new_ver.insert(query);
   kv_.put(key,std::make_pair((kv_.get(key)).first, new_ver));
}
  
void remove(const std::string& key){
    if (leader_){
      stage(key, T(), REMOVE, next_query_++);
    }
    else{
      others_[0]->send("remove", key);
    }
  }

void acknowledge(size_t query){
    --queries_[query].acks;
    if (queries_[query].acks == 0){
      commit(query);
    }
  }
  
 void stage(const std::string& key, const T& val, Action act, size_t query){
    if (leader_){
      if (others_.size() == 0){
        switch(act){
          case PUT:
    	    kv_.put(key, std::make_pair(std::make_pair(val,query), CircularBuffer<size_t>() )); 			//new value and empty list
  	  break;
          case REMOVE:
	    kv_.remove(key);
	  break;
        }
        return;
      }
      queries_.insert(query, Query(key, val, act, others_.size()));
      add_version(key,query);			
      for (size_t i = 0; i < others_.size(); ++i){
        others_[i]->send("stage", key, val, act, query);
      }
    }
    else {
      queries_.insert(query, Query(key, val, act));
      add_version(key,query);				
      others_[0]->send("acknowledge", query);
    }
  }


  void commit(size_t query){
    Query q = queries_[query];
    queries_.remove(query);
    CircularBuffer<size_t> tmp_ver((kv_.get(q.key)).second);
    tmp_ver.remove_element(query);			//Removes from circular buffer				
    switch (q.action){
      case PUT:
	kv_.put(q.key, std::make_pair(std::make_pair(q.val,query),tmp_ver));     //update latest commit value and query number 
        break;
      case REMOVE:
        if(tmp_ver.size()==0) kv_.remove(q.key);			//is this necessary?
        else{
          kv_.put(q.key, std::make_pair(std::make_pair(T(),query),tmp_ver));
        }
        break;
    }
    if (leader_){
      for (size_t i = 0; i < others_.size(); ++i){
	others_[i]->send("commit", query);
      }
    }
  }

/* Leader returns latest committed version number */
size_t get_version(const std::string& key){
      	return ((kv_.get(key)).first).second;
}  

/* Value of the given version/query */
T get_val(size_t query){
   Query q = queries_[query];
   switch(q.action){
      case PUT:
        return q.val;
      case REMOVE:
        return T();
   }
   return T();
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
