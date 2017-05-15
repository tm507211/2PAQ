/*************************************************************************************
    Author: Charlie Murphy
    Email:  tcm3@cs.princeton.edu

    Date:   April 22, 2017

    Description: A simple implementation for Replicated Key-Value Store using
                 Two Phase Commit For Consensus
 *************************************************************************************/

#include "rpc/server.h"
#include "rpc/client.h"
#include "key_value.h"
#include "hash_table.h"
#include "circular_buffer.h"
#include <vector>
#include <string>
#include <iostream>
#include <future>
#include <chrono>
#include <mutex>
#include <atomic>

#ifndef CM_SERVER_2PC
#define CM_SERVER_2PC

#define PUT 0
#define REMOVE 1
#define DONE 2

#define ALIVE_TIME 5000 /* A commit may take upto max(ALIVE_TIME, others_[i]->get_timeout()) ms */

template <class T>
class Server {
  rpc::server* self_;                                    /* self */
  std::vector<rpc::client*> others_;                     /* others[0] == Leader */
  std::vector<std::pair<std::string, size_t>> others_id_;/* Only used by Leader */
  std::vector<bool> alive_;                              /* Did node i respond back in time? */
  bool leader_;                                          /* Am I the Leader? */
  std::atomic<bool> ready_;                              /* Am I finished joining the system? */
  std::atomic<bool> pulse_;                              /* Has the Leader contacted me recently? */


  /* The type of versions */
  struct versions_t{
    versions_t () : current(0), valid(false) {}
    versions_t (size_t v) : current(v), valid(true) {}
    size_t current;
    bool valid;
    CircularBuffer<size_t> versions;
  };
  
  KeyValueStore<std::string, versions_t> kv_;                     /* self's key value storage */

  typedef char Action;
  typedef std::chrono::steady_clock::time_point TIME_STAMP;
  
  struct Query{
    Query() {}
    Query(const std::string& k, const T& val, Action act, TIME_STAMP now, size_t a = 0) : key(k), val(val), action(act), time(now), acks(a) { who.resize(a, false); }
    std::string key;
    T val;
    Action action;
    std::vector<bool> who;
    TIME_STAMP time;
    size_t acks;           /* If acks == 0 then ready to commit */
  };

  struct time_info{
    time_info() {}
    time_info(TIME_STAMP s, size_t t, Action act) : start(s), time(t), action(act) {}
    TIME_STAMP start;
    size_t time;
    Action action;
  };

  std::vector<time_info> times_;

  /* The set of inprogress commits */
  HashTable<size_t, Query> queries_;
  size_t next_query_;

  /* Locks for multi-thread access to the respective containers */
  std::mutex alive_mutex_;
  std::mutex others_mutex_;
  std::mutex queries_mutex_;
  std::mutex times_mutex_;
  
  void register_funcs(){
    self_->bind("get", [this](std::string key){ return this->get(key); });
    self_->bind("put", [this](std::string key, T val){ this->put(key, val); });
    self_->bind("remove", [this](std::string key){ this->remove(key); });
    self_->bind("acknowledge", [this](size_t query, size_t index){ this->acknowledge(query, index); });
    self_->bind("join", [this](std::string address, size_t port = 8080){ this->join(address, port); });
    self_->bind("stage", [this](std::string key, T val, Action act, size_t query, size_t index){ this->stage(key, val, act, query, index); });
    self_->bind("commit", [this](size_t query){ this->commit(query); });
    self_->bind("ready", [this](){ this->ready_ = true; });
    /* Testing aliveness */
    self_->bind("alive", [this](size_t index){ this->alive(index); });
    self_->bind("check", [this](std::string addr, size_t port){ return this->check(addr, port); });
    self_->bind("ping", [](){});
  }

  T get(const std::string& key){
    typename KeyValueStore<std::string, versions_t>::find_t found = kv_.find(key);
    versions_t vers = versions_t();
    if (found.found){
      vers = found.value;
    }
    if (leader_ || vers.versions.size() <= 1){
      if (vers.valid){
	return queries_[vers.current].val;
      }
      return T();
    }
    std::unique_lock<std::mutex> lock(others_mutex_);
    return others_[0]->call("get", key).template as<T>();
  }

  void put(const std::string& key, const T& val){
    if (leader_){
      stage(key, val, PUT, next_query_++);
    } else {
      std::unique_lock<std::mutex> lock(others_mutex_);
      others_[0]->send("put", key, val); /* All calls must be redirected to leader */
    }
  }

  void remove(const std::string& key){
    if (leader_){
      stage(key, T(), REMOVE, next_query_++);
    } else {
      std::unique_lock<std::mutex> lock(others_mutex_);
      others_[0]->send("remove", key);
    }
  }

  void acknowledge(size_t query, size_t index){
    std::unique_lock<std::mutex> qlock(queries_mutex_);
    if (queries_[query].who[index]) return;
    --queries_[query].acks;
    queries_[query].who[index] = true;
    if (queries_[query].acks == 0){
      std::unique_lock<std::mutex> olock(others_mutex_);
      commit(query);
    }
  }

  /* This relies on the fact that only 1 thread is executing the server calls
     -- Otherwise a query in progress (or a new query) could cause a lot of issues */
  void join(const std::string& addr, const size_t port){
    if (!leader_) return;

    std::unique_lock<std::mutex> qlock(queries_mutex_);
    std::unique_lock<std::mutex> olock(others_mutex_);
    size_t ind = others_.size();

    others_.push_back(new rpc::client(addr, port));
    while(others_[ind]->get_connection_state() != rpc::client::connection_state::connected);
    
    /* Send all of the in progress queries / staged versions */
    std::vector<std::future<clmdep_msgpack::object_handle>> futures;
    typename HashTable<size_t, Query>::iterator qit;
    for (qit = queries_.begin(); qit != queries_.end(); ++qit){
      if ((*qit).value.action == DONE){   /* Commited Value */
	(*qit).value.who.push_back(true);
      } else {
        (*qit).value.who.push_back(false);
        ++(*qit).value.acks;
      }
      others_[ind]->call("stage", (*qit).value.key, (*qit).value.val, (*qit).value.action, (*qit).key, ind);
    }
    /* Make sure everything got there -- If it fails be pessemistic */
    for (size_t i = 0; i < futures.size(); ++i){
      auto wait = futures[i].wait_for(std::chrono::milliseconds(others_[ind]->get_timeout()));
      if (wait == std::future_status::timeout){ /* Error occured -- Remove Server from followers */
	delete others_[ind];
	others_.pop_back();
	return;
      }
    }
    /* We've successfully got the new follower upto speed on all staged versions */
    futures.clear();

    /* Send all commited data (Reconstructs from queries and using commit) */
    typename KeyValueStore<std::string,versions_t>::iterator it;
    for (it = kv_.begin(); it != kv_.end(); ++it){
      if ((*it).value.valid){
        futures.push_back(others_[ind]->async_call("commit", (*it).value.current));
      }
    }
    /* Make sure everything got there -- If it fails be pessemistic */
    for (size_t i = 0; i < futures.size(); ++i){
      auto wait = futures[i].wait_for(std::chrono::milliseconds(others_[ind]->get_timeout()));
      if (wait == std::future_status::timeout){ /* Error occured -- Remove Server from followers */
	delete others_[ind];
	others_.pop_back();
	return;
      }
    }
    /* The new node is all caught up and is ready to join the party :) */
    std::unique_lock<std::mutex> alock(alive_mutex_);
    others_id_.push_back(std::make_pair(addr, port));
    alive_.push_back(true);  /* The new node is infact still alive ... */
    try {
      others_[ind]->call("ready");
    } catch (...) {
      /* We Should Do Something Here */
    }
  }

  void stage(const std::string& key, const T& val, Action act, size_t query, size_t index = 0){
    /* Add this version to the version history of key */
    typename KeyValueStore<std::string, versions_t>::find_t found = kv_.find(key);
    versions_t vers = versions_t();
    if (found.found){
      vers = found.value;
    }
    vers.versions.insert(query);
    kv_.put(key, vers);
    /* Continue with normal staging of 2pc */
    std::unique_lock<std::mutex> qlock(queries_mutex_);
    std::unique_lock<std::mutex> olock(others_mutex_);
    if (leader_){
      queries_.insert(query, Query(key, val, act, std::chrono::steady_clock::now(), others_.size()));
      if (others_.size() == 0){
	commit(query);
	return;
      }
      for (size_t i = 0; i < others_.size(); ++i){
        others_[i]->send("stage", key, val, act, query, i);
      }
    }
    else {
      queries_.insert(query, Query(key, val, act, std::chrono::steady_clock::now()));
      if (act != DONE){ /* Only sent when joining */
        others_[0]->send("acknowledge", query, index);
      }
    }
  }

  /* Assumes thread already have control of queries_mutex_ and others_mutex_ */
  void commit(size_t query){
    Query q = queries_[query];
    versions_t vers = kv_.get(q.key); /* q.key must be in the kv_ (it was inserted in stage) */
    switch (q.action){
      case PUT:
	if (vers.valid){
	  queries_.remove(vers.current);
	  vers.versions.remove_element(vers.current);
	}
	vers.current = query;
	vers.valid = true;
	kv_.put(q.key, vers);
	queries_[query].action = DONE; /* This is the most recently commited query for key */
        break;
      case REMOVE:
	if (vers.valid){
	  queries_.remove(vers.current);
	  vers.versions.remove_element(query);
	}
	queries_.remove(query);
	vers.versions.remove_element(query);
	if (vers.versions.size() != 0){
	  vers.valid = false;
	  kv_.put(q.key, vers);
	} else {
	  kv_.remove(q.key);
	}
        break;
      case DONE: /* This is the current successfully commited value -- sent during a join request */
	vers.current = query;
	vers.valid = true;
	kv_.put(q.key, vers);
	break;
    }
    if (leader_){
      for (size_t i = 0; i < others_.size(); ++i){
	others_[i]->send("commit", query);
      }
      auto now = std::chrono::steady_clock::now();
      size_t taken = std::chrono::duration_cast<std::chrono::nanoseconds>(now - q.time).count();
      std::unique_lock<std::mutex> tlock(times_mutex_);
      times_.push_back(time_info(q.time, taken, q.action));
    }
  }

  void alive(size_t index){
    if (leader_){
      std::unique_lock<std::mutex> lock(alive_mutex_);
      alive_[index] = true;
    } else {
      std::unique_lock<std::mutex> lock(others_mutex_);
      pulse_ = true;
      others_[0]->send("alive", index);
    }
  }

  bool check(const std::string& addr, size_t port){
    std::unique_lock<std::mutex> lock(others_mutex_);
    std::pair<std::string, size_t> look(addr, port);
    for (size_t i = 0; others_id_.size(); ++i){
      if (others_id_[i] == look)
	return true;
    }
    return false;
  }

  void cull(const std::vector<size_t>& dead){
    /* always use q o a (nested locks) to avoid dead lock */
    std::unique_lock<std::mutex> qlock(queries_mutex_);
    std::unique_lock<std::mutex> olock(others_mutex_);
    std::unique_lock<std::mutex> alock(alive_mutex_);
    typename HashTable<size_t, Query>::iterator it;
    for (int i = dead.size()-1; 0 <= i; --i){
      delete others_[dead[i]];
      others_.erase(others_.begin()+dead[i]);
      others_id_.erase(others_id_.begin()+dead[i]);
      alive_.erase(alive_.begin()+dead[i]);
      /* Acknowledge on behalf of dead nodes */
      for (it = queries_.begin(); it != queries_.end(); ++it){
	if (!(*it).value.who[dead[i]])
	  --(*it).value.acks;
	(*it).value.who.erase((*it).value.who.begin()+dead[i]);
        if ((*it).value.acks == 0){
          commit((*it).key);
        }
      }
    }
  }
  
 public:
   Server(size_t port=8080) : self_(new rpc::server(port)), leader_(false), ready_(false), pulse_(false), next_query_(0) {
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
    self_->async_run();
    if (leader == std::make_pair(self_addr, self_port)){
      leader_ = true;
      ready_ = true;
      pulse_ = true;
      auto BEGINING_OF_TIME = std::chrono::steady_clock::now();
      while (1){
	auto start = std::chrono::steady_clock::now();

	std::vector<size_t> dead;
	{
	  std::unique_lock<std::mutex> lock(alive_mutex_);
  	  for (int i = 0; i < alive_.size(); ++i){
	    if (!alive_[i]){
	      dead.push_back(i);
	    }
	    alive_[i] = false;
	  }
	}

	/* Remove the "dead" followers */
	cull(dead);

	{
	  std::unique_lock<std::mutex> lock(others_mutex_);
  	  for (int i = 0; i < others_.size(); ++i){
	    others_[i]->send("alive", i);
	  }
	}
	
	{ std::unique_lock<std::mutex> tlock(times_mutex_);
	  for (size_t i = 0; i < times_.size(); ++i){
	    switch (times_[i].action){
	      case PUT:
		std::cout << "PUT ";
		break;
  	      case REMOVE:
		std::cout << "REMOVE ";
	        break;
	    }
	    double start = 1.0 * std::chrono::duration_cast<std::chrono::nanoseconds>(times_[i].start - BEGINING_OF_TIME).count() / 1000000000; /* 1 second */
	    std::cout << start << " " << 1.0 * times_[i].time / 1000000 << std::endl;
	  }
	  times_.clear();
	}

	/* Calculate how Long to sleep to keep approximately ALIVE_TIME between iterations */
	auto end = std::chrono::steady_clock::now();
	size_t time_taken = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	if (time_taken < ALIVE_TIME)
  	  std::this_thread::sleep_for(std::chrono::milliseconds(ALIVE_TIME - time_taken));
      }
    } else {
      others_.push_back(new rpc::client(leader.first, leader.second));
      while (others_[0]->get_connection_state() != rpc::client::connection_state::connected);
      others_[0]->send("join", self_addr, self_port);
      while (!ready_){
	std::this_thread::sleep_for(std::chrono::milliseconds(100)); /* sleep for 100 ms and check again */
      }
      pulse_ = true;
      while(1){
	auto start = std::chrono::steady_clock::now();
	if (!pulse_){
	  bool found = false;
	  std::unique_lock<std::mutex> lock(others_mutex_);
	  if (others_[0]->get_connection_state() == rpc::client::connection_state::connected){
	    try {
	      found = others_[0]->call("check", self_addr, self_port).template as<bool>();
	    } catch (...){
	      /* timed out do nothing */
	    }
	  }
	  if (!found){
	    /* keep trying to rejoin the system */
	    self_->stop(); /* Stop all ongoing services */
	    delete self_;
	    self_ = new rpc::server(self_port);
	    register_funcs();
	    lock.unlock();
	    kv_ = KeyValueStore<std::string, versions_t>();
	    queries_ = HashTable<size_t, Query>();
	    ready_ = false;
	    while (others_[0]->get_connection_state() != rpc::client::connection_state::connected){
  	      delete others_[0];
	      others_[0] = new rpc::client(leader.first, leader.second);
	      std::this_thread::sleep_for(std::chrono::milliseconds(ALIVE_TIME));
	    }
	    self_->async_run();
	    std::this_thread::sleep_for(std::chrono::milliseconds(ALIVE_TIME));
	    others_[0]->send("join", self_addr, self_port);
            while (!ready_){
   	      std::this_thread::sleep_for(std::chrono::milliseconds(100)); /* sleep for 100 ms and check again */
            }
	    pulse_ = true;
	  } else {
	    pulse_ = false;
	    lock.unlock();
	  }
	} else {
  	  pulse_ = false;
	}

	/* Calculate how Long to sleep to keep approximately ALIVE_TIME between iterations */
	auto end = std::chrono::steady_clock::now();
	size_t time_taken = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
	if (time_taken < ALIVE_TIME)
	  std::this_thread::sleep_for(std::chrono::milliseconds(ALIVE_TIME - time_taken));
      }
    }
  }
};

#endif
