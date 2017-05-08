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
#include <mutex>
#include <thread>
#include <iostream>

#ifndef CM_SERVER_2PC_AQ
#define CM_SERVER_2PC_AQ

#define PUT 0
#define REMOVE 1

std::mutex mtx_lead;

template <class T>
class Server {
  rpc::server self_;                                     /* self */
  std::vector<rpc::client*> others_;     /* others[0] == Leader */
  bool leader_;                                          /* Am I the Leader? */
  bool pulse_;
  size_t id_;
  std::vector<bool> alive_others_;			/*Are others alive? */

  KeyValueStore<std::string, std::pair< std::pair<T,size_t>,CircularBuffer<size_t> >> kv_;   		/*Latest committed value and list of pending queries (new versions) */
  
  typedef char Action;

  struct Query{
    Query() {}
    Query(const std::string& k, const T& val, Action act, size_t a = 0, std::vector<bool> a_v ={true}) : key(k), val(val), action(act), acks(a), ack_vec(a_v) {}
    std::string key;
    T val;
    Action action;
    std::vector<bool> ack_vec;
    size_t acks;    /* If acks == 0 then ready to commit */
  };

  HashTable<size_t, Query> queries_;
  size_t next_query_;

  void register_funcs(){
    self_.bind("get", [this](std::string key){ return this->get(key); });
    self_.bind("put", [this](std::string key, T val){ this->put(key, val); });
    self_.bind("remove", [this](std::string key){ this->remove(key); });

    self_.bind("acknowledge", [this](size_t query, size_t id_no){ this->acknowledge(query,id_no); });
    self_.bind("join", [this](std::string address, size_t port = 8080){ this->others_.push_back( new rpc::client(address, port)); this->alive_others_.push_back(true); });
    self_.bind("version", [this](std::string key){ return this->get_version(key);});			/*Leader gives version number*/
    self_.bind("alive", [this](size_t id_no){ (this->alive_others_[id_no]) = true; }) ;              /*Alive children*/

    self_.bind("stage", [this](std::string key, T val, Action act, size_t query, size_t id_no = 0){ this->stage(key, val, act, query, id_no); });
    self_.bind("commit", [this](size_t query){ this->commit(query); });
    self_.bind("hello",  [this](size_t id_no){this->pulse_ = true; this->id_ = id_no; this->holler_back();}); 			/*still connected to leader*/
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

void acknowledge(size_t query, size_t id_no){
    if(!(queries_[query].ack_vec)[id_no]){                //no double counting
       --queries_[query].acks;
       (queries_[query].ack_vec)[id_no] = true;          //keep track of who acknowledges
    }
    if (queries_[query].acks == 0){
      commit(query);
    }
  }
  
 void stage(const std::string& key, const T& val, Action act, size_t query, size_t id_no =0){
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
      queries_.insert(query, Query(key, val, act, others_.size(),std::vector<bool>(others_.size()) ));
      add_version(key,query);			
      for (size_t i = 0; i < others_.size(); ++i){
        others_[i]->send("stage", key, val, act, query, i);
      }
    }
    else {
      queries_.insert(query, Query(key, val, act));
      add_version(key,query);				
      others_[0]->send("acknowledge", query, id_no);
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

void hello_world()
{
  for(size_t i=0; i< others_.size(); ++i){
    alive_others_[i]=false;
    others_[i]->send("hello",i);
  }
}

void holler_back(){
  pulse_=false;
  others_[0]->send("alive",id_);
}

/*
void kill(size_t id_no){
  typename HashTable<size_t, Query>::iterator qit;
 // alive_others_.erase(alive_others_.begin() + id_no);
 // others_.erase(others_.begin() +id_no);
  for (qit = queries_.begin(); qit != queries_.end(); ++qit){
      if(((*qit).value.ack_vec)[id_no] == false){
        // --(*qit).value.acks;
         acknowledge((*qit).key,id_no);             //not sorted by query no.
        //self_c.send("acknowledge",id_no);   //this will ensure that too many things don't happen in the locked thread
      }
     // ((*qit).value.ack_vec).erase(id_no);                   //do I need this?
  }
}
*/

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
    //rpc::client self_c(self_addr,self_port);
    std::pair<std::string, size_t> leader = client.call("leader", self_addr, self_port).as<std::pair<std::string, size_t>>();
    typename HashTable<size_t, Query>::iterator qit;
    self_.async_run();
    if (leader == std::make_pair(self_addr, self_port)){
      leader_ = true;
       while(1){
        hello_world();
        std::this_thread::sleep_for (std::chrono::milliseconds(10));
        mtx_lead.lock();
        for(size_t i=0; i<others_.size();i++){
           if(!alive_others_[i]){
             for (qit = queries_.begin(); qit != queries_.end(); ++qit){
                 if( ((*qit).value.ack_vec)[i] == false ){
                    acknowledge((*qit).key,i);             //send acknowledgement on behalf of dead nodes (not sorted by query no.)
                    //self_c.send("acknowledge",i);   //this will ensure that too many things don't happen in the locked thread
                 }  
               }
           }     
        }
        size_t i=0;
        while(i<others_.size()){
           if(alive_others_[i]){
              i++;
              continue;
           }
           alive_others_.erase(alive_others_.begin() + i);
           others_.erase(others_.begin() +i);   /*Remove dead nodes*/
        }
           
        mtx_lead.unlock();
       }
     } else {
      others_.push_back(new rpc::client(leader.first, leader.second));
      while (others_[0]->get_connection_state() != rpc::client::connection_state::connected);
      others_[0]->send("join", self_addr, self_port);
      //holler_back();
      }
  }
};

#endif
