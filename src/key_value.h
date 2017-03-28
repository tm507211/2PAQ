/***********************************************************************************
    Author: Charlie Murphy
    Email:   tcm3@cs.princeton.edu

    Date:    March 27, 2017

    Description: A simple Key-Value storage (* Simple Interface over hash_table *)
 ***********************************************************************************/
#include "hash_table.h"

template <class T>
class KeyValueStore{
 public:
  /*********************************************************
     Constructor / Destructor / etc.
   *********************************************************/
  KeyValueStore(){}

  /*********************************************************
     Key Value Semantics Operations
   *********************************************************/
  T get(const std::string& key) {
    return kv_table_[key];
  }

  void put(const std::string& key, const T& value){
    kv_table_.insert(key, value);
  }

  void remove(const std::string& key){
    kv_table_.remove(key);
  }
 private:
  HashTable<T> kv_table_;
};
