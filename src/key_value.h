/***********************************************************************************
    Author: Charlie Murphy
    Email:  tcm3@cs.princeton.edu

    Date:   March 27, 2017

    Description: A simple Key-Value storage (* Simple Interface over hash_table *)
 ***********************************************************************************/
#include "hash_table.h"

#ifndef CM_KEY_VALUE_STORE
#define CM_KEY_VALUE_STORE

template <class K, class V>
class KeyValueStore{
 public:
  /*********************************************************
     Constructor / Destructor / etc.
   *********************************************************/
  KeyValueStore(){}

  /*********************************************************
     Key Value Semantics Operations
   *********************************************************/

  /* KV find_t type is just the underlying hashtable find_t */
  typedef typename HashTable<K, V>::find_t find_t;
  typedef typename HashTable<K, V>::value_t value_t;
  typedef typename HashTable<K, V>::iterator iterator;
  typedef typename HashTable<K, V>::const_iterator const_iterator;

  find_t find(const K& key){
    return kv_table_.find(key);
  }
  
  V get(const K& key) {
    return kv_table_[key];
  }

  void put(const K& key, const V& value){
    kv_table_.insert(key, value);
  }

  void remove(const K& key){
    kv_table_.remove(key);
  }

  iterator begin() const {
    return kv_table_.begin();
  }

  iterator end() const {
    return kv_table_.end();
  }
  
 private:
  HashTable<K, V> kv_table_;
};

#endif
