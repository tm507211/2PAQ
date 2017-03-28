/******************************************************************************************
    Author: Charlie Murphy
    Email:  tcm3@cs.princeton.edu

    Date: March 27, 2017

    Description: A simple hash_table with string keys (using std::hash<std::string>{} as
                 the hash function).

 ******************************************************************************************/
#include <string>
#include <vector>
#include <functional>

template <class T>
class HashTable {
 public:
  /* Type of values for the hash_table */
  struct value_t{
    value_t() : hash(0), key("") {}
    value_t(size_t h, const std::string& k, const T& v) : hash(h), key(k), value(v) {}
    size_t hash;
    std::string key;
    T value;
  };

  /**********************************************************************
                Iterator and Constant Iterator Types
   **********************************************************************/
  class const_iterator;
  class iterator {
    std::vector<value_t>* items_;
    size_t index_;
    size_t size_;
    typename std::vector<value_t>::iterator it_;
  public:
    iterator(std::vector<value_t>* items, size_t index, size_t size, typename std::vector<value_t>::iterator it) : items_(items), index_(index), size_(size), it_(it) {}
    iterator(const iterator& it) : items_(it.items_), index_(it.index_), size_(it.size_), it_(it.it_) {}
    iterator& operator++() {
      if (index_ == size_) return *this;
      if (++it_ != items_[index_].end()) return *this;
      while (it_ == items_[index_].end() && (index_+1) != size_){
	it_ = items_[++index_].begin();
      }
      if (it_ == items_[index_].end()) { index_ = size_; }
      return *this;
    }
    iterator operator++(int) {
      iterator tmp(*this); ++(*this); return tmp;
    }
    bool operator ==(const iterator& it) { return (items_ == it.items_) && (index_ == it.index_) && (it_ == it.it_); }
    bool operator !=(const iterator& it) { return !((*this) == it); }
    const T& operator*() const {return (*it_); }
    T& operator*() { return (*it_).value; }
    friend const_iterator;
  };

  class const_iterator {
    std::vector<value_t>* items_;
    size_t index_;
    size_t size_;
    typename std::vector<value_t>::const_iterator it_;
  public:
    const_iterator(std::vector<value_t>* items, size_t index, size_t size, typename std::vector<value_t>::const_iterator it) : items_(items), index_(index), size_(size), it_(it) {}
    const_iterator(const const_iterator& it) : items_(it.items_), index_(it.index_), size_(it.size_), it_(it.it_) {}
    const_iterator(const iterator& it) : items_(it.items_), index_(it.index_), size_(it.size_), it_(it.it_) {}
    const_iterator& operator++() {
      if (index_ == size_) return *this;
      if (++it_ != items_[index_].end()) return *this;
      while (it_ == items_[index].end() && (index_+1) != size_){
	it_ = items_[++index_].begin();
      }
      if (it_ == items_[index_].end()) { index_ = size_; }
      return *this;
    }
    const_iterator operator++(int) {
      iterator tmp(*this); ++(*this); return tmp;
    }
    bool operator ==(const const_iterator& it) { return (items_ == it.items_) && (index_ == it.index_) && (it_ == it.it_); }
    bool operator !=(const const_iterator& it) { return !((*this) == it); }
    const T& operator*() const {return (*it_); }
  };

  /************************************************************************
             Constructors, Destructors, and Assigment Operators
   ************************************************************************/
  /* Default Constructor */
  HashTable() : vals_(NULL), size_(0), capacity_(0) {}
  
  /* Copy Constructor */
  HashTable(const HashTable& other) : vals_(NULL), size_(other.size_), capacity_(other.capacity_) {
    vals_ = new std::vector<value_t>[size_]();
    for (size_t i = 0; i < size_; ++i){
      vals_[i] = other.vals_[i];
    }
  }

  /* Move Constructor */
  HashTable(HashTable&& other) noexcept : vals_(other.vals_), size_(other.size_), capacity_(other.capacity_) {
    other.vals_ = NULL;
  }

  /* Destructor */
  ~HashTable() noexcept {
    if (vals_ != NULL){
      delete[] vals_;
    }
  }

  /* Copy Assignment */
  HashTable& operator = (const HashTable& other){
    HashTable tmp(other);
    (*this) = std::move(tmp);
    return *this;
  }

  /* Move Assignment */
  HashTable& operator = (HashTable && other) noexcept {
    if (vals_ != NULL){
      delete[] vals_;
    }
    vals_ = other.vals_;
    size_ = other.size_;
    capacity_ = other.capacity_;
    other.vals_ = NULL;
    return *this;
  }


  /*******************************************************************
              Insert, Remove, and Find operations
  ********************************************************************/
  void insert(const std::string& key, const T& val){
    if (size_*2 >= capacity_){
      resize();
    }
    size_t hash = hash_func(key);
    size_t index = hash%capacity_;
    for (size_t i = 0; i < vals_[index].size(); ++i){
      if (vals_[index][i].hash == hash && vals_[index][i].key == key){
	vals_[index][i].value = val;
	return;
      }
    }
    vals_[hash%capacity_].push_back(value_t(hash, key, val));
    ++size_;
  }

  void remove(const std::string& key){
    size_t hash = hash_func(key);
    size_t index = hash%capacity_;
    for (size_t i = 0; i < vals_[index].size(); ++i){
      if (vals_[index][i].hash == hash && vals_[index][i].key == key){
	vals_[index][i] = vals_[index][vals_[index].size()-1];
	vals_[index].pop_back();
	--size_;
      }
    }
  }

  iterator find(const std::string& key){
    size_t hash = hash_func(key);
    size_t index = hash%capacity_;
    typename std::vector<value_t>::iterator it;
    for (it = vals_[index].begin(); it != vals_[index].end(); ++it){
      if (it->hash == hash && it->key == key){
	return iterator(vals_, index, capacity_, it);
      }
    }
    return end();
  }

  const_iterator find(const std::string& key) const {
    size_t hash = hash_func(key);
    size_t index = hash%capacity_;
    typename std::vector<value_t>::iterator it;
    for (it = vals_[index].begin(); it != vals_[index].end(); ++it){
      if (it->hash == hash && it->key == key){
	return iterator(vals_, index, capacity_, it);
      }
    }
    return end();
  }

  /********************************************************************
      Iterators to the first element and end of the hash_table
   ********************************************************************/
  iterator begin() const {
    for (size_t i = 0; i < capacity_; ++i){
      if (vals_[i].size() != 0){
	return iterator(vals_, i, capacity_, vals_[i].begin());
      }
    }
    return end();
  }

  iterator end() const {
    return iterator(vals_, capacity_, capacity_, vals_[capacity_-1].end());
  }

  /***********************************************************************
       Overloaded [] operators. These should only be used if the key is
       known to be in the hash_table. Otherwise use find.
   ***********************************************************************/
  const T& operator[](const std::string& key) const {
    size_t hash = hash_func(key);
    size_t index = hash%capacity_;
    for (size_t i = 0; i < vals_[index].size(); ++i){
      if (vals_[index][i].hash == hash && vals_[index][i].key == key){
	return vals_[index][i].value;
      }
    }
    return ref_val_; /* bogus value */
  }
  
  T& operator[](const std::string& key) {
    size_t hash = hash_func(key);
    size_t index = hash%capacity_;
    for (size_t i = 0; i < vals_[index].size(); ++i){
      if (vals_[index][i].hash == hash && vals_[index][i].key == key){
	return vals_[index][i].value;
      }
    }
    return ref_val_; /* bogus value */
  }

 private:
  std::vector<value_t>* vals_;
  T ref_val_;
  size_t capacity_;
  size_t size_;
  std::hash<std::string> hash_func;

  /* Functions used to resize the hash_table */
  bool is_prime(size_t n) const {
    if (n == 2 || n == 3){
      return true;
    }
    if (n % 2 == 0 || n % 3 == 0){
      return false;
    }
    int divisor = 6;
    while (divisor * divisor - 2 * divisor + 1 <= n){
      if (n % (divisor - 1) == 0){
        return false;
      }
      if (n% (divisor + 1) == 0){
        return false;
      }
      divisor += 6;
    }
    return true;
  }
  
  size_t next_prime(size_t n) const {
    if (n < 2) return 2;
    while(!is_prime(n)){
      n += 2;
    }
    return n;
  }

  void resize(){
    size_t size = next_prime(2*size_+1);
    std::vector<value_t>* tmp = new std::vector<value_t>[size]();
    /* rehash the key value pairs based on key/hash */
    for (size_t i = 0; i < capacity_; ++i){
      for (size_t j = 0; j < vals_[i].size(); ++j){
	tmp[vals_[i][j].hash%size].push_back(vals_[i][j]);
      }
    }
    delete[] vals_;
    vals_ = tmp;
    capacity_ = size;
  }
};
