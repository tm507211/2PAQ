/*********************************************************************************************
   Author:  Charlie Murphy
   Email:   tcm3@cs.princeton.edu

   Date:    March 27, 2017

   Description: A simple templated circular buffer class.
                Provides automatically resizing circular buffer (capacity is always a power of 2)
                Provides users with simple access, insertion, and removal.
                If element a is inserted before element b then a will have a smaller index.
                All indexes into buffer are translated to be between [0, size of buffer).
                By default the oldest element is removed upon removal.
                
 *********************************************************************************************/

template <class T>
class CircularBuffer{
  T* buff_;
  size_t start_;    /* First element in the circular buffer */
  size_t size_;     /* size_ is number of elements [0, size_] */
  size_t capacity_; /* capacity_ is a power of 2 */


  /* Next Power of 2 >= n assume size_t is 64-bit */
  inline size_t next_pow2(size_t n){
    --n;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    ++n;
    return n;
  }

  /* size must be a power of 2 /\ size >= size_ */
  inline void resize_aux(const size_t size){
    T* tmp = new T[size]();
    for (size_t i = 0; i < size_; ++i){ /* Realign from [0,size()) */
      tmp[i] = (*this)[i];
    }
    start_ = 0;
    capacity_ = size;
    if (buff_ != NULL){
      delete[] buff_;
    }
    buff_ = tmp;
  }
  
 public:
  CircularBuffer(size_t size = 0) : buff_(NULL), start_(0), size_(0), capacity_(size) {
    if (capacity_ == 0) return;
    resize(size);
  }

  /* Copy Constructor */
  CircularBuffer(const CircularBuffer& other) : buff_(NULL), start_(0), size_(0), capacity_(other.capacity_) {
    if (capacity_ == 0) return;
    buff_ = new T[capacity_]();
    for (; size_ < other.size_; ++size_){
      buff_[size_] = other[size_];
    }
  }

  /* Move Constructor */
  CircularBuffer(CircularBuffer&& other) noexcept : buff_(other.buff_), start_(other.start_), size_(other.size_), capacity_(other.capacity_) {
    other.buff_ = NULL;
  }

  /* Destructor */
  ~CircularBuffer() noexcept {
    if (buff_ != NULL){
      delete[] buff_;
    }
  }

  /* Copy Assignment Operator */
  CircularBuffer& operator = (const CircularBuffer& other) {
    CircularBuffer tmp(other);  // copy constructor
    *this = std::move(tmp);     // move-assignment
    return *this;
  }

  /* Move Assignment Operator */
  CircularBuffer& operator = (CircularBuffer&& other) noexcept {
    if (buff_ != NULL){
      delete[] buff_;
    }
    buff_ = other.buff_;
    start_ = other.start_;
    size_ = other.size_;
    capacity_ = other.capacity_;
    other.buff_ = NULL;
    return *this;
  }

  void resize(size_t size){
    resize_aux(next_pow2(size));
  }

  /* Most recent goes at the end of the buffer */
  void insert(const T& val){
    if (size_ == capacity_){
      resize_aux((1 < 2*capacity_) ? 2*capacity_ : 1);
    }
    (*this)[size_++] = val;
  }

  /* preserves ordering of non-removed objects */
  void remove(size_t index = 0){
    if (index >= size_) return;

    T val = (*this)[index];
    for (; index != 0; --index){
      (*this)[index] = (*this)[index-1];
    }
    ++start_;
    start_ &= (capacity_ - 1);
    --size_;
  }

  /* removes first item of buffer such that val == buff_[index] */
  void remove_element(const T& val){
    size_t index(0);
    for (; index < size_ && (val != (*this)[index]); ++index);
    remove(index); /* does nothing if val is not found in buff_ */
  }

  size_t size() const {
    return size_;
  }

  size_t capacity() const {
    return capacity_;
  }

  void compact() {
    resize(size());
  }

  /* These access the wrong element if i >= size_, undefined behaivor when capacity_ == 0 */
  const T& operator[](size_t i) const {
    i += start_;
    i &= (capacity_ - 1);
    return buff_[i];
  }
  
  T& operator[](size_t i) {
    i += start_;
    i &= (capacity_ - 1);
    return buff_[i];
  }
};
