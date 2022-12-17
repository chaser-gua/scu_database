/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

using namespace std;

namespace scudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator();

// 增加有参数的构造函数
  IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *,int, BufferPoolManager *);

  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

private:
  // add your own private member variables here
  int index_;
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_;
  BufferPoolManager *buff_pool_manager_;
};

} // namespace scudb
