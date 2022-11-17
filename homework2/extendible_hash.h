/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>

#include <map>
#include <memory>
#include <mutex>
#include <cmath>

#include "hash/hash_table.h"

using namespace std;

namespace scudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
// 定义桶结构
struct Bucket
{
  // Bucket(int depth)
  // {
  //   localDepth = depth;
  // };
  Bucket(int depth) : localDepth(depth) {};
  int localDepth;
  map<K,V> kmap;
  mutex latch;
};

public:
  // constructor
  ExtendibleHash(size_t size);
  ExtendibleHash();
  // helper function to generate hash addressing
  size_t HashKey(const K &key) const;
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

  int getIndex(const K &key) const;     //为了方便其它函数的实现，增加一个得到桶的id的辅助方法

private:
  // add your own member variables here
  int globalDepth;
  size_t bucketSize;
  int bucketNum;
  // buckets是整个顶层的索引，里面装的是每一个桶
  vector<shared_ptr<Bucket>> buckets;   //shared_ptr 是C++11提供的一种智能指针类，它足够智能，可以在任何地方都不使用时自动删除相关指针，从而帮助彻底消除内存泄漏和悬空指针的问题。
  mutable mutex latch;

};
} // namespace scudb
