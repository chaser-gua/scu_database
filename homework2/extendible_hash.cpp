#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

using namespace std;

namespace scudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
// template <typename K, typename V>
// ExtendibleHash<K, V>::ExtendibleHash(size_t size) {       //初始化顶层索引
//   globalDepth = 0;
//   bucketSize = size;
//   bucketNum = 1;
//   buckets.push_back(std::make_shared<Bucket>(0));      
// }
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) :  globalDepth(0),bucketSize(size),bucketNum(1) {
  buckets.push_back(make_shared<Bucket>(0));
}

template<typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash() {
  ExtendibleHash(64);
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) const{
  return hash<K>{}(key);              //直接调用库中的哈希函数
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {       //注意多线程问题 ock_guard：创建时加锁，析构时解锁。作用：为了防止在线程使用mutex加锁后异常退出导致死锁的问题，建议使用lock_guard代替mutex。
  lock_guard<mutex> lock(latch);
  return globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {    //正常情况返回局部深度，如果桶不存在或者。。。。返回-1异常
  if(buckets[bucket_id])   //需要判断桶是否存在
  {
    lock_guard<mutex> lck(buckets[bucket_id]->latch); //解决多线程问题,之后的每个函数都要解决多线程问题
    if(buckets[bucket_id]->kmap.size() == 0)
      return -1;
    return buckets[bucket_id]->localDepth;
  }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  lock_guard<mutex> lock(latch);
  return bucketNum;
}

//实现得到桶id的辅助方法
template <typename K, typename V>
int ExtendibleHash<K, V>::getIndex(const K &key) const
{
  lock_guard<mutex> lck(latch);
  // 要找到放在哪个桶里，就是要看地址的后几位是多少，后几位的几根据全局深度来确定。
  // return HashKey(key) & (pow(2,globalDepth)-1);
  // 参考中的位运算速度更快
  return HashKey(key) & ((1 << globalDepth)-1);
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {     
  int index = getIndex(key);
  lock_guard<mutex> lck(buckets[index]->latch);
  if(buckets[index]->kmap.find(key) != buckets[index]->kmap.end())     //如果能找到，利用了map的find函数
  {
    value = buckets[index]->kmap[key];       //？传引用，修改值
    return true;
  }
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  int index = getIndex(key);
  lock_guard<mutex> lck(buckets[index]->latch);
  shared_ptr<Bucket> cur = buckets[index];        //用这种方式的原因可能是由于智能指针的特性？
  if (cur->kmap.find(key) == cur->kmap.end())
    return false;
  cur->kmap.erase(key);
  return true;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  int index = getIndex(key);
  shared_ptr<Bucket> cur = buckets[index];
  while(true)
  {
    lock_guard<mutex> lck(cur->latch);
    if(cur->kmap.find(key) != cur->kmap.end() || cur->kmap.size() < bucketSize)    //不会发生溢出的情况
    {
      cur->kmap[key] = value;
      break;
    }

    //接下来是可能发生溢出的情况
    //需要对比几位
    int mask = (1 << (cur->localDepth));
    (cur->localDepth)++;        //所处的桶的局部深度增加

    //规定作用域
    {
      lock_guard<mutex> lck2(latch);

      //进行分裂
      if(cur->localDepth > globalDepth)
      {
        size_t len = buckets.size();
        for(size_t i = 0; i < len; ++i)
        {
          buckets.push_back(buckets[i]);
        }
        globalDepth++;
      }
      bucketNum++;
      auto newBucket = make_shared<Bucket>(cur->localDepth);

      //重新分配桶
      typename map<K, V>::iterator it;
      for(it = cur->kmap.begin(); it != cur->kmap.end();)
      {
        if(HashKey(it->first) & mask)         
        {
          newBucket->kmap[it->first] = it->second;
          it = cur->kmap.erase(it);
        }
        else
          it++;
      } 
      for(size_t i = 0; i < buckets.size(); i++)
        if(buckets[i] == cur && (i & mask))
          buckets[i] == newBucket;
    }
    index = getIndex(key);
    cur = buckets[index];
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;

} // namespace scudb
