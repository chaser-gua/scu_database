/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

using namespace std;

namespace scudb {

template <typename T> LRUReplacer<T>::LRUReplacer() 
{    //构造函数，初始化
  head = make_shared<Node> ();
  tail = make_shared<Node> ();
  head->next = tail;
  tail->pre = head;
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) 
{
  lock_guard<mutex> lck(latch);       //确保多线程安全，以后都要注意
  shared_ptr<Node> cur;               //创建当前指向当前插入值的指针

  // 开始插入
  // 如果能找到，更新
  if(map.find(value) != map.end())
  {
    cur = map[value];
    shared_ptr<Node> pre = cur->pre;
    shared_ptr<Node> suc = cur->next;
    pre->next = suc;
    suc->pre = pre;
  }
    cur = make_shared<Node>(value);
    // 一系列的插入过程
    shared_ptr<Node> fir = head->next;
    cur->next = fir;
    fir->pre = cur;
    cur->pre = head;
    head->next = cur;
    map[value] = cur;
    return;
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) 
{
  lock_guard<mutex> lck(latch);
  if(map.size() == 0)
    return false;
  
   // 把最后一个弹出
  shared_ptr<Node> last = tail->pre;
  tail->pre = last->pre;
  last->pre->next = tail;
  value = last->value;     //传引用
  map.erase(last->value);
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) 
{
  lock_guard<mutex> lck(latch);
  // 删除结点的过程
  if(map.find(value) != map.end())
  {
    shared_ptr<Node> cur = map[value];
    cur->pre->next = cur->next;
    cur->next->pre = cur->pre;
  }
  return map.erase(value);              //利用了erase函数返回值的特性
}

template <typename T> size_t LRUReplacer<T>::Size() {
  lock_guard<mutex> lck(latch);
  return map.size(); 
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace scudb

