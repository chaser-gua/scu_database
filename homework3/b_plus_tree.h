/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <queue>
#include <vector>

#include "concurrency/transaction.h"
#include "index/index_iterator.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"

namespace scudb {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>
// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
public:
  explicit BPlusTree(const std::string &name,
                           BufferPoolManager *buffer_pool_manager,
                           const KeyComparator &comparator,
                           page_id_t root_page_id = INVALID_PAGE_ID);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value,
              Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> &result,
                Transaction *transaction = nullptr);

  // index iterator
  INDEXITERATOR_TYPE Begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);

  // Print this B+ tree to stdout using a simple command-line
  std::string ToString(bool verbose = false);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);
  // expose for test purpose
  B_PLUS_TREE_LEAF_PAGE_TYPE *FindLeafPage(const KeyType &key,
                                           bool leftMost = false,
                                           OpType op = OpType::READ,
                                           Transaction *transaction = nullptr);
private:
  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value,
                      Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                        BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  template <typename N> N *Split(N *node);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  template <typename N>
  bool Coalesce(
      N *&neighbor_node, N *&node,
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
      int index, Transaction *transaction = nullptr);

  template <typename N> void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = false);


  void UnlockUnpinPages(Operation op, Transaction* transaction)
  {
    if (transaction == nullptr)
        return;

    for (auto* page : *transaction->GetPageSet())
    {
        if (op == Operation::READONLY)
        {
            page->RUnlatch();
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        }
        else
        {
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        }
    }
    transaction->GetPageSet()->clear();

    for (auto page_id : *transaction->GetDeletedPageSet())
    {
        buffer_pool_manager_->DeletePage(page_id);
    }
    transaction->GetDeletedPageSet()->clear();

    if (root_is_locked) 
    {
        root_is_locked = false;
        unlockRoot();
    }
  }

  template <typename N>
  bool isSafe(N* node, Operation op)
  {
    if (op == Operation::INSERT)
    {
        return node->GetSize() < node->GetMaxSize();
    }
    else if (op == Operation::DELETE)
    {
        return node->GetSize() > node->GetMinSize() + 1;
    }
    return true;
  }

  inline void lockRoot() { mutex_.lock(); }
  inline void unlockRoot() { mutex_.unlock(); }

  // member variable
  class Checker {
  public:
      explicit Checker(BufferPoolManager* b) : buffer(b) {}
      ~Checker() {}
  private:
      BufferPoolManager* buffer;
  };
  std::mutex mutex_;                       // 保证线程安全
  static thread_local bool root_is_locked; 
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
};

} // namespace scudb
