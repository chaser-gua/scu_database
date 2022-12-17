/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"



namespace scudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const 
{ 
   return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) 
{
  // 找到leaf
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf = FindLeafPage(key,false,OpType::READ,transaction);
  bool ret = false;
  if (leaf == nullptr)
    return false;
 
  if (leaf != nullptr)
  {
        ValueType value;
        if (leaf->Lookup(key, value, comparator_))
        {
            result.push_back(value);
            ret = true;
        }

        UnlockUnpinPages(Operation::READONLY, transaction);

        if (transaction == nullptr)
        {
            auto page_id = leaf->GetPageId();
            buffer_pool_manager_->FetchPage(page_id)->RUnlatch();
            buffer_pool_manager_->UnpinPage(page_id, false);

            buffer_pool_manager_->UnpinPage(page_id, false);
        }
  }
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) 
{
  std::lock_guard<std::mutex> lock(mutex_);
    if (IsEmpty())
    {
      StartNewTree(key, value);
      return true;
    }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) 
{
  page_id_t newPageId;
  Page *rootPage = buffer_pool_manager_->NewPage(newPageId);
  assert(rootPage != nullptr);

  B_PLUS_TREE_LEAF_PAGE_TYPE *root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(rootPage->GetData());

  root->Init(newPageId,INVALID_PAGE_ID);
  root_page_id_ = newPageId;
  UpdateRootPageId(true);
  

  root->Insert(key,value,comparator_);

  buffer_pool_manager_->UnpinPage(rootPage->GetPageId(),true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) 
{
    auto* leaf = FindLeafPage(key, false, Operation::INSERT, transaction);
    if (leaf == nullptr)
    {
        return false;
    }
    ValueType v;
    if (leaf->Lookup(key, v, comparator_))
    {
        UnlockUnpinPages(Operation::INSERT, transaction);
        return false;
    }
    if (leaf->GetSize() < leaf->GetMaxSize())
    {
        leaf->Insert(key, value, comparator_);
    }
    else
    {
        auto* leaf2 = Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(leaf);
        if (comparator_(key, leaf2->KeyAt(0)) < 0)
        {
            leaf->Insert(key, value, comparator_);
        }
        else
        {
            leaf2->Insert(key, value, comparator_);
        }
        if (comparator_(leaf->KeyAt(0), leaf2->KeyAt(0)) < 0)
        {
            leaf2->SetNextPageId(leaf->GetNextPageId());
            leaf->SetNextPageId(leaf2->GetPageId());
        }
        else
        {
            leaf2->SetNextPageId(leaf->GetPageId());
        }

        InsertIntoParent(leaf, leaf2->KeyAt(0), leaf2, transaction);
    }

    UnlockUnpinPages(Operation::INSERT, transaction);
    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) 
{ 
  // 拿到新page
  page_id_t newPageId;
  Page* const newPage = buffer_pool_manager_->NewPage(newPageId);
  assert(newPage != nullptr);
  newPage->WLatch();
  transaction->AddIntoPageSet(newPage);
  

  N *newNode = reinterpret_cast<N *>(newPage->GetData());
  newNode->Init(newPageId, node->GetParentPageId());
  node->MoveHalfTo(newNode, buffer_pool_manager_);

 
  return newNode; 
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) 
{
    if (old_node->IsRootPage()) 
    {
    Page* const newPage = buffer_pool_manager_->NewPage(root_page_id_);
    assert(newPage != nullptr);
    assert(newPage->GetPinCount() == 1);

    B_PLUS_TREE_INTERNAL_PAGE *newRoot = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(newPage->GetData());
    newRoot->Init(root_page_id_);
    newRoot->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId();
  
    buffer_pool_manager_->UnpinPage(newRoot->GetPageId(),true);
    return;
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) 
{
  if (IsEmpty())
    return;

  auto* leaf = FindLeafPage(key, false, Operation::DELETE, transaction);
  if (leaf != nullptr)
  {
      int size_before_deletion = leaf->GetSize();
      if (leaf->RemoveAndDeleteRecord(key, comparator_) != size_before_deletion)
      {
          if (CoalesceOrRedistribute(leaf, transaction))
          {
              transaction->AddIntoDeletedPageSet(leaf->GetPageId());
          }
      }
      UnlockUnpinPages(Operation::DELETE, transaction);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) 
{
  auto *leaf = FindLeafPage(key_t, false, Operation::INSERT, transaction);
    if (leaf == nullptr)
    {
      if (node->IsRootPage())
      {
        return AdjustRoot(node);
      }
    if (node->IsLeafPage())
    {
        if (node->GetSize() >= node->GetMinSize())
        {
            return false;
        }
    }
    else
    {
        if (node->GetSize() > node->GetMinSize())
        {
            return false;
        }
    }

    auto* page = buffer_pool_manager_->FetchPage(node->GetParentPageId());

    auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,KeyComparator>*>(page->GetData());
    int value_index = parent->ValueIndex(node->GetPageId());

    assert(value_index != parent->GetSize());

    int sibling_page_id;
    if (value_index == 0)
    {
        sibling_page_id = parent->ValueAt(value_index + 1);
    }
    else
    {
        sibling_page_id = parent->ValueAt(value_index - 1);
    }

    page = buffer_pool_manager_->FetchPage(sibling_page_id);
    
    page->WLatch();
    transaction->AddIntoPageSet(page);
    auto sibling = reinterpret_cast<N*>(page->GetData());
    bool redistribute = false;

    if (sibling->GetSize() + node->GetSize() > node->GetMaxSize())
    {
        redistribute = true;
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }
    if (redistribute) 
    {
        if (value_index == 0)
        {
            Redistribute<N>(sibling, node, 1);
        }
        return false;
    }

    bool ret;
    if (value_index == 0) 
    {
        Coalesce<N>(node, sibling, parent, 1, transaction);
        transaction->AddIntoDeletedPageSet(sibling_page_id);
        ret = false;
    }
    else 
    {
        Coalesce<N>(sibling, node, parent, value_index, transaction);
        ret = true;
    }
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return ret;
    }
    ValueType v;
    if (leaf->Lookup(key, v, comparator_))
    {
      UnlockUnpinPages(Operation::INSERT, transaction);
      if (node->IsRootPage())
      {
        return AdjustRoot(node);
      }
    if (node->IsLeafPage())
    {
        if (node->GetSize() >= node->GetMinSize())
        {
            return false;
        }
    }
    else
    {
        if (node->GetSize() > node->GetMinSize())
        {
            return false;
        }
    }

    auto* page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    if (page == nullptr)

    auto parent =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,KeyComparator>*>(page->GetData());
    int value_index = parent->ValueIndex(node->GetPageId());

    assert(value_index != parent->GetSize());

    int sibling_page_id;
    if (value_index == 0)
    {
        sibling_page_id = parent->ValueAt(value_index + 1);
    }
    else
    {
        sibling_page_id = parent->ValueAt(value_index - 1);
    }

    page = buffer_pool_manager_->FetchPage(sibling_page_id);

    
    page->WLatch();
    transaction->AddIntoPageSet(page);
    auto sibling = reinterpret_cast<N*>(page->GetData());
    bool redistribute = false;

    if (sibling->GetSize() + node->GetSize() > node->GetMaxSize())
    {
        redistribute = true;
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }
    if (redistribute) 
    {
        if (value_index == 0)
        {
            Redistribute<N>(sibling, node, 1);
        }
        return false;
    }

    bool ret;
    if (value_index == 0) 
    {
        Coalesce<N>(node, sibling, parent, 1, transaction);
        transaction->AddIntoDeletedPageSet(sibling_page_id);
        ret = false;
    }
    else 
    {
        Coalesce<N>(sibling, node, parent, value_index, transaction);
        ret = true;
    }
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return ret;
    }
    if (leaf->GetSize() < leaf->GetMaxSize())
    {
      leaf->Insert(key, value, comparator_);
    }
    else
    {
      auto *leaf2 = Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(leaf);
      if (comparator_(key, leaf2->KeyAt(0)) < 0)
      {
        leaf->Insert(key, value, comparator_);
      }
      else
      {
        leaf2->Insert(key, value, comparator_);
      }
      if (comparator_(leaf->KeyAt(0), leaf2->KeyAt(0)) < 0)
      {
        leaf2->SetNextPageId(leaf->GetNextPageId());
        leaf->SetNextPageId(leaf2->GetPageId());
      }
      else
      {
        leaf2->SetNextPageId(leaf->GetPageId());
      }

      InsertIntoParent(leaf, leaf2->KeyAt(0), leaf2, transaction);
    }

    UnlockUnpinPages(Operation::INSERT, transaction);
    return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) 
{
  
  assert(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize());
  
  // 移动后一个
  node->MoveAllTo(neighbor_node,index,buffer_pool_manager_);
  transaction->AddIntoDeletedPageSet(node->GetPageId());
  parent->Remove(index);
  if (parent->GetSize() <= parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent,transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) 
{
  if (index == 0)
  {
     neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  }
  else
  {
      auto* page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
      if (page == nullptr)
      {
          throw Exception(EXCEPTION_TYPE_INDEX,
              "all page are pinned while Redistribute");
      }
      auto parent =
          reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,KeyComparator>*>(page->GetData());
      int idx = parent->ValueIndex(node->GetPageId());
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      neighbor_node->MoveLastToFrontOf(node, idx, buffer_pool_manager_);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) 
{
  if (old_root_node->IsLeafPage()) 
  {
    assert(old_root_node->GetSize() == 0);
    assert (old_root_node->GetParentPageId() == INVALID_PAGE_ID);
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }

  if (old_root_node->GetSize() == 1) 
  {
    B_PLUS_TREE_INTERNAL_PAGE *root = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(old_root_node);
    const page_id_t newRootId = root->RemoveAndReturnOnlyChild();
    root_page_id_ = newRootId;
    UpdateRootPageId();
    
    // 设置为无效
    Page *page = buffer_pool_manager_->FetchPage(newRootId);
    assert(page != nullptr);
    B_PLUS_TREE_INTERNAL_PAGE *newRoot =
            reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
    newRoot->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(newRootId, true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() 
{ 
  KeyType key{};
  return IndexIterator<KeyType, ValueType, KeyComparator>(FindLeafPage(key, true), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) 
{
    auto* leaf = FindLeafPage(key, false);
    int index = 0;
    if (leaf != nullptr)
      index = leaf->KeyIndex(key, comparator_);

    return IndexIterator<KeyType, ValueType, KeyComparator>(leaf, index, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *FindLeafPage(const KeyType &key,
                                           bool leftMost = false,
                                           OpType op = OpType::READ,
                                           Transaction *transaction = nullptr)
{
  if (op != Operation::READONLY)
  {
    lockRoot();
    root_is_locked = true;
  }

  if (IsEmpty())
  {
    return nullptr;
  }

  auto* parent = buffer_pool_manager_->FetchPage(root_page_id_);
  
  if (op == Operation::READONLY)
  {
      parent->RLatch();
  }
  else
  {
      parent->WLatch();
  }
  if (transaction != nullptr)
  {
      transaction->AddIntoPageSet(parent);
  }
  auto* node = reinterpret_cast<BPlusTreePage*>(parent->GetData());
  while (!node->IsLeafPage()) 
  {
      auto internal =
          reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
          KeyComparator>*>(node);
      page_id_t parent_page_id = node->GetPageId(); 
      page_id_t child_page_id;
      if (leftMost)
      {
          child_page_id = internal->ValueAt(0);
      }
      else 
      {
          child_page_id = internal->Lookup(key, comparator_);
      }
      auto* child = buffer_pool_manager_->FetchPage(child_page_id);

      if (op == Operation::READONLY)
      {
          child->RLatch();
          UnlockUnpinPages(op, transaction);
      }
      else
      {
          child->WLatch();
      }
      node = reinterpret_cast<BPlusTreePage*>(child->GetData());
      assert(node->GetParentPageId() == parent_page_id);
      if (op != Operation::READONLY && isSafe(node, op))
      {
          UnlockUnpinPages(op, transaction);
      }
      if (transaction != nullptr)
      {
          transaction->AddIntoPageSet(child);
      }
      else
      {
          parent->RUnlatch();
          buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
          parent = child;
      }
  }
  return reinterpret_cast<BPlusTreeLeafPage<KeyType,
      ValueType, KeyComparator>*>(node);
}



/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) 
{
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) 
{
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) 
{
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace scudb
