/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

using namespace std;

namespace scudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bufferPoolManager) : index_(index),leaf_(leaf), buff_pool_manager_(bufferPoolManager){}


INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() 
{
  buff_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
  buff_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
  buff_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
};

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd()
{
  return (leaf_ == nullptr || (index_ == leaf_->GetSize() && leaf_->GetNextPageId() == INVALID_PAGE_ID));
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*()
{
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++()
{
  ++index_;

  if (index_ == leaf_->GetSize() && leaf_->GetNextPageId() != INVALID_PAGE_ID) 
  {
    page_id_t next_page_id = leaf_->GetNextPageId();

    auto *page = buff_pool_manager_->FetchPage(next_page_id);

    page->RLatch();

    buff_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
    buff_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
    buff_pool_manager_->UnpinPage(leaf_->GetPageId(), false);

    auto next_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
    assert(next_leaf->IsLeafPage());
    index_ = 0;
    leaf_ = next_leaf;
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace scudb
