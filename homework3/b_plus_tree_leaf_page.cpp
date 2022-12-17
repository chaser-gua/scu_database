/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"

using namespace std;

namespace scudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) 
{

  SetPageType(IndexPageType::LEAF_PAGE);

  SetSize(0);
  assert(sizeof(BPlusTreeLeafPage) == 28);
  
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);

  int size = (PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / (sizeof(KeyType) + sizeof(ValueType));
  SetMaxSize(size); //minus 1 for insert first then split
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const 
{
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) 
{
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const 
{
  for (int i = 0; i < GetSize(); ++i) 
  {
    if (comparator(key, array[i].first) <= 0) 
    {
      return i;
    }
  }
  return GetSize();
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const 
{
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) 
{
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) 
{
  // 第一个比key大的
  int idx = KeyIndex(key,comparator);
  assert(idx >= 0);
  IncreaseSize(1);
  int curSize = GetSize();
  for (int i = curSize - 1; i > idx; i--) 
  {
    array[i] = array[i - 1];
  }
  array[idx] = {key,value};
  return curSize;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) 
{
  assert(recipient != nullptr);
  int total = GetMaxSize() + 1;
  assert(GetSize() == total);
  
  //复制
  int copyIdx = (total) / 2;
  for (int i = copyIdx; i < total; i++) 
  {
    recipient->array[i - copyIdx] = array[i];
  }

  //连接指针
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());


  SetSize(copyIdx);
  recipient->SetSize(total - copyIdx);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) 
{

}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const 
{
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 ||
      comparator(key, KeyAt(GetSize() - 1)) > 0) 
  {
    return false;
  }

  int myBegin = 0, myEnd = GetSize() - 1, myMiddle;
  while (myBegin <= myEnd) 
  {
    myMiddle = myBegin + (myEnd - myBegin) / 2;
    if (comparator(key, KeyAt(myMiddle)) > 0) 
    {
      myBegin = myMiddle + 1;
    } 
    else if (comparator(key, KeyAt(myMiddle)) < 0) 
    {
      myEnd = myMiddle - 1;
    } 
    else 
    {
      value = array[myMiddle].second;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) 
{
  int firIdxLargerEqualThanKey = KeyIndex(key,comparator);
  if (firIdxLargerEqualThanKey >= GetSize() || comparator(key,KeyAt(firIdxLargerEqualThanKey)) != 0) 
  {
    return GetSize();
  }

  // 快速检测，也可以查找
  int tarIdx = firIdxLargerEqualThanKey;
  memmove((void*)(array + tarIdx), (void*)(array + tarIdx + 1),
          static_cast<size_t>((GetSize() - tarIdx - 1)*sizeof(MappingType)));
  IncreaseSize(-1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) 
{
  recipient->CopyAllFrom(array, GetSize());
  recipient->SetNextPageId(GetNextPageId());
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) 
{
  assert(GetSize() + size <= GetMaxSize());
  auto start = GetSize();
  for (int i = 0; i < size; ++i) {
    array[start + i] = *items++;
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) 
{
  MappingType pair = GetItem(0);
  IncreaseSize(-1);
  memmove((void*)(array), (void*)(array + 1), static_cast<size_t>(GetSize()*sizeof(MappingType)));
  recipient->CopyLastFrom(pair);
  
  //update relavent key & value pair in its parent page.
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  parent->SetKeyAt(parent->ValueIndex(GetPageId()), array[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) 
{
  assert(GetSize() + 1 <= GetMaxSize());
  array[GetSize()] = item;
  IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) 
{
  MappingType pair = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) 
{
  assert(GetSize() + 1 < GetMaxSize());
  memmove((void*)(array + 1), (void*)array, GetSize()*sizeof(MappingType));
  IncreaseSize(1);
  array[0] = item;

  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  parent->SetKeyAt(parentIndex, array[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace scudb
