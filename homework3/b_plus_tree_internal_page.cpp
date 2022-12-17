/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

using namespace std;

namespace scudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) 
{
  // 设置page类型，current size，pageid，parentid
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  // 设置最大pagesize
  int size = (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / (sizeof(KeyType) + sizeof(ValueType));
  SetMaxSize(size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) 
{
  assert(index >= 0 && index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const 
{
  int count = GetSize();
  for (int i = 0; i < count; i++)
  {
    if (array[i].second == value)
    {
      return i;
    }
  }
  return count;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const 
{ 
  assert(index >= 0 && index < GetSize());
  return array[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) 
{
  assert(0 <= index && index < GetSize());
  array[index].second = value;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const 
{
  assert(GetSize() > 1);
  
  int myBegin = 1, myEnd = GetSize() - 1, myMiddle;
  while (myBegin <= myEnd) 
  { //find the last key in array <= input
    myMiddle = (myEnd - myBegin) / 2 + myBegin;
    if (comparator(array[myMiddle].first,key) <= 0) 
    {
      myBegin = myMiddle + 1;
    }
    else 
    {
      myEnd = myMiddle - 1;
    }
  }
  return array[myBegin - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) 
{
  array[0].second = old_value;
  array[1] = {new_key, new_value};
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) 
{
  int idx = ValueIndex(old_value) + 1;
  assert(idx > 0);
  IncreaseSize(1);
  int curSize = GetSize();
  for (int i = curSize - 1; i > idx; i--) 
  {
    array[i] = array[i - 1];
  }
  array[idx].first = new_key;
  array[idx].second = new_value;
  return curSize;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) 
{
  assert(recipient != nullptr);
  int total = GetMaxSize() + 1;
  assert(GetSize() == total);
  //复制过程
  int copyIdx = (total) / 2;
  page_id_t recipPageId = recipient->GetPageId();
  for (int i = copyIdx; i < total; i++) 
  {
    recipient->array[i - copyIdx].first = array[i].first;
    recipient->array[i - copyIdx].second = array[i].second;
    // 更新
    auto childRawPage = buffer_pool_manager->FetchPage(array[i].second);
    BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
    childTreePage->SetParentPageId(recipPageId);

    buffer_pool_manager->UnpinPage(array[i].second,true);
  }
  //set size,is odd, bigger is last part
  SetSize(copyIdx);
  recipient->SetSize(total - copyIdx);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) 
{
  assert(0 <= index && index < GetSize());
  for (int i = index; i < GetSize() - 1; ++i) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() 
{
  IncreaseSize(-1);
  assert(GetSize() == 0);
  return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) 
{
  int start = recipient->GetSize();
  page_id_t recipPageId = recipient->GetPageId();

  // 首先找父亲结点
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  assert(page != nullptr);
  BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

  // 从父亲结点分离
  SetKeyAt(0, parent->KeyAt(index_in_parent));
  buffer_pool_manager->UnpinPage(parent->GetPageId(), false);
  for (int i = 0; i < GetSize(); ++i) 
  {
    recipient->array[start + i].first = array[i].first;
    recipient->array[start + i].second = array[i].second;

    //更新子结点
    auto childRawPage = buffer_pool_manager->FetchPage(array[i].second);
    BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
    childTreePage->SetParentPageId(recipPageId);
    buffer_pool_manager->UnpinPage(array[i].second,true);
  }

  //更新兄弟结点
  recipient->SetSize(start + GetSize());
  assert(recipient->GetSize() <= GetMaxSize());
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) 
{

}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) 
{
  assert(GetSize() > 1);
  MappingType pair{KeyAt(1), ValueAt(0)};
  page_id_t child_page_id = ValueAt(0);
  SetValueAt(0, ValueAt(1));
  Remove(1);

  recipient->CopyLastFrom(pair, buffer_pool_manager);

  // 更新pageid
  auto *page = buffer_pool_manager->FetchPage(child_page_id);

  auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(recipient->GetPageId());

  assert(child->GetParentPageId() == recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) 
{
  assert(GetSize() + 1 <= GetMaxSize());

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());

  auto parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

  auto index = parent->ValueIndex(GetPageId());
  auto key = parent->KeyAt(index + 1);

  array[GetSize()] = {key, pair.second};
  IncreaseSize(1);
  parent->SetKeyAt(index + 1, pair.first);

  // 完成之后unpin
  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) 
{
  MappingType pair {KeyAt(GetSize() - 1),ValueAt(GetSize() - 1)};
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) 
{
  assert(GetSize() + 1 < GetMaxSize());
  memmove((void*)(array + 1), (void*)array, GetSize()*sizeof(MappingType));
  IncreaseSize(1);
  array[0] = pair;


  page_id_t childPageId = pair.second;
  Page *page = buffer_pool_manager->FetchPage(childPageId);
  assert (page != nullptr);
  BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(GetPageId());
  assert(child->GetParentPageId() == GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);


  page = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE_TYPE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  parent->SetKeyAt(parent_index, array[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace scudb
