#ifndef _TWO_WAY_LIST_C
#define _TWO_WAY_LIST_C

#include "TwoWayList.h"
#include <stdlib.h>
#include <iostream>

using namespace std;

// create an alias of the given TwoWayList
template <class Type>
TwoWayList<Type>::TwoWayList(TwoWayList &me) {
  list = new (std::nothrow) Header;
  if (list == NULL) {
    cout << "ERROR : Not enough memory. EXIT !!!\n";
    exit(1);
  }

  list->first = me.list->first;
  list->last = me.list->last;
  list->current = me.list->current;
  list->leftSize = me.list->leftSize;
  list->rightSize = me.list->rightSize;
}

// basic constructor function
template <class Type>
TwoWayList<Type>::TwoWayList() {
  // allocate space for the header
  list = new (std::nothrow) Header;
  if (list == NULL) {
    cout << "ERROR : Not enough memory. EXIT !!!\n";
    exit(1);
  }

  // set up the initial values for an empty list
  list->first = new (std::nothrow) Node;
  if (list->first == NULL) {
    cout << "ERROR : Not enough memory. EXIT !!!\n";
    exit(1);
  }

  list->last = new (std::nothrow) Node;
  if (list->last == NULL) {
    cout << "ERROR : Not enough memory. EXIT !!!\n";
    exit(1);
  }

  list->current = list->first;
  list->leftSize = 0;
  list->rightSize = 0;
  list->first->next = list->last;
  list->last->previous = list->first;
}

// basic deconstructor function
template <class Type>
TwoWayList<Type>::~TwoWayList() {
  // remove everything
  MoveToStart();
  while (RightLength() > 0) {
    Type temp;
    Remove(&temp);
  }

  // kill all the nodes
  for (int i = 0; i <= list->leftSize + list->rightSize; i++) {
    list->first = list->first->next;
    delete list->first->previous;
  }
  delete list->first;

  // kill the header
  delete list;
}

// swap operator
template <class Type>
void TwoWayList<Type>::operator&=(TwoWayList &List) {
  Header *temp = List.list;
  List.list = list;
  list = temp;
}

// make the first node the current node
template <class Type>
void TwoWayList<Type>::MoveToStart() {
  list->current = list->first;
  list->rightSize += list->leftSize;
  list->leftSize = 0;
}

// make the first node the current node
template <class Type>
void TwoWayList<Type>::MoveToFinish() {
  list->current = list->last->previous;
  list->leftSize += list->rightSize;
  list->rightSize = 0;
}

// determine the number of items to the left of the current node
template <class Type>
int TwoWayList<Type>::LeftLength() {
  return (list->leftSize);
}

// determine the number of items to the right of the current node
template <class Type>
int TwoWayList<Type>::RightLength() {
  return (list->rightSize);
}

// swap the right sides of two lists
template <class Type>
void TwoWayList<Type>::SwapRights(TwoWayList &List) {
  // swap out everything after the current nodes
  Node *left_1 = list->current;
  Node *right_1 = list->current->next;
  Node *left_2 = List.list->current;
  Node *right_2 = List.list->current->next;

  left_1->next = right_2;
  right_2->previous = left_1;
  left_2->next = right_1;
  right_1->previous = left_2;

  // set the new endpoints
  Node *temp = list->last;
  list->last = List.list->last;
  List.list->last = temp;

  int tempint = List.list->rightSize;
  List.list->rightSize = list->rightSize;
  list->rightSize = tempint;
}

// swap the leftt sides of the two lists
template <class Type>
void TwoWayList<Type>::SwapLefts(TwoWayList &List) {
  // swap out everything after the current nodes
  Node *left_1 = list->current;
  Node *right_1 = list->current->next;
  Node *left_2 = List.list->current;
  Node *right_2 = List.list->current->next;

  left_1->next = right_2;
  right_2->previous = left_1;
  left_2->next = right_1;
  right_1->previous = left_2;

  // set the new frontpoints
  Node *temp = list->first;
  list->first = List.list->first;
  List.list->first = temp;

  // set the new current nodes
  temp = list->current;
  list->current = List.list->current;
  List.list->current = temp;

  int tempint = List.list->leftSize;
  List.list->leftSize = list->leftSize;
  list->leftSize = tempint;
}

// move forwards through the list
template <class Type>
void TwoWayList<Type>::Advance() {
  (list->rightSize)--;
  (list->leftSize)++;
  list->current = list->current->next;
}

// move backwards through the list
template <class Type>
void TwoWayList<Type>::Retreat() {
  (list->rightSize)++;
  (list->leftSize)--;
  list->current = list->current->previous;
}

// insert an item at the current poition
template <class Type>
void TwoWayList<Type>::Insert(Type *Item) {
  Node *temp = new (std::nothrow) Node;
  if (temp == NULL) {
    cout << "ERROR : Not enough memory. EXIT !!!\n";
    exit(1);
  }

  Node *left = list->current;
  Node *right = list->current->next;

  left->next = temp;
  temp->previous = left;
  temp->next = right;
  temp->data = new (std::nothrow) Type;
  if (temp->data == NULL) {
    cout << "ERROR : Not enough memory. EXIT !!!\n";
    exit(1);
  }

  right->previous = temp;

  /* This method needs to be defined on Template Type */
  temp->data->Consume(Item);

  list->rightSize += 1;
}

// get a reference to the currentitemin the list
template <class Type>
Type *TwoWayList<Type>::Current(int offset) {
  Node *temp = list->current->next;
  for (int i = 0; i < offset; i++) {
    temp = temp->next;
  }
  return temp->data;
}

// remove an item from the current poition
template <class Type>
void TwoWayList<Type>::Remove(Type *Item) {
  Node *temp = list->current->next;
  list->current->next = temp->next;
  temp->next->previous = list->current;

  Item->Consume(temp->data);

  delete temp;

  (list->rightSize)--;
}

template <class Type>
template <typename F>
void TwoWayList<Type>::Sort(F &compare) {
  MoveToStart();
  int length = RightLength();
  if (length > 0) {
    Header *sortedList = new Header;
    InitializeHeader(sortedList);
    MergeSort(sortedList, list->first->next, length, compare);
    delete list;
    list = sortedList;
  }
}

template <class Type>
template <typename F>
void TwoWayList<Type>::MergeSort(Header *sortedList, Node *toSort, int length,
                                 F &compare) {
  if (length == 1) {
    MergeSortHelper(sortedList, toSort);
    return;
  }

  // Get the two lists two merge
  Node *list1 = toSort;
  Node *list2 = toSort;
  int count = 0;
  while (list2 != NULL && count < length / 2) {
    count++;
    list2 = list2->next;
  }

  Header *sortedList1 = new Header;
  InitializeHeader(sortedList1);
  MergeSort(sortedList1, list1, count, compare);

  Header *sortedList2 = new Header;
  InitializeHeader(sortedList2);
  MergeSort(sortedList2, list2, length - count, compare);

  Merge(sortedList, sortedList1->first->next, count, sortedList2->first->next,
        length - count, compare);

  delete sortedList1;
  delete sortedList2;
}

template <class Type>
template <typename F>
void TwoWayList<Type>::Merge(Header *mergedList, Node *list1, int length1,
                             Node *list2, int length2, F &compare) {
  Node *iter1 = list1;
  int count1 = 0;
  Node *iter2 = list2;
  int count2 = 0;

  ComparisonEngine comp;

  while (iter1 != NULL && count1 < length1 && iter2 != NULL &&
         count2 < length2) {
    if (compare(iter1->data, iter2->data)) {
      MergeSortHelper(mergedList, iter1);
      iter1 = iter1->next;
      count1++;
    } else {
      MergeSortHelper(mergedList, iter2);
      iter2 = iter2->next;
      count2++;
    }
  }

  while (iter1 != NULL && count1 < length1) {
    MergeSortHelper(mergedList, iter1);
    iter1 = iter1->next;
    count1++;
  }

  while (iter2 != NULL && count2 < length2) {
    MergeSortHelper(mergedList, iter2);
    iter2 = iter2->next;
    count2++;
  }
}

template <class Type>
void TwoWayList<Type>::MergeSortHelper(Header *mergedList, Node *iter) {
  // Start mergedList initialization
  Node *toBeMergedNode = new Node;
  toBeMergedNode->data = iter->data;

  Node *right = mergedList->last;
  Node *left = mergedList->last->previous;

  left->next = toBeMergedNode;
  toBeMergedNode->previous = left;
  toBeMergedNode->next = right;
  right->previous = toBeMergedNode;

  mergedList->rightSize += 1;
}

template <class Type>
void TwoWayList<Type>::InitializeHeader(Header *mergedList) {
  if (mergedList == NULL) {
    cout << "ERROR : Not enough memory. EXIT !!!\n";
    exit(1);
  }

  // set up the initial values for an empty list
  mergedList->first = new (std::nothrow) Node;
  if (mergedList->first == NULL) {
    cout << "ERROR : Not enough memory. EXIT !!!\n";
    exit(1);
  }

  mergedList->last = new (std::nothrow) Node;
  if (mergedList->last == NULL) {
    cout << "ERROR : Not enough memory. EXIT !!!\n";
    exit(1);
  }

  mergedList->current = mergedList->first;
  mergedList->leftSize = 0;
  mergedList->rightSize = 0;
  mergedList->first->next = mergedList->last;
  mergedList->last->previous = mergedList->first;
  // End mergedList initialization
}

#endif