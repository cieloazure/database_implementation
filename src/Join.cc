#include "Join.h"
#include <iostream>
#include "BigQ.h"

void debugProjectAndPrint(Record *rec, OrderMaker *orderMaker, Schema *schema,
                          Schema *proj_schema) {
  Record *copy = new Record();
  copy->Copy(rec);
  copy->Project(*orderMaker, schema->GetNumAtts());
  copy->Print(proj_schema);
}
// Merge left with right
// Need both the left and right records and their schemas
// In order to prevent duplicate of attributes we need the order maker of right
// to remove the grouping attributes from the right record and keep it in the
// left record
void Join::ComposeMergedRecord(Record &left, Record &right, Schema *leftSchema,
                               Schema *rightSchema, OrderMaker &rightOrderMaker,
                               Record *mergedRec) {
  // Remove the join attributes from right schema as they will be present in
  // left schema
  int numAttsRightAfter =
      rightSchema->GetNumAtts() - rightOrderMaker.GetNumAtts();

  int *rightAtts = new int[numAttsRightAfter];
  rightSchema->DifferenceWithOrderMaker(rightOrderMaker, rightAtts);

  // num of attributes to keep is number of attributes in left schema and right
  // schema
  int numAttsToKeep = leftSchema->GetNumAtts() + numAttsRightAfter;

  // Allocate new array for attributes to keep
  int *attsToKeep = new int[numAttsToKeep];
  // Get attributes to keep from left
  for (int i = 0; i < leftSchema->GetNumAtts(); i++) {
    attsToKeep[i] = i;
  }
  // Set start of right
  int startOfRight = leftSchema->GetNumAtts();
  // Get attributes to keep from right
  for (int i = 0, j = startOfRight; i < numAttsRightAfter; i++, j++) {
    attsToKeep[j] = rightAtts[i];
  }

  // Merge Record Now!
  mergedRec->MergeRecords(&left, &right, leftSchema->GetNumAtts(),
                          rightSchema->GetNumAtts(), attsToKeep, numAttsToKeep,
                          startOfRight);
}

void *Join ::JoinWorkerThreadRoutine(void *threadparams) {
  struct JoinWorkerThreadParams *params;
  params = (struct JoinWorkerThreadParams *)threadparams;

  Pipe *inPipeL = params->inPipeL;
  Pipe *inPipeR = params->inPipeR;
  Pipe *outPipe = params->outPipe;
  CNF *selOp = params->selOp;
  Record *literal = params->literal;

  // Join logic starts
  OrderMaker leftOrderMaker;
  OrderMaker rightOrderMaker;

  Pipe *sortedOutPipeLeft = new Pipe(100);
  Pipe *sortedOutPipeRight = new Pipe(100);
  if (selOp->GetSortOrders(leftOrderMaker, rightOrderMaker)) {
    // sort-merge join start
    cout << "Starting sort-merge join" << endl;

    Schema *leftSchema = leftOrderMaker.GetSchema();
    Schema *rightSchema = rightOrderMaker.GetSchema();

    // *TODO:* How to decide runlength
    int runLengthLeft = 5;
    int runLengthRight = 10;
    BigQ leftbigq(*inPipeL, *sortedOutPipeLeft, leftOrderMaker, runLengthLeft);
    BigQ rightbigq(*inPipeR, *sortedOutPipeRight, rightOrderMaker,
                   runLengthRight);

    // Debug!!!!
    Schema leftJoinAttributeSchema((char *)"left_join_att", &leftOrderMaker,
                                   leftSchema);

    Schema rightJoinAttributeSchema((char *)"right_join_att", &rightOrderMaker,
                                    rightSchema);

    Record *left = new Record();
    Record *right = new Record();

    int isLeftPresent = sortedOutPipeLeft->Remove(left);
    // debugProjectAndPrint(left, &leftOrderMaker, leftSchema,
    //                      &leftJoinAttributeSchema);
    left->Print(leftSchema);

    int isRightPresent = sortedOutPipeRight->Remove(right);
    right->Print(rightSchema);
    // debugProjectAndPrint(right, &rightOrderMaker, rightSchema,
    //                      &rightJoinAttributeSchema);

    // ComparisonEngine comp;
    // while (isLeftPresent && isRightPresent) {
    //   int status = comp.Compare(left, &leftOrderMaker, right,
    //   &rightOrderMaker); if (status < 0) {
    //     cout << "Left less than right! Advance left" << endl;
    //     left = new Record();
    //     isLeftPresent = sortedOutPipeLeft->Remove(left);
    //   } else if (status > 0) {
    //     cout << "Right less than left! Advance right" << endl;
    //     right = new Record();
    //     isRightPresent = sortedOutPipeRight->Remove(right);
    //   } else {
    //     Schema mySchema("catalog", "orders");
    //     cout << "Join attribute is equal here!" << endl;

    //     Record *firstLeftRecord = new Record();
    //     firstLeftRecord->Copy(left);
    //     firstLeftRecord->Print(&mySchema);

    //     Record *firstRightRecord = new Record();
    //     firstRightRecord->Copy(right);
    //     firstRightRecord->Print(&mySchema);

    //     Page *leftBuffer = new Page();
    //     while (comp.Compare(left, &leftOrderMaker, right, &rightOrderMaker)
    //     ==
    //            0) {
    //       if (leftBuffer->Append(left) == 0) {
    //         cout << "Buffer for left is full" << endl;
    //         break;
    //       }
    //       left = new Record();
    //       sortedOutPipeLeft->Remove(left);
    //     }

    //     // Debug
    //     Record *temp = new Record();
    //     while (leftBuffer->GetFirst(temp)) {
    //       temp->Print(&mySchema);
    //       delete temp;
    //       temp = new Record();
    //     }
    //     break;
    //     // Here, either the leftBuffer is full or the left record's join key
    //     is
    //     // not equal to right record's join key

    //     // Page *rightBuffer = new Page();
    //     // rightBuffer->Append(right);
    //     // while (comp.Compare(firstLeftRecord, &leftOrderMaker, right,
    //     //                     &rightOrderMaker) == 0) {
    //     //   if (rightBuffer->Append(right) == 0) {
    //     //     cout << "Buffer for right is full" << endl;
    //     //     break;
    //     //   }
    //     //   right = new Record();
    //     //   sortedOutPipeRight->Remove(right);
    //     // }
    //     // Here, either the rightBuffer is full or the right record's join
    //     key
    //     // is not equal to firstLeftRecord's join key

    //     // BlockNestedLoopJoin(leftBuffer, rightBuffer);
    //   }
    // }
    // sort-merge join end
  } else {
    // nested loop join start
    // nested loop join end
  }

  // Join logic ends
  outPipe->ShutDown();
  // delete sortedOutPipeLeft;
  // delete sortedOutPipeRight;
  pthread_exit(NULL);
}

Join::Join() {}
Join::~Join() {}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp,
               Record &literal) {
  pthread_attr_t attr;

  if (pthread_attr_init(&attr) == 0) {
    if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE) == 0) {
    } else {
      throw runtime_error("Error spawning Project worker");
    }
  } else {
    throw runtime_error("Error spawning Project worker");
  }

  struct JoinWorkerThreadParams *thread_data =
      (struct JoinWorkerThreadParams *)malloc(
          sizeof(struct JoinWorkerThreadParams));

  thread_data->inPipeL = &inPipeL;
  thread_data->inPipeR = &inPipeR;
  thread_data->outPipe = &outPipe;
  thread_data->selOp = &selOp;
  thread_data->literal = &literal;

  pthread_create(&threadid, &attr, JoinWorkerThreadRoutine,
                 (void *)thread_data);
}

void Join::WaitUntilDone() {
  cout << "Join waiting....." << endl;
  pthread_join(threadid, NULL);
  cout << "Join done!" << endl;
}

void Join::Use_n_Pages(int n) {}