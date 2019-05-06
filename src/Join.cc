#include "Join.h"
#include <iostream>
#include "BigQ.h"
#include "HeapDBFile.h"

void debugProjectAndPrint(Record *rec, OrderMaker *orderMaker, Schema *schema,
                          Schema *proj_schema) {
  Record *copy = new Record();
  copy->Copy(rec);
  copy->Project(*orderMaker, schema->GetNumAtts());
  copy->Print(proj_schema);
}

void Join ::BlockNestedLoopJoinForSortMerge(
    vector<Page *> leftBuffers, vector<Page *> rightBuffers, Schema *leftSchema,
    Schema *rightSchema, OrderMaker &rightOrderMaker, Pipe *outPipe) {
  for (Page *lbuffer : leftBuffers) {
    Record *leftRec = new Record();
    while (lbuffer->GetFirst(leftRec)) {
      vector<Page *> resetBuffers;
      for (Page *rbuffer : rightBuffers) {
        Page *resetPage = new Page();
        Record *rightRec = new Record();
        while (rbuffer->GetFirst(rightRec)) {
          Record *mergedRec = new Record();
          ComposeMergedRecord(*leftRec, *rightRec, leftSchema, rightSchema,
                              rightOrderMaker, mergedRec);
          outPipe->Insert(mergedRec);
          resetPage->Append(rightRec);
          rightRec = new Record();
        }
        resetBuffers.push_back(resetPage);
      }
      rightBuffers.clear();
      for (Page *resetPage : resetBuffers) {
        rightBuffers.push_back(resetPage);
      }
      leftRec = new Record();
    }
  }
}
// Merge left with right
// Need both the left and right records and their schemas
// This will produce a record with the right attributes after left attributes
void Join::ComposeMergedRecord(Record &left, Record &right, Schema *leftSchema,
                               Schema *rightSchema, Record *mergedRec) {
  // num of attributes to keep is number of attributes in left schema and right
  // schema
  int numAttsToKeep = leftSchema->GetNumAtts() + rightSchema->GetNumAtts();

  // Allocate new array for attributes to keep
  int *attsToKeep = new int[numAttsToKeep];

  // Get attributes to keep from left
  for (int i = 0; i < leftSchema->GetNumAtts(); i++) {
    attsToKeep[i] = i;
  }
  // Set start of right
  int startOfRight = leftSchema->GetNumAtts();
  // Get attributes to keep from right
  for (int i = 0, j = startOfRight; i < rightSchema->GetNumAtts(); i++, j++) {
    attsToKeep[j] = i;
  }

  // Merge Record Now!
  mergedRec->MergeRecords(&left, &right, leftSchema->GetNumAtts(),
                          rightSchema->GetNumAtts(), attsToKeep, numAttsToKeep,
                          startOfRight);
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
    // cout << "[JOIN]: Starting sort-merge join..." << endl;

    Schema *leftSchema = leftOrderMaker.GetSchema();
    Schema *rightSchema = rightOrderMaker.GetSchema();

    // *TODO:* How to decide runlength
    int runLengthLeft = 5;
    int runLengthRight = 10;
    BigQ leftbigq(*inPipeL, *sortedOutPipeLeft, leftOrderMaker, runLengthLeft);
    BigQ rightbigq(*inPipeR, *sortedOutPipeRight, rightOrderMaker,
                   runLengthRight);

    Schema leftJoinAttributeSchema((char *)"left_join_att", &leftOrderMaker,
                                   leftSchema);

    Schema rightJoinAttributeSchema((char *)"right_join_att", &rightOrderMaker,
                                    rightSchema);

    Record *left = new Record();
    Record *right = new Record();

    int isLeftPresent = sortedOutPipeLeft->Remove(left);

    int isRightPresent = sortedOutPipeRight->Remove(right);

    ComparisonEngine comp;
    while (isLeftPresent && isRightPresent) {
      int status = comp.Compare(left, &leftOrderMaker, right, &rightOrderMaker);
      if (status < 0) {
        left = new Record();
        isLeftPresent = sortedOutPipeLeft->Remove(left);
      } else if (status > 0) {
        right = new Record();
        isRightPresent = sortedOutPipeRight->Remove(right);
      } else {
        Record *firstLeftRecord = new Record();
        firstLeftRecord->Copy(left);

        Record *firstRightRecord = new Record();
        firstRightRecord->Copy(right);

        vector<Page *> leftBuffers;
        Page *leftBuffer = new Page();
        while (isLeftPresent &&
               comp.Compare(left, &leftOrderMaker, firstRightRecord,
                            &rightOrderMaker) == 0) {
          if (leftBuffer->Append(left) == 0) {
            leftBuffers.push_back(leftBuffer);
            leftBuffer = new Page();
          }
          left = new Record();
          isLeftPresent = sortedOutPipeLeft->Remove(left);
        }

        if (leftBuffer->GetNumRecords() > 0) {
          leftBuffers.push_back(leftBuffer);
        }

        vector<Page *> rightBuffers;
        Page *rightBuffer = new Page();
        while (isRightPresent &&
               comp.Compare(right, &rightOrderMaker, firstLeftRecord,
                            &leftOrderMaker) == 0) {
          if (rightBuffer->Append(right) == 0) {
            rightBuffers.push_back(rightBuffer);
            rightBuffer = new Page();
          }
          right = new Record();
          isRightPresent = sortedOutPipeRight->Remove(right);
        }

        if (rightBuffer->GetNumRecords() > 0) {
          rightBuffers.push_back(rightBuffer);
        }

        BlockNestedLoopJoinForSortMerge(leftBuffers, rightBuffers, leftSchema,
                                        rightSchema, rightOrderMaker, outPipe);
      }
    }
    // sort-merge join end
  } else {
    // nested loop join start
    Record *left = new Record();
    Record *right = new Record();

    Schema *leftSchema = leftOrderMaker.GetSchema();
    Schema *rightSchema = rightOrderMaker.GetSchema();

    int isLeftPresent = inPipeL->Remove(left);
    int isRightPresent = inPipeR->Remove(right);
    int leftRecCount = 0;

    HeapDBFile *dbFile = new HeapDBFile;
    fType t = heap;
    dbFile->Create("nestedLoopJoin.bin", t, NULL);

    while (isLeftPresent) {
      // empty it out in DBFile
      Record *copy = new Record();
      copy->Copy(left);

      dbFile->Add(*copy);

      left = new Record;
      isLeftPresent = inPipeL->Remove(left);
      leftRecCount++;
    }

    while (isRightPresent) {
      // while get next on DBFile
      // merge left and right records
      // push onto output queue.
      // move first
      Record *temp = new Record();
      while (dbFile->GetNext(*temp)) {
        Record *outPipeRec = new Record();
        ComposeMergedRecord(*right, *temp, rightSchema, leftSchema, outPipeRec);
        outPipe->Insert(outPipeRec);
        outPipeRec = new Record();
        temp = new Record();
      }

      dbFile->MoveFirst();
      right = new Record();
      isRightPresent = inPipeR->Remove(right);
    }
    dbFile->Close();
    // sortedOutPipeRight->Remove(right);
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
  // cout << "Run started" << endl;
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
  // cout << "Join waiting....." << endl;
  pthread_join(threadid, NULL);
  // cout << "Join done!" << endl;
}

void Join::Use_n_Pages(int n) {}