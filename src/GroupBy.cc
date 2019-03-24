#include "GroupBy.h"
#include <iostream>

struct GroupByWorkerThreadParams {
  Pipe *inPipe;
  Pipe *outPipe;
  OrderMaker *groupAtts;
  Function *computeMe;
};

struct GroupByWorkerThreadParams group_by_thread_data;

void computeAndAggregate(Function *computeMe, Record *temp, int &intAggregator,
                         double &doubleAggregator) {
  int intResult = 0;
  double doubleResult = 0.0;
  Type t = computeMe->Apply(*temp, intResult, doubleResult);
  switch (t) {
    case Int:
      intAggregator += intResult;
      break;
    case Double:
      doubleAggregator += doubleResult;
      break;
    case String:
      break;
  }
}

void resetAggregators(int &intAggregator, double &doubleAggregator) {
  intAggregator = 0;
  doubleAggregator = 0.0;
}

void composeRecord(Record rec, OrderMaker groupAtts, int intAggregator,
                   double doubleAggregator) {}

void *GroupByWorkerThreadRoutine(void *threadparams) {
  struct GroupByWorkerThreadParams *params;
  params = (struct GroupByWorkerThreadParams *)threadparams;

  Pipe *inPipe = params->inPipe;
  Pipe *outPipe = params->outPipe;
  OrderMaker *groupAtts = params->groupAtts;
  Function *computeMe = params->computeMe;

  // GroupBy logic here
  groupAtts->Print();
  Schema *s = computeMe->GetSchema();
  if (s == NULL) {
    std::cout
        << "Abort GroupBy! Schema not present to project and compose grouping"
        << std::endl;
    pthread_exit(NULL);
  }

  Pipe *sortedOutPipe = new Pipe(100);
  BigQ sortedGroupByQ(*inPipe, *sortedOutPipe, *groupAtts, 10);

  Record temp;
  Record prev;

  Record aggregate;
  int intAggregator;
  double doubleAggregator;

  resetAggregators(intAggregator, doubleAggregator);

  if (!sortedOutPipe->Remove(&prev)) {
    std::cout << "cannot sort! Internal BigQ pipe to GroupBy is closed"
              << std::endl;
    pthread_exit(NULL);
  }

  computeAndAggregate(computeMe, &prev, intAggregator, doubleAggregator);
  prev.Project(*groupAtts, s->GetNumAtts());
  Schema print_schema("pschema", groupAtts, s);
  prev.Print(&print_schema);
  string prevText = prev.TextFileVersion(&print_schema);

  prevText =
      to_string(computeMe->GetReturnsInt() ? intAggregator : doubleAggregator) +
      '|' + prevText;

  cout << prevText << endl;
  Attribute sum_attr;
  sum_attr.name = "sum";
  sum_attr.myType = computeMe->GetReturnsInt() ? Int : Double;

  print_schema.AddAttribute(sum_attr);

  Record opRec;
  opRec.ComposeRecord(&print_schema, prevText.c_str());
  opRec.Print(&print_schema);

  // ComparisonEngine comp;
  // while (sortedOutPipe->Remove(&temp)) {
  //   // compare with previous record
  //   // if equal,
  //   if (comp.Compare(&temp, &prev, groupAtts) == 0) {
  //     //   - keep aggregating
  //   } else {
  //     // else its a new group,
  //     //   - stop aggregating and start aggregation for the new group
  //   }
  // }

  outPipe->ShutDown();
  pthread_exit(NULL);
}

void GroupBy ::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
                   Function &computeMe) {
  pthread_attr_t attr;

  if (pthread_attr_init(&attr) == 0) {
    if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE) == 0) {
    } else {
      throw runtime_error("Error spawning GroupBy worker");
    }
  } else {
    throw runtime_error("Error spawning GroupBy worker");
  }

  group_by_thread_data.inPipe = &inPipe;
  group_by_thread_data.outPipe = &outPipe;
  group_by_thread_data.groupAtts = &groupAtts;
  group_by_thread_data.computeMe = &computeMe;

  pthread_create(&threadid, &attr, GroupByWorkerThreadRoutine,
                 (void *)&group_by_thread_data);
}

GroupBy ::GroupBy() {}
GroupBy ::~GroupBy() {}
void GroupBy ::WaitUntilDone() {
  cout << "Group By waiting....." << endl;
  pthread_join(threadid, NULL);
  cout << "Group by done...." << endl;
}
void GroupBy ::Use_n_Pages(int n) {}