#include "GroupBy.h"
#include <iostream>

void GroupBy ::computeAndAggregate(Function *computeMe, Record *temp,
                                   int *intAggregator,
                                   double *doubleAggregator) {
  int intResult = 0;
  double doubleResult = 0.0;
  Type t = computeMe->Apply(*temp, intResult, doubleResult);
  switch (t) {
    case Int:
      (*intAggregator) += intResult;
      break;
    case Double:
      (*doubleAggregator) += doubleResult;
      break;
    case String:
      break;
  }
}

void GroupBy ::composeAggregateRecord(Record *example_rec,
                                      OrderMaker *groupAtts, Schema *schema,
                                      Schema *group_att_schema,
                                      Schema *group_by_schema,
                                      string aggregate_result, Pipe *out) {
  // Project example_rec to extract grouping attributes
  example_rec->Project(*groupAtts, schema->GetNumAtts());

  // Convert the projected example_rec to text
  string projected_rec = example_rec->TextFileVersion(group_att_schema);

  // Append the aggregate result to the front of projected_rec
  string aggregated_rec_string;
  aggregated_rec_string.append(aggregate_result);
  aggregated_rec_string += '|';
  aggregated_rec_string.append(projected_rec);

  // Compose aggregate record based on the schema and the aggregated_rec_string
  Record aggregateRec;
  aggregateRec.ComposeRecord(group_by_schema, aggregated_rec_string.c_str());
  aggregateRec.Print(group_by_schema);
  out->Insert(&aggregateRec);
}

void *GroupBy ::GroupByWorkerThreadRoutine(void *threadparams) {
  struct GroupByWorkerThreadParams *params;
  params = (struct GroupByWorkerThreadParams *)threadparams;

  Pipe *inPipe = params->inPipe;
  Pipe *outPipe = params->outPipe;
  OrderMaker *groupAtts = params->groupAtts;
  Function *computeMe = params->computeMe;

  // GroupBy logic here
  groupAtts->Print();

  // Create a schema to group records
  Schema *schema = computeMe->GetSchema();
  if (schema == NULL) {
    std::cout
        << "Abort GroupBy! Schema not present to project and compose grouping"
        << std::endl;
    pthread_exit(NULL);
  }

  Schema only_group_attributes_schema("group_schema", groupAtts, schema);
  Attribute sum_attr;
  sum_attr.name = "sum";
  sum_attr.myType = computeMe->GetReturnsInt() ? Int : Double;
  Schema group_by_schema(only_group_attributes_schema);
  group_by_schema.AddAttribute(sum_attr);

  // Initialize aggregators
  int intAggregator;
  double doubleAggregator;

  auto resetAggregators = [&intAggregator, &doubleAggregator]() -> void {
    intAggregator = 0;
    doubleAggregator = 0.0;
  };

  auto get_aggregate = [&computeMe, &intAggregator,
                        &doubleAggregator]() -> string {
    return to_string(computeMe->GetReturnsInt() ? intAggregator
                                                : doubleAggregator);
  };

  resetAggregators();
  // Create a BigQ which will give us sorted records
  Pipe *sortedOutPipe = new Pipe(100);
  BigQ sortedGroupByQ(*inPipe, *sortedOutPipe, *groupAtts, 10);

  // Get the first record in the pipe
  Record *prev = new Record();
  if (!sortedOutPipe->Remove(prev)) {
    std::cout << "cannot sort! Internal BigQ pipe to GroupBy is closed"
              << std::endl;
    pthread_exit(NULL);
  }

  // Start the aggregation with the first record
  computeAndAggregate(computeMe, prev, &intAggregator, &doubleAggregator);

  ComparisonEngine comp;
  Record *curr = new Record();
  while (sortedOutPipe->Remove(curr)) {
    // compare with previous record
    // if equal,
    if (comp.Compare(curr, prev, groupAtts) == 0) {
      //   - keep aggregating
      computeAndAggregate(computeMe, curr, &intAggregator, &doubleAggregator);
      prev = curr;
      curr = new Record();
    } else {
      // else its a new group,
      // output the record for old group
      composeAggregateRecord(prev, groupAtts, schema,
                             &only_group_attributes_schema, &group_by_schema,
                             get_aggregate(), outPipe);
      //   - stop aggregating and start aggregation for the new group
      prev = curr;
      curr = new Record();
      resetAggregators();
      computeAndAggregate(computeMe, prev, &intAggregator, &doubleAggregator);
    }
  }

  composeAggregateRecord(prev, groupAtts, schema, &only_group_attributes_schema,
                         &group_by_schema, get_aggregate(), outPipe);

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

  struct GroupByWorkerThreadParams *thread_data =
      (struct GroupByWorkerThreadParams *)malloc(
          sizeof(struct GroupByWorkerThreadParams));

  thread_data->inPipe = &inPipe;
  thread_data->outPipe = &outPipe;
  thread_data->groupAtts = &groupAtts;
  thread_data->computeMe = &computeMe;

  pthread_create(&threadid, &attr, GroupByWorkerThreadRoutine,
                 (void *)thread_data);
}

GroupBy ::GroupBy() {}
GroupBy ::~GroupBy() {}
void GroupBy ::WaitUntilDone() {
  cout << "Group By waiting....." << endl;
  pthread_join(threadid, NULL);
  cout << "Group by done...." << endl;
}
void GroupBy ::Use_n_Pages(int n) {}