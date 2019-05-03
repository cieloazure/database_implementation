#ifndef GROUPBY_H
#define GROUPBY_H

#include "BigQ.h"
#include "Function.h"
#include "Pipe.h"
#include "RelationalOp.h"

class GroupBy : public RelationalOp {
  // Physical operator for Group By operation. Is quite involved in its operation.
 private:
  static void computeAndAggregate(Function *computeMe, Record *temp,
                                  int *intAggregator, double *doubleAggregator);

  static void composeAggregateRecord(Record *example_rec, OrderMaker *groupAtts,
                                     Schema *schema, Schema *group_att_schema,
                                     Schema *group_by_schema,
                                     string aggregate_result, Pipe *out);

  static void *GroupByWorkerThreadRoutine(void *threadparams);

 public:
  GroupBy();
  ~GroupBy();
  void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
           Function &computeMe);

  //   blocks the caller until the particular relational operator has run to
  //       completion virtual void
  void WaitUntilDone();

  // tells how much internal memory the operation can use virtual void
  void Use_n_Pages(int n);

  struct GroupByWorkerThreadParams {
    Pipe *inPipe;
    Pipe *outPipe;
    OrderMaker *groupAtts;
    Function *computeMe;
  };
};

#endif