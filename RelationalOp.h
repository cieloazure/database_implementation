class RelationalOp {
 public:
  //   blocks the caller until the particular relational operator has run to
  //       completion virtual void
  virtual void WaitUntilDone() = 0;

  // tells how much internal memory the operation can use virtual void
  virtual void Use_n_Pages (int n) = 0;
};