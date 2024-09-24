typedef struct {
  int abcd;
  struct {
    int a;
    double b;
  } first_struct;
  struct {
    double c;
    int d;
  } second_struct;
} ByValue;

//test both passing and returning by value
extern ByValue byvalue_nested(ByValue bv);
