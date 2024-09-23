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

extern const char* byvalue_nested(ByValue bv);
