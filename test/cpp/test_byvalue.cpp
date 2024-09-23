#include <cstdio>
extern "C" {
#include "byvalue_nested.h"
}

using namespace std;


int main (int c, char** v) {
  ByValue bc = { 10, { 5, 4.0 }, {3.0, 9}};
  printf("%s\n", byvalue_nested(bc));
  return 0;
}
