#include <cstdio>
extern "C" {
#include "byvalue_nested.h"
}

using namespace std;


int main (int c, char** v) {
  ByValue bc = { 10, { 5, 4.0 }, {3.0, 9}};
  ByValue bv = byvalue_nested(bc);
  printf("{\"abcd\":%d \"a\":%d \"b\":%lf \"c\":%lf \"d\":%d}\n", bv.abcd,
	 bv.first_struct.a, bv.first_struct.b,
	 bv.second_struct.c, bv.second_struct.d);
  return 0;
}
