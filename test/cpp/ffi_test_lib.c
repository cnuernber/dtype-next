#include <stdio.h>
#include "byvalue_nested.h"


char json_buffer[1024] = { 0 };


ByValue byvalue_nested(ByValue bv) {
  ByValue rr = { bv.abcd, { bv.first_struct.a, bv.first_struct.b }, { bv.second_struct.c, bv.second_struct.d } };
  return rr;
}
