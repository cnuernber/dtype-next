#include <stdio.h>
#include "byvalue_nested.h"


char json_buffer[1024] = { 0 };


const char* byvalue_nested(ByValue bv) {
  snprintf(json_buffer,1024,"{\"abcd\":%d \"a\":%d \"b\":%lf \"c\":%lf \"d\":%d}", bv.abcd,
	   bv.first_struct.a, bv.first_struct.b,
	   bv.second_struct.c, bv.second_struct.d);
  return json_buffer;
}
