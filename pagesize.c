
#include <stdio.h>
#include <stdlib.h>
#ifdef _WIN32
#include <windows.h>

long getPageSize () {
  SYSTEM_INFO si;
  getSystemInfo(&si);
  return si.dwPageSize;
}

#else
#include <unistd.h>


long getPageSize () {
	return sysconf(_SC_PAGESIZE);
}
#endif

#ifdef TEST
int main (int argc, char *argv[]) {
	printf ("PageSize = %ld\n", getPageSize());
}
#endif
