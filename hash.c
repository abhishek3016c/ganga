// C program to illustrate
// strcmp() function
#include<stdio.h>
#include<string.h>

unsigned long hash_string(unsigned char *str)
{
		unsigned long hash = 5387;
		int c;

		while ((c = *str++))
				hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

		return hash;
}

unsigned int hash_integer(unsigned int x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

#define MAX_LENGTH 128

int main()
{

  unsigned char leftStr[] = "000000000000000";
  unsigned char rightStr[] = "000000000000000";

  int ret;
  char buffer[MAX_LENGTH];
  unsigned long hashLeft;
  unsigned long hashRight;

	// Using strcmp()
	//int res = strcmp(hash(leftStr), hash(rightStr));

  /* The C library function
  int memcmp(const void *str1, const void *str2, size_t n))
  compares the first n bytes of memory area str1 and memory area str2. */

  printf("Hash output for leftStr: %ld\n", hash_string(leftStr));
  printf("Hash output for rightStr: %ld\n", hash_string(rightStr));
/*
  #memcpy(hashLeft, snprintf(buffer, MAX_LENGTH+1, "%lu", hash(leftStr)), sizeof(leftStr));
  #memcpy(hashRight, snprintf(buffer, MAX_LENGTH+1, "%lu", hash(leftStr)), sizeof(rightStr));

  ret = memcmp(hashLeft, hashRight, 32);

	if (ret==0)
		printf("Hash outputs are equal");
	else
		printf("Hash outputs are unequal");
*/
	//printf("\nValue returned by strcmp() is: %d" , res);
	return 0;
}
