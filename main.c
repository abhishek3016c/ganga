// C program to illustrate
// strcmp() function
#include<stdio.h>
#include<string.h>

unsigned long hash(unsigned char *str)
{
		unsigned long hash = 5381;
		int c;

		while ((c = *str++))
				hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

		return hash;
}

int main()
{

	char leftStr[] = "Abhishek ";
	char rightStr[] = "Abhishek ";

	// Using strcmp()
	int res = strcmp(leftStr, rightStr);

	if (res==0)
		printf("Strings are equal");
	else
		printf("Strings are unequal");

	printf("\nValue returned by strcmp() is: %d" , res);

	unsigned char stringSample[] = "Abhishek ";
	printf("\nHash value for stringSample is : %ld\n", hash(stringSample));
	return 0;
}
