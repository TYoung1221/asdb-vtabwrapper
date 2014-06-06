//! gcc -o dummy.bin dummy.c -W -Wall -O3 -std=gnu99 

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <CppUTest/CommandLineTestRunner.h>

TEST_GROUP(ASDBTestGroup)
{
    void setup()
    {
        printf("setup\n");
    }

    void teardown()
    {
        printf("teardown\n");
    }
};

TEST(ASDBTestGroup, TestDummy) {
    CHECK_EQUAL(true, true);
}
