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
    }

    void teardown()
    {
    }
};

TEST(ASDBTestGroup, TestDummy) {
    CHECK_EQUAL(true, true);
}
