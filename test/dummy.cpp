//! gcc -o dummy.bin dummy.c -W -Wall -O3 -std=gnu99 

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <CppUTest/CommandLineTestRunner.h>

TEST_GROUP(DummyTestGroup)
{
    void setup()
    {
    }

    void teardown()
    {
    }
};

TEST(DummyTestGroup, TestDummy) {
    CHECK(true);
}
