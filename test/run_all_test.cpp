#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <CppUTest/CommandLineTestRunner.h>

IMPORT_TEST_GROUP(IndexTestGroup);
IMPORT_TEST_GROUP(DummyTestGroup);

int main(int argc, char *argv[])
{
    return CommandLineTestRunner::RunAllTests(argc, argv);
}









