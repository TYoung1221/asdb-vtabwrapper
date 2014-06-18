//! gcc -o new-engtext.bin new-engtext.c -W -Wall -O3 -std=gnu99 

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>

#include "asdb_newapi.h"

#define ENG_LINEBUF_SIZE 1024

asdb_schema schema =
{
    .tname = NULL,
    .cname = {"word"}
};

bool asdb_loop(asdb_request *req, int argc, char *argv[])
{
    /* open file */
    char *filename = (argc > 3) ? argv[3] : "a.txt";
    FILE *fp = fopen(filename, "r");
    if (fp == NULL) return false;
    fprintf(stderr, "open: %s\n", filename);
    /* allocate buffer */
    char *line = (char *)malloc(sizeof(char) * ENG_LINEBUF_SIZE);
    if (line == NULL) return false;
    char *delim = " \t\n\r";
    /* main loop */
    while (fgets(line, ENG_LINEBUF_SIZE, fp) != NULL) {
        /* fprintf(stderr, "%s\n", line); */
        /* split a line into words */
        char *cur = strtok(line, delim);
        while (cur != NULL) {
            asdb_row *row = asdb_create_row(req);
            char *word = malloc(sizeof(char)*(strlen(cur)+1));
            strcpy(word, cur);
            asdb_yield_column(row, word, ASDB_TYPE_TEXT, 0);
            asdb_yield_row(req, row);
            cur = strtok(NULL, delim);
        }
    }
    fprintf(stderr, "closed..\n");
    fclose(fp);
    free(line);
    return true;
}
