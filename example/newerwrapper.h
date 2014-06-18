#pragma once

#include <stdbool.h>
#include <pthread.h>
#include "asdb.h"
#include "asdb_newapi.h"

typedef struct {
    asdb_request *req;
    int argc;
    char **argv;
} nw_targ;

typedef struct _nw_vtab {
    /* provied by system */
    const asdb_schema *schema;
    int ncol;
    /* defined by user */
    int argc;
    char **argv;
} nw_vtab;

typedef struct _nw_cursor {
    /* provied by system */
    nw_vtab *vtab;
    sqlite3_int64 rowid;
    /* defined by user */
    pthread_t th;
    asdb_request *req;
    nw_targ *targ;
} nw_cursor;

typedef struct _nw_row {
    /* defined by user */
    GPtrArray *array;
    bool eof;
} nw_row;

typedef struct _nw_cell {
    /* defined by user */
    char *word;
} nw_cell;

extern bool asdb_loop(asdb_request *req, int argc, char *argv[]);
