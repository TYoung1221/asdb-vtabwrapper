#pragma once

#include <glib-2.0/glib.h>
#include <pthread.h>
#include "asdb.h"

typedef struct _asdb_request {
    GQueue *rowq;
    pthread_mutex_t mtx;
} asdb_request;

extern asdb_row *asdb_create_row(asdb_request *req);
extern void asdb_yield_column(asdb_row *row, void *data, asdb_type type, int icol);
extern void asdb_yield_row(asdb_request *req, asdb_row *row);

