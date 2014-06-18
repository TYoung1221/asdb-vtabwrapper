//! gcc -std=gnu99 -Wl,-rpath=. -shared -fPIC -I/usr/include/glib-2.0 -I/usr/lib/x86_64-linux-gnu/glib-2.0/include  -o adapter.so new-engtext.c newerwrapper.c  -lasdb -lglib-2.0

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <pthread.h>
#include <string.h>
#include <glib-2.0/glib.h>

#include "asdb.h"
#include "newerwrapper.h"

const char const *asdb_module_name = "adapter";

extern asdb_schema schema;

static void *loop_handler(void *targ);

asdb_row *asdb_create_row(asdb_request *req)
{
    nw_row *row = malloc(sizeof(nw_row));
    if (row == NULL) return NULL;
    row->array = g_ptr_array_new();
    row->eof = false;
    return (asdb_row *)row;
}

void asdb_yield_column(asdb_row *row, void *data, asdb_type type, int icol)
{
    nw_row *r = (nw_row *)row;
    /* assertion */
    if (type != ASDB_TYPE_TEXT) {
        fprintf(stderr, "not yet implemented!\n");
        return;
    }
    if (icol >= ASDB_MAX_NCOLUMN) {
        fprintf(stderr, "column number is larger than limitation\n");
        return;
    }
    if (schema.cname[icol] == NULL) {
        fprintf(stderr, "null column\n");
        return;
    }
    g_ptr_array_insert(r->array, icol, data);
}

void asdb_yield_row(asdb_request *req, asdb_row *row)
{
    g_queue_push_head(req->rowq, row);
}


asdb_request *asdb_request_new()
{
    asdb_request *req = malloc(sizeof(asdb_request));
    if (req == NULL) return NULL;
    req->rowq = g_queue_new();
    if (req->rowq == NULL) {
        free(req);
        return NULL;
    }
    return req;
}

void asdb_request_destroy(asdb_request *req)
{
    g_queue_free(req->rowq);
    free(req);
}

nw_targ *nw_targ_new(nw_vtab *vtab)
{
    nw_targ *targ = malloc(sizeof(nw_targ));
    if (targ == NULL) return NULL;
    targ->req = asdb_request_new();
    if (targ->req == NULL) {
        free(targ);
        return NULL;
    }
    targ->argc = vtab->argc;
    targ->argv = vtab->argv;
    return targ;
}

asdb_cursor *asdb_open(asdb_vtab *pVtab)
{
    nw_cursor* cur = malloc(sizeof(nw_cursor));
    nw_targ *targ = nw_targ_new((nw_vtab *)pVtab);
    if (targ == NULL) return NULL;
    pthread_t th;
    int s = pthread_create(&th, NULL, loop_handler, targ);
    if (s != 0) {
        perror("pthread_create");
        return NULL;
    }
    cur->th = th;
    cur->targ = targ;
    return (asdb_cursor*)cur;
}

asdb_rc asdb_close(asdb_cursor *pVtabcursor)
{
    nw_cursor *cur = (nw_cursor *)pVtabcursor;
    pthread_join(cur->th, NULL);
    asdb_request_destroy(cur->req);
    free(cur->targ);
    free(cur);
    return ASDB_OK;
}

asdb_row *asdb_next(asdb_cursor *pVabcursor)
{
    nw_cursor *cur = (nw_cursor *)pVabcursor;
    asdb_request *req = cur->req;
    nw_row *row;
    while((row = g_queue_pop_tail(req->rowq)) == NULL); /* spin wait */
    if (row->eof) {
        g_ptr_array_free(row->array, true);
        free(row);
        return NULL;
    }
    return (asdb_row *)row;
}

asdb_cell *asdb_get_cell(const asdb_row *row, int icol)
{
    nw_row * r = (nw_row *)row;
    GPtrArray *a = r->array;
    nw_cell *cell = malloc(sizeof(nw_cell));
    if (cell == NULL) return NULL;
    cell->word = g_ptr_array_index(a, icol);
    return (asdb_cell *)cell;
}

asdb_value asdb_get_value(const asdb_cell *cell)
{
    asdb_value val;
    asdb_set_value_text(&val, ((nw_cell*)cell)->word);
    return val;
}

asdb_vtab *asdb_create_vtab(int argc, const char *const* argv)
{
    nw_vtab *v = malloc(sizeof(nw_vtab));
    if (v == NULL) return NULL;
    char *tname = sqlite3_mprintf( "%s", (argc > 2 ? argv[2] : ""));
    schema.tname = tname;
    v->schema = &schema;
    /* copy */
    v->argc = argc;
    v->argv = malloc(sizeof(char*) * argc);
    if (v->argv == NULL) {
        free(v);
        return NULL;
    }
    for (int i = 0; i < argc; i++) {
        char *arg = malloc(sizeof(char)*(strlen(argv[i])+1));
        if (arg == NULL) {
            for (int j = 0; j < i; j++)
                free(v->argv[j]);
            free(v);
            return NULL;
        }
        strcpy(v->argv[i], argv[i]);
    }
    return (asdb_vtab *)v;
}

asdb_rc asdb_destroy_vtab(asdb_vtab *pVtab)
{
    nw_vtab *v = (nw_vtab *)pVtab;
    for (int i = 0; i < v->argc; i++) {
        if (v->argv[i] != NULL) free(v->argv[i]);
    }
    free(v->argv);
    return ASDB_OK;
}

const char* const asdb_get_module_name(void)
{
    return asdb_module_name;
}

static void *loop_handler(void *targ)
{
    nw_targ *arg = (nw_targ *)targ;
    asdb_loop(arg->req, arg->argc, arg->argv);
    /* finalize */
    nw_row *row = malloc(sizeof(nw_row));
    if (row == NULL) return NULL;
    row->array = NULL;
    row->eof = true;
}
