#pragma once

#ifdef __TEST__
#  include <sqlite3.h>
#else 
#  include <sqlite3ext.h>
extern const sqlite3_api_routines *sqlite3_api;
#endif /* Def: __TEST__ */


/* defines */
#define ASDB_MAX_NCOLUMN 50

typedef struct {
    char *tname;
    char *cname[ASDB_MAX_NCOLUMN+1];
} asdb_schema;

typedef struct {
    const asdb_schema *schema;
    int ncol;                   /**< # of columns */
} asdb_vtab;


typedef enum {
    ASDB_TYPE_NULL = 0,
    ASDB_TYPE_INT64,
    ASDB_TYPE_TEXT
} asdb_type;

typedef struct {
    void *value;
    asdb_type type;
} asdb_value;

typedef struct {
    asdb_vtab *vtab;
    sqlite3_int64 rowid;        /**< user must not change */
} asdb_cursor;

typedef struct {
    /* user-defined */
} asdb_row;

typedef struct {
    /* user-defined */
} asdb_cell;

typedef enum {
    ASDB_NOMEM = -2,
    ASDB_ERROR = -1,
    ASDB_OK    = 0,
    ASDB_EOF   = 1
} asdb_rc;


/* User Defined Functions */
extern asdb_cursor *asdb_open( asdb_vtab * );
extern asdb_rc asdb_close( asdb_cursor * );
extern asdb_row *asdb_next( asdb_cursor * );
extern asdb_cell *asdb_get_cell( const asdb_row *, int );
extern asdb_value asdb_get_value( const asdb_cell * );
extern asdb_vtab *asdb_create_vtab( int, const char*const*);
extern asdb_rc asdb_destroy_vtab( asdb_vtab * );
/* User Defined Variables */
extern const char* const asdb_get_module_name();

/* Utilities */
asdb_rc asdb_set_value_int64( asdb_value *col, sqlite3_int64 i );
asdb_rc asdb_set_value_text( asdb_value *col, const char * str );
asdb_rc asdb_set_value_null( asdb_value *col );
int asdb_get_column_no( const asdb_schema *schema, const char *cname );


