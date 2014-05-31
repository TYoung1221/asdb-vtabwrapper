#ifndef ASDB_DATA_H
#define ASDB_DATA_H

#include <glib-2.0/glib.h>

/***** Data Structures *****/

/*** index ***/
typedef char * asdb_index_key;

typedef struct _asdb_index_node {
    asdb_index_key key;
    /* uint64_t *irow_array; */
    array_t ira;                /**< irow_array: uint64_t */
    /* struct _asdb_index_node *next; */
} asdb_index_node;

typedef struct _asdb_index {
    int icolumn;
    /* asdb_index_node **data; */
    /* array_t data;               /\**< hash: asdb_index_node *\/ */
    GHashTable *table;
} asdb_index;

typedef struct _asdb_pm {
    asdb_row  *row;
    array_t    cell;            /**< array of asdb_cell */
    uint64_t   rid;             /**< identification number */
} asdb_pm;                      /* positional map */

typedef enum {
    USABLE,
    CONSTRUCTING,
    UNUSABLE,
} asdb_pmstate_t;

typedef struct _asdb_plan {
    array_t idx_icols;          /**< <int> usable indexes */
    array_t prep_icols;         /**< <int> prepare indexes */
} asdb_plan;

typedef struct sqlite3_asdb_vtab {
    const sqlite3_module *pModule;
    int            nRef;
    char          *zErrMsg;
    int            magic;           /* 918729 */
    array_t        indexes;         /**< <asdb_index> */
    /* asdb_pm *pm */
    asdb_pmstate_t pm_state;        /**< usable/constructing/unusable */
    array_t        pm;              /**< <asdb_pm> positional map */
    asdb_vtab     *vtab;            /**< user defined vtab */
    array_t        plans;           /**< <asdb_plan_t> set in BestIndex */
} sqlite3_asdb_vtab;

typedef struct {
    sqlite3_asdb_vtab *pVtab;
    int          magic;           /* 9180729 */
    asdb_rc      status;           
    bool         ira_usable;       
    array_t      ira;             /**< irow array: uint64_t */
    asdb_pmstate_t pm_state;      /**< usable/constructing/unusable */
    asdb_pm      nowmap;          /**< active positional map */
    uint64_t     nowmap_pos;      /**< nowmap = pm[ira[nowmap_pos]] */
    asdb_plan    plan;            /**< index plan */
    array_t      idxes_preparing; /**< <asdb_index> */
    asdb_cursor *cursor;          /**< user defined cursor */
} sqlite3_asdb_cursor;


#endif
