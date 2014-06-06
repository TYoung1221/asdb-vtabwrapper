//! gcc -c dummy.c -W -Wall -O3 -std=gnu99 -lsqlite3 -fsyntax-only -Wno-unused-parameter -Wno-unused-variable

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>

#include "asdb.h"

/***** parameters *****/
const char* const asdb_get_module_name()
{
    return "dummy";
}

/***** schema *****/

/***** open / close / next *****/
asdb_cursor *asdb_open( asdb_vtab *pVtab )
{
    /* return NULL; */
    return malloc( sizeof( asdb_cursor ) );
}

asdb_rc asdb_close( asdb_cursor *pVtabcursor )
{
    return ASDB_OK;
}

asdb_row *asdb_next( asdb_cursor *pVtabcursor )
{
    return NULL;
}

asdb_cell *asdb_get_cell( const asdb_row* row, int icol )
{
    return NULL;
}

asdb_value asdb_get_value( const asdb_cell *cell )
{
    return (asdb_value) { .value = NULL, .type = ASDB_TYPE_NULL };
}

asdb_vtab *asdb_create_vtab( int argc, const char *const*argv )
{
    return malloc( sizeof( asdb_vtab ) );
}

asdb_rc asdb_destroy_vtab( asdb_vtab *pVtab )
{
    free( pVtab );
    return ASDB_OK;
}
