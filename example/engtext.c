//! gcc -c medline.c -W -Wall -O3 -std=gnu99 -lsqlite3 -fsyntax-only -Wno-unused-parameter -Wno-unused-variable

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>

#include "asdb.h"
#include "engtext.h"

/***** parameters *****/
#define ENG_LINEBUF_SIZE 1024
#define ENG_MAX_NSCHEMA 5
const char const *asdb_module_name = "text";
const int ENG_DBG = 0;

/***** schema *****/
static const asdb_schema eng_schema =
{ .tname = NULL,
  .cname = {"word"}
};

/***** open / close / next *****/
asdb_cursor *asdb_open( asdb_vtab *pVtab )
{
    if ( ENG_DBG >= 2 ) fprintf( stderr, "asdb_open\n" );

    eng_vtab *vtab = ( eng_vtab * ) pVtab;
    /* create cursor */
    eng_cursor *c =
        (eng_cursor *) sqlite3_malloc( sizeof( eng_cursor ) );
    if ( c == NULL ) {
        if ( ENG_DBG >= 2 ) fprintf( stderr,  "-> ASDB_NOMEM\n" );
        return NULL;
    }
    /* set variables */
    if ( ENG_DBG >= 2 ) fprintf( stderr, "### create buffer\n");
    char *buffer = sqlite3_malloc( sizeof( char ) * ENG_LINEBUF_SIZE );
    if ( buffer == NULL ) {
        if ( ENG_DBG >= 2 ) fprintf( stderr, "-> ASDB_ERROR\n" );
        sqlite3_free( c );
        return NULL;
    }
    buffer[0] = '\0';
    
    if ( ENG_DBG >= 2 ) fprintf( stderr, "### file open: %s\n", vtab->filename );
    FILE *fp = fopen( vtab->filename, "r" );
    if ( fp == NULL ) {
        if ( ENG_DBG >= 2 ) fprintf( stderr, "-> ASDB_ERROR\n" );
        sqlite3_free( c );
        sqlite3_free( buffer );
        return NULL;
    }
    c->line = buffer;
    c->cur  = NULL;
    c->fp = fp;
    return (asdb_cursor *) c;
}

asdb_rc asdb_close( asdb_cursor *pVtabcursor )
{
    if ( ENG_DBG >= 2 ) fprintf( stderr, "asdb_close\n" );

    eng_cursor *c = ( eng_cursor * ) pVtabcursor;
    sqlite3_free( c->line );
    return ASDB_OK;
}

asdb_row *asdb_next( asdb_cursor *pVtabcursor )
{
    if ( ENG_DBG >= 2 ) fprintf( stderr, "asdb_next\n" );
    
    eng_cursor *c = ( eng_cursor * ) pVtabcursor;
    /* get new line */
    if ( c->cur == NULL ) {
        if ( fgets( c->line, ENG_LINEBUF_SIZE, c->fp ) == NULL )
            return NULL;        /* EOF */
        c->cur = strtok( c->line, " \t\n\r" );
    }
    /* create row object */
    eng_row *row = malloc( sizeof( eng_row ) );
    if ( row == NULL ) return NULL;
    row->word = sqlite3_mprintf( "%s", c->cur );
    /* prepare next word */
    c->cur = strtok( NULL, " \t\n\r" );
    return ( asdb_row * ) row;
}

asdb_cell *asdb_get_cell( const asdb_row* row, int icol )
{
    if ( ENG_DBG >= 2 ) fprintf( stderr, "asdb_get_cell\n" );

    const eng_row *r = ( const eng_row * ) row;

    eng_cell *cell = malloc( sizeof( eng_cell ) );
    if ( cell == NULL ) return NULL;
    asdb_set_value_text( &cell->value, r->word ); /* value set */

    return (asdb_cell *)cell;
}

asdb_value asdb_get_value( const asdb_cell *cell )
{
    if ( ENG_DBG >= 2 ) fprintf( stderr, "asdb_get_value\n" );
    
    const eng_cell *c = ( const eng_cell * ) cell;
    return c->value;
}

/***** vtable create / destroy *****/
/*
**   argv[0]   -> module name
**   argv[1]   -> database name
**   argv[2]   -> table name
**   argv[...] -> column names...
 */
asdb_vtab *asdb_create_vtab( int argc, const char *const*argv )
{
    if ( ENG_DBG >= 2 ) fprintf( stderr, "asdb_create_vtab\n" );
    if ( ENG_DBG >= 2 ) {
        for ( int i = 0; i < argc; i++ ) {
            fprintf( stderr, "# argv[%d] = %s\n", i, argv[i] );
        }
    }
    
    /* create vtab object */
    eng_vtab *vtab =
        ( eng_vtab * ) sqlite3_malloc( sizeof( eng_vtab ) );
    if ( vtab == NULL ) {
        if ( ENG_DBG >= 1 ) fprintf(stderr, "-> ASDB_NOMEM\n" );
        return NULL;
    }
    
    /* extract file name */
    char *filename = sqlite3_mprintf( "%s", (argc > 3 ? argv[3] : "a.txt"));
    if ( filename == NULL ) {
        if ( ENG_DBG >= 1 ) fprintf(stderr,"-> ASDB_NOMEM\n");
        sqlite3_free( vtab );
        return NULL;
    }
    
    /* extract table name */
    char *tablename = sqlite3_mprintf( "%s", (argc > 2 ? argv[2] : ""));
    if ( tablename == NULL ) {
        if ( ENG_DBG >= 1 ) fprintf(stderr,"-> ASDB_NOMEM\n");
        sqlite3_free( vtab );
        sqlite3_free( filename );
        return NULL;
    }

    /* create schema */
    asdb_schema *sch  = sqlite3_malloc( sizeof( asdb_schema ) );
    if ( sch == NULL ) {
        if ( ENG_DBG >= 1 ) fprintf( stderr, "-> ASDB_NOMEM\n" );
        sqlite3_free( vtab );
        sqlite3_free( filename );
        sqlite3_free( tablename );
        return NULL;
    }
    *sch = eng_schema;
    sch->tname = tablename;

    /* set variables */
    vtab->schema = sch;
    vtab->filename  = filename;

    return (asdb_vtab *)vtab;
}

asdb_rc asdb_destroy_vtab( asdb_vtab *pVtab ) {
    eng_vtab *vtab = ( eng_vtab * )pVtab;
    sqlite3_free( vtab->filename );
    sqlite3_free( vtab );
    return ASDB_OK;
}

const char const *asdb_get_module_name( void )
{
    return asdb_module_name;
}
