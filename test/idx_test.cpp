#include <CppUTest/CommandLineTestRunner.h>

extern "C" {
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include "array.h"
#include "asdb.h"
#include "asdb_data.h"
#include <sqlite3.h>

extern sqlite3_asdb_vtab * mk_sqlite3_asdb_vtab( int argc, const char *const*argv );
extern sqlite3_asdb_cursor *mk_sqlite3_asdb_cursor(sqlite3_asdb_vtab *vtab);


extern asdb_index_key   idxkey_create( asdb_value col );
extern asdb_index_key   idxkey_create_from_value( sqlite3_value *val );
extern asdb_index_key   idxkey_create_from_text( const char *key );
extern int              idxkey_cmp( const asdb_index_key a, const asdb_index_key b );
extern void             idxkey_destroy( asdb_index_key key );

extern asdb_index_node *idxnode_create( asdb_index_key key );
extern asdb_rc          idxnode_append_to_ira( asdb_index_node *node, uint64_t irow );
extern array_t          idxnode_get_ira( asdb_index_node *node ); /* mada */
extern void             idxnode_destroy( asdb_index_node *node );
/* extern asdb_index_node *idxnode_search( asdb_index *index, asdb_index_key key ); */

extern asdb_index       idx_create( int icol );
/* extern array_t          idx_search_by_key( asdb_index *index, const asdb_index_key key ); */
/* extern array_t          idx_search_by_node( asdb_index *index, const asdb_index_node *node ); */
extern asdb_index_node *idx_search_by_key( asdb_index *index, const asdb_index_key key );
extern asdb_index_node *idx_search_by_node( asdb_index *index, const asdb_index_node *node );
extern asdb_rc          idx_add_node( asdb_index *index, const asdb_index_node *node );
extern asdb_rc          idx_delete_node( asdb_index *index, const asdb_index_node *node );
extern asdb_rc          idx_append_newirow( asdb_index *index,
                                     const asdb_index_key key,
                                     uint64_t irow );

extern bool             idxes_is_exist( array_t indexes, int icol );
extern asdb_index       idxes_get_index( array_t indexes, int icol );
extern asdb_rc          idxes_add_index( array_t *indexes, asdb_index idx );

extern array_t          irow_intersect( array_t a_arr, array_t b_arr );
extern int              idxnum_next( int *n );
extern asdb_rc          idx_calc_target( array_t *irows,
                                  sqlite3_asdb_cursor *c,
                                  int idxNum, const char *idxStr,
                                  int argc, const char *argv[] );

extern asdb_plan   *plan_create();
extern void         plan_destroy( asdb_plan *plan );
extern asdb_rc      plan_update( asdb_plan *plan, array_t indexes, int icol );

extern asdb_rc      plans_init( array_t *plans );
extern void         plans_destroy( array_t *plans );
extern asdb_plan   *plans_get_plan( array_t *plans, int n );
extern int          plans_add_plan( array_t *plans, asdb_plan *plan );


DECLARE_ARRAY_TYPE( uint64_t );
DECLARE_ARRAY_TYPE( int );

}

TEST_GROUP(IndexTestGroup)
{
    void setup()
    {
    }

    void teardown()
    {
    }
};

TEST(IndexTestGroup, idxkey_cmp)
{
    asdb_value val0, val1, val2;    
    asdb_set_value_text( &val0, "nemui" );
    asdb_set_value_text( &val1, "nemukunai" );
    asdb_set_value_text( &val2, "nemukunai" );
    asdb_index_key key0 = idxkey_create( val0 );
    asdb_index_key key1 = idxkey_create( val1 );
    asdb_index_key key2 = idxkey_create( val2 );
    CHECK_TRUE( idxkey_cmp( key0, key1 ) != 0 );
    CHECK_TRUE( idxkey_cmp( key1, key2 ) == 0 );
}

TEST(IndexTestGroup, idxnode_operation)
{
    asdb_index index = idx_create(0);
    
    asdb_value       val[5];
    asdb_index_key   key[5];
    asdb_index_node *node[5];
    asdb_index_node *n;
    /*** init values ***/
    asdb_set_value_text( &val[0], "nemui" );
    asdb_set_value_text( &val[1], "nemukunai1" );
    asdb_set_value_text( &val[2], "nemukunai2" );
    asdb_set_value_text( &val[3], "nemukunai3" );
    asdb_set_value_text( &val[4], "nemukunai4" );
    /*** init keys ***/
    for ( int i = 0; i < 5; i++ ) {
       key[i] = idxkey_create( val[i] );
    }

    /*** null check ***/
    asdb_index_node *noden = idxnode_create( NULL );
    CHECK_EQUAL( 0, noden );
    /* create / append to ira */
    for ( int i = 0; i < 3; i++ ) {
        node[i]= idxnode_create( key[i] );
        CHECK_EQUAL( ASDB_OK, idxnode_append_to_ira( node[i], i ) );
    }
    /*** check ira ***/
    for ( int i = 0; i < 3; i++ ) {
        /* get ira -> check */
        array_t   ira = idxnode_get_ira( node[i] );
        uint64_t *raw = array_uint64_t( ira );
        CHECK_EQUAL( 1, array_size( ira ) );
        CHECK_EQUAL( (uint64_t)i, raw[0] );
    }
    
    /*** null search ***/
    n = idx_search_by_node( &index, node[0] );
    CHECK_EQUAL( 0, n );
    n = idx_search_by_key( &index, key[0] );
    CHECK_EQUAL( 0, n );

    /*** add nodes ***/
    for ( int i = 0; i < 3; i++ ) {
        idx_add_node( &index, node[i] );
    }
    
    /*** append new irow ***/
    for ( int i = 0; i < 3; i++ ) {
        /* append 10 on each irow array */
        CHECK_EQUAL( ASDB_OK, idx_append_newirow( &index, key[i], 10 ) );
        /* because 9 < 10, this addition must fail. */
        CHECK_EQUAL( ASDB_ERROR, idx_append_newirow( &index, key[i],  9 ) );
    }
    /*** add new key using idx_append_newirow ***/
    CHECK_EQUAL( ASDB_OK, idx_append_newirow( &index, key[3],  3 ) );
    CHECK_EQUAL( ASDB_OK, idx_append_newirow( &index, key[3], 10 ) );
    CHECK_EQUAL( ASDB_ERROR, idx_append_newirow( &index, key[3],  9 ) );

    /*** check ira contents ***/
    for ( uint64_t i = 0; i < 4; i++ ) {
        asdb_index_node *n = idx_search_by_key( &index, key[i] );
        array_t   ira = idxnode_get_ira( n );
        uint64_t *row = array_uint64_t( ira );
        CHECK_EQUAL( 2, array_size( ira ) );
        CHECK_EQUAL( i, row[0] );
        CHECK_EQUAL( 10, row[1] );
    }

    /*** search nodes ***/
    for ( int i = 0; i < 3; i++ ) {
        n = NULL;
        n = idx_search_by_node( &index, node[i] );
        /* printf( "node: %s == %s\n", n->key, node[i]->key ); */
        CHECK_EQUAL( node[i], n );
        n = NULL;
        n = idx_search_by_key( &index, key[i] );
        /* printf( "key:  %s == %s\n", n->key, key[i] ); */
        CHECK_EQUAL( node[i], n );
    }

    /*** delete nodes ***/
    CHECK_EQUAL( ASDB_OK, idx_delete_node( &index, node[0] ) );
    n = idx_search_by_node( &index, node[0] );
    CHECK_EQUAL( 0, n );
}

TEST(IndexTestGroup, idxes_operation)
{
    array_t indexes = array_init( asdb_index, 1 );
    asdb_index idx[3];
    for ( int i = 0; i < 3; i++ ) {
        idx[i] = idx_create( i );
    }

    for ( int i = 0; i < 3; i++ ) {
        CHECK_FALSE( idxes_is_exist( indexes, i ) );
        CHECK_EQUAL( ASDB_OK, idxes_add_index( &indexes, idx[i] ) );
        CHECK_TRUE( idxes_is_exist( indexes, i ) );
    }

    asdb_index pidx;
    /* pidx = idxes_get_index( indexes, 1 ); */
    /* CHK_CTX_BOOL( pidx == asdb_index_null() ); */
    for ( int i = 0; i < 3; i++ ) {
        pidx = idxes_get_index( indexes, i );
        /* CHK_CTX_BOOL( pidx == idx[i] ); */
        CHECK_EQUAL( idx[i].icolumn, pidx.icolumn );
    }
}

TEST(IndexTestGroup, irow_intersection)
{
    const int N = 1000;
    array_t a = array_init( uint64_t, 1 );
    array_t b = array_init( uint64_t, 1 );

    for ( int i = 0; i < N; i++ ) {
        array_uint64_t_append( &a, (uint64_t)( i ) );
        array_uint64_t_append( &b, (uint64_t)( i * 2 ) );
    }
    array_t c = irow_intersect( a, b );
    uint64_t *rawc = array_uint64_t( c );
    CHECK_EQUAL( (uint64_t)N/2, array_size(c) );
    for ( int i = 0; i < N/2; i++ ) {
        CHECK_EQUAL( (uint64_t)i*2, rawc[i] );
    }
}

TEST(IndexTestGroup, idx_calc)
{
    int32_t n = 0x55555555;     /* 0b0101010101010101 */
    for ( int i = 0; i < (int)sizeof(n)*8; i++ ) {
        int ret = idxnum_next( &n );
        if ( ret == -1 ) {
            CHECK_EQUAL( (int)(sizeof(n)*8/2), i );
            break;
        }
        CHECK_EQUAL( i*2, ret );
    }

    const char *dummy;
    sqlite3_asdb_vtab   *v = mk_sqlite3_asdb_vtab( 0, &dummy );
    sqlite3_asdb_cursor *c = mk_sqlite3_asdb_cursor( v );
    c->pVtab = v;
    
    int argc = 0;
    const char *argv[10];
    int idxNum = 0x0;
    const char *idxStr = "";
    array_t ira;

    c->plan = *plan_create();
    c->idxes_preparing = array_init( asdb_index, 1 );
    
    /*** null test ***/
    CHECK_EQUAL( ASDB_OK, idx_calc_target( &ira, c, idxNum, idxStr, argc, argv ) );
    /* check ira */
    /* array_destroy( &ira ); */

    /*** Prepare some dataset ***/
    /* index construction */
    const int N = 3;
    for ( int p = 0; p < 3; p++ ) {
        /* create target idx */
        asdb_index idx = idx_create( p );
        /* create answer data */
        asdb_index_node *node[N];
        asdb_value       val[N];
        asdb_index_key   key[N];
        for ( int n = 0; n < N; n++ ) {
            char str[10];
            sprintf( str, "%d-%d", p, n );
            asdb_set_value_text( &val[n], str );
            key[n] = idxkey_create( val[n] );
            node[n] = idxnode_create( key[n] );
            for ( uint64_t i = 0; i < 10; i++ )
                array_uint64_t_append( &(node[n]->ira), (i*(p+1) + n*10) );
            idx_add_node( &idx, node[n] );
        }
        idxes_add_index( &v->indexes, idx );
    }
    
    /*** Test1 - use no intersection ***/
    /* init auguments */
    c->plan = *plan_create();
    plan_update( &c->plan, v->indexes, 0 );
    argc = 1;
    ira = array_null();
    /* init test set */
    argv[0] = "0-0";
    uint64_t ans1[10] = {0,1,2,3,4,5,6,7,8,9};
    /* do test */
    CHECK_EQUAL( ASDB_OK, idx_calc_target( &ira, c, idxNum, idxStr, argc, argv ) );
    CHECK_EQUAL( 10, array_size( ira ) );
    {
        FOREACH( uint64_t, ira, x ) {
            static int i = 0;
            CHECK_EQUAL( ans1[i++], x );
        }
    }
    /*** Test2 - full intersection ***/
    /* init auguments */
    c->plan = *plan_create();
    plan_update( &c->plan, v->indexes, 0 );
    plan_update( &c->plan, v->indexes, 1 );
    plan_update( &c->plan, v->indexes, 2 );
    argc = 3;
    ira = array_null();
    /* init test set */
    argv[0] = "0-0";
    argv[1] = "1-0";
    argv[2] = "2-0";
    uint64_t ans2[] = {0, 6};
    /* do test */
    CHECK_EQUAL( ASDB_OK, idx_calc_target( &ira, c, idxNum, idxStr, argc, argv ) );
    CHECK_EQUAL( 2, array_size( ira ) );
    {
        FOREACH( uint64_t, ira, x ) {
            static int i = 0;
            CHECK_EQUAL( ans2[i++], x );
        }
    }
    array_destroy( &ira );
    /*** Test3 - not sequential idxNum ***/
    /* init auguments */
    argc = 2;
    c->plan = *plan_create();
    plan_update( &c->plan, v->indexes, 0 );
    plan_update( &c->plan, v->indexes, 2 );
    ira = array_null();
    /* init test set */
    argv[0] = "0-1";
    argv[1] = "2-0";
    uint64_t ans3[] = {12, 15, 18};
    /* do test */
    CHECK_EQUAL( ASDB_OK, idx_calc_target( &ira, c, idxNum, idxStr, argc, argv ) );
    CHECK_EQUAL( 3, array_size( ira ) );
    {
        FOREACH( uint64_t, ira, x ) {
            static int i = 0;
            CHECK_EQUAL( ans3[i++], x );
        }
    }

}

TEST(IndexTestGroup, plan)
{
    array_t indexes = array_init( asdb_index, 1 );
    array_t plans;
    plans_init( &plans );

    int idxnum[3];
    for ( int i = 0; i < 3; i++ ) {
        asdb_index idx = idx_create( i );
        CHECK_EQUAL( ASDB_OK, idxes_add_index( &indexes, idx ) );
    }
    for ( int j = 0; j < 3; j++ ) {
        asdb_plan *plan = plan_create();
        for ( int i = 0; i < 3; i++ ) {
            CHECK_EQUAL( ASDB_OK, plan_update( plan, indexes, i+j ) );
        }
        idxnum[j] = plans_add_plan( &plans, plan );
    }

    for ( int j = 0; j < 3; j++ ) {
        asdb_plan *plan = plans_get_plan( &plans, idxnum[j] );
        CHECK_EQUAL( (uint64_t)(3-j), array_size( plan->idx_icols ) );
        CHECK_EQUAL( (uint64_t)j, array_size( plan->prep_icols ) );
        int *idx  = array_int( plan->idx_icols );
        int *prep = array_int( plan->prep_icols );
        for ( int i = 0; i < 3; i++ ) {
            if ( i+j < 3 ) {
                CHECK_EQUAL( i+j, idx[i] );
            } else {
                CHECK_EQUAL( i+j, prep[i+j-3] );
            }
        }
    }
}
