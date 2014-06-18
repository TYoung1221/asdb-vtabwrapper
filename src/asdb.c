//! gcc -o asdb.so asdb.c -W -Wall -O0 -g -std=gnu99 -shared -fPIC -I/usr/include/glib-2.0 -I/usr/lib/x86_64-linux-gnu/glib-2.0/include  -lglib-2.0 -lsqlite3 -fsyntax-only -Wno-unused-parameter -Wno-unused-variable -I/home/amiq/local/include

/**
 * @file   asdb.c
 * @author Makoto Shimazu <makoto.shimaz@gmail.com>
 * @date   Thu Sep 26 20:25:50 2013
 * 
 * @brief  Any Source DB - This module is a simplest wrapper of Virtual Table
 * 
 * 
 */


#if !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_ASDB)
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <glib-2.0/glib.h>

#include "ASDBConfig.h"

#include "array.h"

#include "asdb.h"
#include "asdb_data.h"

#ifdef __TEST__
#  define STATIC
#else
#  define STATIC static
#endif /* Def: __TEST__ */

#ifndef __TEST__
SQLITE_EXTENSION_INIT1
#endif /* Not def: __TEST__ */

#define ASDB_MAGIC_VTAB 918729
#define ASDB_MAGIC_CURSOR 9180729

/* #define ASDB_LINEBUF_SIZE 1024 */

/* const values */
static const int ASDB_DBG = 0;
static const uint64_t ASDB_DEFAULT_HASHSIZE = 65536;


/***** Public Functions *****/
/** 
 * columnオブジェクトにint64型の値をセットする
 * 
 * @param col 対象のオブジェクト
 * @param i 値
 * 
 * @return ASDB_NOMEM / ASDB_OK
 */
asdb_rc asdb_set_value_int64( asdb_value *col, sqlite3_int64 i )
{
    if ( col == NULL ) return ASDB_ERROR;
    /* malloc */
    sqlite3_int64 *tmp =
        ( sqlite3_int64 * ) sqlite3_malloc( sizeof( sqlite3_int64 ) );
    if ( tmp == NULL ) {
        if ( ASDB_DBG >= 2 ) fprintf( stderr, "-> ASDB_NOMEM\n" );
        return ASDB_NOMEM;
    }
    /* set value */
    *tmp = i;
    col->value = tmp;
    col->type  = ASDB_TYPE_INT64;
    return ASDB_OK;
}

/** 
 * columnオブジェクトにtextをセットする
 * 
 * @param col 
 * @param str 文字列
 * 
 * @return ASDB_NOMEM / ASDB_OK
 */
asdb_rc asdb_set_value_text( asdb_value *col, const char * str )
{
    if ( col == NULL ) return ASDB_ERROR;
    /* malloc */
    char *tmp = sqlite3_mprintf( "%s", str );
    if ( tmp == NULL ) {
        if ( ASDB_DBG >= 2 ) fprintf( stderr, "-> ASDB_NOMEM\n" );
        return ASDB_NOMEM;
    }
    /* set value */
    col->value = tmp;
    col->type  = ASDB_TYPE_TEXT;
    return ASDB_OK;
}

/** 
 * columnオブジェクトにnullをセットする
 * 
 * @param col 対象
 * 
 * @return ASDB_OK
 */
asdb_rc asdb_set_value_null( asdb_value *col )
{
    if ( col == NULL ) return ASDB_ERROR;
    /* set value */
    col->value = NULL;
    col->type  = ASDB_TYPE_NULL;
    return ASDB_OK;
}

/** 
 * スキーマオブジェクトに対してカラム名を指定し、そのカラム番号を取得する
 * 
 * @param schema スキーマ
 * @param cname カラム名
 * 
 * @return カラム番号(見つからなかったら-1)
 */
int asdb_get_column_no( const asdb_schema *schema, const char *cname )
{
    int id;
    for ( id = 0; id < ASDB_MAX_NCOLUMN; id++ ) {
        if ( schema->cname[id] == NULL ) {
            id = ASDB_MAX_NCOLUMN;
            break;
        }
        if ( strcmp( schema->cname[id], cname ) == 0 ) break;
    }
    return ( id == ASDB_MAX_NCOLUMN ) ? -1 : id;
}


/***** Private Functions *****/

#define DEFAULT_ARRSIZE 1024

DECLARE_ARRAY_TYPE(uint64_t);
DECLARE_ARRAY_TYPE(int);
DECLARE_ARRAY_TYPE(asdb_pm);
DECLARE_ARRAY_TYPE(asdb_index);
DECLARE_ARRAY_TYPE(asdb_index_node);
DECLARE_ARRAY_TYPE(asdb_cell);
DECLARE_ARRAY_TYPE(asdb_plan);
/*** asdb_value ***/
static void asdb_free_column( asdb_value *column, int ncol )
{
    if ( column == NULL ) return;
    for ( int i = 0; i < ncol; i++ ) {
        if ( column[i].value != NULL ) sqlite3_free( column[i].value );
    }
    sqlite3_free( column );
}
static void asdb_value_attach_to_ctx( sqlite3_context *ctx, asdb_value val )
{
    switch( val.type ) {
    case ASDB_TYPE_NULL:
        sqlite3_result_null( ctx );
        break;
    case ASDB_TYPE_INT64:
        sqlite3_result_int64( ctx, *(sqlite3_int64*)val.value );
        break;
    case ASDB_TYPE_TEXT:
        sqlite3_result_text( ctx, (char*)val.value,
                             strlen( (char*)val.value ), NULL );
        break;
    default:
        sqlite3_result_null( ctx );
        break;
    }
}

/*** positional map ***/
static void pm_print( array_t pm )
{
    {
        FOREACH( asdb_pm, pm, x ) {
            printf( "recordID: %"PRIu64"\n", x.rid );
        }
    }
}
static asdb_rc pm_init( asdb_pm *pm, uint64_t rid, asdb_row *row )
{
    array_t tmp = array_init( asdb_cell, 1 );
    if ( array_isnull( tmp ) ) {
        return ASDB_NOMEM;
    }
    pm->rid  = rid;
    pm->row  = row;
    pm->cell = tmp;
    return ASDB_OK;
}

static asdb_rc asdb_next_with_index( sqlite3_asdb_cursor *c )
{
    sqlite3_asdb_vtab *v = c->pVtab;
    asdb_cursor *usr_c = c->cursor;
    /* if no column is existed, return immediately */
    if ( c->nowmap_pos >= array_size(c->ira) ) {
        c->status = ASDB_EOF;
        return ASDB_EOF;
    } else {
        /* set first record */
        uint64_t *ira = array_uint64_t( c->ira );
        uint64_t n = ira[ c->nowmap_pos ];
        asdb_pm *pma = array_asdb_pm( v->pm );
        c->nowmap    = pma[n];
        usr_c->rowid = pma[n].rid;
        c->status = ASDB_OK;
        return ASDB_OK;
    }
}
static asdb_rc asdb_next_with_pm( sqlite3_asdb_cursor *c )
{
    sqlite3_asdb_vtab *v = c->pVtab;
    asdb_cursor *usr_c = c->cursor;
    /* use positional map */
    /* size check */
    if ( c->nowmap_pos >= array_size( v->pm ) ) {
        c->status = ASDB_EOF;
        return ASDB_EOF;
    } else {
        asdb_pm *pma = array_asdb_pm( v->pm );
        uint64_t n   = c->nowmap_pos;
        c->nowmap    = pma[n];
        usr_c->rowid = pma[n].rid;
        return ASDB_OK;
    }
}
static asdb_rc asdb_next_with_pm_construction( sqlite3_asdb_cursor *c )
{
    sqlite3_asdb_vtab *v = (sqlite3_asdb_vtab *)c->pVtab;
    asdb_cursor *usr_c = c->cursor;
    asdb_row *r;
    uint64_t rid = c->nowmap_pos;
    /* set next rid */
    usr_c->rowid = rid;
    /* get next row object */
    r = asdb_next( usr_c );
    if ( r == NULL ) {
        c->status = ASDB_EOF;
        return ASDB_EOF;
    }
    /* set row obj to positonal map */
    asdb_pm pm;
    asdb_rc pmrc = pm_init( &pm, rid, r );
    if ( pmrc != ASDB_OK ) {
        c->status = ASDB_NOMEM;
        return ASDB_NOMEM;
    }
    int rc = array_asdb_pm_append( &v->pm, pm );
    if ( rc ) {
        array_destroy( &pm.cell );
        c->status = ASDB_NOMEM;
        return ASDB_NOMEM;
    }
    /* set new line position to cursor */
    c->nowmap = pm;
    c->status = ASDB_OK;
    return ASDB_OK;
}
static asdb_rc asdb_next_without_pm( sqlite3_asdb_cursor *c )
{
    sqlite3_asdb_vtab *v = (sqlite3_asdb_vtab *)c->pVtab;
    asdb_cursor *usr_c = c->cursor;
    asdb_row *r;
    uint64_t rid = c->nowmap_pos;
    /* set next rid */
    usr_c->rowid = rid;
    /* get next row object */
    r = asdb_next( usr_c );
    if ( r == NULL ) {
        c->status = ASDB_EOF;
        return ASDB_EOF;
    }
    /* set new line position to cursor */
    asdb_pm pm;
    asdb_rc pmrc = pm_init( &pm, rid, r );
    if ( pmrc != ASDB_OK ) {
        c->status = ASDB_NOMEM;
        return ASDB_NOMEM;
    }
    c->nowmap = pm;
    c->status = ASDB_OK;
    return ASDB_OK;
}

/*** index key ***/
/* static DECLARE_ARRAY_TYPE_APPEND(uint64_t); */
/* static DECLARE_ARRAY_TYPE_APPEND(asdb_pm); */

/*** index ***/
/* key */
/** 
 * columnに対応したkeyを作成
 * 
 * @param col keyに変換する対象
 * 
 * @return key
 */
STATIC asdb_index_key idxkey_create( asdb_value col )
{
    switch ( col.type ) {
    case ASDB_TYPE_TEXT:
        return sqlite3_mprintf( "%s", (char *)col.value );
    case ASDB_TYPE_INT64:
        return sqlite3_mprintf( "%d", *(sqlite3_int64*)col.value );
    default:
        if ( ASDB_DBG >= 2 ) fprintf( stderr, "unknown key type\n" );
    }
    return NULL;
}
STATIC asdb_index_key idxkey_create_from_value( sqlite3_value *val )
{
    switch ( sqlite3_value_type( val ) ) {
    case SQLITE_INTEGER:
        return sqlite3_mprintf( "%d", sqlite3_value_int( val ) );
    case SQLITE_TEXT:
        return (char *) sqlite3_value_text( val );
    default:
        return NULL;
    }
}
STATIC asdb_index_key idxkey_create_from_text( const char *key )
{
    return sqlite3_mprintf( "%s", key );
}
STATIC void idxkey_destroy( asdb_index_key key )
{
    sqlite3_free( key );
}
static void g_idxkey_destroy( gpointer key )
{
    idxkey_destroy( (asdb_index_key) key );
}
/** 
 * keyの比較
 * 
 * @param a 比較対象
 * @param b 比較対象
 * 
 * @return 一致していれば0
 */
STATIC int idxkey_cmp( const asdb_index_key a, const asdb_index_key b )
{
    return strcmp( a, b );
}

/*** node ***/
/** 
 * index_nodeの作成と初期化
 * 
 * @return NULL / pointer to new node
 */
STATIC asdb_index_node *idxnode_create( asdb_index_key key )
{
    if ( key == NULL ) return NULL;
    asdb_index_node *node =
        sqlite3_malloc( sizeof( asdb_index_node ) );
    if ( node == NULL ) return NULL;
    memset( node, 0, sizeof( *node ) );
    node->key = key;
    node->ira = array_init( uint64_t, 1 );
    return node;
}
STATIC void idxnode_destroy( asdb_index_node *node )
{
    sqlite3_free( node );
}
static void g_idxnode_destroy( gpointer node )
{
    idxnode_destroy( node );
}
STATIC asdb_rc idxnode_append_to_ira( asdb_index_node *node, uint64_t irow )
{
    if ( array_uint64_t_append( &node->ira, irow ) )
        return ASDB_NOMEM;
    else
        return ASDB_OK;
}
STATIC array_t idxnode_get_ira( asdb_index_node *node )
{
    return node->ira;
    /* return array_null(); */
}

/*** index ***/
STATIC asdb_index idx_create( int icol )
{
    /* return (asdb_index) { .icolumn = -1 }; */
    GHashTable *table = g_hash_table_new_full( g_str_hash, g_str_equal,
                                               g_idxkey_destroy, g_idxnode_destroy );
    if ( table == NULL )
        return (asdb_index) { .icolumn = -1, .table = NULL };
    else
        return (asdb_index) { .icolumn = icol, .table = table };
}
/** 
 * asdb_indexから、keyに対応したasdb_index_node(irow_array, iraを保持)を拾ってくる
 * 
 * @param index 探してくる対象のindex
 * @param key キー
 * 
 * @return node
 */
STATIC asdb_index_node *idx_search_by_key( asdb_index *index, const asdb_index_key key )
{
    return g_hash_table_lookup( index->table, key );
}

STATIC asdb_index_node *idx_search_by_node( asdb_index *index, const asdb_index_node *node )
{
    return g_hash_table_lookup( index->table, node->key );
}
STATIC asdb_rc          idx_add_node( asdb_index *index, asdb_index_node *node )
{
    g_hash_table_insert( index->table, node->key, node );
    return ASDB_OK;
}
STATIC asdb_rc          idx_delete_node( asdb_index *index, const asdb_index_node *node )
{
    g_hash_table_remove( index->table, node->key );
    return ASDB_OK;
}
/** 
 * indexのうち、指定されたkeyのirow arrayの最後にirowを追加する
 * ただし、irow arrayの要素全てはirow以下であり、ソートされていると考える。
 * 指定されたkeyのnodeが存在しない場合は追加する。
 * @param index 
 * @param key 
 * @param irow 
 * 
 * @return 
 */
STATIC asdb_rc
idx_append_newirow( asdb_index *index, const asdb_index_key key, uint64_t irow )
{
    asdb_index_node *node = idx_search_by_key( index, key );
    if ( node == NULL ) {
        node = idxnode_create( key );
        if ( idx_add_node( index, node ) != ASDB_OK )
            return ASDB_NOMEM;
    }
    array_t  *ira   = &node->ira;
    uint64_t *array = array_uint64_t( *ira );
    uint64_t  pos   = array_size( *ira );
    if ( pos == 0 ) {
        array_uint64_t_append( ira, irow );
        return ASDB_OK;
    }
    
    uint64_t  last  = array[pos-1];
    if ( last > irow ) {
        fprintf( stderr, "idx_append_newirow: unconsidered use!\n" );
        return ASDB_ERROR;
    } else if ( last < irow ) {
        array_uint64_t_append( ira, irow );
    }
    return ASDB_OK;
}


/*** index array ***/
/** 
 * icolに対応するインデックスが存在するならtrue
 * 
 * @param indexes 
 * @param icol 行番号(asdb_schemaのcolに対応)
 * 
 * @return true / false
 */
STATIC bool idxes_is_exist( array_t indexes, int icol )
{
    bool is_exist = false;
    {
        FOREACH( asdb_index, indexes, idx ) {
            if ( idx.icolumn == icol ) {
                is_exist = true;
                break;
            }
        }
    }
    return is_exist;
}
/** 
 * asdb_indexの配列から、icolに対応したものを拾ってくる
 * 
 * @param indexes 
 * @param icol 
 * 
 * @return asdb_index
 */
STATIC asdb_index idxes_get_index( array_t indexes, int icol )
{
    asdb_index ans = { .icolumn = -1 };

    {
        FOREACH( asdb_index, indexes, x ) {
            if ( x.icolumn == icol ) {
                ans = x;
                break;
            }
        }
    }
    
    return ans;
}

STATIC asdb_rc idxes_add_index( array_t *indexes, asdb_index idx )
{
    if ( array_asdb_index_append( indexes, idx ) )
        return ASDB_NOMEM;
    else
        return ASDB_OK;
}

/*** asdb_plan ***/
STATIC asdb_plan *plan_create( void )
{
    asdb_plan *plan = malloc( sizeof( struct _asdb_plan ) );
    if ( plan == NULL ) return NULL;
    plan->idx_icols  = array_init( int, 1 );
    plan->prep_icols = array_init( int, 1 );
    return plan;
}
STATIC void       plan_destroy( asdb_plan *plan )
{
    array_destroy( &plan->idx_icols );
    array_destroy( &plan->prep_icols );
    free( plan );
}
/** 
 * 指定されたicolがindexesに含まれているかどうか判定し、planに追加する
 * 
 * @param plan 今計画中のplan
 * @param indexes 現状利用できるindex群
 * @param icol 指定したいicolumn
 * 
 * @return 0: indexをprepにした 1: indexが利用可 -1:エラー
 */
STATIC int        plan_update( asdb_plan *plan, array_t indexes, int icol )
{
    array_t *array;
    int rc;
    if ( !idxes_is_exist( indexes, icol ) ) {
        array = &(plan->prep_icols);
        rc = 0;
    } else {
        array = &(plan->idx_icols);
        rc = 1;
    }
    if ( array_int_append( array, icol ) != ASDB_OK ) {
        return -1;
    } else {
        return rc;
    }
}
static void plan_print( asdb_plan *plan )
{
    fprintf( stderr,
             "# PLAN: idx  = %2"PRIu64"\n"
             "#       prep = %2"PRIu64"\n",
             array_size(plan->idx_icols),
             array_size(plan->prep_icols));
}

STATIC asdb_rc    plans_init( array_t *plans )
{
    *plans = array_init( asdb_plan, 1 );
    return ASDB_OK;
}
STATIC void       plans_destroy( array_t *plans )
{
    array_destroy( plans );
}
STATIC int        plans_add_plan( array_t *plans, asdb_plan *plan )
{
    int n = array_size( *plans );
    if ( array_asdb_plan_append( plans, *plan ) != ASDB_OK )
        return -1;
    else
        return n;
}
STATIC asdb_plan *plans_get_plan( array_t *plans, int n )
{
    if ( array_size( *plans ) <= (uint64_t)n ) return NULL;
    asdb_plan *p = array_asdb_plan( *plans );
    return &p[n];
}

/** 
 * calculate intersection of two uint64_t arrays;
 * 
 * @param a_arr sorted uint64_t array
 * @param b_arr sorted uint64_t array
 * 
 * @return new array
 */
STATIC array_t irow_intersect( array_t a_arr, array_t b_arr )
{
    uint64_t resultsize;
    array_t result;

    /* init */
    /* l_ means larger_, s_ means smaller_ */
    array_t s_arr;
    uint64_t *l_data;
    uint64_t  l_pos = 0;
    uint64_t  l_max;
    if ( array_size(a_arr) < array_size(b_arr) ) {
        s_arr   = a_arr;
        l_data  = array_uint64_t( b_arr );
        l_max   = array_size( b_arr );
        result  = array_init( uint64_t, array_size(a_arr) );
    } else {
        s_arr   = b_arr;
        l_data  = array_uint64_t( a_arr );
        l_max   = array_size( a_arr );
        result  = array_init( uint64_t, array_size(b_arr) );
    }
    /* search */
    {
        FOREACH( uint64_t, s_arr, x ) {
            while ( l_pos < l_max && x > l_data[l_pos] ) l_pos++;
            if ( l_pos >= l_max ) break;
            if ( x == l_data[l_pos] )
                array_uint64_t_append( &result, x );
        }
    }
    
    return result;
}


/** 
 * LSBな1を0に変えつつ、何ビット目か返す
 * 
 * @param n 元のデータ
 * 
 * @return LSBな1の場所
 */
STATIC int idxnum_next( int *n )
{
    /* return xxxx; */
    for ( int i = 0; i < 32; i++ ) {
        if ( *n & (1 << i) ) {
            *n &= ~(1 << i);
            return i;
        }
    }
    return -1;
}



/** 
 * argvの値とidxNumのcolumn-idを使って必要なirow arrayを取得する
 * 
 * @param irows 返り値となるirow array
 * @param c 
 * @param idxNum argvに対応するicolumnが1になっている数値
 * @param idxStr 
 * @param argc 
 * @param argv キーになるvalue (iColumnの小さい順)
 * 
 * @return 
 */
STATIC asdb_rc idx_calc_target( array_t *irows,
                                sqlite3_asdb_cursor *c,
                                int idxNum, const char *idxStr,
                                int argc, const char *argv[] )
{
    sqlite3_asdb_vtab *v       = c->pVtab;

    /*** Range check ***/
    if ( argc < 1 ) {
        *irows = array_null();
        return ASDB_OK;
    }

    /*** Prepare plan ***/
    asdb_plan *plan    = &c->plan;
    array_t   *icols_a = &plan->idx_icols;
    int       *icols   = array_int( *icols_a );
    assert( array_size( *icols_a ) == (uint64_t) argc );

    /*** Prepare the first irow array ***/
    /* get target index */
    int icol = icols[0];
    if ( icol < 0 ) {
        *irows = array_null();
        return ASDB_ERROR;
    }
    asdb_index idx = idxes_get_index( v->indexes, icol );
    /* create target key */
    asdb_index_key key = idxkey_create_from_text( argv[0] );
    /* search */
    asdb_index_node *node = idx_search_by_key( &idx, key );
    array_t irows_;
    if ( node == NULL ) {
        irows_ = array_init( uint64_t, 1 );
    } else {
        /* extract the first array */
        irows_ = idxnode_get_ira( node ); /* first irow array */
    }

    /*** Iterative search ***/
    for ( int i = 1; i < argc; i++ ) {
        icol = icols[i];
        key = idxkey_create_from_text( argv[i] );
        idx = idxes_get_index( v->indexes, icol );
        node = idx_search_by_key( &idx, key );
        array_t irows_tmp = idxnode_get_ira( node );
        array_t irows__ = irow_intersect( irows_, irows_tmp );
        array_destroy( &irows_ );
        irows_ = irows__;
    }
    *irows = irows_;
    
    return ASDB_OK;
}

/*** vtab ***/
STATIC sqlite3_asdb_vtab * 
mk_sqlite3_asdb_vtab( int argc, const char*const* argv ) {
    /* create vtab */
    sqlite3_asdb_vtab * vtab =
        sqlite3_malloc(sizeof(sqlite3_asdb_vtab));
    if (vtab == NULL) return NULL;
    memset(vtab, 0, sizeof(*vtab));

    /* init variables */
    vtab->magic    = ASDB_MAGIC_VTAB;
    vtab->indexes  = array_init( asdb_index, DEFAULT_ARRSIZE );
    if ( array_isnull( vtab->indexes ) ) return NULL;
    vtab->pm       = array_init( asdb_pm, DEFAULT_ARRSIZE );
    if ( array_isnull( vtab->pm ) ) return NULL;
    vtab->pm_state = UNUSABLE;
    vtab->plans    = array_init( asdb_plan, 1 );
    if ( array_isnull( vtab->plans ) ) return NULL;
    
    /* create user defined vtab */
    asdb_vtab *asdb_vtab = asdb_create_vtab( argc, argv );
    if ( asdb_vtab == NULL ) {
        sqlite3_free( vtab );
        return NULL;
    }
    
    /* register user vtab */
    vtab->vtab = asdb_vtab;
    return vtab;
}
static void del_sqlite3_asdb_vtab(sqlite3_asdb_vtab * vtab)
{
    if ( asdb_destroy_vtab( vtab->vtab ) ) {
        /* error */
        if ( ASDB_DBG >= 1 ) fprintf( stderr, "DEL_ASDB_VTAB: FAILURE\n" );
    }
    array_destroy( &vtab->pm );
    array_destroy( &vtab->indexes );
    sqlite3_free( vtab );
}

/*** cursor ***/
STATIC sqlite3_asdb_cursor *mk_sqlite3_asdb_cursor(sqlite3_asdb_vtab *vtab)
{
    /* create cursor */
    sqlite3_asdb_cursor * c = sqlite3_malloc(sizeof(sqlite3_asdb_cursor));
    if ( c == NULL ) return NULL;
    if (ASDB_DBG>=2) {
        fprintf(stderr,"asdbOpen: magic = %d\n",
               vtab->magic);
    }
    
    /* init variables */
    c->magic           = ASDB_MAGIC_CURSOR;
    c->status          = ASDB_OK;
    c->idxes_preparing = array_null();

    /* create user defined cursor */
    asdb_cursor *usr_c = asdb_open( vtab->vtab );
    if ( usr_c == NULL ) {
        sqlite3_free( c );
        return NULL;
    }
    /* init members */
    usr_c->vtab   = ( asdb_vtab * ) vtab->vtab;
    usr_c->rowid  = 0;

    /* register user cursor */
    c->cursor = usr_c;
    
    return c;
}
static void del_asdb_cursor(sqlite3_asdb_cursor * c)
{
    sqlite3_asdb_vtab *vtab = (sqlite3_asdb_vtab *)c->pVtab;
    if ( asdb_close( c->cursor ) ) {
        /* error */
        if ( ASDB_DBG >= 1 ) fprintf( stderr, "DEL_ASDB_CURSOR: FAILURE\n" );
    }
    sqlite3_free( c );
}
static asdb_rc asdb_cursor_prepare_index( sqlite3_asdb_cursor *c )
{
    asdb_plan *plan = &c->plan;
    array_destroy( &c->idxes_preparing );
    c->idxes_preparing = array_init( asdb_index, 1 );
    {
        FOREACH( int, plan->prep_icols, icol ) {
            asdb_index idx = idx_create( icol );
            if ( idxes_add_index( &c->idxes_preparing, idx ) != ASDB_OK )
                return ASDB_NOMEM;
        }
    }
    return ASDB_OK;
}
static asdb_rc asdb_cursor_set_target( sqlite3_asdb_cursor *c,
                                       int idxNum, const char *idxStr,
                                       int argc, sqlite3_value **argv
    )
{
    sqlite3_asdb_vtab   * v = (sqlite3_asdb_vtab   *)c->pVtab;
    switch( v->pm_state ) {
    case USABLE:
        /* do constraints exist? */
        /* this implementation is a little complicatied for speedup */
        /* to simplify, irows' initializer has to be an array of 0..pm.size, */
        /*    and iterator i has to be initialized by 0 */
        if ( argc != 0 ) {
            array_t irows;
            const char *cargv[ argc ];
            /* get target icolumn */
            for ( int i = 0; i < argc; i++ ) {
                cargv[i] = (const char *)sqlite3_value_text( argv[i] );
            } 
            asdb_rc rc = idx_calc_target( &irows, c, idxNum, idxStr, argc, cargv );
            c->ira        = irows;
            c->ira_usable = true;
            /* for debug print */
            if ( ASDB_DBG >= 1 ) 
                fprintf( stderr, "##### table: %s irows= %"PRIu64"\n",
                        v->vtab->schema->tname,
                        array_size(irows) );

        } else {
            /* use nowmap_pos to point pm[ nowmap_pos ] */
            c->ira        = array_null();
            c->ira_usable = false;
        }
        break;
    case CONSTRUCTING:
        c->ira = array_null();
        c->ira_usable = false;
        c->pm_state   = UNUSABLE; /* because another cursor is creating pm */
        break;
    case UNUSABLE:
        c->ira = array_null();
        c->ira_usable = false;
        c->pm_state   = CONSTRUCTING;
        v->pm_state   = CONSTRUCTING;
        break;
    }
    return ASDB_OK;
}

static asdb_rc asdb_cursor_prepare_for_nextrow( sqlite3_asdb_cursor *c )
{
    asdb_rc rc;
    /* does index exist? */
    if ( !array_isnull( c->ira ) ) {
        rc = asdb_next_with_index( c );
    } else {
        /* get first line */
        switch ( c->pm_state ) {
        case USABLE:
            /* use pm */
            rc = asdb_next_with_pm( c );
            break;
        case CONSTRUCTING:
            /* construct pm */
            rc = asdb_next_with_pm_construction( c );
            break;
        case UNUSABLE:
            rc = asdb_next_without_pm( c );
            break;
        default:
            assert(0);
        }
    }
    return rc;
}

/*** utils ***/
static asdb_value *asdb_create_column( asdb_vtab *vtab )
{
    asdb_value *line =
        ( asdb_value * ) sqlite3_malloc( sizeof( asdb_value ) * vtab->ncol );
    memset( line, 0, sizeof( asdb_value ) * vtab->ncol );
    return line;
}
static char *columncat( char *a, const char *b )
{
    char *tmp = sqlite3_mprintf( "%s, %s", a, b );
    if ( tmp != NULL ) {
        sqlite3_free( a );
    }
    return tmp;
}
static void print_idxinfo( sqlite3_index_info *info )
{
    fprintf( stderr, " %d constraints\n", info->nConstraint );
    for ( int i = 0; i < info->nConstraint; i++ ) {
        fprintf( stderr,
                 "  %2d : iColumn = %2d, op = %#02x, usable = %d\n",
                 i,
                 info->aConstraint[i].iColumn,
                 info->aConstraint[i].op,
                 info->aConstraint[i].usable );
    }
    fprintf( stderr, " %d order by\n", info->nOrderBy);
    for ( int i = 0; i < info->nOrderBy; i++ ) {
        fprintf( stderr, "  %d : iColumn = %d, op = %d\n",
                 i, 
                 info->aOrderBy[i].iColumn,
                 info->aOrderBy[i].desc);
    }
}
static void print_pmstate( sqlite3_asdb_vtab *v )
{
    /* pm state */
    char *pmstate =
        (v->pm_state == UNUSABLE)     ? "unusable"     :
        (v->pm_state == CONSTRUCTING) ? "constructing" :
        (v->pm_state == USABLE)       ? "usable"       : "unknown";
    fprintf( stderr,
             " VTab index state:\n"
             "     pm_state: %s\n"
             "   # of elems: %"PRIu64"\n"
             "      alloced: %"PRIu64"\n",
             pmstate,
             array_size(v->pm),
             v->pm.alloced );
}

/*** Module Functions ***/

/* 
** This function is the implementation of both the xConnect and xCreate methods
**
**   argv[0]   -> module name
**   argv[1]   -> database name
**   argv[2]   -> table name
**   argv[...] -> column names...
*/
static int asdbCreateOrConnect(
    sqlite3 *db,                        /* Database connection */
    void *pAux,                         /* One of the RTREE_COORD_* constants */
    int argc, const char *const* argv,   /* Parameters to CREATE TABLE statement */
    sqlite3_vtab **ppVtab,              /* OUT: New virtual table */
    char **pzErr,                       /* OUT: Error message, if any */
    int isCreate                        /* True for xCreate, false for xConnect */
    )
{
    /* debug message */
    if (ASDB_DBG>=2) {
        int i;
        fprintf(stderr,"asdbCreateOrConnect\n");
        for (i = 0; i < argc; i++) fprintf(stderr," %s", argv[i]);
        fprintf(stderr,"\n");
    }
    /* create vtab structure */
    sqlite3_asdb_vtab * vtab = mk_sqlite3_asdb_vtab( argc, argv );
    if (vtab == NULL) {
        if (ASDB_DBG>=2) fprintf(stderr,"-> SQLITE_NOMEM\n");
        return SQLITE_NOMEM;
    }

    /* declare table schema */
    char *column=NULL;
    const asdb_schema *schema = vtab->vtab->schema;
    /* first column */
    if ( schema->cname[0] != NULL ) {
        column = sqlite3_mprintf("%s", schema->cname[0]);
        if ( column == NULL ) {
            if ( ASDB_DBG >= 2 ) fprintf(stderr, "->SQLITE_NOMEM\n");
            return SQLITE_NOMEM;
        }
    }
    /* cat each cname */
    int i;
    for ( i = 1; i < ASDB_MAX_NCOLUMN; i++ ) {
        if ( schema->cname[i] == NULL ) break;
        column = columncat( column, schema->cname[i] );
    }
    fprintf(stderr, "%s\n", column);
    /* set ncol */
    vtab->vtab->ncol = i;
    /* create query */
    char *query = sqlite3_mprintf( "create table whatever(%s)", column );
    if ( query == NULL ) {
        if ( ASDB_DBG >= 2 ) fprintf(stderr, "->SQLITE_NOMEM\n");
        return SQLITE_NOMEM;
    }
    /* if ( ASDB_DBG >= 2 ) puts( query ); */
    if ( ASDB_DBG >= 2 ) fprintf( stderr, "%s\n", query );
    /* execute query */
    int rc = sqlite3_declare_vtab(db, query);
    if (rc == SQLITE_OK) *ppVtab = (sqlite3_vtab*)vtab;

    /* debug message */
    if (ASDB_DBG>=2) {
        if (rc == SQLITE_OK) {
            fprintf(stderr,"-> SQLITE_OK\n");
        } else {
            fprintf(stderr,"-> %d\n", rc);
        }
    }
    
    return rc;
}


static int asdbCreate(
    sqlite3 *db,
    void *pAux,
    int argc, const char *const* argv,
    sqlite3_vtab **ppVtab,
    char **pzErr
    )
{
    int rc = SQLITE_OK;
    if (ASDB_DBG>=2) fprintf(stderr,"asdbCreate\n");
    /* contents */
    return asdbCreateOrConnect(db, pAux, argc, argv, ppVtab, pzErr, 1);
}

static int asdbConnect(
    sqlite3 *db,
    void *pAux,
    int argc, const char *const* argv,
    sqlite3_vtab **ppVtab,
    char **pzErr
    )
{
    if (ASDB_DBG>=2) fprintf(stderr,"asdbConnect\n");
    /* contents */
    return asdbCreateOrConnect(db, pAux, argc, argv, ppVtab, pzErr, 0);
}

static int asdbDisconnect(sqlite3_vtab *pVtab)
{
    sqlite3_asdb_vtab * vtab = (sqlite3_asdb_vtab *)pVtab;
    if (ASDB_DBG>=2) fprintf(stderr,"asdbDisconnect\n");
    del_sqlite3_asdb_vtab(vtab);

    if (ASDB_DBG>=2) fprintf(stderr,"-> SQLITE_OK\n");
    return SQLITE_OK;
}

static int asdbDestroy(sqlite3_vtab *pVtab)
{
    if (ASDB_DBG>=2) fprintf(stderr,"asdbDestroy\n");
    asdbDisconnect(pVtab);

    if (ASDB_DBG>=2) fprintf(stderr,"-> SQLITE_OK\n");
    return SQLITE_OK;
}

static int asdbOpen(sqlite3_vtab *pVtab, 
                     sqlite3_vtab_cursor **ppCursor)
{
    if (ASDB_DBG>=2) fprintf(stderr,"asdbOpen\n");
    sqlite3_asdb_cursor * c
        = mk_sqlite3_asdb_cursor((sqlite3_asdb_vtab*)pVtab);
   
    if (c == NULL) {
        if (ASDB_DBG>=2) fprintf(stderr,"-> SQLITE_NOMEM\n");
        return SQLITE_NOMEM;
    }

    *ppCursor = (sqlite3_vtab_cursor *)c;

    if (ASDB_DBG>=2) fprintf(stderr,"-> SQLITE_OK\n");
    return SQLITE_OK;
}

static int asdbClose(sqlite3_vtab_cursor *cur)
{
    if (ASDB_DBG>=2) fprintf(stderr,"asdbClose\n");

    sqlite3_asdb_cursor *c = (sqlite3_asdb_cursor *)cur;
    sqlite3_asdb_vtab   *v = c->pVtab;
    asdb_index *src = array_asdb_index( c->idxes_preparing );
    uint64_t    elem = array_size( c->idxes_preparing );
    array_block_append( asdb_index, &v->indexes, src, elem );
    
    del_asdb_cursor( (sqlite3_asdb_cursor *) cur );

    if (ASDB_DBG>=2) fprintf(stderr,"-> SQLITE_OK\n");
    return SQLITE_OK;
}

static int asdbBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo)
{
    sqlite3_asdb_vtab  *v    = (sqlite3_asdb_vtab *)tab;
    sqlite3_index_info *info = pIdxInfo;

    /* print debug messages */
    if ( ASDB_DBG >= 2 ) {
        fprintf( stderr, "asdbBestIndex\n" );
        print_idxinfo( info );
        print_pmstate( v );
    }
    
    /* init output */
    info->idxNum = 0;
    info->idxStr = "";
    /* if ( plans_init( &v->plans ) != ASDB_OK ) return SQLITE_NOMEM; */
    
    /* create plan */
    double cost = 0;
    int n = 0;
    asdb_plan *plan = plan_create();
    if ( plan == NULL ) return SQLITE_NOMEM;
    for ( int i = 0; i < info->nConstraint; i++ ) {
        struct sqlite3_index_constraint *c = &info->aConstraint[i];
        struct sqlite3_index_constraint_usage *c_u = &info->aConstraintUsage[i];
        if ( c->usable &&
             c->op == SQLITE_INDEX_CONSTRAINT_EQ ) {
            int rc = plan_update( plan, v->indexes, c->iColumn );
            if ( rc < 0 ) return SQLITE_NOMEM;
            else if ( rc ) {
                c_u->omit = 1;
                c_u->argvIndex = ++n;
                cost += 1;
            } else {
                cost += array_size( v->pm ) / 2;
            }
        } else {
            uint64_t size = array_size( v->pm );
            if ( size == 0 ) cost += 1E6;
            cost += size;
        }
    }
    if ( ASDB_DBG >= 2 )
        plan_print( plan );
    int idxNum = plans_add_plan( &v->plans, plan );
    if ( idxNum < 0 ) return SQLITE_NOMEM;
    info->idxNum = idxNum;

    /* setup the others */
    info->needToFreeIdxStr = 0;
    info->orderByConsumed = 0;
    info->estimatedCost = cost;
    if (ASDB_DBG>=2) fprintf(stderr,"    cost = %lf\n", cost );
    if (ASDB_DBG>=2) printf("-> SQLITE_OK\n");

    return SQLITE_OK;
}

/** 
 * get first record
 * 
 * @param pVtabCursor 
 * @param idxNum 
 * @param idxStr 
 * @param argc 
 * @param argv 
 * 
 * @return 
 */
static int asdbFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
)
{

    if (ASDB_DBG>=2) {
        fprintf( stderr, "asdbFilter 呼ばれましたけど何か?\n" );
        fprintf( stderr, "  argc = %d\n", argc );
        fprintf( stderr, "  idxn = %d\n", idxNum );
    }
    
    sqlite3_asdb_cursor * c = (sqlite3_asdb_cursor *)pVtabCursor;
    sqlite3_asdb_vtab   * v = (sqlite3_asdb_vtab   *)c->pVtab;
    asdb_cursor *usr_c = c->cursor;
    
    /* set plan */
    c->plan = *plans_get_plan( &v->plans, idxNum );
    if ( ASDB_DBG >= 2 )
        plan_print( &c->plan );

    /* calc/search target irow_array, and set to cursor */
    asdb_cursor_set_target( c, idxNum, idxStr, argc, argv );

    /* prepare for index creation */
    if ( asdb_cursor_prepare_index( c ) != ASDB_OK )
        return SQLITE_ERROR;
    
    /* prepare for first row */
    c->nowmap_pos  = 0;
    
    /* cursor settings is set to read the next line */
    asdb_cursor_prepare_for_nextrow( c );
    
    /* check the state */
    switch ( c->status ) {
    case ASDB_ERROR:
        if ( ASDB_DBG >= 2 ) fprintf( stderr, "-> SQLITE_ERROR\n" );
        return SQLITE_ERROR;
    case ASDB_NOMEM:
        if ( ASDB_DBG >= 2 ) fprintf( stderr, "-> SQLITE_NOMEM\n" );
        return SQLITE_NOMEM;
    case ASDB_EOF:
        if ( ASDB_DBG >= 2 ) fprintf( stderr, "-> SQLITE_EOF\n" );
        return ASDB_OK;
    default:
        if ( ASDB_DBG >= 2 ) fprintf( stderr, "-> SQLITE_OK\n" );
        return ASDB_OK;
    }
}

/** 
 * get next record of target table
 * 
 * @param pVtabCursor 
 * 
 * @return return code
 */
static int asdbNext(sqlite3_vtab_cursor *pVtabCursor)
{
    sqlite3_asdb_cursor *c = (sqlite3_asdb_cursor *)pVtabCursor;
    sqlite3_asdb_vtab   *v = (sqlite3_asdb_vtab   *)c->pVtab;
    asdb_cursor *usr_c = c->cursor;
    if (ASDB_DBG>=2) {
        fprintf(stderr,"asdbNext (magic = %d, cur_line_no = %lld)\n", 
               c->magic, c->cursor->rowid);
    }
    assert(c->magic == ASDB_MAGIC_CURSOR);

    /* renew rowid */
    c->nowmap_pos++;

    asdb_cursor_prepare_for_nextrow( c );

    switch ( c->status ) {
    case ASDB_ERROR:
        if ( ASDB_DBG >= 2 ) fprintf( stderr, "-> SQLITE_ERROR\n" );
        return SQLITE_ERROR;
    case ASDB_NOMEM:
        if ( ASDB_DBG >= 2 ) fprintf( stderr, "-> SQLITE_NOMEM\n" );
        return SQLITE_NOMEM;
    case ASDB_EOF:
        if ( ASDB_DBG >= 2 ) fprintf( stderr, "-> SQLITE_EOF\n" );
        return SQLITE_OK;
    default:
        if ( ASDB_DBG >= 2 ) fprintf( stderr, "-> SQLITE_OK\n" );
        return SQLITE_OK;
    }
}

static int asdbEof(sqlite3_vtab_cursor *cur)
{
    sqlite3_asdb_cursor *c = (sqlite3_asdb_cursor *)cur;
    sqlite3_asdb_vtab   *v = (sqlite3_asdb_vtab *)c->pVtab;
    if (ASDB_DBG>=2) {
        fprintf(stderr,"asdbEof (magic = %d, cur_line_no = %lld)\n", 
               c->magic, c->cursor->rowid);
    }
    assert(c->magic == ASDB_MAGIC_CURSOR);

    bool rc = ( c->status == ASDB_OK ) ? false : true;
    
    if ( rc ) {
        if ( v->pm_state == CONSTRUCTING ) v->pm_state = USABLE;
        if ( ASDB_DBG>=2 ) fprintf(stderr,"-> 1\n");
        return 1;
    } else {
        if (ASDB_DBG>=2) fprintf(stderr,"-> 0\n");
        return 0;
    }
}

static int asdbColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i)
{
    sqlite3_asdb_cursor * c = (sqlite3_asdb_cursor *)cur;
    asdb_cursor *usr_c = c->cursor;
    sqlite3_asdb_vtab *v = c->pVtab;
    if (ASDB_DBG>=2) {
        fprintf(stderr, "asdbColumn (magic = %d, col = %d, cur_line_no = %lld)\n", 
               c->magic, i, usr_c->rowid);
        fprintf(stderr, "  ira_usable = %s\n", (c->ira_usable) ? "true" : "false" );
        fprintf(stderr, "  ira.size   = %"PRIu64"\n", array_size( c->ira ) );
        fprintf(stderr, "  nowmap.rid = %"PRIu64"\n", c->nowmap.rid );
        fprintf(stderr, "  nowmap_pos = %"PRIu64"\n", c->nowmap_pos );
    }
    assert(c->magic == ASDB_MAGIC_CURSOR);

    
    /* cell is not added to pm yet */
    asdb_cell *cell = asdb_get_cell( c->nowmap.row, i );
    
    if ( cell != NULL ) {
        /* attach cell to pm */
        asdb_value val  = asdb_get_value( cell );
        asdb_value_attach_to_ctx( ctx, val );
        /* prepare for the first creation */
        asdb_index idx = idxes_get_index( c->idxes_preparing, i );
        if ( idx.icolumn >= 0 ) {
            asdb_index_key key = idxkey_create( val );
            /* printf( "%lld, ", c->nowmap_pos ); */
            idx_append_newirow( &idx, key, c->nowmap_pos );
        }
    } else {
        asdb_value val;
        asdb_set_value_null( &val );
        asdb_value_attach_to_ctx( ctx, val );
    }

    
    if (ASDB_DBG>=2) fprintf(stderr,"-> SQLITE_OK\n");
    return SQLITE_OK;
}

static int asdbRowid(sqlite3_vtab_cursor *pVtabCursor, sqlite_int64 *pRowid)
{
    sqlite3_asdb_cursor * c = (sqlite3_asdb_cursor *)pVtabCursor;
    if (ASDB_DBG>=2) {
        fprintf(stderr,"asdbRowid (magic = %d, cur_line_no = %lld)\n", 
               c->magic, c->cursor->rowid);
    }
    assert(c->magic == ASDB_MAGIC_CURSOR);

    *pRowid = c->cursor->rowid;

    if (ASDB_DBG>=2) fprintf(stderr,"-> SQLITE_OK\n");
    return SQLITE_OK;
}




static sqlite3_module asdbModule = {
    0,               /* iVersion */
    asdbCreate,      /* xCreate - create a table */
    asdbConnect,     /* xConnect - connect to an existing table */
    asdbBestIndex,   /* xBestIndex - Determine search strategy */
    asdbDisconnect,  /* xDisconnect - Disconnect from a table */
    asdbDestroy,     /* xDestroy - Drop a table */
    asdbOpen,        /* xOpen - open a cursor */
    asdbClose,       /* xClose - close a cursor */
    asdbFilter,      /* xFilter - configure scan constraints */
    asdbNext,        /* xNext - advance a cursor */
    asdbEof,         /* xEof */
    asdbColumn,      /* xColumn - read data */
    asdbRowid,       /* xRowid - read data */
    0,               /* xUpdate - write data */
    0,               /* xBegin - begin transaction */
    0,               /* xSync - sync transaction */
    0,               /* xCommit - commit transaction */
    0,               /* xRollback - rollback transaction */
    0,               /* xFindFunction - function overloading */
    0,               /* xRename - rename the table */
    0,               /* xSavepoint */
    0,               /* xRelease */
    0                /* xRollbackTo */
};

int sqlite3_asdb_init(sqlite3 *db){
    if (ASDB_DBG>=2) fprintf(stderr,"sqlite3_asdb_init (%s)\n", asdb_get_module_name());
    fflush(stdout);
    return sqlite3_create_module_v2(db, asdb_get_module_name(), &asdbModule, NULL, 0);
}

#if !SQLITE_CORE && !defined(__TEST__)
int sqlite3_extension_init(sqlite3 *db,
                           char **pzErrMsg,
                           const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi)
        return sqlite3_asdb_init(db);
}
#endif

#endif /*  */

