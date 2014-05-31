#ifndef ARRAY_H
#define ARRAY_H

#include <assert.h>
#include "array_type.h"

typedef struct _arr {
    void *data;
    uint64_t elem;
    uint64_t alloced;
    array_type_t t;
    uint32_t tsize;
} array_t;

/** 
 * check whether the array is null or not
 * 
 * @param a array_t
 * 
 * @return true if array is null-array
 */
static inline bool array_isnull( array_t a )
{
    return (a.data == NULL) ? true : false;
}

/** 
 * null array is defined by this function
 * 
 * @return null array
 */
static inline array_t array_null( void )
{
    return (array_t){
        .data = NULL, .elem = 0, .alloced = 0, .t = type_null, .tsize = 0 };
}

/** 
 * getter of the number of elements (neither malloced size nor bytes of elementes)
 * 
 * @param a array
 * 
 * @return # of elementes
 */
static inline uint64_t array_size( array_t a )
{
    return a.elem;
}

/** 
 * YOU DO NOT HAVE TO USE DIRECTLY THIS FUNCTION
 * array_init macro you must use alternatively.
 * 
 * @param n 
 * @param size 
 * @param type 
 * 
 * @return 
 */
array_t _array_init( size_t n, size_t size, array_type_t type );

/** 
 * initializer of array_t
n *
 * usage: array_t a = array_init( int, 10 ); // int array (size=10) will be declared
 * 
 * @param type array type (int, char ...)
 * @param n    alloced size
 * 
 * @return array_t
 */
#define array_init(type, n) _array_init( n, sizeof(type), type_##type )

/** 
 * free your array and set null array
 * 
 * @param a array_t
 */
void array_destroy( array_t *a );

int _array_block_append( array_t *arr, const void *data, size_t size );
/** 
 * appending block data 
 * 
 * @param type array and source data type
 * @param array_p destination array
 * @param src_p data source
 * @param nelem # of elements you want to copy
 * 
 * @return int 0: succeeded, 1: failed
 */
#define array_block_append( type, array_p, src_p, nelem )               \
    do {                                                                \
        __attribute__((unused))                                         \
            type *src_type_check = src_p;                               \
        assert( (array_p)->t == type_##type );                          \
        _array_block_append( array_p, ((type *)(src_p)), sizeof(type)*(nelem) ); \
    } while(0)                                                          \

/** 
 * declaration of a method appending an element
 *
 * declaration: DECLARE_ARRAY_TYPE_APPEND(int);
 * usage:       array_t a = array_init( int, 1 );
 *              int rc = array_int_append( a, 10 );
 * 
 * @param type 
 * 
 * @return 0: success, 1: nomem
 */
#define DECLARE_ARRAY_TYPE_APPEND( type )                               \
    static int array_##type##_append(                                   \
        array_t *arr, type data )__attribute__ ((unused));              \
    static int array_##type##_append(                                   \
        array_t *arr, type data )                                       \
    {                                                                   \
        assert( arr->t == type_##type );                                \
        uint64_t e = arr->elem;                                         \
        uint64_t l = arr->alloced;                                      \
        type    *a = (type *)arr->data;                                 \
        if ( e < l ) {                                                  \
            a[e] = data;                                                \
            arr->elem += 1;                                             \
        } else {                                                        \
            type *tmp = ( type *)realloc( a, sizeof(type)*l*2 );        \
            if ( tmp == NULL ) return 1;                                \
            arr->data              = tmp;                               \
            arr->alloced          *= 2;                                 \
            ((type *)arr->data)[e] = data;                              \
            arr->elem             += 1;                                 \
        }                                                               \
        return 0;                                                       \
    }

/** 
 * extract array with checking type
 * 
 * declaration: DECLARE_ARRAY_TYPE_GETTER(int);
 * usage:       array_t a = array_init( int, 1 );
 *              printf( "%d\n", array_int( a )[0] );
 *
 * @param type: int, char...
 *
 * @return pointer of "type"
 * 
 */
#define DECLARE_ARRAY_TYPE_GETTER( type )           \
    static inline type *array_##type( array_t a ) { \
        assert( a.t == type_##type );               \
        return (type *)a.data;                      \
    }                    


/** 
 * you have to put on your source files
 * 
 * @param type 
 * 
 * @return 
 */
#define DECLARE_ARRAY_TYPE( type )              \
    DECLARE_ARRAY_TYPE_GETTER( type );          \
    DECLARE_ARRAY_TYPE_APPEND( type );

/** 
 * iterative method
 * you have to place into blackets (create foreach scope).
 * 
 * usage: array_t a = array_init( int, 1 );
 *        array_int_append( a, 1 );
 *        array_int_append( a, 2 );
 *        array_int_append( a, 3 );
 *        {
 *          FOREACH( int, a, x ) {
 *            printf( "%d, ", x ); // "1, 2, 3, " will be printed.
 *          }
 *          printf( "%d\n" );
 *        }
 * 
 * @param type int, char ...
 * @param arr array_t
 * @param x   itarator (will declare internally)
 * 
 * @return nothing
 */
#define FOREACH(type, arr, x)                               \
    uint64_t _i; type *_a; type x;                          \
    for( _i = 0, _a = array_##type( arr ), x = _a[0];       \
         _i < arr.elem;                                     \
         _i++, x = _a[_i] )

#endif
