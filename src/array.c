//! gcc -o array.bin array.c -W -Wall -O3 -std=gnu99 -D__ARRAY_DEBUG__

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "array.h"

array_t _array_init( size_t n, size_t size, array_type_t type )
{
    array_t arr;
    void *t = malloc( size * n );
    if ( t == NULL ) return (array_t){NULL, 0, 0, type_null, 0};
    arr.data = t;
    arr.elem = 0;
    arr.alloced = n;
    arr.t = type;
    arr.tsize = size;
    return arr;
}

void array_destroy( array_t *a )
{
    free( a->data );
    *a = array_null();
}

/** 
 * sizeバイトだけdataからarrにコピー
 * 基本的にこれを用いるためのマクロを用いること。
 * 
 * @param arr コピー先のarray
 * @param data コピー元の配列 arrayと型が揃っていると仮定する
 * @param size コピーする大きさ
 * 
 * @return 0: succeeded 1: failed
 */
int _array_block_append( array_t *arr, const void *data, size_t size )
{
    int i;
    uint64_t alloced = arr->alloced;
    uint64_t elem    = arr->elem;
    uint32_t tsize   = arr->tsize;
    uint64_t rest    = tsize * (alloced - arr->elem);
    /* Free space is enough? */
    if ( rest >= size ) {
        memcpy( &((char*)arr->data)[elem * tsize], data, size );
        arr->elem += size / tsize;
        return 0;
    }
    /* calculate new buffer size */
    for ( i = 0; i < 64; i++ ) {
        alloced *= 2;
        if ( alloced * tsize >= size ) break;
    }
    if ( i == 64 ) return 1;
    void *tmp = realloc( arr->data, alloced*tsize );
    if ( tmp == NULL ) return 1;
    memcpy( &((char*)tmp)[elem * tsize], data, size );
    /* update */
    arr->data  = tmp;
    arr->alloced = alloced;
    arr->elem += size / tsize;
    
    return 0;
}


#ifdef __ARRAY_DEBUG__

#include <sys/time.h>

double gettime_ms()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec*1E3 + (double)tv.tv_usec/1E3;
}

DECLARE_ARRAY_TYPE( int );
DECLARE_ARRAY_TYPE( char );

int test1()
{
    double pre, post;
    int *tmp;
    array_t a = array_init( int, 1 );
    pre = gettime_ms();
    for ( int i = 0; i < 1E7; i++ ) array_int_append( &a, i );
    post = gettime_ms();

    int success = 0;
    int counter = 0;
    for ( int i = 0; i < 1E7; i++ ) {
        tmp = array_int( a );
        /* char *dame = array_char( a ); /\**< incorrect type will be asserted *\/ */
        /* printf( "%c\n", dame[i] ); */
        if ( tmp[i] == i ) success++;
        counter++;
    }
    printf( "# array has %lld elems and malloced %lld Bytes\n",
            array_size( a ), a.alloced * sizeof(int) );
    printf( "# %5d / %5d are correct.\n", success, counter );
    printf( "# %6.2lf[ms] is elapsed.\n", post - pre );

    array_destroy( &a );

    return success==counter;
}

int test2()
{
    double pre, post;
    array_t a = array_init( int, 1 );

    int *src = malloc( sizeof( int ) * 1E7 );
    for ( int i = 0; i < 1E7; i++ ) src[i] = i;
    pre = gettime_ms();
    array_block_append( int, &a, src, 1E7 );
    post = gettime_ms();
   
    int success = 0;
    int counter = 0;
    for ( int i = 0; i < 1E7; i++ ) {
        int *tmp = array_int( a );
        /* char *dame = array_char( a ); /\**< incorrect type will be asserted *\/ */
        /* printf( "%c\n", dame[i] ); */
        /* printf( "%d\n", tmp[i] ); */
        if ( tmp[i] == i ) success++;
        counter++;
    }
    printf( "# array has %lld elems and malloced %lld Bytes\n",
            array_size( a ), a.alloced * sizeof(int) );
    printf( "# %5d / %5d are correct.\n", success, counter );
    printf( "# %6.2lf[ms] is elapsed.\n", post - pre );

    array_destroy( &a );
    free(src);
    
    return success==counter;
}

#define CHK_TEST(test)                                     \
    do {                                                   \
        char *str = (test())? "SUCCEEDED!":"FAILED...";    \
        printf( #test " is %s\n", str );                   \
    }while(0)                                             

    

int main( void )
{
    CHK_TEST(test1);
    CHK_TEST(test2);
    return 0;
}
#endif
