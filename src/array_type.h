/**
 * @file   array_type.h
 * @author Makoto Shimazu <makoto.shimaz@gmail.com>
 * @date   Mon Oct 28 20:40:17 2013
 * 
 * @brief  Your type settings for array library
 *         array_type_t has to contain the array types you will use.
 *         (ex: type_int, if you use int-array )
 * 
 */


#ifndef ARRAY_TYPE_H
#define ARRAY_TYPE_H

/* you have to change this */

typedef enum {
    type_null,                  /**< this is used internally */
    type_char,
    type_unsinged_char,
    type_int,
    type_unsinged_int,
    type_float,
    type_double,
    type_int8_t,
    type_uint8_t,
    type_int32_t,
    type_uint32_t,
    type_int64_t,
    type_uint64_t,
    /* you have to put the types you will use below */
    type_asdb_pm,
    type_asdb_cell,
    type_asdb_index,
    type_asdb_index_node,
    type_asdb_plan
} array_type_t;

#endif
