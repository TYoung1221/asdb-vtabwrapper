#pragma once

/***** My Vtab / Cursor *****/
typedef struct {
    /* provied by system */
    const asdb_schema *schema;
    int ncol;
    char *filename;
} eng_vtab;

typedef struct {
    /* provied by system */
    eng_vtab *vtab;
    sqlite3_int64 rowid;
    /* defined by user */
    FILE *fp;
    char *line;
    char *cur;
} eng_cursor;

typedef struct {
    /* defined by user */
    char *word;
} eng_row;

typedef struct {
    /* defined by user */
    asdb_value value;
} eng_cell;
