#pragma once

/* weather list */
struct http_weather_list {
     char pref_id[32];		/* 都道府県 */
     char area_id[32];		/* 地域 */
     char date1[32];		/* 今日 */
     char date2[32];		/* 明日 */
     char date3[32];		/* 明後日 */
     struct http_weather_list *next;
};

/***** My Vtab / Cursor *****/
typedef struct {
    /* provied by system */
    const asdb_schema *schema;
    int ncol;
     struct http_weather_list *head;
    /* char *filename; */
} weather_vtab;

typedef struct {
    /* provied by system */
    weather_vtab *vtab;
    sqlite3_int64 rowid;
    /* defined by user */
    /* FILE *fp; */
     struct http_weather_list *list;
    /* char *line; */
    /* char *cur; */
} weather_cursor;

typedef struct {
    /* defined by user */
     char *pref_id;		/* 都道府県 */
     char *area_id;		/* 地域 */
     char *date1;		/* 今日 */
     char *date2;		/* 明日 */
     char *date3;		/* 明後日 */
} weather_row;

typedef struct {
    /* defined by user */
    asdb_value value;
} weather_cell;

/* データを持ってくる部分 */
#define HTTP_BUF_LEN 1024                      /* バッファのサイズ */


struct http_weather_list* http_make_weather_list(char*,char*,char*,char*,char*);
void http_free_weather_list(struct http_weather_list *head);
struct http_weather_list* http_weather_xml_parse(int);
void http_fetch_weather(struct http_weather_list **head);
