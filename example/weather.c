//! gcc -c medline.c -W -Wall -O3 -std=gnu99 -lsqlite3 -fsyntax-only -Wno-unused-parameter -Wno-unused-variable

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/param.h>
#include <sys/uio.h>
#include <unistd.h>

#include "asdb.h"
#include "weather.h"

/***** parameters *****/
#define WEATHER_LINEBUF_SIZE 1024
#define WEATHER_MAX_NSCHEMA 5
const char const *asdb_module_name = "weather";
const int WEATHER_DBG = 0;

/***** schema *****/
static const asdb_schema weather_schema =
{ .tname = NULL,
  .cname = {"pref","area","date1","date2","date3"}
};

/***** open / close / next *****/
asdb_cursor *asdb_open( asdb_vtab *pVtab )
{
    if ( WEATHER_DBG >= 2 ) fprintf( stderr, "asdb_open\n" );
    weather_vtab *vtab = ( weather_vtab * ) pVtab;
    /* create cursor */
    weather_cursor *c =
        (weather_cursor *) sqlite3_malloc( sizeof( weather_cursor ) );
    if ( c == NULL ) {
        if ( WEATHER_DBG >= 2 ) fprintf( stderr,  "-> ASDB_NOMEM\n" );
        return NULL;
    }
    /* set variables */
    if ( WEATHER_DBG >= 2 ) fprintf( stderr, "### create buffer\n");
    char *buffer = sqlite3_malloc( sizeof( char ) * WEATHER_LINEBUF_SIZE );
    if ( buffer == NULL ) {
        if ( WEATHER_DBG >= 2 ) fprintf( stderr, "-> ASDB_ERROR\n" );
        sqlite3_free( c );
        return NULL;
    }
    buffer[0] = '\0';
    
    /* if ( WEATHER_DBG >= 2 ) fprintf( stderr, "### file open: %s\n", vtab->filename ); */
    /* FILE *fp = fopen( vtab->filename, "r" ); */
    /* if ( fp == NULL ) { */
    /*     if ( WEATHER_DBG >= 2 ) fprintf( stderr, "-> ASDB_ERROR\n" ); */
    /*     sqlite3_free( c ); */
    /*     sqlite3_free( buffer ); */
    /*     return NULL; */
    /* } */
    /* c->line = buffer; */
    /* c->cur  = NULL; */
    c->list = vtab->head;
    /* c->fp = fp; */
    return (asdb_cursor *) c;
}

asdb_rc asdb_close( asdb_cursor *pVtabcursor )
{
    if ( WEATHER_DBG >= 2 ) fprintf( stderr, "asdb_close\n" );

    weather_cursor *c = ( weather_cursor * ) pVtabcursor;
    /* sqlite3_free( c->line ); */
    /* http_free_weather_list(c->list); */
    return ASDB_OK;
}

asdb_row *asdb_next( asdb_cursor *pVtabcursor )
{
    if ( WEATHER_DBG >= 2 ) fprintf( stderr, "asdb_next\n" );
     
    weather_cursor *c = ( weather_cursor * ) pVtabcursor;
    /* null check */
    if ( c->list == NULL ) return NULL;
     
    /* create row object */
    weather_row *row = malloc( sizeof( weather_row ) );
    if ( row == NULL ) return NULL;
    if ( WEATHER_DBG >= 2 ) {
        fprintf( stderr, "# row\n" );
        if ( c->list == NULL )
            fprintf( stderr, "## NULL LIST!\n" );
    }
    row->pref_id = sqlite3_mprintf( "%s", c->list->pref_id );
    row->area_id = sqlite3_mprintf( "%s", c->list->area_id );
    row->date1 = sqlite3_mprintf( "%s", c->list->date1 );
    row->date2 = sqlite3_mprintf( "%s", c->list->date2 );
    row->date3 = sqlite3_mprintf( "%s", c->list->date3 );

    /* prepare next word */
    if ( WEATHER_DBG >= 2 ) fprintf( stderr, "# copied!\n" );
    c->list = c->list->next;
    return ( asdb_row * ) row;
}

asdb_cell *asdb_get_cell( const asdb_row* row, int icol )
{
    if ( WEATHER_DBG >= 2 ) fprintf( stderr, "asdb_get_cell\n" );

    const weather_row *r = ( const weather_row * ) row;

    weather_cell *cell = malloc( sizeof( weather_cell ) );
    if ( cell == NULL ) return NULL;
    switch (icol) {
    case 0:
        asdb_set_value_text( &cell->value, r->pref_id ); /* value set */
        break;
    case 1:
        asdb_set_value_text( &cell->value, r->area_id ); /* value set */
        break;
    case 2:
        asdb_set_value_text( &cell->value, r->date1 ); /* value set */
        break;
    case 3:
        asdb_set_value_text( &cell->value, r->date2 ); /* value set */
        break;
    case 4:
        asdb_set_value_text( &cell->value, r->date3 ); /* value set */
        break;
    default:
        return NULL;
    }
    return (asdb_cell *)cell;
}

asdb_value asdb_get_value( const asdb_cell *cell )
{
    if ( WEATHER_DBG >= 2 ) fprintf( stderr, "asdb_get_value\n" );
    
    const weather_cell *c = ( const weather_cell * ) cell;
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
    if ( WEATHER_DBG >= 2 ) fprintf( stderr, "asdb_create_vtab\n" );
    if ( WEATHER_DBG >= 2 ) {
        for ( int i = 0; i < argc; i++ ) {
            fprintf( stderr, "# argv[%d] = %s\n", i, argv[i] );
        }
    }
    
    /* create vtab object */
    weather_vtab *vtab =
        ( weather_vtab * ) sqlite3_malloc( sizeof( weather_vtab ) );
    if ( vtab == NULL ) {
        if ( WEATHER_DBG >= 1 ) fprintf(stderr, "-> ASDB_NOMEM\n" );
        return NULL;
    }
    
    /* extract file name */
    /* char *filename = sqlite3_mprintf( "%s", (argc > 3 ? argv[3] : "a.txt")); */
    /* if ( filename == NULL ) { */
    /*     if ( WEATHER_DBG >= 1 ) fprintf(stderr,"-> ASDB_NOMEM\n"); */
    /*     sqlite3_free( vtab ); */
    /*     return NULL; */
    /* } */
    
    /* extract table name */
    char *tablename = sqlite3_mprintf( "%s", (argc > 2 ? argv[2] : ""));
    if ( tablename == NULL ) {
        if ( WEATHER_DBG >= 1 ) fprintf(stderr,"-> ASDB_NOMEM\n");
        sqlite3_free( vtab );
        /* sqlite3_free( filename ); */
        return NULL;
    }

    /* create schema */
    asdb_schema *sch  = sqlite3_malloc( sizeof( asdb_schema ) );
    if ( sch == NULL ) {
        if ( WEATHER_DBG >= 1 ) fprintf( stderr, "-> ASDB_NOMEM\n" );
        sqlite3_free( vtab );
        /* sqlite3_free( filename ); */
        sqlite3_free( tablename );
        return NULL;
    }
    *sch = weather_schema;
    sch->tname = tablename;
   
    /* set variables */
    vtab->schema = sch;
    /* vtab->filename  = filename; */
    http_fetch_weather(&vtab->head);



    return (asdb_vtab *)vtab;
}

asdb_rc asdb_destroy_vtab( asdb_vtab *pVtab ) {
    weather_vtab *vtab = ( weather_vtab * )pVtab;
    /* sqlite3_free( vtab->filename ); */
    http_free_weather_list(vtab->head);
    sqlite3_free( vtab );

    return ASDB_OK;
}

const char const *asdb_get_module_name( void )
{
    return asdb_module_name;
}




/********************************/
/* ここからxmlを取ってくる関数！ */
/********************************/

struct http_weather_list* http_make_weather_list (char *pref,char *area,char *date1,char *date2,char *date3)
{
    struct http_weather_list* tmp = (struct http_weather_list*)malloc(sizeof(struct http_weather_list));

    strcpy(tmp->pref_id,pref);
    strcpy(tmp->area_id,area);
    strcpy(tmp->date1,date1);
    strcpy(tmp->date2,date2);
    strcpy(tmp->date3,date3);
    tmp->next = NULL;
     
    return tmp;
}

void http_free_weather_list(struct http_weather_list* head)
{
    struct http_weather_list *tmp,*cur;

    if (head->next == NULL) {
        free(head);
    } else {
        for (cur = head,tmp = head->next;tmp->next != NULL;cur=tmp,tmp=tmp->next) {
            free(cur);
        }
        free(cur);
        free(tmp);
    }
}

struct http_weather_list* http_weather_xml_parse(int s)
{
    FILE *fp;
    fp = fdopen(s,"r");
    if (fp == NULL) {
        printf("fdopen error ww\n");
        return NULL;
    }

    struct http_weather_list *tmp,*head = NULL;
    char buf[HTTP_BUF_LEN],parse_data[5][32]; /* ここにデータを退避させて揃ったらリストを作る */
    int parse_count = 0,i;
     
    while (fgets(buf,HTTP_BUF_LEN,fp) != NULL){
        for (i = 0;i < strlen(buf);i++) {
            if (buf[i] == '<') { 
                if(!strncmp(buf+i+1,"pref id",7) && parse_count == 0) { /* 都道府県を入れる */
                    char *c = strtok(buf+i+10,"\"");
                    strcpy(parse_data[parse_count++],c);
                } else if (!strncmp(buf+i+1,"area id",7)) { /* 地域を入れる */
                    if (parse_count == 1) {
                        char *c = strtok(buf+i+10,"\"");
                        strcpy(parse_data[parse_count++],c);
                    } else { /* データが3つ未満のとき */
                        while (parse_count < 5) {
                            strcpy(parse_data[parse_count++],"情報なし");
                        }
                        if (head == NULL) {
                            tmp = http_make_weather_list(parse_data[0],parse_data[1],parse_data[2],parse_data[3],parse_data[4]);
                            head = tmp;
                        } else {
                            tmp->next = http_make_weather_list(parse_data[0],parse_data[1],parse_data[2],parse_data[3],parse_data[4]);
                            tmp = tmp->next;
                        }
                        char *c = strtok(buf+i+10,"\"");
                        strcpy(parse_data[1],c); /* 新たなデータをわすれずに記録しとく */
                        parse_count = 2;
                    }
                } else if (!strncmp(buf+i+1,"weather>",8) && parse_count > 1) {
                    char *c = strtok(buf+i+9,"<");
                    strcpy(parse_data[parse_count++],c);
                }

                if (parse_count == 5) {
                    if (head == NULL) {
                        tmp = http_make_weather_list(parse_data[0],parse_data[1],parse_data[2],parse_data[3],parse_data[4]);
                        head = tmp;
                    } else {
                        tmp->next = http_make_weather_list(parse_data[0],parse_data[1],parse_data[2],parse_data[3],parse_data[4]);
                        tmp = tmp->next;
                    }
                    parse_count = 1; /* ひとつのxmlファイルは同じ都道府県であると想定 */
                }
                break;
            }
        }
    }

    if (parse_count >= 2) {	/* 最後のデータが2つ未満だと読み込まれないので */
        while (parse_count < 5) {
            strcpy(parse_data[parse_count++],"情報なし");
        }
        if (head == NULL) {
            tmp = http_make_weather_list(parse_data[0],parse_data[1],parse_data[2],parse_data[3],parse_data[4]);
            head = tmp;
        } else {
            tmp->next = http_make_weather_list(parse_data[0],parse_data[1],parse_data[2],parse_data[3],parse_data[4]);
            tmp = tmp->next;
        }
    }
    fclose(fp);
    return head;     
}

void http_fetch_weather(struct http_weather_list **head)
{
    int s;                               /* ソケットのためのファイルディスクリプタ */
    struct hostent *servhost;            /* ホスト名と IP アドレスを扱うための構造体 */
    struct sockaddr_in server;           /* ソケットを扱うための構造体 */
    struct servent *service;             /* サービス (http など) を扱うための構造体 */

    char send_buf[HTTP_BUF_LEN];              /* サーバに送る HTTP プロトコル用バッファ */
    unsigned short port = 80;            /* 接続するポート番号 */
     
    char host[] = "www.drk7.jp";
    char path[] = "weather/xml/01.xml";

    /* ホストの情報(IPアドレスなど)を取得 */
    servhost = gethostbyname(host);
    if ( servhost == NULL ){
        fprintf(stderr, "[%s] から IP アドレスへの変換に失敗しました。\n", host);
        return;
    }

    bzero(&server, sizeof(server));            /* 構造体をゼロクリア */

    server.sin_family = AF_INET;

    /* IPアドレスを示す構造体をコピー */
    bcopy(servhost->h_addr, &server.sin_addr, servhost->h_length);

    if (port != 0) {                          /* 引数でポート番号が指定されていたら */
        server.sin_port = htons(port);
    } else {                                   /* そうでないなら getservbyname でポート番号を取得 */
        service = getservbyname("http", "tcp");
        if (service != NULL) {                /* 成功したらポート番号をコピー */
            server.sin_port = service->s_port;
        } else {                               /* 失敗したら 80 番に決め打ち */
            server.sin_port = htons(80);
        }
    }


    int i,write_result;
    struct http_weather_list *tmp;
    struct http_weather_list *tmp_head = NULL;
    for (i = 1;i <= 47;i++) {
        /* ソケット生成 */
        if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0){
            fprintf(stderr, "ソケットの生成に失敗しました。\n");
            return;
        }
        /* サーバに接続 */
        if (connect(s, (struct sockaddr *)&server, sizeof(server)) == -1) {
            fprintf(stderr, "connect に失敗しました。\n");
            return;
        }
     
        path[12] = '0' + (i / 10);
        path[13] = '0' + (i % 10);
	  
        /* HTTP プロトコル生成 & サーバに送信 */
        sprintf(send_buf, "GET %s HTTP/1.0\r\n", path);
        write_result = write(s, send_buf, strlen(send_buf));
	  
        sprintf(send_buf, "Host: %s:%d\r\n", host, port);
        write_result = write(s, send_buf, strlen(send_buf));
	  
        sprintf(send_buf, "\r\n");
        write_result = write(s, send_buf, strlen(send_buf));

        
        tmp = http_weather_xml_parse(s);
        close(s);
        /* 最後までたどって頑張る */
        if ( tmp_head == NULL ) tmp_head = tmp;
        else {
            struct http_weather_list *pcur;
            for ( pcur = tmp; pcur->next!=NULL; pcur=pcur->next );
            pcur->next = tmp_head;
            tmp_head = tmp;
        }
    }
    *head = tmp_head;
}
