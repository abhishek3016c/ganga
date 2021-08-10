//
// Created by Abhishek Chaturvedi on 12/11/19.
//

#include <stdio.h>

#ifdef WIN32
#define  int8_t     signed char
  #define  int16_t    short
  #define  int32_t    long
  #define  int64_t    __int64
  #define  uint8_t    unsigned char
  #define  uint16_t   unsigned short
  #define  uint32_t   unsigned long
  #define  uint64_t   unsigned __int64
  #include <windows.h>
  #if (_MSC_VER>=1400)
    #pragma warning(disable:4018)
    #pragma warning(disable:4996)
  #endif
#else
#if defined(__linux__)
#include <stdint.h>
    #include <stdarg.h>
#endif

#if defined(__MVS__)
#include <stdarg.h>
    #include <varargs.h>
    #include <inttypes.h>
#endif

#if !defined(__MVS__) && !defined(__linux__)
#include <stdarg.h>
#endif

#if defined (_OSS_HOST)
#include <stdint.h>
#endif

#include <string.h>
#include <sys/types.h>
#include <stdlib.h>
#include <ctype.h>
#endif

#include "usrdecs.h"

/* ER callback routine */
#ifndef WIN32
void ERCALLBACK(ercallback_function_codes function_code,
                void *buf, short *presult_code);
#else
typedef void (*FPERCALLBACK)(ercallback_function_codes function_code,
                             void *buf, short *presult_code);

HINSTANCE hEXE; /* EXE handle */
FPERCALLBACK fp_ERCallback; /* Callback function pointer */

/***************************************************************************
  Open the ER callback function explicitly.
***************************************************************************/
short open_callback (char *executable_name,
                     char *function_name)
{
    printf ("Opening callback for %s, %s.\n",
            executable_name,
            function_name);

    hEXE = LoadLibrary (executable_name);
    if (hEXE != NULL)
    {
        /* Function should always be exported in uppercase, since
           GetProcAddress converts to uppercase then does a case-sensitive
           search.  Don't use /NOIGNORE (/NOI) linker option. */
        fp_ERCallback = (FPERCALLBACK)GetProcAddress(hEXE, function_name);

        if (!fp_ERCallback)
        {
            FreeLibrary (hEXE);
            return 0;
        }
    }
    else
        return 0;

    return 1;
}
#endif

/***************************************************************************
  Close the callback function.
***************************************************************************/
void close_callback (void)
{
#ifdef WIN32
    FreeLibrary (hEXE);
#endif
}

/***************************************************************************
  Call the callback function.
***************************************************************************/
void call_callback (ercallback_function_codes function_code,
                    void *buf, short *result_code)
{
#ifdef WIN32
    fp_ERCallback (function_code, buf, result_code);
#else
    ERCALLBACK (function_code, buf, result_code);
#endif
}

/***************************************************************************
  Test for Little Endian machine format
***************************************************************************/
short LittleEndian( void )
{
    short little_endian = 1;
    char *little_endian_c = (char *)&little_endian;

    if (*little_endian_c)
    {
        return 1;
    }
    return 0;
}

/***************************************************************************
  Reverse bytes for Little Endian/Big Endian
***************************************************************************/
void ReverseBytes (void *buf, short cnt)
{
    char *b = (char *)buf, *e = (char *)buf + cnt - 1;
    char temp;

    for (; b < e; b++, e--)
    {
        temp = *b;
        *b = *e;
        *e = temp;
    }
}

/***************************************************************************
  Output a message to the report file (or console).
***************************************************************************/
void output_msg (char *msg,...)
{
    short result_code;
    char temp_msg[1000];

    va_list args;
    va_start (args, msg);
    vsprintf (temp_msg, msg, args);
    va_end (args);
    call_callback (OUTPUT_MESSAGE_TO_REPORT, temp_msg, &result_code);
}

short IsPrint(int c)
{
    if (c != EOF) c = (unsigned char)c;
    return isprint(c) ? 1 : 0;
}

/***************************************************************************
  Display record information.
***************************************************************************/
short display_record (short source_or_target,
                      short ascii_or_internal)
{
    int i, j, k;
    unsigned short col_len;
    char hex_str[31];
    char ascii_str[11];
    char *a;
    short result_code;
    table_def table;
    column_def column;
    env_value_def env_value;

    /* initialize */
    memset (&env_value, 0, sizeof(env_value_def));
    env_value.max_length = 500;
    env_value.buffer = (char *)malloc(500);
    if (source_or_target == EXIT_FN_CURRENT_VAL)
        env_value.source_or_target = EXIT_FN_TARGET_VAL;
    else
        env_value.source_or_target = source_or_target;

    /* Get catalog name only */
    call_callback (GET_CATALOG_NAME_ONLY, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving catalog name.\n", result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Catalog name: %.*s \n",
                env_value.actual_length, env_value.buffer);

    /* Get schema name only */
    call_callback (GET_SCHEMA_NAME_ONLY, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving catalog name.\n", result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Schema  name: %.*s \n",
                env_value.actual_length, env_value.buffer);

    /* Get table name only */
    call_callback (GET_TABLE_NAME_ONLY, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving table name.\n", result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Table   name: %.*s \n",
                env_value.actual_length, env_value.buffer);

    /* Get fully qualified table name */
    call_callback (GET_TABLE_NAME, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving fully qualified table name.\n",
                    result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Fully qualified table name: %.*s \n",
                env_value.actual_length, env_value.buffer);

    /* Get table column information */
    memset (&table, 0, sizeof(table_def));
    if (source_or_target == EXIT_FN_CURRENT_VAL)
        table.source_or_target = EXIT_FN_TARGET_VAL;
    else
        table.source_or_target = source_or_target;

    call_callback (GET_TABLE_COLUMN_COUNT, &table, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving table column count.\n",
                    result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Number of columns: %hd\n", table.num_columns);

    memset(&column, 0, sizeof (column_def));
    column.source_or_target = source_or_target;
    column.column_value_mode = ascii_or_internal;
    column.column_value = (char*)malloc(4000);
    column.max_value_length = 4000;

    for (i = 0; i < table.num_columns; i++)
    {
        env_value.index = column.column_index = i;

        /* get column name */
        call_callback (GET_COLUMN_NAME_FROM_INDEX, &env_value, &result_code);
        if (result_code != EXIT_FN_RET_OK)
        {
            output_msg ("Error (%hd) retrieving column name.\n", result_code);
            free (env_value.buffer);
            free (column.column_value);
            return result_code;
        }
        output_msg ("Column index (%d) = %.*s.\n",
                    i, env_value.actual_length, env_value.buffer);

        /* get column value */
        call_callback (GET_COLUMN_VALUE_FROM_INDEX, &column, &result_code);
        if (result_code != EXIT_FN_RET_OK &&
            result_code != EXIT_FN_RET_COLUMN_NOT_FOUND)
        {
            output_msg ("Error (%hd) retrieving column value.\n", result_code);
            free (env_value.buffer);
            free (column.column_value);
            return result_code;
        }

        if (result_code == EXIT_FN_RET_COLUMN_NOT_FOUND)
            output_msg ("Column not found in record.\n");
        else
        if (column.null_value)
        {
            if (column.is_hidden_column)
                output_msg ("Hidden column value is NULL.\n");
            else
                output_msg ("Column value is NULL.\n");
        }
        else
        {
            if (ascii_or_internal == EXIT_FN_ASCII_FORMAT)
                output_msg ("Column value = %s.\n", column.column_value);
            else
            {
                /* Ouput internal value in hex */
                if (column.is_hidden_column)
                    output_msg ("Internal source hidden column value = \n");
                else
                    output_msg ("Internal source column value = \n");

                col_len = column.actual_value_length;

                for (j = 0; j < col_len; j += 10)
                {
                    for (a = column.column_value + j, k = 0;
                         k < 10 && j + k < col_len;
                         a++, k++)
                    {
                        ascii_str[k] =
                                IsPrint (column.column_value[j + k]) ?
                                column.column_value[j + k] : '.';
                        sprintf (hex_str + (k * 3), "%02X ",
                                 (unsigned char)column.column_value[j + k]);
                    }
                    ascii_str[k] = 0;

                    output_msg ("%5d:  %-10s   %-30s\n",
                                j, ascii_str, hex_str);
                }
            }
        }
    }

    free (env_value.buffer);
    free (column.column_value);

    return EXIT_FN_RET_OK;
}

/***************************************************************************
  Display DDL information.
***************************************************************************/
short display_ddl (short source_or_target,
                   short ascii_or_internal)
{
    short result_code;
    ddl_record_def ddl_rec;
    env_value_def env_value;

    /* Setup DDL record structure */
    memset (&ddl_rec, 0, sizeof(ddl_record_def));
    if (source_or_target == EXIT_FN_CURRENT_VAL)
        ddl_rec.source_or_target = EXIT_FN_TARGET_VAL;
    else
        ddl_rec.source_or_target = source_or_target;

    ddl_rec.ddl_type_max_length = 500;
    ddl_rec.ddl_type = (char *)malloc(500);

    ddl_rec.object_type_max_length = 500;
    ddl_rec.object_type = (char *)malloc(500);

    ddl_rec.object_max_length = 500;
    ddl_rec.object_name = (char *)malloc(500);

    ddl_rec.owner_name = (char *)malloc(500);
    ddl_rec.owner_max_length = 500;

    ddl_rec.ddl_text_max_length = 1000;
    ddl_rec.ddl_text = (char *)malloc(1000);

    /* Get DDL properties */
    call_callback (GET_DDL_RECORD_PROPERTIES, &ddl_rec, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving DDL properties.\n", result_code);
        free(ddl_rec.ddl_type);
        free(ddl_rec.object_type);
        free(ddl_rec.object_name);
        free(ddl_rec.owner_name);
        free(ddl_rec.ddl_text);
        return result_code;
    }

    output_msg ("DDL    text: %.*s \n", ddl_rec.ddl_text_length,
                ddl_rec.ddl_text);
    output_msg ("DDL    type: %.*s \n", ddl_rec.ddl_type_length,
                ddl_rec.ddl_type);
    output_msg ("Object type: %.*s \n", ddl_rec.object_type_length,
                ddl_rec.object_type);
    output_msg ("Object name: %.*s \n", ddl_rec.object_length,
                ddl_rec.object_name);
    output_msg ("----------------------------------------- \n");

    free(ddl_rec.ddl_type);
    free(ddl_rec.object_type);
    free(ddl_rec.object_name);
    free(ddl_rec.owner_name);
    free(ddl_rec.ddl_text);


    /* initialize env_value*/
    memset (&env_value, 0, sizeof(env_value_def));
    env_value.max_length = 500;
    env_value.buffer = (char *)malloc(500);
    if (source_or_target == EXIT_FN_CURRENT_VAL)
        env_value.source_or_target = EXIT_FN_TARGET_VAL;
    else
        env_value.source_or_target = source_or_target;

    /* Get table name only */
    call_callback (GET_TABLE_NAME_ONLY, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving table name.\n", result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Table   name only: %.*s \n",
                env_value.actual_length, env_value.buffer);

    /* Get object name only */
    call_callback (GET_OBJECT_NAME_ONLY, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving object name.\n", result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Object  name only: %.*s \n",
                env_value.actual_length, env_value.buffer);

    /* Get schema name only */
    call_callback (GET_SCHEMA_NAME_ONLY, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving schema name.\n", result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Schema  name only: %.*s \n",
                env_value.actual_length, env_value.buffer);

    /* Get catalog name only */
    call_callback (GET_CATALOG_NAME_ONLY, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving catalog name.\n", result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Catalog name only: %.*s \n",
                env_value.actual_length, env_value.buffer);

    /* Get fully qualified table name */
    call_callback (GET_TABLE_NAME, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving fully qualified table name.\n", result_code);
        free (env_value.buffer);
        return result_code;
    }

    output_msg ("Table   name full: %.*s \n",
                env_value.actual_length, env_value.buffer);

    /* Get fully qualified object name */
    call_callback (GET_OBJECT_NAME, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving fully qualified object name.\n", result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Object  name full: %.*s \n",
                env_value.actual_length, env_value.buffer);

    /* Get base object name only */
    call_callback (GET_BASE_OBJECT_NAME_ONLY, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving base object name.\n", result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Base object name only: %.*s \n",
                env_value.actual_length, env_value.buffer);

    /* Get base object schema name only */
    call_callback (GET_BASE_SCHEMA_NAME_ONLY, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving base schema name.\n", result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Base schema name only: %.*s \n",
                env_value.actual_length, env_value.buffer);

    /* Get fully qualified base object name */
    call_callback (GET_BASE_OBJECT_NAME, &env_value, &result_code);
    if (result_code != EXIT_FN_RET_OK)
    {
        output_msg ("Error (%hd) retrieving fully qualified base object name.\n", result_code);
        free (env_value.buffer);
        return result_code;
    }
    output_msg ("Base object name full: %.*s \n",
                env_value.actual_length, env_value.buffer);

    free(env_value.buffer);
    return EXIT_FN_RET_OK;
}

/***************************************************************************
  ER user exit object called from various user exit points in extract and
  replicat.
***************************************************************************/
#ifdef WIN32
__declspec(dllexport) void DDLEXTRACT (exit_call_type_def exit_call_type,
                      exit_result_def    *exit_call_result,
                      exit_params_def    *exit_params)
#else
void DDLEXTRACT (exit_call_type_def exit_call_type,
                exit_result_def    *exit_call_result,
                exit_params_def    *exit_params)
#endif
{
    static short callback_opened = 0;
    short result_code;
    char table_name[200];
    char column_name_buf[200];
    char print_msg[500];
    exit_rec_buf_def *record_buffer = NULL;
    exit_rec_buf_ascii_def *ascii_record_buffer = NULL;
    record_def *record = NULL;
    position_def *position_rec = NULL;
    token_value_def *token_rec = NULL;
    marker_info_def marker_info;
    error_info_def error_info;
    statistics_def statistics;
    env_value_def env_value;
    column_def column;
    col_metadata_def col_meta;
    table_metadata_def table_meta;
    table_def table;
    session_def    session_cs;
    database_defs  dbMetadata;
    event_record_def event_record;
    short i;
    char *tbl_name;
    char srcDB_name[200];
    char tgtDB_name[200];
    char srcDB_locale[200];
    char tgtDB_locale[200];
    size_t actualLen;

    char *dbObjTyp [] = {
            "GENERIC",
            "TABLE",
            "TABLESPACE",
            "TRIGGER",
            "INDEX",
            "CONSTRAINT",
            "SEQUENCE",

            "SCHEMA",
            "CATALOG",
            "NODE",

            "SERVER",
            "LOGIN",
            "PASSWORD",

            "COLUMN",
            "QUALSERVER",
            "MEMBER"
    };

    uint32_t seqno;
    int32_t rba;

    typedef struct
    {
        char ch_seqno[4];
        char ch_rba[4];
    } char_position;
    char_position position_char;

    if (!callback_opened)
    {
        callback_opened = 1;

        /* If Windows, need to load the exported callback function explicitly.
           A Unix Shared object can reference the symbols within the original
           process image file.  There's no need to retrieve a pointer to
           the callback function. */
#ifdef WIN32
        if (!open_callback (exit_params->program_name, "ERCALLBACK"))
        {
            printf ("Error opening ER callback function.\n");
            *exit_call_result = EXIT_ABEND_VAL;
            return;
        }
#endif
    }

    record = (record_def *) malloc(sizeof (record_def));
    record_buffer = (exit_rec_buf_def *) malloc(sizeof(exit_rec_buf_def));
    ascii_record_buffer  = (exit_rec_buf_ascii_def *) malloc(sizeof (exit_rec_buf_ascii_def));

    switch (exit_call_type)
    {
        case EXIT_CALL_START:
            output_msg ("\nUser exit: EXIT_CALL_START.  Called from program: %s\n",
                        exit_params->program_name);
            break;

        case EXIT_CALL_STOP:
            output_msg ("\nUser exit: EXIT_CALL_STOP.\n");
            break;

        case EXIT_CALL_BEGIN_TRANS:
            output_msg ("\nUser exit: EXIT_CALL_BEGIN_TRANS.\n");
            break;

        case EXIT_CALL_END_TRANS:
            output_msg ("\nUser exit: EXIT_CALL_END_TRANS.\n");
            break;

        case EXIT_CALL_CHECKPOINT:
            /*output_msg ("\nUser exit: Extract just performed an EXIT_CALL_CHECKPOINT.\n");*/
            break;

        case EXIT_CALL_PROCESS_MARKER:
            output_msg ("\nUser exit: EXIT_CALL_PROCESS_MARKER.\n");
            break;

        case EXIT_CALL_PROCESS_RECORD:
            /* For extract called before a record buffer is output to the trail */
            output_msg ("\nUser exit: EXIT_CALL_PROCESS_RECORD.\n");

            position_rec = (position_def *) malloc (sizeof(position_def));
            position_rec->position = (char *) malloc (sizeof (uint32_t) + sizeof (int32_t));  /* current expected size of seqno and rba */

            position_rec->ascii_or_internal = EXIT_FN_INTERNAL_FORMAT;
            position_rec->position_type = STARTUP_CHECKPOINT;
            call_callback (GET_POSITION,  position_rec,&result_code);
            if (!result_code)
            {
                memcpy (&position_char, position_rec->position, position_rec->position_len);
                if (LittleEndian())
                {
                    ReverseBytes (&position_char.ch_seqno, sizeof (position_char.ch_seqno));
                    ReverseBytes (&position_char.ch_rba, sizeof (position_char.ch_rba));
                }
                memcpy (&seqno, position_char.ch_seqno, sizeof (seqno));
                memcpy (&rba, position_char.ch_rba, sizeof (rba));
                sprintf (print_msg, "\nGET_POSITION STARTUP_CHECKPOINT, values seqno %ld rba %ld\n",
                         (long) seqno, (long) rba );
                call_callback (OUTPUT_MESSAGE_TO_REPORT, &print_msg, &result_code);
            }

            position_rec->ascii_or_internal = EXIT_FN_INTERNAL_FORMAT;
            position_rec->position_type = CURRENT_CHECKPOINT;
            call_callback (GET_POSITION,  position_rec,&result_code);
            if (!result_code)
            {
                memcpy (&position_char, position_rec->position, position_rec->position_len);
                if (LittleEndian())
                {
                    ReverseBytes (&position_char.ch_seqno, sizeof (position_char.ch_seqno));
                    ReverseBytes (&position_char.ch_rba, sizeof (position_char.ch_rba));
                }
                memcpy (&seqno, position_char.ch_seqno, sizeof (seqno));
                memcpy (&rba, position_char.ch_rba, sizeof (rba));
                sprintf (print_msg, "\nGET_POSITION CURRENT_CHECKPOINT, values seqno %ld rba %ld\n",
                         (long) seqno, (long) rba );
                call_callback (OUTPUT_MESSAGE_TO_REPORT, &print_msg, &result_code);
            }

            free(position_rec->position);
            free(position_rec);

            memset (record, 0, sizeof(record));
            record->source_or_target = EXIT_FN_SOURCE_VAL;
            record->buffer = (char *) record_buffer;

            call_callback (GET_OPERATION_TYPE, record, &result_code);
            if (result_code != EXIT_FN_RET_OK)
            {
                output_msg ("Error (%hd) retrieving operation type.\n", result_code);
                free (record);
                free (record_buffer);
                free (ascii_record_buffer);
                *exit_call_result = EXIT_ABEND_VAL;
                return;
            }

            /* Process DDL Commands */
            if (record->io_type == SQL_DDL_VAL)
            {
                output_msg ("\n*** SOURCE DDL COMMAND***\n");
                result_code = display_ddl (EXIT_FN_SOURCE_VAL,
                                           EXIT_FN_INTERNAL_FORMAT);
                if (result_code != EXIT_FN_RET_OK)
                {
                    output_msg ("Error (%hd) displaying DDL information.\n",
                                result_code);
                    free (record);
                    free (record_buffer);
                    free (ascii_record_buffer);
                    *exit_call_result = EXIT_ABEND_VAL;
                    return;
                }

                /* If we are in Replicat print target DDL info as well */
                output_msg ("\n*** TARGET DDL COMMAND***\n");
                result_code = display_ddl (EXIT_FN_TARGET_VAL,
                                           EXIT_FN_INTERNAL_FORMAT);
                if (result_code != EXIT_FN_RET_NOT_SUPPORTED && result_code != EXIT_FN_RET_OK)
                {
                    output_msg ("Error (%hd) displaying DDL information.\n",
                                result_code);
                    free (record);
                    free (record_buffer);
                    free (ascii_record_buffer);
                    *exit_call_result = EXIT_ABEND_VAL;
                    return;
                }
                break;
            }

        case EXIT_CALL_FATAL_ERROR:
            output_msg ("\nUser exit: EXIT_CALL_FATAL_ERROR.\n");

            memset (&error_info, 0, sizeof(error_info));
            error_info.error_msg = (char *)malloc(500);
            error_info.max_length = 500; /* Including null terminator */

            call_callback (GET_ERROR_INFO, &error_info, &result_code);
            if (result_code != EXIT_FN_RET_OK)
            {
                output_msg ("Error (%hd) retrieving error information.\n", result_code);
                free (error_info.error_msg);
                free (record);
                free (record_buffer);
                free (ascii_record_buffer);
                *exit_call_result = EXIT_ABEND_VAL;
                return;
            }

            output_msg ("Error information:\n"
                        "Error number: %ld.\n"
                        "Error message: %s.\n\n",
                        error_info.error_num,
                        error_info.error_msg);

            free (error_info.error_msg);
            close_callback();
            break;
    }

    free (record);
    free (record_buffer);
    free (ascii_record_buffer);
    *exit_call_result = EXIT_OK_VAL;
    fflush (stdout);
}
