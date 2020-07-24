/* Copyright (c) 2008, 2009, Oracle and/or its affiliates. 
All rights reserved. */

/*

   NAME         xsfile - XStreamOut writing LCRs to file Demo

   DESCRIPTION  
   This program attaches to XStream outbound server and waits for 
   transactions from that server. For each LCR received it prints the 
   contents of each LCR to a file in either CSV format or sqlldr data format. 

   This program waits indefinitely for transactions from the outbound server.
   Hit control-C to interrupt the program. 

   Usage: xsfile -svr <svr_name> -db <db_name> -usr <conn_user>
          -tracing <true|false> -interval <interval_value> -mode <mode>

     svr  : outbound server name
     db   : database name of outbound server
     usr  : connect user to outbound server
     tracing  : true or false for turning on or off tracing
     interval : number of batches before switching to next data file,
                each batch in XStream by default is 30 sec, default interval is 1
     mode     : currently support 'csv' or 'sqlldr' mode

   NOTES:
     -- "supplemental logging on all columns" must be turned on for the 
        tables you want to capture in order to have unchanged columns in 
        the LCRs. 
     -- this demo uses a CSV formatted file, "xsfile.ts", as the tableshape 
        input file. each line in the file contains the shape description 
        for one table. 

   MODIFIED   (MM/DD/YY)
   tianli      09/10/08 - Creation
     
*/

#ifndef OCI_ORACLE
#include <oci.h>
#endif

#ifndef _STDIO_H
#include <stdio.h>
#endif

#ifndef _STDLIB_H
#include <stdlib.h>
#endif

#ifndef _STRING_H
#include <string.h>
#endif

#ifndef _MALLOC_H
#include <malloc.h>
#endif

#ifndef _CTYPE_H
#include <ctype.h>
#endif

/*---------------------------------------------------------------------- 
 *           Internal structures
 *----------------------------------------------------------------------*/ 
#define XSFILE_SUCCESS     1
#define XSFILE_ERROR       0

#define TABLESHAPE_FILE    "xsfile.ts"
#define PROGRESS_FILE      "xsfile.prg"

#define CSV_MODE           1
#define SQLLDR_MODE        2

#define MAX_COLUMNS        1000                       /* max cols in a table */
#define MAX_PRINT_BYTES    (50) /* print max of 50 bytes per col/chunk value */
#define BUF_SIZE           80
#define NUM_BUF_SIZE       256
#define DEFAULT_FS_PREC    6             /* default fractional sec precision */
#define DEFAULT_LF_PREC    9              /* default leading field precision */
#define TS_FORMAT          (oratext *)"DD-MON-YYYY HH24:MI:SS.FF"
#define TSZ_FORMAT         (oratext *)"DD-MON-YYYY HH24:MI:SS.FF TZH:TZM"
#define M_IDEN             30

#define INSERT_CMD         "INSERT"
#define UPDATE_CMD         "UPDATE"
#define DELETE_CMD         "DELETE"
#define M_COL              1000
#define M_FILE_NAMEL       82     /* max lenght of schema_objname_seqnum.dat 
                                   * M_IDEN +"_"+M_IDEN+"_"+16+".dat" */
#define CSV_INTERVAL       1         /* interval how often we close a csv file,
                                      * bump up sequence number and open the 
                                      * new csv file for incoming LCRs */
#define UB2LEN             2
#define UB4LEN             4
#define UB8LEN             8

#define SQLLDR_CHAR         "char"
#define SQLLDR_VARCHAR      "varchar"
#define SQLLDR_VARRAW       "varraw"
#define SQLLDR_LONGVARRAW   "long varraw"
#define SQLLDR_DATE         "date \"mm/dd/yyyy hh24:mi:ss\""
#define SQLLDR_TIMESTAMP    "timestamp \"DD-MON-YYYY HH24:MI:SS.FF\""
#define SQLLDR_TIMESTAMPTZ  "timestamp with time zone \"DD-MON-YYYY HH24:MI:SS.FF TZH:TZM\""
#define SQLLDR_TIMESTAMPLTZ "timestamp with local time zone \"DD-MON-YYYY HH24:MI:SS.FF\""
#define SQLLDR_INTEGER      "integer external"
#define SQLLDR_DECIMAL      "decimal external"
#define SQLLDR_FLOAT        "float external"
#define SQLLDR_IYM          "interval year to month"
#define SQLLDR_IDS          "interval day to second"

#define CHARTYPE_MAX        32767
#define LOBTYPE_MAX         16777215

#define samestr(cmdstr1, cmdlen1, cmdstr2, cmdlen2) \
 (((cmdlen1) == (cmdlen2)) && !memcmp((cmdstr1), (cmdstr2), cmdlen1))

#define samestrci(cmdstr1, cmdlen1, cmdstr2, cmdlen2) \
 (((cmdlen1) == (cmdlen2)) && !strncasecmp((cmdstr1), (cmdstr2), cmdlen1))

#define IS_COMMIT_CMD(cmd_type, cmd_type_len) \
   (samestr((cmd_type), (cmd_type_len), \
             OCI_LCR_ROW_CMD_COMMIT, strlen(OCI_LCR_ROW_CMD_COMMIT)))

#define IS_INSERT_CMD(cmd_type, cmd_type_len) \
   (samestr((cmd_type), (cmd_type_len), \
             OCI_LCR_ROW_CMD_INSERT, strlen(OCI_LCR_ROW_CMD_INSERT)))

#define IS_UPDATE_CMD(cmd_type, cmd_type_len) \
   (samestr((cmd_type), (cmd_type_len), \
             OCI_LCR_ROW_CMD_UPDATE, strlen(OCI_LCR_ROW_CMD_UPDATE)))

#define IS_DELETE_CMD(cmd_type, cmd_type_len) \
   (samestr((cmd_type), (cmd_type_len), \
             OCI_LCR_ROW_CMD_DELETE, strlen(OCI_LCR_ROW_CMD_DELETE)))

#define IS_LOBOP_CMD(cmd_type, cmd_type_len) \
  (samestr((cmd_type), (cmd_type_len), \
           OCI_LCR_ROW_CMD_LOB_WRITE, strlen(OCI_LCR_ROW_CMD_LOB_WRITE)) || \
   samestr((cmd_type), (cmd_type_len), \
           OCI_LCR_ROW_CMD_LOB_TRIM, strlen(OCI_LCR_ROW_CMD_LOB_TRIM)) || \
   samestr((cmd_type), (cmd_type_len), \
           OCI_LCR_ROW_CMD_LOB_ERASE, strlen(OCI_LCR_ROW_CMD_LOB_ERASE)))

#define XSCVS_RAW_DATATYPE(dty) \
  (SQLT_BIN == (dty) || SQLT_BFLOAT == (dty) || SQLT_BDOUBLE == (dty))

#define initctx(csvctx) \
do { \
  (csvctx)->tslist = (tableshape *)0; \
  (csvctx)->tslist_size = 0; \
  (csvctx)->oflist = (openfile *)0; \
  (csvctx)->oflist_size = 0; \
  (csvctx)->seqnum = 1; \
} while (0)

typedef struct params
{
  oratext * user; 
  ub4       userlen;
  oratext   passw[M_IDEN+1];
  ub4       passwlen;
  oratext * dbname;
  ub4       dbnamelen;
  oratext * applynm;
  ub4       applynmlen;
  boolean   tracing;
  ub1       mode;
  int       file_interval;
} params_t;

typedef struct oci                                            /* OCI handles */
{
  OCIEnv      *envp;                                   /* Environment handle */
  OCIError    *errp;                                         /* Error handle */
  OCIServer   *srvp;                                        /* Server handle */
  OCISvcCtx   *svcp;                                       /* Service handle */
  OCISession  *authp;
} oci_t;

/* chunked column data */
typedef struct chunk
{
  void           *data;
  ub4             data_len;
  struct chunk   *next;
} chunk;

/* lob/long/xml column info */
typedef struct chunkColumn
{
  oratext        *colname;
  ub2             colname_len;
  chunk          *chunklist_head;                  /* chunk data list header */
  chunk          *chunklist_tail;                    /* chunk data list tail */
  ub4             chunklist_size;                  /* size of the chunk data */
  ub4             size;                    /* total data size for the column */
  ub2             dty;                                   /* column data type */
  ub2             csid;                                  /* character set id */
  oraub8          flag;                                 /* column level flag */
  boolean         in_dml;         /* whether this column is presented in DML */
  struct chunkColumn *next;
} chunkColumn;

/* scalar column info */
typedef struct columns
{
  oratext       *colname[MAX_COLUMNS]; 
  ub2            colnamel[MAX_COLUMNS]; 
  ub2            coldty[MAX_COLUMNS]; 
  void          *colval[MAX_COLUMNS]; 
  ub2            collen[MAX_COLUMNS]; 
  OCIInd         colind[MAX_COLUMNS]; 
  ub1            colcsf[MAX_COLUMNS]; 
  oraub8         colflg[MAX_COLUMNS]; 
  ub2            colcsid[MAX_COLUMNS];
  boolean        haschunkdata[MAX_COLUMNS];
  chunkColumn   *columnp[MAX_COLUMNS];
  ub2            num_cols;
} columns_t;

/* materialized LCR with all the chunk data */
typedef struct mlcr
{
  void        *lcrp;
  chunkColumn *chunkcollist_head;        /* a list of column with chunk data */
  chunkColumn *chunkcollist_tail;        /* a list of column with chunk data */
  ub2          chunkcollist_size;                 /* size of the chunkcolist */
  oratext     *src_db_name;
  ub2          src_db_namel;
  oratext     *cmd_type;
  ub2          cmd_typel;
  oratext     *owner;
  ub2          ownerl;
  oratext     *oname;
  ub2          onamel;
  oratext     *txid;
  ub2          txidl;
  ub1         *position;
  ub2          positionl;
  columns_t    new_columns;
  columns_t    old_columns;
} mlcr_t;

/* table shape */
typedef struct tableshape
{
  oratext             schema_name[M_IDEN];
  ub2                 schema_name_len;
  oratext             obj_name[M_IDEN];
  ub2                 obj_name_len;
  oratext            *column_namelist[M_COL];
  ub2                 column_namelen[M_COL];
  oratext            *column_deflist[M_COL];
  ub2                 column_deflen[M_COL];
  boolean             column_rawtype[M_COL];
  boolean             column_lobtype[M_COL];
  ub2                 columnlist_size;
  struct tableshape  *next;
} tableshape;

typedef struct openfile
{
  oratext       filename[M_FILE_NAMEL];
  ub2           filename_len;
  FILE         *fp;
  struct openfile *next;
} openfile;

typedef struct xsfilectx
{
  oci_t         *src_ocip;
  oci_t         *dst_ocip;
  params_t       params;
  tableshape    *tslist;                            /* a list of table shape */
  ub4            tslist_size;
  openfile      *oflist;                            /* a list of opened file */
  ub4            oflist_size;
  ub8            seqnum;
} xsfilectx;

static void connect_db(params_t *opt_params_p, oci_t ** ocip);
static void disconnect_db(oci_t * ocip);
static void ocierror(oci_t * ocip, char * msg);
static void ocierror2(oci_t * ocip, char * msg);
static void attach(oci_t *ocip, params_t *paramsp);
static void detach(oci_t *ocip);
static void get_lcrs(oci_t *ocip);
static void print_chunk (ub1 *chunk_ptr, ub4 chunk_len, ub2 dty);
static void print_ddl(oci_t *ocip, void *lcrp);      
static void get_inputs(params_t *params, int argc, char ** argv);
static void getTableShape(oci_t *ocip, mlcr_t *mlcrp, tableshape **ts, 
                          ub1 lcrtyp);
static sword materializeLCR(oci_t *ocip, mlcr_t *mlcrp, oraub8 flag, 
                           ub1 lcrtype);
static sword writeMLCR(oci_t *ocip, mlcr_t *mlcrp, ub1 lcrtype, FILE *fp, 
                       tableshape *ts);
static void freeMLCR(mlcr_t *mlcrp);
static void print_mlcr(oci_t *ocip, mlcr_t *mlcrp, ub1 lcrtype);
static void print_col_data(oci_t *ocip, ub2 col_value_type,
                           oratext **colname, ub2 *colnamel,
                           ub2 *coldty, void **colval,
                           ub2 *collen, OCIInd *colind,
                           ub1 *colcsf, oraub8 *colflg,
                           ub2 *colcsid, ub2 num_cols);
static sword fwrite_mlcr(FILE *fp, oci_t *ocip, mlcr_t *mlcrp, tableshape *ts);
static sword fwrite_field (FILE *fp, const void *src, size_t src_size);
static sword fwrite_raw_field (FILE *fp, const void *src, 
                               size_t src_size, ub2 lenfield_len);
static sword fwrite_field_unquote(FILE *fp, const void *src, size_t src_size);
static sword fwrite_mlcr_ctl_cols(FILE *fp, oci_t *ocip, mlcr_t *mlcrp, 
                                  ub2 col_value_type);
static sword fwrite_mlcr_cols(FILE *fp, oci_t *ocip, mlcr_t *mlcrp, 
                              ub2 col_value_type, tableshape *ts);
static sword fwrite_one_col(FILE *fp, oci_t *ocip, mlcr_t *mlcrp, 
                            ub2 col_value_type, columns_t *cols, ub2 idx,
                            boolean previous_raw);
static void initts(xsfilectx *ctx);
static sword get_token(FILE *fp, oratext *token, ub2 *len, int *c);
static sword clean_token(oratext **token, ub2 *len);
static sword getDataFileHandle(mlcr_t *mlcrp, FILE **fp);
static sword gen_one_ctlfile(tableshape *ts);

#define OCICALL(ocip, function) do {\
sword status=function;\
if (OCI_SUCCESS==status) break;\
else if (OCI_ERROR==status) \
{ocierror(ocip, (char *)"OCI_ERROR");\
exit(1);}\
else {printf("Error encountered %d\n", status);\
exit(1);}\
} while(0)

#define writebytes(fp, buf, len) do { \
  if ((buf)) \
    fwrite((void *)buf, sizeof(ub1), (len), (fp)); \
} while (0)

static xsfilectx csvctx;
static boolean tracing = FALSE;
static ub1 mode = CSV_MODE;
static ub4 interval = CSV_INTERVAL;


/*---------------------------------------------------------------------
 *                M A I N   P R O G R A M
 *---------------------------------------------------------------------*/
int main(int argc, char **argv)
{
  oci_t        *ocip = (oci_t *)NULL;

  initctx(&csvctx);                                     /* initialize csvctx */

  get_inputs(&(csvctx.params), argc, argv);  /* parse command line arguments */

  initts(&csvctx);                   /* initialize tableshape list in csvctx */
  
  /* connect to the database */
  connect_db(&(csvctx.params), &ocip);

  /* Attach to outbound server */
  attach(ocip, &(csvctx.params));

  /* Get LCR loop */
  get_lcrs(ocip);

  /* Detach from outbound server */
  detach(ocip);

  /* Disconnect from database */
  disconnect_db(ocip);

  free(ocip);
  return 0;

}

/*---------------------------------------------------------------------
 * initialize tableshape list by parsing the xsfile.ts file
 *---------------------------------------------------------------------*/
static void initts(xsfilectx *ctx)
{
  FILE *fp = NULL;
  int c = 0;
  tableshape *ts = NULL;
  oratext token[M_IDEN+2];
  oratext *tmptext = (oratext *)0;
  ub2 i = 0;
  sword ret = OCI_SUCCESS;
  ub2 len = 0;
  ub2 tmplen = 0;
  oratext *colname = (oratext *)0;
  oratext *coldef = (oratext *)0;

  fp = fopen(TABLESHAPE_FILE,"r");
  if (fp)
  {
    while(TRUE)           /* loop for processing one line in tableshape file */
    {
      if (c == EOF)
        break;

      if (c == '\n' || c == '\r')          /* line feed or carriage return */
      {
        /* handle one tableshape entry in the list */
        c =0;
  
        if (ctx->tslist)
        {
          tableshape *tmp = ctx->tslist;
          while (tmp->next) tmp = tmp->next;
          tmp->next = ts;  
        }
        else
          ctx->tslist = ts;
        
        continue;
      }
      else
      {
        int col_idx = 0;
        
        ts = malloc(sizeof(tableshape));

        /* first get schema name and table name */
        ret = get_token(fp, token, &len, &c);
        if (OCI_SUCCESS != ret)
        {
          printf ("read invalid field (%.*s) in tableshape file\n", 
                  len, token);
          exit(0);  
        }
        else
        {
          if (c == EOF)
            break;
        }
        
        tmptext = token;
        tmplen = len;
        
        ret = clean_token(&tmptext,&tmplen);
        if (OCI_SUCCESS != ret)
        {
          printf ("tableshape field %.*s is invalid \n",len, token);
          exit(0);  
        }
        
        memcpy(ts->schema_name, tmptext, tmplen);
        ts->schema_name_len = tmplen;

        if (tracing)
          printf("read field (%.*s), current char = %d \n",
                 ts->schema_name_len,ts->schema_name, c);  

        /* expect c == ',' if not, then raise error */
        if (c != ',')
        {
          printf ("tableshape must start with schema & table name\n");
          exit(0);
        }
        
        ret = get_token(fp, token, &len, &c);
        if (OCI_SUCCESS != ret)
        {
          printf ("tableshape field %.*s is invalid \n",len, token);
          exit(0);  
        }
        tmptext = token;
        tmplen = len;
        
        ret = clean_token(&tmptext,&tmplen);
        if (OCI_SUCCESS != ret)
        {
          printf ("tableshape field %.*s is invalid \n",len, token);
          exit(0);  
        }

        memcpy(ts->obj_name, tmptext, tmplen);
        ts->obj_name_len = tmplen;

        if (tracing)
          printf("read field (%.*s), current char = %d \n",
                 ts->obj_name_len,ts->obj_name, c);
   
        /* now we are processing column name and definition pair */
        while(TRUE)
        {

          if (c == '\n' || c == '\r' || c == EOF)
            break;

          /* get the column name token first */
          ret = get_token(fp, token, &len, &c);
          if (OCI_SUCCESS != ret)
          {
            printf ("read invalid field (%.*s) in tableshape file\n",
                    len, token);
            exit(0);  
          }
          tmptext = token;
          tmplen = len;
          
          ret = clean_token(&tmptext,&tmplen);
          if (OCI_SUCCESS != ret)
          {
            printf ("tableshape field %.*s is invalid \n",len, token);
            exit(0);  
          }
          
          /* expect c == ',' if not, then raise error */
          if (c != ',')
          {
            printf ("tableshape must start with schema & table name\n");
            exit(0);
          }
          colname = malloc(sizeof(oratext)*tmplen);
          memcpy(colname, tmptext, tmplen);
          (ts->column_namelist)[col_idx] = colname;
          (ts->column_namelen)[col_idx] = tmplen; 

          if (tracing)
            printf("read colname (%.*s), current char = %d \n",
                   (ts->column_namelen)[col_idx],
                   (ts->column_namelist)[col_idx], c);  

          /* then get the column definition token */
          ret = get_token(fp, token, &len, &c);
          if (OCI_SUCCESS != ret)
          {
            printf ("read invalid field (%.*s) in tableshape file\n",
                    len, token);
            exit(0);  
          }
          tmptext = token;
          tmplen = len;
          
          ret = clean_token(&tmptext,&tmplen);
          if (OCI_SUCCESS != ret)
          {
            printf ("tableshape field %.*s is invalid \n",len, token);
            exit(0);  
          }

          coldef = malloc(sizeof(oratext)*tmplen);
          memcpy(coldef, tmptext, tmplen);
          (ts->column_deflist)[col_idx] = coldef;
          (ts->column_deflen)[col_idx] = tmplen;
          
          if (tracing)
            printf("read coldef (%.*s), current char = %d \n",
                   (ts->column_deflen)[col_idx],
                   (ts->column_deflist)[col_idx], c);  

          /* down with one column, increase size counters */
          ts->columnlist_size++;
          col_idx++;
        }
      }
    }
  }
  else
  {
    printf("fail to open tableshape file: xsfile.ts\n");
    exit(0);
  }

  fclose(fp);

  /* read sequence number from progress file */
  fp = fopen(PROGRESS_FILE,"r");
  if (fp)
  {
    fscanf(fp, "%d",&(csvctx.seqnum));
    fclose(fp);
    csvctx.seqnum++;
    printf("xsfile demo restarts from sequence# %d\n", csvctx.seqnum);
  }
  else
  {
    printf("no progress file, xsfile demo starts with sequence# 1\n");
  }

  ts = csvctx.tslist;
  /* generate control files for each table shape */
  if (SQLLDR_MODE == mode)
  {
    while (ts)
    {
      gen_one_ctlfile(ts);
      ts = ts->next;
    }
  }
}

/*---------------------------------------------------------------------
 * generate the sql*loader control file for one table
 *---------------------------------------------------------------------*/
static sword gen_one_ctlfile(tableshape *ts)
{
  sword ret = OCI_SUCCESS;
  oratext ctlfilename[M_FILE_NAMEL];
  ub2 prefix = ts->schema_name_len + ts->obj_name_len + 1;
  oratext *tmp = ctlfilename;
  FILE *fp = (FILE *)fp;
  ub2 i = 0;
  
  memcpy(tmp, ts->schema_name, ts->schema_name_len);
  tmp += ts->schema_name_len;
  memcpy(tmp, "_", strlen("_"));
  tmp += strlen("_");
  memcpy(tmp, ts->obj_name, ts->obj_name_len);
  tmp += ts->obj_name_len;
  memcpy(tmp, ".ctl", strlen(".ctl"));  

  ctlfilename[ts->schema_name_len+ts->obj_name_len+1+4] = '\0';
  if (tracing)
  {
    printf("generating control file %s for %.*s.%.*s\n",
           ctlfilename, 
           ts->schema_name_len, ts->schema_name, 
           ts->obj_name_len, ts->obj_name);
  }
  
  fp = fopen((char *)ctlfilename,"w");
  
  /* for control columns, default size should be sufficient, no need
   * to include max size 
   */
  fprintf(fp, "LOAD DATA\n");
  fprintf(fp, "infile * \n");
  fprintf(fp, "append\n");
  fprintf(fp, "into table %.*s_chg\n",ts->obj_name_len,ts->obj_name);
  fprintf(fp, "fields terminated by ','\n");
  fprintf(fp, "(\n");

  fprintf(fp, "%s,\n", "  command_type      char enclosed by '\"'");
  fprintf(fp, "%s,\n", "  object_owner      char enclosed by '\"'");
  fprintf(fp, "%s,\n", "  object_name       char enclosed by '\"'");
  fprintf(fp, "%s,\n", "  position          varraw");
  fprintf(fp, "%s,\n", "  column_list_type  char enclosed by '\"'");

  for (i=0; i<ts->columnlist_size;i++)
  {
    oratext *def = ts->column_deflist[i];
    ub2 def_len = ts->column_deflen[i];
    ub4 max_size = 0;
    boolean enclosed = TRUE;
    ts->column_rawtype[i] = FALSE;
    ts->column_lobtype[i] = FALSE;

    /* character type, char, varchar */ 
    if (((ts->column_deflen[i] >= strlen("char")) &&
         samestrci((char *)ts->column_deflist[i], strlen("char"),
                   "char", strlen("char"))) ||
        ((ts->column_deflen[i] >= strlen("varchar")) &&
         samestrci((char *)ts->column_deflist[i],strlen("varchar"),
                   "varchar", strlen("varchar"))))
    {
      def = (oratext *)SQLLDR_CHAR;
      def_len = (ub2)strlen(SQLLDR_CHAR);
      max_size = CHARTYPE_MAX;
    }
    /* date */ 
    else if ((ts->column_deflen[i] >= strlen("date")) &&
             samestrci((char *)ts->column_deflist[i],strlen("date"),
                       "date",strlen("date")))
    {
      def = (oratext *)SQLLDR_DATE;
      def_len = (ub2)strlen(SQLLDR_DATE);
    }
    /* timestamp */
    else if ((ts->column_deflen[i] >= strlen("timestamp")) &&
             samestrci((char *)ts->column_deflist[i],strlen("timestamp"),
                       "timestamp",strlen("timestamp")))
    {
      def = (oratext *)SQLLDR_TIMESTAMP;
      def_len = (ub2)strlen(SQLLDR_TIMESTAMP);
    }
    /* timestamp with timezone*/
    else if ((ts->column_deflen[i] >= strlen("timestamp with time zone")) &&
             samestrci((char *)ts->column_deflist[i],
                       strlen("timestamp with time zone"),
                       "timestamp with time zone",
                       strlen("timestamp with time zone")))
    {
      def = (oratext *)SQLLDR_TIMESTAMPTZ;
      def_len = (ub2)strlen(SQLLDR_TIMESTAMPTZ);
    }
    /* timestamp with local timezone*/
    else if ((ts->column_deflen[i] >= 
              strlen("timestamp with local time zone")) &&
             samestrci((char *)ts->column_deflist[i],
                       strlen("timestamp with local time zone"),
                       "timestamp with local time zone",
                       strlen("timestamp with local time zone")))
    {
      def = (oratext *)SQLLDR_TIMESTAMPLTZ;
      def_len = (ub2)strlen(SQLLDR_TIMESTAMPLTZ);
    }
    /* numeric type, integer, number, float, double */
    else if (((ts->column_deflen[i] >= strlen("number")) &&
              samestrci((char *)ts->column_deflist[i],strlen("number"),
                        "number",strlen("number"))) ||
             ((ts->column_deflen[i] >= strlen("float")) &&
              samestrci((char *)ts->column_deflist[i],strlen("float"),
                        "float",strlen("float"))) ||
             ((ts->column_deflen[i] >= strlen("double")) &&
              samestrci((char *)ts->column_deflist[i],strlen("double"),
                        "double",strlen("double"))) ||
             ((ts->column_deflen[i] >= strlen("integer")) &&
              samestrci((char *)ts->column_deflist[i],strlen("integer"),
                        "integer",strlen("integer"))))
    {
      def = (oratext *)SQLLDR_DECIMAL;
      def_len = (ub2)strlen(SQLLDR_DECIMAL);
    }
    /* clob, long */
    else if (((ts->column_deflen[i] >= strlen("clob")) &&
             samestrci((char *)ts->column_deflist[i],strlen("clob"),
                       "clob",strlen("clob"))) ||
             ((ts->column_deflen[i] >= strlen("long")) &&
              samestrci((char *)ts->column_deflist[i],strlen("long"),
                        "long",strlen("long"))))
    {
      ts->column_lobtype[i] = TRUE;
      def = (oratext *)SQLLDR_CHAR;
      def_len = (ub2)strlen(SQLLDR_CHAR);
      max_size = LOBTYPE_MAX;
    }
    /* raw, bfloat, bdouble */
    else if (((ts->column_deflen[i] >= strlen("raw")) &&
              samestrci((char *)ts->column_deflist[i],strlen("raw"),
                        "raw",strlen("raw"))) ||
             ((ts->column_deflen[i] >= strlen("binary_float")) &&
              samestrci((char *)ts->column_deflist[i],strlen("binary_float"),
                        "binary_float",strlen("binary_float"))) ||
             ((ts->column_deflen[i] >= strlen("binary_double")) &&
              samestrci((char *)ts->column_deflist[i],strlen("binary_double"),
                        "binary_double",strlen("binary_double"))))
    {
      ts->column_rawtype[i] = TRUE;
      def = (oratext *)SQLLDR_VARRAW;
      def_len = (ub2)strlen(SQLLDR_VARRAW);
      max_size = CHARTYPE_MAX;
      enclosed = FALSE;
    }
    /* blob, longraw */
    else if (((ts->column_deflen[i] >= strlen("blob")) &&
              samestrci((char *)ts->column_deflist[i],strlen("blob"),
                        "blob",strlen("blob"))) ||
             ((ts->column_deflen[i] >= strlen("long raw")) &&
              samestrci((char *)ts->column_deflist[i],strlen("long raw"),
                        "long raw",strlen("long raw"))))
    {
      ts->column_rawtype[i] = TRUE;
      ts->column_lobtype[i] = TRUE;
      def = (oratext *)SQLLDR_LONGVARRAW;
      def_len = (ub2)strlen(SQLLDR_LONGVARRAW);
      max_size = LOBTYPE_MAX;
      enclosed = FALSE;
    }
    else
    {
      printf("envounter unsupportted data type (%.*s)\n", 
              ts->column_deflen[i], ts->column_deflist[i]);
    }
    

    fprintf(fp, "  %.*s    %.*s", 
              ts->column_namelen[i], ts->column_namelist[i],
              def_len, def);      

    if (0 != max_size)
    {
      fprintf(fp, "(%u)",max_size);  
    }

    if (enclosed)
      fprintf(fp, " enclosed by '\"'");

    if (i == ts->columnlist_size -1)
    {
      fprintf(fp, "\n");
    }
    else
    {
      fprintf(fp, ",\n");
    }
  }
  

  fprintf(fp, ")\n");
  fprintf(fp, "BEGINDATA\n");
  fclose(fp);
  return ret;
  
}

/*---------------------------------------------------------------------
 * This function remove leading space and enclosing '"' in the token
 *---------------------------------------------------------------------*/
static sword clean_token(oratext **token, ub2 *len)
{
  ub2 i = 0;
  sword ret = OCI_SUCCESS;
  
  while (isspace((*token)[0]))
  {
    (*token)++;
    (*len)--;
  }

  if ((*token)[i] == '"')
  {
    (*token)++;
    (*len)--;
    /* check if '"' is properly paired */
    if ((*token)[(*len)-1] != '"')
      ret = OCI_ERROR;
    else
      (*len)--;
  }
  
  return ret;
}

/*---------------------------------------------------------------------
 * This function get one token from the tableshape csv file
 *---------------------------------------------------------------------*/
static sword get_token(FILE *fp, oratext *token, ub2 *len, int *c)
{
  ub2 i = 0;
  sword ret = OCI_SUCCESS;

  *c = getc(fp);
  
  while (*c != EOF && *c != ',' && *c != '\n' && *c != '\r')
  {
    if (i >= M_IDEN+2)
    {
      ret = OCI_ERROR;
      break;
    }

    token[i] = (oratext)*c;
    i++;
    
    *c = getc(fp);
  }

  *len = i;
  return ret;
}


/*---------------------------------------------------------------------
 * connect_db - Connect to the database
 *---------------------------------------------------------------------*/
void connect_db(params_t *params_p, oci_t ** ociptr)
{
  oci_t        *ocip;

  printf ("Connect to Oracle as %.*s@%.*s\n",
          params_p->userlen, params_p->user, 
          params_p->dbnamelen, params_p->dbname);
       
  ocip = (oci_t *)malloc(sizeof(oci_t));

  if (OCIEnvCreate(&ocip->envp, OCI_OBJECT, (dvoid *)0,
                   (dvoid * (*)(dvoid *, size_t)) 0,
                   (dvoid * (*)(dvoid *, dvoid *, size_t))0,
                   (void (*)(dvoid *, dvoid *)) 0,
                   (size_t) 0, (dvoid **) 0 ))
  {
    ocierror(ocip, (char *)"OCIEnvCreate() failed");
  }

  if (OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **) &ocip->errp,
                     (ub4) OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0))
  {
    ocierror(ocip, (char *)"OCIHandleAlloc(OCI_HTYPE_ERROR) failed");
  }

  /* allocate the server handle */
  OCICALL(ocip,
          OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **) &ocip->srvp,
                         OCI_HTYPE_SERVER, (size_t) 0, (dvoid **) 0));

  /* create a server context */
  OCICALL(ocip,
          OCIServerAttach (ocip->srvp, ocip->errp, 
                           (oratext *)params_p->dbname, 
                           (sb4)params_p->dbnamelen, OCI_DEFAULT));

  /* allocate the service handle */
  OCICALL(ocip,
          OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **) &ocip->svcp,
                         OCI_HTYPE_SVCCTX, (size_t) 0, (dvoid **) 0));

  /* set attribute server context in the service context */
  OCICALL(ocip,
          OCIAttrSet((dvoid *) ocip->svcp, OCI_HTYPE_SVCCTX,
                     (dvoid *) ocip->srvp, (ub4) 0, OCI_ATTR_SERVER,
                     (OCIError *) ocip->errp));

  /* allocate a session handle */
  OCICALL(ocip,
          OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **)&ocip->authp,
                        (ub4) OCI_HTYPE_SESSION, (size_t) 0, (dvoid **) 0));

  /* set the username in the session */
  OCICALL(ocip,
          OCIAttrSet((dvoid *) ocip->authp, (ub4) OCI_HTYPE_SESSION,
                     (dvoid *) params_p->user, (ub4)  params_p->userlen,
                     (ub4) OCI_ATTR_USERNAME, ocip->errp));

  /* set the password in the session */
  OCICALL(ocip,
          OCIAttrSet((dvoid *) ocip->authp, (ub4) OCI_HTYPE_SESSION,
                     (dvoid *) params_p->passw, (ub4) params_p->passwlen,
                     (ub4) OCI_ATTR_PASSWORD, ocip->errp));

  OCICALL(ocip,
          OCISessionBegin(ocip->svcp,  ocip->errp, ocip->authp,
                          OCI_CRED_RDBMS, (ub4)OCI_DEFAULT));

  OCICALL(ocip,
          OCIAttrSet((dvoid *) ocip->svcp, (ub4) OCI_HTYPE_SVCCTX,
                     (dvoid *) ocip->authp, (ub4) 0,
                     (ub4) OCI_ATTR_SESSION, ocip->errp));

  if (*ociptr == (oci_t *)NULL)
  {
    *ociptr = ocip;
  }
}

/*---------------------------------------------------------------------
 * attach - Attach to outbound server
 *---------------------------------------------------------------------*/
static void attach(oci_t * ocip, params_t *paramsp)
{
  sword       err;

  printf ("Attach to XStream Outbound '%.*s'\n", 
          paramsp->applynmlen, paramsp->applynm);

  err = OCIXStreamOutAttach(ocip->svcp, ocip->errp, paramsp->applynm, 
                            (ub2)paramsp->applynmlen, (ub1 *)0, 0, 
                            OCI_DEFAULT);
  if (err)
    ocierror(ocip, (char *)"OCIXStreamOutAttach failed");
}



/*---------------------------------------------------------------------
 * getTableShape - get the table shape by searching the tableshape list
 *                 if none exists, print a waring. this will cause the 
 *                 LCR being dropped later on since there is no tableshape
 *                 defined for that object. 
 *---------------------------------------------------------------------*/      
static void getTableShape(oci_t *ocip, mlcr_t *mlcrp, tableshape **ts, 
                          ub1 lcrtype)
{
  tableshape *tmpts = NULL;
  boolean found = FALSE;
  
  /* check if it's already in the tableshape list, if not print warning
   * and the LCR will be ignored. Alternatively one can contact target 
   * DB to get the tableshape */
  tmpts = csvctx.tslist;
  
  while (tmpts)
  {
    if (samestrci((char *)tmpts->schema_name, 
                  tmpts->schema_name_len,
                  (char *)mlcrp->owner, 
                  mlcrp->ownerl) &&
        samestrci((char *)tmpts->obj_name, 
                  tmpts->obj_name_len,
                  (char *)mlcrp->oname, 
                  mlcrp->onamel))
    {
      found = TRUE;
      *ts = tmpts;
      break;
    }
    tmpts = tmpts->next;
  }

  if (!found)
  {
    printf("cannot find object (%.*s).(%.*s) in tableshape list\n\n",
           mlcrp->ownerl, mlcrp->owner, mlcrp->onamel, mlcrp->oname);
    if (tracing)
    {
      print_mlcr(ocip, mlcrp, lcrtype);
    }
  }
}

/*---------------------------------------------------------------------
 * getDataFileHandle - get the data file handle for the LCR
 *---------------------------------------------------------------------*/
static sword getDataFileHandle(mlcr_t *mlcrp, FILE **fp)
{
  sword        ret      = OCI_SUCCESS; 
  openfile    *oflist   = csvctx.oflist;
  oratext     *filename  = (oratext *)0;
  ub2          filenamel = 0;
  boolean      found    = FALSE;
  oratext      buf[16]; /* for converting sequence # to char */
  ub2          tmplen = 0;
  oratext     *tmp;
  
  /* construct the csv file name based on schema/object name */
  tmplen = (ub2)snprintf((char *)buf, 16, "%d", csvctx.seqnum);

  filenamel = mlcrp->ownerl + (ub2)strlen("_") + 
    mlcrp->onamel + (ub2)strlen("_") + tmplen + (ub2)strlen(".dat");

  if (tracing)
    printf("tmplen = %d, file name length is: %d\n",tmplen, filenamel);
  
  filename = malloc((filenamel+1)*sizeof(oratext));
  tmp = filename;
  
  memcpy(tmp, mlcrp->owner, mlcrp->ownerl);
  tmp += mlcrp->ownerl;

  memcpy(tmp, "_", strlen("_"));
  tmp += strlen("_");

  memcpy(tmp, mlcrp->oname, mlcrp->onamel);
  tmp += mlcrp->onamel;

  memcpy(tmp, "_", strlen("_"));
  tmp += strlen("_");

  memcpy(tmp, buf, tmplen);
  tmp += tmplen;

  if (SQLLDR_MODE == mode)
    memcpy(tmp, ".dat", strlen(".dat"));
  else if (CSV_MODE == mode)
    memcpy(tmp, ".csv", strlen(".csv"));

  filename[filenamel] = '\0';

  if (tracing)     
    printf("file name: %.*s\n",filenamel, filename);
  
  while(oflist)
  {
    if (samestrci((char *)oflist->filename, 
                  oflist->filename_len,
                  (char *)filename, 
                  filenamel))
    {
      *fp = oflist->fp;
      found = TRUE;
      break;
    }
    oflist = oflist->next;
  }
  
  if (!found)
  {
    openfile *tmp1 = csvctx.oflist;
    openfile *tmp2 = malloc(sizeof(openfile));
    
    *fp = fopen((char *)filename,"w");
    if (!(*fp))
    {
      printf("cannot open the csv file (%.*s)for received LCR\n",
             filenamel, filename);
      exit(0);
    }
    
    memcpy(tmp2->filename, filename, filenamel);
    tmp2->filename_len = filenamel;
    tmp2->fp = (*fp);
    csvctx.oflist = tmp2;
    tmp2->next = tmp1;
    csvctx.oflist_size++;
  }

  free(filename);

  return OCI_SUCCESS;
}

/*---------------------------------------------------------------------
 * get_lcrs - Execute loop to get lcrs from outbound server.
 *---------------------------------------------------------------------*/
static void get_lcrs(oci_t * ocip)
{
  sword       status = OCI_SUCCESS;
  void       *lcr;
  ub1         lcrtype;
  oraub8      flag;
  mlcr_t      mlcr;
  FILE       *fp;
  tableshape *ts;
  ub1         proclwm[OCI_LCR_MAX_POSITION_LEN];
  ub2         proclwm_len = 0;
  ub2         lcrcnt = 0;
  ub2         batchcnt = 0;
  
  while (status == OCI_SUCCESS)
  {
    /* Execute non-callback call to receive LCRs */
    while ((status = OCIXStreamOutLCRReceive(ocip->svcp, ocip->errp,
                                             &lcr, &lcrtype, &flag, 
                                             (ub1 *)0, (ub2 *)0, OCI_DEFAULT))
                          == OCI_STILL_EXECUTING)
    {
      mlcr.lcrp = (void *)lcr;
      mlcr.chunkcollist_head = (chunkColumn *)0;
      mlcr.chunkcollist_tail = (chunkColumn *)0;
      mlcr.chunkcollist_size = 0;
      ts = NULL;
      
      /* skip DDL lcrs */
      if (OCI_LCR_XDDL == lcrtype)
      {
        if (tracing)
        {
          printf("skip DDL LCR:\n");
          print_ddl(ocip, lcr);     
        }
        continue;
      }
      
      /* fully materialize the lcr */
      materializeLCR(ocip, &mlcr, flag, lcrtype);

      if(IS_COMMIT_CMD(mlcr.cmd_type, mlcr.cmd_typel))
      {
        if (tracing)
        {
          printf("skip COMMIT LCR:\n");
          print_mlcr(ocip, &mlcr, lcrtype);
        }
        /* update the processed low watermark */
        memcpy(proclwm, mlcr.position, mlcr.positionl);
        proclwm_len = mlcr.positionl;
        continue;
      }
 
      /* get the table shape for the lcr */
      getTableShape(ocip, &mlcr, &ts, lcrtype);

      if (!ts)
      {
        /* lcr is not in the table shape list, ignore it */  
        printf("Warning: ignoring the LCR, no tableshape\n");
        /* update the processed low watermark */
        memcpy(proclwm, mlcr.position, mlcr.positionl);
        proclwm_len = mlcr.positionl;
        continue;
      }
      
      /* set the column name list */
      if (tracing)
      {
        ub2 i = 0;
        
        for (i=0; i< (ts)->columnlist_size;i++)
        {
          printf("%dth column name is: %s\n",i,ts->column_namelist[i]);
          printf("%dth column name len is: %d\n",i,
                 strlen((char *)ts->column_namelist[i]));
        }
      }

      /* get the file handler for the LCR */
      getDataFileHandle(&mlcr, &fp);
      
      /* write the mlcr to the file */
      if (fp)
        writeMLCR(ocip, &mlcr, lcrtype, fp, ts);
      else
      {
        printf("fail to write mlcr:\n");
        exit(0);
      }

      lcrcnt ++;
      /* update the processed low watermark */
      memcpy(proclwm, mlcr.position, mlcr.positionl);
      proclwm_len = mlcr.positionl;
      
      /* free the mlcr including all materialized chunk data */
      freeMLCR(&mlcr);
    }

    if (status == OCI_ERROR)
      ocierror(ocip, (char *)"get_lcrs() failed");

    /* once get out of while loop, batch is end, need to do 
     * some bookkeeping and status update */

    if (lcrcnt > 0)
      batchcnt++;
    
    if (batchcnt >= interval)
    {
      FILE *pfp = (FILE *)0;
      openfile *tmp = csvctx.oflist;
      openfile *prv = csvctx.oflist;

      /* close all open files and bump up sequence # and
       * free all the openfile list */
      while (tmp)
      {
        fclose(tmp->fp);
        tmp = tmp->next;
        free(prv);
        prv = tmp;
      }
            
      csvctx.oflist = (openfile *)0;

      /* write seq # to progress file */
      pfp = fopen(PROGRESS_FILE,"w");
      if (pfp)
      {
        fprintf(pfp, "%d",csvctx.seqnum);
        fclose(pfp);
      }
      else
      {
        printf("fail to write progress into %s\n",PROGRESS_FILE);
        exit(0);
      }

      /* bump up seqnum */
      csvctx.seqnum++;

      if (proclwm_len > 0)
      {
        /* now that we have writen all the lcrs within specified CSV_INTERVAL 
         * into data files, we need to tell outbound server we are done with
         * those LCRs */
        OCICALL(ocip, 
                OCIXStreamOutProcessedLWMSet(ocip->svcp, ocip->errp, 
                                             proclwm, proclwm_len, 
                                             OCI_DEFAULT));
      }
      else
      {
        printf("the processed lwm is invalid: length=%d\n",proclwm_len);
        exit(0);
      }

      batchcnt = 0;
    }
    

    if (status == OCI_ERROR)
      ocierror(ocip, (char *)"get_lcrs() failed");

    /* clear the lcr count before go back to receive lcr loop */
    lcrcnt = 0;
  }
}

/*---------------------------------------------------------------------
 * print_ddl - Print info of given DDL LCR.
 *---------------------------------------------------------------------*/
static void print_ddl(oci_t *ocip, void *lcrp)
{
  sword     status;
  oratext  *object_type;
  ub2       object_typel;
  oratext  *ddl_text;
  ub4       ddl_textl;
  oratext  *logon_user;
  ub2       logon_userl;
  oratext  *current_schema;
  ub2       current_schemal;
  oratext  *base_table_owner;
  ub2       base_table_ownerl;
  oratext  *base_table_name;
  ub2       base_table_namel;

  status = OCILCRDDLInfoGet(ocip->svcp, ocip->errp,
                            &object_type, &object_typel,
                            &ddl_text, &ddl_textl,
                            &logon_user, &logon_userl,
                            &current_schema, &current_schemal,
                            &base_table_owner, &base_table_ownerl,
                            &base_table_name, &base_table_namel,
                            (oraub8*)0, lcrp, 0);

  printf("DDL LCR: obj_type=%.*s logon_usr=%.*s schema=%.*s\n", 
         object_typel, object_type, logon_userl, logon_user,
         current_schemal, current_schema);
  if (base_table_namel)
    printf("Table=%.*s.%.*s\n", base_table_ownerl, base_table_owner,
           base_table_namel, base_table_name);
  printf("Statement=%.*s\n", ddl_textl, ddl_text);
}

/*---------------------------------------------------------------------
 * print_col_data - Print row LCR column values.
 *---------------------------------------------------------------------*/
static void print_col_data(oci_t *ocip, ub2 col_value_type,
                           oratext **colname, ub2 *colnamel,
                           ub2 *coldty, void **colval,
                           ub2 *collen, OCIInd *colind,
                           ub1 *colcsf, oraub8 *colflg,
                           ub2 *colcsid, ub2 num_cols)
{
  ub2        idx;
  oratext    buf[BUF_SIZE];

  printf ("=== %s column list (num_columns=%d) ===\n",
          col_value_type == OCI_LCR_ROW_COLVAL_OLD ? "OLD" : "NEW", num_cols); 
  for (idx = 0; idx < num_cols; idx++)
  {
    printf ("Column[%d](name,dty,ind,len,csf): %.*s %d %d %d %d", idx+1,
             colnamel[idx], colname[idx], coldty[idx], colind[idx],
             collen[idx], colcsf[idx]);

    if (colind[idx] == OCI_IND_NULL)
      printf(" value=NULL");
    else
    {
      ub2  printlen;

      /* Print column value based on its datatype */
      switch (coldty[idx])
      {
        case SQLT_AFC:
        case SQLT_CHR:
          /* print out max of MAX_PRINT_BYTES bytes of data */
          if (collen[idx] > MAX_PRINT_BYTES)
            printlen = MAX_PRINT_BYTES;
          else
            printlen = collen[idx];
          printf (" value=%.*s", printlen, colval[idx]);
          break;
        case SQLT_VNU:
        {
          OCINumber  *rawnum = colval[idx];
          oratext     numbuf[BUF_SIZE+1];
          ub4         numsize = BUF_SIZE;

          OCICALL(ocip, 
                  OCINumberToText(ocip->errp, (const OCINumber *)rawnum,
                                 (const oratext *)"TM9", 3, 
                                 (const oratext *)0, 0,
                                 &numsize, numbuf));

          printf (" value=%.*s", numsize, numbuf);
          break;
        }
        case SQLT_ODT:
        {
          OCIDate    *datevalue = colval[idx];

          printf (" value(mm/dd/yyyy hh:mi:ss)=%d/%d/%d %d:%d:%d", 
                  datevalue->OCIDateMM, datevalue->OCIDateDD, 
                  datevalue->OCIDateYYYY, datevalue->OCIDateTime.OCITimeHH,
                  datevalue->OCIDateTime.OCITimeMI,
                  datevalue->OCIDateTime.OCITimeSS);
          break;
        }
        case SQLT_BFLOAT:
        {
          float      *fltvalue = colval[idx];

          printf (" value=%f ", *fltvalue);
          break;
        }
        case SQLT_BDOUBLE:
        {
          double     *dblvalue = colval[idx];

          printf (" value=%f ", *dblvalue);
          break;
        }
        case SQLT_TIMESTAMP:
        case SQLT_TIMESTAMP_LTZ:
        {
          OCIDateTime  *dtvalue = colval[idx];
          ub4           bufsize = sizeof(buf);

          OCICALL(ocip,
                  OCIDateTimeToText(ocip->envp, ocip->errp, dtvalue, TS_FORMAT,
                                    (ub1) strlen((const char *)TS_FORMAT),
                                    (ub1) DEFAULT_FS_PREC, (const oratext*)0, 
                                    (size_t) 0, &bufsize, buf)); 
          printf (" value=%.*s ", bufsize, buf);
          
          break;
        }
        case SQLT_TIMESTAMP_TZ:
        {
          OCIDateTime  *dtvalue = colval[idx];
          ub4           bufsize = sizeof(buf);

          OCICALL(ocip,
                  OCIDateTimeToText(ocip->envp, ocip->errp, dtvalue, 
                                    TSZ_FORMAT,
                                    (ub1) strlen((const char *)TSZ_FORMAT),
                                    (ub1) DEFAULT_FS_PREC, (const oratext*)0, 
                                    (size_t) 0, &bufsize, buf)); 
          printf (" value=%.*s ", bufsize, buf);
          
          break;
        }
        case SQLT_INTERVAL_YM:
        case SQLT_INTERVAL_DS:
        {
          OCIInterval  *intv = colval[idx];
          size_t        reslen;                    /* result interval length */

          OCICALL(ocip,
                  OCIIntervalToText(ocip->envp, ocip->errp, intv, 
                                    DEFAULT_LF_PREC, DEFAULT_FS_PREC, 
                                    buf, sizeof(buf), &reslen)); 
          printf (" value=%.*s ", reslen, buf);
          
          break;
        }
        case SQLT_RDD:
        {
          OCIRowid     *rid = colval[idx];
          ub2           reslen = (ub2)sizeof(buf);

          OCICALL(ocip,
                  OCIRowidToChar(rid, buf, &reslen, ocip->errp)); 
          printf (" value=%.*s ", reslen, buf);
          
          break;
        }
        default:
        {
          ub2  idx2; 
          ub1 *byteptr = (ub1 *)colval[idx];

          /* dump out raw bytes */
          printf (" value=");
          /* print out max of MAX_PRINT_BYTES bytes of data */
          if (collen[idx] > MAX_PRINT_BYTES)
            printlen = MAX_PRINT_BYTES;
          else
            printlen = collen[idx];
          for (idx2 = 0; idx2 < printlen; idx2++)
            printf("%02x ", byteptr[idx2]); 
          break;
        }
      }
    }
    printf ("\n");
  }
}

/*---------------------------------------------------------------------
 * print_chunk - Print chunked column information. Only print the first
 *               50 bytes for each chunk. 
 *---------------------------------------------------------------------*/
static void print_chunk (ub1 *chunk_ptr, ub4 chunk_len, ub2 dty)
{

  ub4  print_bytes;

  if (chunk_len == 0)
    return;

  print_bytes = chunk_len > MAX_PRINT_BYTES ? MAX_PRINT_BYTES : chunk_len;

  printf("Data = ", chunk_len);
  if (dty == SQLT_CHR)
    printf("%.*s", print_bytes, chunk_ptr);
  else
  {
    ub2  idx;

    for (idx = 0; idx < print_bytes; idx++)
      printf("%02x", chunk_ptr[idx]);
  }
  printf("\n");
}

/*---------------------------------------------------------------------
 * detach - Detach from outbound server
 *---------------------------------------------------------------------*/
static void detach(oci_t * ocip)
{
  sword  err;

  printf ("Detach from XStream outbound server\n");

  err = OCIXStreamOutDetach(ocip->svcp, ocip->errp, OCI_DEFAULT);

  if (err)
    ocierror(ocip, (char *)"detach_session() failed");
}

/*---------------------------------------------------------------------
 * disconnect_db  - Logoff from the database
 *---------------------------------------------------------------------*/
void disconnect_db(oci_t * ocip)
{
  if (OCILogoff(ocip->svcp, ocip->errp))
  {
    ocierror(ocip, (char *)"OCILogoff() failed");
  }

  if (ocip->errp)
    OCIHandleFree((dvoid *) ocip->errp, (ub4) OCI_HTYPE_ERROR);

  if (ocip->envp)
    OCIHandleFree((dvoid *) ocip->envp, (ub4) OCI_HTYPE_ENV);
}

/*---------------------------------------------------------------------
 * ocierror - Print error status and exit program
 *---------------------------------------------------------------------*/
static void ocierror(oci_t * ocip, char * msg)
{
  sb4 errcode=0;
  text bufp[4096];

  if (ocip->errp)
  {
    OCIErrorGet((dvoid *) ocip->errp, (ub4) 1, (text *) NULL, &errcode,
                bufp, (ub4) 4096, (ub4) OCI_HTYPE_ERROR);
    printf("%s\n%s", msg, bufp);
  }
  else
    puts(msg);

  printf ("\n");
  exit(1);
}

/*---------------------------------------------------------------------
 * ocierror - Print error status only, don't exit
 *---------------------------------------------------------------------*/
static void ocierror2(oci_t * ocip, char * msg)
{
  sb4 errcode=0;
  text bufp[4096];

  if (ocip->errp)
  {
    OCIErrorGet((dvoid *) ocip->errp, (ub4) 1, (text *) NULL, &errcode,
                bufp, (ub4) 4096, (ub4) OCI_HTYPE_ERROR);
    printf("%s\n%s", msg, bufp);
  }
  else
    puts(msg);

  printf ("\n");
}

/*--------------------------------------------------------------------
 * print_usage - Print command usage
 *---------------------------------------------------------------------*/
static void print_usage(int exitcode)
{
  puts((char *)"\nUsage: xsfile -svr <svr_name> -db <db_name> "
               "-usr <conn_user> -tracing <true|false> "
               "-interval <interval_value> -mode <mode>\n");

  puts("  svr      : outbound server name\n"
       "  db       : database name of outbound server\n"
       "  usr      : connect user to outbound server\n"
       "  tracing  : turn on|off the tracing\n"
       "  interval : number of batches before switching to the next data file,\n"
       "             a batch in XStream by default is 30 sec, default interval is 1\n" 
       "  mode     : currently support 'csv' or 'sqlldr' mode\n");

  exit(exitcode);
}


/*--------------------------------------------------------------------
 * get_inputs - Get user inputs from command line
 *---------------------------------------------------------------------*/
static void get_inputs(params_t *params, int argc, char ** argv)
{
  char * option;
  char * value;

  memset (params, 0, sizeof(*params));
  while(--argc)
  {
    /* get the option name */
    argv++;
    option = *argv;

    /* check that the option begins with a "-" */
    if (!strncmp(option, (char *)"-", 1))
    {
      option ++;
    }
    else
    {
      printf("Error: bad argument '%s'\n", option);
      print_usage(1);
    }

    /* get the value of the option */
    --argc;
    argv++;

    value = *argv;    

    if (!strncmp(option, (char *)"db", 2))
    {
      params->dbname = (oratext *)value;
      params->dbnamelen = strlen(value);
    }
    else if (!strncmp(option, (char *)"usr", 3))
    {
      char *nl;      

      params->user = (oratext *)value;
      params->userlen = strlen(value);

      /* get the passwd via stdin*/ 
      printf ("Enter passwd for user \"%.*s\": ",
              params->userlen, params->user);
      /* ensure prompt appears */
      fflush (stdout); 
      if (fgets ((char *)params->passw, sizeof(params->passw), stdin) == NULL)
      {
        printf("Error: fail to read passwd\n");
        print_usage(1);
      }
      /* exit if getting an empty string */
      if (params->passw[0] == '\n')
      {
        printf("Error: passwd is empty\n");
        print_usage(1);
      }

      /* remove end-of-line */
      if ((nl = strchr((char *)params->passw, '\n')) != NULL)
        *nl = '\0';
      params->passwlen= strlen((char *)params->passw);
    }
    else if (!strncmp(option, (char *)"svr", 3))
    {
      params->applynm = (oratext *)value;
      params->applynmlen = strlen(value);
    }
    else if (!strncmp(option, (char *)"tracing", 7))
    {
      if (!strncasecmp(value, (char *)"true", 4))
      {
        params->tracing = TRUE;
        tracing = TRUE;
      }
    }
    else if (!strncmp(option, (char *)"mode", 4))
    {
      if (!strncasecmp(value, (char *)"csv", 3))
      {
        params->mode = CSV_MODE;
      }
      else if (!strncasecmp(value, (char *)"sqlldr", 6))
      {
        params->mode = SQLLDR_MODE;
      }
      mode = params->mode;
    }
    else if (!strncmp(option, (char *)"interval", 8))
    {
      params->file_interval = atoi(value);
      interval = params->file_interval;
    }
    else
    {
      printf("Error: unknown option '%s'.\n", option);
      print_usage(1);
    }
  }

  /* print usage and exit if any argument is not specified */
  if (!params->applynmlen || !params->passwlen || !params->userlen || 
      !params->dbnamelen)
  {
    printf("Error: missing command arguments. \n");
    print_usage(1);
  }

  if(tracing)
  {
    printf("xsfile interval set to %d\n", params->file_interval); 
  }
  

}

/*---------------------------------------------------------------------
 * fully materialize LCR, this includes copying all the chunk data.
 *---------------------------------------------------------------------*/
static sword materializeLCR(oci_t *ocip, mlcr_t *mlcrp, oraub8 flag, 
                           ub1 lcrtype)
{
  sword        ret;
  oraub8       row_flag;
  void        *lcrp=mlcrp->lcrp;
  sword        status = OCI_SUCCESS;
  chunkColumn *chunkcol = (chunkColumn *)0;
  ub4          idx = 0;
  ub2          extra_col = 0;
  
  /* Get LCR Header information */
  ret = OCILCRHeaderGet(ocip->svcp, ocip->errp, 
                        &mlcrp->src_db_name, 
                        &mlcrp->src_db_namel,                   /* source db */
                        &mlcrp->cmd_type, 
                        &mlcrp->cmd_typel,                   /* command type */
                        &mlcrp->owner, 
                        &mlcrp->ownerl,                        /* owner name */
                        &mlcrp->oname, 
                        &mlcrp->onamel,                       /* object name */
                        (ub1 **)0, (ub2 *)0,                      /* lcr tag */
                        &mlcrp->txid, 
                        &mlcrp->txidl,                             /* txn id */
                        (OCIDate *)0,                            /* src time */
                        (ub2 *)0, (ub2 *)0,              /* OLD/NEW col cnts */
                        &mlcrp->position, 
                        &mlcrp->positionl,                   /* LCR position */
                        (oraub8*)0, lcrp, OCI_DEFAULT);

  if (ret != OCI_SUCCESS)
  {
    ocierror2(ocip, (char *)"OCILCRHeaderGet failed");
    return XSFILE_ERROR;
  }

  /* get LCR column information */
  /* If delete or update command populate OLD column list */
  if (IS_DELETE_CMD(mlcrp->cmd_type, mlcrp->cmd_typel) ||
      IS_UPDATE_CMD(mlcrp->cmd_type, mlcrp->cmd_typel))
  {
    ret = OCILCRRowColumnInfoGet (ocip->svcp, ocip->errp, 
                                  OCI_LCR_ROW_COLVAL_OLD, 
                                  &mlcrp->old_columns.num_cols, 
                                  mlcrp->old_columns.colname, 
                                  mlcrp->old_columns.colnamel, 
                                  mlcrp->old_columns.coldty, 
                                  mlcrp->old_columns.colval, 
                                  mlcrp->old_columns.colind, 
                                  mlcrp->old_columns.collen, 
                                  mlcrp->old_columns.colcsf, 
                                  mlcrp->old_columns.colflg, 
                                  mlcrp->old_columns.colcsid,
                                  lcrp, MAX_COLUMNS, OCI_DEFAULT);
  }
  if (ret != OCI_SUCCESS)
  {
    ocierror2(ocip, (char *)"OCILCRRowColumnInfoGet failed");
    return XSFILE_ERROR;
  }
  
  /* If insert and update populate NEW column list */
  if (IS_INSERT_CMD(mlcrp->cmd_type, mlcrp->cmd_typel) ||
      IS_UPDATE_CMD(mlcrp->cmd_type, mlcrp->cmd_typel))
  {
    ret = OCILCRRowColumnInfoGet (ocip->svcp, ocip->errp, 
                                  OCI_LCR_ROW_COLVAL_NEW,
                                  &mlcrp->new_columns.num_cols, 
                                  mlcrp->new_columns.colname, 
                                  mlcrp->new_columns.colnamel, 
                                  mlcrp->new_columns.coldty, 
                                  mlcrp->new_columns.colval, 
                                  mlcrp->new_columns.colind, 
                                  mlcrp->new_columns.collen, 
                                  mlcrp->new_columns.colcsf, 
                                  mlcrp->new_columns.colflg, 
                                  mlcrp->new_columns.colcsid,
                                  lcrp, MAX_COLUMNS, OCI_DEFAULT);
  }
  if (ret != OCI_SUCCESS)
  {
    ocierror2(ocip, (char *)"OCILCRRowColumnInfoGet failed");
    return XSFILE_ERROR;
  }

  /* sort the column list in the order of column list in table shape */

  /* clear the haschunkdata flag and columnp first */
  for (idx = 0; idx<mlcrp->new_columns.num_cols; idx++)
  {
    mlcrp->new_columns.haschunkdata[idx] = FALSE;
    mlcrp->new_columns.columnp[idx] = chunkcol;
  }
  
  /* If LCR has chunked columns (i.e, has LOB/Long/XMLType columns) */
  if (flag & OCI_XSTREAM_MORE_ROW_DATA)
  {
    boolean      newcol = TRUE;
    ub1         *tmpbuf = (void *) 0;
    ub4          tmpsize = 0;
    chunk       *chunkp;    
    chunkColumn *chunkcol;
    
    do 
    {
      
      if (newcol)
      {
        chunkcol = (chunkColumn *)malloc(sizeof(chunkColumn));
        chunkcol->next = (chunkColumn *)0;
        chunkcol->chunklist_head = (chunk *)0;
        chunkcol->chunklist_tail = (chunk *)0;
        chunkcol->chunklist_size = 0;
        chunkcol->in_dml = FALSE;
        chunkcol->size = 0;
        newcol = FALSE;
      }

      /* Execute non-callback call to receive each chunk */
      status = OCIXStreamOutChunkReceive(ocip->svcp, ocip->errp, 
                                         &(chunkcol->colname),
                                         &(chunkcol->colname_len), 
                                         &(chunkcol->dty), 
                                         &(chunkcol->flag), 
                                         &(chunkcol->csid), 
                                         &tmpsize,
                                         &tmpbuf, 
                                         &row_flag, OCI_DEFAULT);
      if (status != OCI_SUCCESS)
      {
        ocierror2(ocip, (char *)"ReceiveChunk() failed");
        return XSFILE_ERROR;          
      }

      if (status == OCI_ERROR)
      {
        ocierror2(ocip, (char *)"get_lcrs() failed");
        return XSFILE_ERROR;  
      }

      /* create a chunk to hold this chunk data */
      chunkp = (chunk *)malloc(sizeof(chunk));
      chunkp->next = (chunk *)0;
      chunkp->data = malloc(tmpsize);
      chunkp->data_len = tmpsize;
      memcpy(chunkp->data, tmpbuf, tmpsize);      

      /* add this chunk to the chunklist for the current column */
      if (NULL == chunkcol->chunklist_head)
      {
        chunkcol->chunklist_head = chunkp;
        chunkcol->chunklist_tail = chunkp;
      }
      else
      {
        chunkcol->chunklist_tail->next = chunkp;
        chunkcol->chunklist_tail = chunkp;
      }
      chunkcol->chunklist_size = chunkcol->chunklist_size+1;
      chunkcol->size += tmpsize;
      
      /* if it's end of current column, add chunkColumn to the chunkcollist */
      if (chunkcol->flag & OCI_LCR_COLUMN_LAST_CHUNK)
      {
        newcol = TRUE;
        if (NULL == mlcrp->chunkcollist_head)
        {
          mlcrp->chunkcollist_head = chunkcol;
          mlcrp->chunkcollist_tail = chunkcol;          
        }
        else
        {
          mlcrp->chunkcollist_tail->next = chunkcol;
          mlcrp->chunkcollist_tail = chunkcol;
        }
        mlcrp->chunkcollist_size = mlcrp->chunkcollist_size+1;
      }

    } while (row_flag & OCI_XSTREAM_MORE_ROW_DATA);    


    /* set the flag for each column in the new column list to indicate 
     * whether this column has chunk data */
    chunkcol = mlcrp->chunkcollist_head;
    while (chunkcol)
    {
      
      for (idx=0; idx<mlcrp->new_columns.num_cols;idx++)
      {
        if (samestr(chunkcol->colname, 
                    chunkcol->colname_len,
                    mlcrp->new_columns.colname[idx],
                    mlcrp->new_columns.colnamel[idx]))
        {
          mlcrp->new_columns.haschunkdata[idx] = TRUE;
          mlcrp->new_columns.columnp[idx] = chunkcol;
          chunkcol->in_dml = TRUE;
        }
      }

      /* if a chunked column is not presented in the DML 
       * add it to the new column list */
      if (!chunkcol->in_dml)
      {
        ub4 idx = mlcrp->new_columns.num_cols + extra_col;

        mlcrp->new_columns.colname[idx] = chunkcol->colname;
        mlcrp->new_columns.colnamel[idx] = chunkcol->colname_len;
        mlcrp->new_columns.coldty[idx] = chunkcol->dty;
        mlcrp->new_columns.collen[idx] = 0;
        mlcrp->new_columns.colind[idx] = OCI_IND_NULL;
        mlcrp->new_columns.colval[idx] = (void *)0; 
        mlcrp->new_columns.colcsid[idx] = chunkcol->csid;
        mlcrp->new_columns.colflg[idx] = chunkcol->flag;
        mlcrp->new_columns.haschunkdata[idx] = TRUE;
        mlcrp->new_columns.columnp[idx] = chunkcol;
        
        extra_col++;
      }
      
      chunkcol = chunkcol->next;
    }
    mlcrp->new_columns.num_cols = mlcrp->new_columns.num_cols + extra_col;
  }

  /* print the LCR for debugging purpose */ 
  if (tracing)
    print_mlcr(ocip, mlcrp, lcrtype); 

  return OCI_SUCCESS;
}

/*---------------------------------------------------------------------
 * this free the memory allocated for fully materialized LCR
 *---------------------------------------------------------------------*/
static void freeMLCR(mlcr_t *mlcrp)
{
  chunkColumn *chunkcol = mlcrp->chunkcollist_head;
  chunkColumn *tmpcol;
  
  while(chunkcol)
  {
    chunk *chunkp = chunkcol->chunklist_head;
    chunk *tmpchunk;
    tmpcol = chunkcol;
    
    while(chunkp)
    {
      tmpchunk = chunkp;
      free(chunkp->data);
      chunkp = chunkp->next;
      free(tmpchunk);
    }

    chunkcol = chunkcol->next;
    free(tmpcol);
  }
}

/*---------------------------------------------------------------------
 * this function writes a fully materialized LCR into data file
 *---------------------------------------------------------------------*/
static sword writeMLCR(oci_t *ocip, mlcr_t *mlcrp, ub1 lcrtype, FILE *fp,
                       tableshape *ts)
{
  /* skip the commit */
  if(!IS_COMMIT_CMD(mlcrp->cmd_type, mlcrp->cmd_typel))
    fwrite_mlcr(fp, ocip, mlcrp, ts);

  return OCI_SUCCESS;
}

/*---------------------------------------------------------------------
 * this function prints a fully materialized LCR for debugging purpose
 *---------------------------------------------------------------------*/
static void print_mlcr(oci_t *ocip, mlcr_t *mlcrp, ub1 lcrtype)
{
  ub4 numcol = mlcrp->chunkcollist_size;
  ub4 idx = 0;
  chunkColumn *tmpcol;
  chunk *tmpchunk;
  
  /* print_lcr(ocip, mlcrp->lcrp,lcrtype); */
  printf("src_db_name=%.*s\ncmd_type=%.*s txid=%.*s\n",
         mlcrp->src_db_namel, mlcrp->src_db_name, 
         mlcrp->cmd_typel, mlcrp->cmd_type, 
         mlcrp->txidl, mlcrp->txid);
  
  if (mlcrp->ownerl > 0)
    printf("owner=%.*s oname=%.*s \n", 
           mlcrp->ownerl, mlcrp->owner, 
           mlcrp->onamel, mlcrp->oname);

  printf("position(len=%d) ",mlcrp->positionl); 
  for (idx = 0; idx < mlcrp->positionl; idx++) 
  { 
    printf("%1x", mlcrp->position[idx] >> 4); 
    printf("%1x", mlcrp->position[idx] & 0xf); 
  } 
  printf("\n"); 


  if (IS_DELETE_CMD(mlcrp->cmd_type, mlcrp->cmd_typel) ||
      IS_UPDATE_CMD(mlcrp->cmd_type, mlcrp->cmd_typel))
  {
    print_col_data(ocip,OCI_LCR_ROW_COLVAL_OLD, 
                   mlcrp->old_columns.colname, 
                   mlcrp->old_columns.colnamel, 
                   mlcrp->old_columns.coldty, 
                   mlcrp->old_columns.colval,
                   mlcrp->old_columns.collen, 
                   mlcrp->old_columns.colind,
                   mlcrp->old_columns.colcsf, 
                   mlcrp->old_columns.colflg, 
                   mlcrp->old_columns.colcsid,
                   mlcrp->old_columns.num_cols);
  }
  
  if (IS_INSERT_CMD(mlcrp->cmd_type, mlcrp->cmd_typel) ||
      IS_UPDATE_CMD(mlcrp->cmd_type, mlcrp->cmd_typel))
  {
    print_col_data(ocip,OCI_LCR_ROW_COLVAL_NEW,
                   mlcrp->new_columns.colname, 
                   mlcrp->new_columns.colnamel, 
                   mlcrp->new_columns.coldty, 
                   mlcrp->new_columns.colval,
                   mlcrp->new_columns.collen, 
                   mlcrp->new_columns.colind,
                   mlcrp->new_columns.colcsf, 
                   mlcrp->new_columns.colflg, 
                   mlcrp->new_columns.colcsid,
                   mlcrp->new_columns.num_cols);  
  }

  /* print all the chunks */
  printf(" This LCR has %d columns with chunk data\n", numcol);
  tmpcol = mlcrp->chunkcollist_head;
  
  while (NULL != tmpcol)
  {
    printf("colname=%.*s, dty=%d, csid=%d, totalchunk=%d, flag=%d \n",
           tmpcol->colname_len,tmpcol->colname, 
           tmpcol->dty, tmpcol->csid, tmpcol->chunklist_size, tmpcol->flag);
    printf("total data size =%d\n", tmpcol->size);
    tmpchunk = tmpcol->chunklist_head;
    while (NULL != tmpchunk)
    {
      print_chunk(tmpchunk->data, tmpchunk->data_len, tmpcol->dty);
      tmpchunk = tmpchunk->next;
    }

    tmpcol = tmpcol->next;
  } 
  printf("\n");

}

/*---------------------------------------------------------------------
 * this function writes a binary field into the data file,
 *---------------------------------------------------------------------*/
static sword fwrite_raw_field (FILE *fp, const void *src, 
                               size_t src_size, ub2 lenfield_len)
{
  const oratext *csrc = src;
  
  if (NULL == fp || (NULL == src && 0 != src_size))
    return 0;

  switch(lenfield_len)
  {
    ub2 ub2_len;
    ub4 ub4_len;
    
    case UB2LEN:                                                   /* varraw */
      ub2_len = (ub2)src_size;
      /* first wirte the length in 2 bytes */
      writebytes(fp, &ub2_len, UB2LEN);
      break;
    case UB4LEN:                                              /* long varraw */
      ub4_len = (ub4)src_size;
      /* first wirte the length in 4 bytes */
      writebytes(fp, &ub4_len, UB4LEN);
      break;
    default:
      return  XSFILE_ERROR;
  }
  
  /* then wirte the actually data */
  writebytes(fp, src, src_size);
  
  return XSFILE_SUCCESS;
}

/*---------------------------------------------------------------------
 * this function writes a character field into the data file,
 * if empty data, src_size == 0 write ,"",
 *---------------------------------------------------------------------*/
static sword fwrite_field (FILE *fp, const void *src, size_t src_size)
{
  const oratext *csrc = src;

  if (NULL == fp || (NULL == src && 0 != src_size))
    return 0;

  if (fputc('"', fp) == EOF)
    return XSFILE_ERROR;

  fwrite_field_unquote(fp, src, src_size);

  if (fputc('"', fp) == EOF) {
    return XSFILE_ERROR;
  }

  return XSFILE_SUCCESS;
}


/*---------------------------------------------------------------------
 * this function writes a character field into the data file without
 * enclosing double quote, this is for 
 *---------------------------------------------------------------------*/
static sword fwrite_field_unquote(FILE *fp, const void *src, size_t src_size)
{
  const oratext *csrc = src;

  if (NULL == fp || (NULL == src && 0 != src_size))
    return XSFILE_ERROR;

  while (src_size) 
  {
    /* escape double quote */
    if ('"' == *csrc) 
    {
      if (fputc('"', fp) == EOF)
        return XSFILE_ERROR;
    }

    if (fputc(*csrc, fp) == EOF)
      return XSFILE_ERROR;

    src_size--;
    csrc++;
  }
  return XSFILE_SUCCESS;
}

/*---------------------------------------------------------------------
 * this function writes a fully materialized LCR into the data file.
 * this includes writing all the control columns and any actual data
 * columns
 *---------------------------------------------------------------------*/
static sword fwrite_mlcr(FILE *fp, oci_t *ocip, mlcr_t *mlcrp, tableshape *ts)
{
  sword ret = XSFILE_ERROR;

  if (IS_DELETE_CMD(mlcrp->cmd_type, mlcrp->cmd_typel))
  {
    /* for insert, one row for new value */
    ret = fwrite_mlcr_cols(fp, ocip, mlcrp, OCI_LCR_ROW_COLVAL_OLD, ts);
  }
  else if (IS_INSERT_CMD(mlcrp->cmd_type, mlcrp->cmd_typel))
  { 
    /* for delete, one row for old value */
    ret = fwrite_mlcr_cols(fp, ocip, mlcrp, OCI_LCR_ROW_COLVAL_NEW, ts); 
  }
  else if (IS_UPDATE_CMD(mlcrp->cmd_type, mlcrp->cmd_typel))
  {
    /* for update, two rows, for both old and new */
    ret = fwrite_mlcr_cols(fp, ocip, mlcrp, OCI_LCR_ROW_COLVAL_OLD, ts);
    if (XSFILE_ERROR == ret)
      return XSFILE_ERROR;
    if (fputc('\n', fp) == EOF)
      return XSFILE_ERROR;  

    ret = fwrite_mlcr_cols(fp, ocip, mlcrp, OCI_LCR_ROW_COLVAL_NEW, ts);
  }

  if (XSFILE_ERROR == ret)
    return XSFILE_ERROR;
  
  if (fputc('\n', fp) == EOF)
    return XSFILE_ERROR;  

  return ret;
}

/*---------------------------------------------------------------------
 * this function writes the control columns into the data file.
 * control columns include the following:
 *  command_type      varchar2(10)
 *  object_owner      varchar2(30)
 *  object_name       varchar2(30)
 *  position          raw(64),
 *  column_list_type  varchar2(3)
 *---------------------------------------------------------------------*/
static sword fwrite_mlcr_ctl_cols(FILE *fp, oci_t *ocip, mlcr_t *mlcrp, 
                                  ub2 col_value_type)
{
  sword ret = XSFILE_ERROR;
  
  /* write control columns */
  /* write command type first */
  if (IS_INSERT_CMD(mlcrp->cmd_type, mlcrp->cmd_typel))
  {
    ret = fwrite_field(fp, (oratext *)INSERT_CMD, sizeof(INSERT_CMD)-1);
  }
  else if (IS_UPDATE_CMD(mlcrp->cmd_type, mlcrp->cmd_typel))
  {
    ret = fwrite_field(fp, (oratext *)UPDATE_CMD, sizeof(UPDATE_CMD)-1);
  }
  else if (IS_DELETE_CMD(mlcrp->cmd_type, mlcrp->cmd_typel))
  {
    ret = fwrite_field(fp, (oratext *)DELETE_CMD, sizeof(DELETE_CMD)-1);
  }
  
  if (XSFILE_ERROR == ret)
    return XSFILE_ERROR;
  
  if (fputc(',', fp) == EOF)
    return XSFILE_ERROR;
  
  /* write object owner */
  ret = fwrite_field(fp, mlcrp->owner, mlcrp->ownerl);
  if (XSFILE_ERROR == ret)
    return XSFILE_ERROR;
  
  if (fputc(',', fp) == EOF)
    return XSFILE_ERROR;
  
  /* write object name */    
  ret = fwrite_field(fp, mlcrp->oname, mlcrp->onamel);
  if (XSFILE_ERROR == ret)
    return XSFILE_ERROR;
  
  if (fputc(',', fp) == EOF)
    return XSFILE_ERROR;
  
  /* write position */
  if (SQLLDR_MODE == mode)
  {
    ret = fwrite_raw_field(fp, mlcrp->position, mlcrp->positionl, UB2LEN);
    if (XSFILE_ERROR == ret)
      return XSFILE_ERROR;
  }
  else if (CSV_MODE == mode)
  {
    ret = fwrite_field(fp, mlcrp->position, mlcrp->positionl);
    if (XSFILE_ERROR == ret)
      return XSFILE_ERROR;

    if (fputc(',', fp) == EOF)
      return XSFILE_ERROR;    
  }
           
  /* write column value type */
  if (OCI_LCR_ROW_COLVAL_NEW == col_value_type)
    ret = fwrite_field(fp, (oratext *)"NEW", sizeof("NEW")-1);
  else
    ret = fwrite_field(fp, (oratext *)"OLD", sizeof("OLD")-1);

  if (XSFILE_ERROR == ret)
      return XSFILE_ERROR;

  return ret;
}

/*---------------------------------------------------------------------
 * this function writes all data columns into the data file.
 *---------------------------------------------------------------------*/
static sword fwrite_mlcr_cols(FILE *fp, oci_t *ocip, mlcr_t *mlcrp, 
                              ub2 col_value_type, tableshape *ts)
{
  sword ret = XSFILE_SUCCESS;
  columns_t *cols = (columns_t *)0;
  ub2 idx = 0;
  ub2 tsidx = 0;
  oratext    buf[BUF_SIZE];
  boolean rawtype = FALSE;
  
  /* populate control columns first */
  ret = fwrite_mlcr_ctl_cols(fp, ocip, mlcrp, col_value_type);
  if (XSFILE_ERROR == ret)
    return XSFILE_ERROR;


  /* populate column values */
  if (OCI_LCR_ROW_COLVAL_NEW == col_value_type)
    cols = &mlcrp->new_columns;
  else
    cols = &mlcrp->old_columns;

  for (tsidx = 0; tsidx < ts->columnlist_size; tsidx++)
  {
    boolean  found = FALSE;
    
    for (idx = 0; idx < cols->num_cols; idx++)
    {
      if(samestrci((char *)cols->colname[idx],cols->colnamel[idx],
                   (char *)ts->column_namelist[tsidx],
                   strlen((char *)ts->column_namelist[tsidx])))
      {
        found = TRUE;
        break;
      }
    }

    if (found)
    {
      /* if a column is found in the LCR, write the column */
      ret = fwrite_one_col(fp, ocip, mlcrp, col_value_type, cols, 
                           idx, rawtype);
      if (XSFILE_ERROR == ret)
        return XSFILE_ERROR;
      if (SQLLDR_MODE == mode && XSCVS_RAW_DATATYPE(cols->coldty[idx]))
      {
        rawtype = TRUE;
      }
      else
      {
        rawtype = FALSE; 
      }
    }
    else
    {
      /* column is not presented in the LCR, put a null */
      if (SQLLDR_MODE == mode && ts->column_rawtype[tsidx])
      {
        /* for a raw data type, we need to put 0-length field */
        if(ts->column_lobtype[tsidx])
          fwrite_raw_field(fp,0,0,UB4LEN);
        else
          fwrite_raw_field(fp,0,0,UB2LEN);
      }
      else
      {
        /* TODO: for clob, 0-length is empty lob, need to use NULLIF
         * clause in the control file */
        if (fputc(',', fp) == EOF)
          return XSFILE_ERROR;
        if (fputc('"', fp) == EOF)
          return XSFILE_ERROR;
        if (fputc('"', fp) == EOF)
          return XSFILE_ERROR;
      }

      if (SQLLDR_MODE == mode && ts->column_rawtype[tsidx])
      {
        rawtype = TRUE;
      }
      else
      {
        rawtype = FALSE; 
      }
    }
  }
  
  return ret;
  
}

/*---------------------------------------------------------------------
 * this function writes one data column into the data file.
 *---------------------------------------------------------------------*/
static sword fwrite_one_col(FILE *fp, oci_t *ocip, mlcr_t *mlcrp, 
                            ub2 col_value_type, columns_t *cols, ub2 idx,
                            boolean previous_raw)
{
  sword ret = XSFILE_SUCCESS;
  oratext    buf[BUF_SIZE];
  boolean is_null = FALSE;
  
  if (!previous_raw)
  {
    if (fputc(',', fp) == EOF)
      return XSFILE_ERROR;
  }

  /* if this column has chunk data, write chunk data, this assumes
   * that if there is chunk data, there shouldn't be any data
   * inline, thus, we just wirte chunk data and continue to next column */
  if (OCI_LCR_ROW_COLVAL_NEW == col_value_type && cols->haschunkdata[idx])
  {
    chunkColumn *chunkcolp = cols->columnp[idx];
    chunk *chunkp = chunkcolp->chunklist_head;
    
    if (!(chunkcolp->flag & OCI_LCR_COLUMN_EMPTY_LOB) &&
         (chunkp && !chunkp->data_len))
    {
      /* column value is null, for raw, write a 0-length field,
       * TODO use NULLIF clause */
      if (SQLLDR_MODE == mode && XSCVS_RAW_DATATYPE(cols->coldty[idx]))
        fwrite_raw_field(fp,0,0,UB4LEN);
      return XSFILE_SUCCESS;
    }

    if (SQLLDR_MODE == mode && XSCVS_RAW_DATATYPE(cols->coldty[idx]))
    {
      writebytes(fp, &chunkcolp->size, UB4LEN);

      while(chunkp)
      {
        writebytes(fp, chunkp->data, chunkp->data_len);
        chunkp = chunkp->next;
        
        if (XSFILE_ERROR == ret)
          return XSFILE_ERROR;
      }
    }
    else
    {
      if (fputc('"', fp) == EOF)
        return XSFILE_ERROR;
      
      /* TODO handle the character set conversion */
      while(chunkp)
      {
        ret = fwrite_field_unquote(fp, chunkp->data, chunkp->data_len);
        chunkp = chunkp->next;
        
        if (XSFILE_ERROR == ret)
          return XSFILE_ERROR;
      }
      
      if (fputc('"', fp) == EOF)
        return XSFILE_ERROR;
    }
    
    return XSFILE_SUCCESS;
  }
    
  if (cols->colind[idx] == OCI_IND_NULL)
  {
    /* for null, do nothing, TODO handle raw, write 0-length field */      
    if (SQLLDR_MODE == mode && XSCVS_RAW_DATATYPE(cols->coldty[idx]))
    {
      fwrite_raw_field(fp,0,0,UB2LEN);
    }
  }
  else
  {
    ub2  printlen;
    
    /* Print column value based on its datatype */
    switch (cols->coldty[idx])
    {
    case SQLT_AFC:
    case SQLT_CHR:
      ret = fwrite_field(fp, cols->colval[idx], cols->collen[idx]);
      break;
    case SQLT_VNU:
    {
      OCINumber  *rawnum = cols->colval[idx];
      oratext     numbuf[BUF_SIZE+1];
      ub4         numsize = BUF_SIZE;
      
      OCICALL(ocip, 
              OCINumberToText(ocip->errp, (const OCINumber *)rawnum,
                              (const oratext *)"TM9", 3, 
                              (const oratext *)0, 0,
                              &numsize, numbuf));
      ret = fwrite_field(fp, numbuf, numsize);
      break;
    }
    case SQLT_ODT:
    {
      OCIDate    *datevalue = cols->colval[idx];
      ub4         bufsize = 0;
      
      bufsize = sprintf(buf,"%d/%d/%d %d:%d:%d", 
                        datevalue->OCIDateMM, 
                        datevalue->OCIDateDD, 
                        datevalue->OCIDateYYYY, 
                        datevalue->OCIDateTime.OCITimeHH,
                        datevalue->OCIDateTime.OCITimeMI,
                        datevalue->OCIDateTime.OCITimeSS);
      ret = fwrite_field(fp, buf, bufsize);
      break;
    }
    case SQLT_TIMESTAMP:
    case SQLT_TIMESTAMP_LTZ:
    {
      OCIDateTime  *dtvalue = cols->colval[idx];
      ub4           bufsize = sizeof(buf);
      
      OCICALL(ocip,
              OCIDateTimeToText(ocip->envp, ocip->errp, dtvalue, TS_FORMAT,
                                (ub1) strlen((const char *)TS_FORMAT),
                                (ub1) DEFAULT_FS_PREC, (const oratext*)0, 
                                (size_t) 0, &bufsize, buf)); 
      ret = fwrite_field(fp, buf, bufsize);          
      break;
    }
    case SQLT_TIMESTAMP_TZ:
    {
      OCIDateTime  *dtvalue = cols->colval[idx];
      ub4           bufsize = sizeof(buf);
      
      OCICALL(ocip,
              OCIDateTimeToText(ocip->envp, ocip->errp, dtvalue, TSZ_FORMAT,
                                (ub1) strlen((const char *)TSZ_FORMAT),
                                (ub1) DEFAULT_FS_PREC, (const oratext*)0, 
                                (size_t) 0, &bufsize, buf)); 
      ret = fwrite_field(fp, buf, bufsize);                 
      break;
    }
    case SQLT_INTERVAL_YM:
    case SQLT_INTERVAL_DS:
    {
      OCIInterval  *intv = cols->colval[idx];
      size_t        reslen;                    /* result interval length */
      
      OCICALL(ocip,
              OCIIntervalToText(ocip->envp, ocip->errp, intv, 
                                DEFAULT_LF_PREC, DEFAULT_FS_PREC, 
                                buf, sizeof(buf), &reslen)); 
      ret = fwrite_field(fp, buf, reslen);                
      break;
    }
    case SQLT_RDD:
    {
      OCIRowid     *rid = cols->colval[idx];
      ub2           reslen = (ub2)sizeof(buf);
      
      OCICALL(ocip,
              OCIRowidToChar(rid, buf, &reslen, ocip->errp)); 
      ret = fwrite_field(fp, buf, reslen);
      break;
    }
    case SQLT_BIN:
    case SQLT_BFLOAT:
    case SQLT_BDOUBLE:
    {
      if (SQLLDR_MODE == mode)
        ret = fwrite_raw_field(fp, cols->colval[idx], 
                               cols->collen[idx], UB2LEN);
      else if (CSV_MODE == mode)
      {
        ret = fwrite_field(fp, cols->colval[idx], cols->collen[idx]);
      }
      break;
    }
    default:
    {
      /* error */
      printf("data type %d unsupported \n",cols->coldty[idx]);
      return XSFILE_ERROR;
    }
    }

    if (XSFILE_ERROR == ret)
      return XSFILE_ERROR;
  }
  
  return XSFILE_SUCCESS;
}

/* end of xsfile.c */
