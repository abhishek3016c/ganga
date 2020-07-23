/* Copyright (c) 2008, 2010, Oracle and/or its affiliates. 
All rights reserved. */

/*

  NAME         xoidkey - XStream Out (ID Key LCRs)

   DESCRIPTION
   This program attaches to XStream outbound server and waits for
   transactions from that server. For each ID Key LCR received, it prints the
   contents of the LCR and queries the source database with the rowid
   in that LCR to verify that rowid exists. If the LCR contains
   chunked columns, then it prints the first 50 bytes of each chunk.

   This program waits indefinitely for transactions from the outbound server.
   Hit control-C to interrupt the program.

   Usage: xoidkey -ob_svr <outbound_svr> -ob_db <outbound_db>
                  -ob_usr <conn_user> -ob_pwd <conn_user_pwd>
                  -db_usr <db_user> -db_pwd <db_user_pwd>
   
     ob_svr  : outbound server name
     ob_db   : database name of outbound server
     ob_usr  : connect user to outbound server
     ob_pwd  : password of outbound's connect user
     db_usr  : connect user to database
     db_pwd  : password of database's connect user

   All arguments are optional. Following defaults are used if args are not 
   specified:
     ob_svr =   "XOUT"
     ob_db  =   "inst1"
     ob_usr =   "stradm"
     ob_pwd =   "stradm"
     db_usr =   "oe"
     db_pwd =   "oe"

   Specify -short to skip printing the ROW LCR column data. The default is to
   print the lcr column data.

   MODIFIED   (MM/DD/YY)
   yurxu       06/18/10 - Creation

*/

#ifndef ORATYPES
#include <oratypes.h>
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

#ifndef _TIME_H
#include <time.h>
#endif

#ifndef _CTYPE_H
#include <ctype.h>
#endif

#ifndef _SIGNAL_H
#include <signal.h>
#endif

#ifndef OCI_ORACLE
#include <oci.h>
#endif

#ifndef OCIXSTREAM_ORACLE
#include <ocixstream.h>
#endif

#ifndef ORID_ORACLE 
#include <orid.h> 
#endif

/*
#ifndef KNCLX_ORACLE
#include "knclx.h"
#endif
*/

/*---------------------------------------------------------------------- 
 *           Internal structures
 *----------------------------------------------------------------------*/ 

static const oratext DATE_FMT[] = "YYYY-MON-DD HH24:MI:SS";
#define MAX_PRINT_BYTES         50
#define NUM_BUF_SIZE            80
#define BUFSIZE                 5000
#define DATE_FMT_LEN            sizeof(DATE_FMT)-1
#define DATE_BUF_LEN            80
#define MAX_COLUMNS             1000

#define lbit(x,y)  ((x) & (y))

#define samecmd(cmdstr1, cmdlen1, cmdstr2, cmdlen2) \
 (((cmdlen1) == (cmdlen2)) && !memcmp((cmdstr1), (cmdstr2), cmdlen1))

typedef struct conn_info
{
  oratext * user; 
  ub4       userlen;
  oratext * passw;
  ub4       passwlen;
  oratext * dbname;
  ub4       dbnamelen;
  oratext * svrnm;
  ub4       svrnmlen;
} conn_info_t;

typedef struct params
{
  conn_info_t  xout;
  conn_info_t  db;
} params_t;

typedef struct oci
{
  OCIEnv    * envp;
  OCIError  * errp;
  OCIServer * srvp;
  OCISvcCtx * svcp;
  OCISession * authp;
  OCIStmt   * stmtp;
  OCIStmt   * stmt2p;
  boolean     attached;
  boolean     outbound;
} oci_t;

typedef struct myctx_t 
{
  ub4       lcrcnt;
  oci_t    *outbound_ocip;
  oci_t    *sql_sel_ocip;    /* connection to the source db */
  oratext  *owner;           /* owner of current LCR */
  ub4       ownerlen;        /* length of owner of current LCR */
  oratext  *oname;           /* object name */
  ub4       onamelen;        /* length of object name */  
  oratext  *txid;            /* transaction id */
  ub4       txidlen;         /* length of transaction id */
  void     *lcrp;
  boolean   is_commit;
  ub1       last_lcrpos[OCI_LCR_MAX_POSITION_LEN];
  ub2       last_lcrpos_len;
  boolean   longform;
} myctx_t;

#define OCICALL(ocip, function) do {\
sword status=function;\
if (OCI_SUCCESS==status || OCI_STILL_EXECUTING==status) break;\
else if (OCI_SUCCESS_WITH_INFO==status) \
{puts((char *)"Error: OCI_SUCCESS_WITH_INFO");\
exit(1);}\
else if (OCI_NEED_DATA==status) \
{puts((char *)"Error: OCI_NEED_DATA");\
exit(1);}\
else if (OCI_NO_DATA==status) \
{puts((char *)"Error: OCI_NO_DATA");\
exit(1);}\
else if (OCI_ERROR==status) \
{ocierror(ocip, (char *)"OCI_ERROR", TRUE);\
exit(1);}\
else if (OCI_INVALID_HANDLE==status) \
{puts((char *)"Error: OCI_INVALID_HANDLE");\
exit(1);}\
else if (OCI_CONTINUE==status) \
{puts((char *)"Error: OCI_CONTINUE");\
exit(1);}\
else {printf("Error: unknown status %d\n", status);\
exit(1);}\
} while(0)

static void connect_db(conn_info_t *opt_params_p, oci_t ** ocip, ub2 char_csid,
                       ub2 nchar_csid);
static void disconnect_db(oci_t * ocip);
static void ocierror(oci_t * ocip, char * msg, boolean stop_on_error);
static void attach_session(oci_t *ocip, conn_info_t *paramsp, boolean outbound);
static void query_withROWID(oci_t *ocip, myctx_t *myctx,
                               char* rowid, ub2 rowidl, boolean idkey);
static void detach_session(oci_t *ocip, boolean from_errhandler);
static void get_lcrs(myctx_t *ctx);
static void print_lob_info (myctx_t *myctx);
static void get_db_charsets(conn_info_t *params_p, ub2 *char_csid, 
                            ub2 *nchar_csid);

/*---------------------------------------------------------------------- 
 * static variables used for array inserts
 *----------------------------------------------------------------------*/ 
#define DEFAULT_XOUT_DBNM  ((oratext *)"DBS1.REGRESS.RDBMS.DEV.US.ORACLE.COM")
#define DEFAULT_XOUT_DBNM_LEN    ((ub2)strlen((char *)DEFAULT_XOUT_DBNM))

#define XOUT_USER       (oratext *)"stradm"
#define XOUT_USER_LEN   (strlen((char *)XOUT_USER))
#define XOUT_PSW        (oratext *)"stradm" 
#define XOUT_PSW_LEN    (strlen((char *)XOUT_PSW))
#define XOUT_DB         (oratext *)"inst1" 
#define XOUT_DB_LEN     (strlen((char *)XOUT_DB))
#define XOUT_APPLY      (oratext *)"XOUT"      /* case insensitive name */
#define XOUT_APPLY_LEN  (strlen((char *)XOUT_APPLY))
#define DB_USER         (oratext *)"oe"
#define DB_USER_LEN     (strlen((char *)DB_USER))
#define DB_PSW          (oratext *)"oe"
#define DB_PSW_LEN      (strlen((char *)DB_PSW))

/*--------------------------------------------------------------------
 * print_usage - Print command usage
 *---------------------------------------------------------------------*/
static void print_usage(int exitcode)
{
  puts("\nUsage: xoidkey [-ob_svr <outbound_svr>] [-ob_db <outbound_db>]\n"
       "           [-ob_usr <conn_user>] [-ob_pwd <conn_user_pwd>]\n"
       " [-short]\n");
  puts("  ob_svr  : outbound server name\n"
       "  ob_db   : database name of outbound server\n"
       "  ob_usr  : connect user to outbound server\n"
       "  ob_pwd  : password of outbound's connect user\n"
       "  db_usr  : connect user to database\n"
       "  db_pwd  : password of database's connect user\n");
  
  exit(exitcode);
}

/*--------------------------------------------------------------------
 * get_inputs - Get user inputs from command line
 *---------------------------------------------------------------------*/
static void get_inputs(myctx_t *ctxp, 
                       conn_info_t *xout_params,
                       conn_info_t *db_params,
                       int argc, char ** argv)
{
  char * option;
  char * value;

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

    if (!strncmp(option, (char *)"short", 5))
    {
      ctxp->longform = FALSE;
      continue;
    }

    /* get the value of the option */
    --argc;
    argv++;

    value = *argv;    

    if (!strncmp(option, (char *)"ob_db", 5))
    {
      xout_params->dbname = (oratext *)value;
      xout_params->dbnamelen = strlen(value);
      db_params->dbname = (oratext *)value;
      db_params->dbnamelen = strlen(value);      
    }
    else if (!strncmp(option, (char *)"ob_usr", 6))
    {
      xout_params->user = (oratext *)value;
      xout_params->userlen = strlen(value);
    }
    else if (!strncmp(option, (char *)"ob_pwd", 6))
    {
      xout_params->passw = (oratext *)value;
      xout_params->passwlen = strlen(value);
    }
    else if (!strncmp(option, (char *)"ob_svr", 6))
    {
      xout_params->svrnm = (oratext *)value;
      xout_params->svrnmlen = strlen(value);
    }
    else if (!strncmp(option, (char *)"db_usr", 6))
    {
      db_params->user = (oratext *)value;
      db_params->userlen = strlen(value);
    }
    else if (!strncmp(option, (char *)"db_pwd", 6))
    {
      db_params->passw = (oratext *)value;
      db_params->passwlen = strlen(value);
    }     
    else
    {
      printf("Error: unknown option '%s'.\n", option);
      print_usage(1);
    }
  }
}

static void prompt(oratext *msg)
{
  char c;

  printf("%s", msg);
  scanf("%c", &c);
}
 
/*---------------------------------------------------------------------
 * ocierror - Print error status and exit program
 *---------------------------------------------------------------------*/
static void ocierror(oci_t * ocip, char * msg, boolean stop_on_error)
{
  sb4 errcode=0;
  text bufp[4096];
  time_t  tm;
  time(&tm);

  if (ocip->errp)
  {
    OCIErrorGet((dvoid *) ocip->errp, (ub4) 1, (text *) NULL, &errcode,
                bufp, (ub4) 4096, (ub4) OCI_HTYPE_ERROR);
    printf("%s\n%s", msg, bufp);
  }
  else
    puts(msg);

  printf ("\n=== Current time = %s", ctime(&tm));
  printf ("\n");
  if (stop_on_error)
  {
    printf("\n ocierror>> stop_on_error set, exiting...");
    if (ocip->attached)
      detach_session(ocip, TRUE);
    exit(1);
  }
}

/*---------------------------------------------------------------------
 * connect_db - Connect to the database.
 *---------------------------------------------------------------------*/
void connect_db(conn_info_t *conn, oci_t ** ociptr, ub2 char_csid, ub2 nchar_csid)
{
  oci_t        *ocip;

  printf ("\nConnect to Oracle as %.*s/%.*s@%.*s\n",
          conn->userlen, conn->user, conn->passwlen, conn->passw, 
          conn->dbnamelen, conn->dbname);
       
  if (char_csid && nchar_csid)
    printf ("using char csid=%d and nchar csid=%d\n", char_csid, nchar_csid);

  ocip = (oci_t *)malloc(sizeof(oci_t));

  ocip->attached = FALSE;

  /* Create UTF8 environment */
  if (OCIEnvNlsCreate(&ocip->envp, OCI_OBJECT, (dvoid *)0,
                     (dvoid * (*)(dvoid *, size_t)) 0,
                     (dvoid * (*)(dvoid *, dvoid *, size_t))0,
                     (void (*)(dvoid *, dvoid *)) 0,
                      (size_t) 0, (dvoid **) 0, char_csid, nchar_csid))
  {
    ocierror(ocip, (char *)"OCIEnvCreate() failed", TRUE);
  }

  if (OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **) &ocip->errp,
                     (ub4) OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0))
  {
    ocierror(ocip, (char *)"OCIHandleAlloc(OCI_HTYPE_ERROR) failed", TRUE);
  }

  /* allocate the server handle */
  OCICALL(ocip,
          OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **) &ocip->srvp,
                         OCI_HTYPE_SERVER, (size_t) 0, (dvoid **) 0));

  /* create a server context */
  OCICALL(ocip,
          OCIServerAttach (ocip->srvp, ocip->errp, 
                           (oratext *)conn->dbname, 
                           (sb4)conn->dbnamelen, OCI_DEFAULT));

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
                     (dvoid *) conn->user, (ub4)  conn->userlen,
                     (ub4) OCI_ATTR_USERNAME, ocip->errp));

  /* set the password in the session */
  OCICALL(ocip,
          OCIAttrSet((dvoid *) ocip->authp, (ub4) OCI_HTYPE_SESSION,
                     (dvoid *) conn->passw, (ub4) conn->passwlen,
                     (ub4) OCI_ATTR_PASSWORD, ocip->errp));

  OCICALL(ocip,
          OCISessionBegin(ocip->svcp,  ocip->errp, ocip->authp,
                          OCI_CRED_RDBMS, (ub4)OCI_DEFAULT));

  OCICALL(ocip,
          OCIAttrSet((dvoid *) ocip->svcp, (ub4) OCI_HTYPE_SVCCTX,
                     (dvoid *) ocip->authp, (ub4) 0,
                     (ub4) OCI_ATTR_SESSION, ocip->errp));

  if (OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **) &ocip->stmtp,
                     (ub4) OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0))
  {
    ocierror(ocip, (char *)"OCIHandleAlloc(OCI_HTYPE_STMT) failed", TRUE);
  }

  if (OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **) &ocip->stmt2p,
                     (ub4) OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0))
  {
    ocierror(ocip, (char *)"OCIHandleAlloc(OCI_HTYPE_STMT-2) failed", TRUE);
  }

  if (*ociptr == (oci_t *)NULL)
  {
    *ociptr = ocip;
  }

  /* restore the interrupt signal handler */
  signal(SIGINT, SIG_DFL);
}

/*---------------------------------------------------------------------
 * disconnect_db  - Logoff from the database
 *---------------------------------------------------------------------*/
void disconnect_db(oci_t * ocip)
{
  if (OCILogoff(ocip->svcp, ocip->errp))
  {
    ocierror(ocip, (char *)"OCILogoff() failed", TRUE);
  }

  if (ocip->stmtp)
    OCIHandleFree((dvoid *) ocip->stmtp, (ub4) OCI_HTYPE_STMT);

  if (ocip->stmt2p)
    OCIHandleFree((dvoid *) ocip->stmt2p, (ub4) OCI_HTYPE_STMT);
  
  if (ocip->errp)
    OCIHandleFree((dvoid *) ocip->errp, (ub4) OCI_HTYPE_ERROR);

  if (ocip->envp)
    OCIHandleFree((dvoid *) ocip->envp, (ub4) OCI_HTYPE_ENV);
}

/*---------------------------------------------------------------------
 * print_number - Print NUMBER value
 *---------------------------------------------------------------------*/
static void print_number(oci_t  *ocip, OCINumber *rawnum, boolean seqlcr)
{
  oratext     numbuf[NUM_BUF_SIZE+1];
  ub4         numsize = NUM_BUF_SIZE;
  sword       ret;

  ret = OCINumberToText(ocip->errp, (const OCINumber *)rawnum,
                        (const oratext *)"9999999.999", 10, 
                        (const oratext *)0, 0,
                        &numsize, numbuf);

  if (ret == OCI_ERROR)
  {
    if (seqlcr)
    {
      printf ("Sequence LCR: number too large for given format!");
    }
    else
    {
      ocierror(ocip, (char *)"OCINumberToText error ", FALSE);
    }
  }

  printf ("%.*s", numsize, numbuf);
}

#define TS_FORMAT      (oratext *)"DD-MON-YYYY HH24:MI:SS.FF"
#define TSZ_FORMAT     (oratext *)"DD-MON-YYYY HH24:MI:SS.FF TZH:TZM"

/*---------------------------------------------------------------------
 * print_lcr_data - Print all the columns include ADT in this ROW LCR
 *---------------------------------------------------------------------*/
static sb4 process_lcr_data(myctx_t *ctxp, void *lcrp, ub2 col_value_type,
                            boolean seqlcr)
{
  ub2        collist_type;
  ub2        num_cols;
  oratext   *colname[MAX_COLUMNS]; 
  ub2        colnamel[MAX_COLUMNS]; 
  ub2        coldty[MAX_COLUMNS]; 
  void      *colval[MAX_COLUMNS]; 
  ub2        collen[MAX_COLUMNS]; 
  OCIInd     colind[MAX_COLUMNS]; 
  ub1        colcsf[MAX_COLUMNS]; 
  oraub8     colflg[MAX_COLUMNS]; 
  ub2        colcsid[MAX_COLUMNS]; 
  sword      ret;
  ub2        idx;
  oci_t     *ocip = ctxp->outbound_ocip;
  oratext    buf[80];
  ub4        mode;
  
  printf ("process_lcr_data col_value_type=%s\n",
          col_value_type == OCI_LCR_ROW_COLVAL_OLD ? "OLD" : "NEW");

  if (OCI_LCR_ROW_COLVAL_OLD == col_value_type)
  {
    mode = OCI_DEFAULT;
  }
  else
  {
    /* Only want new columns */
    mode = OCI_DEFAULT | OCILCR_NEW_ONLY_MODE;
  }
  
  ret = OCILCRRowColumnInfoGet(ocip->svcp, ocip->errp, col_value_type, 
                               &num_cols, (oratext **)&colname, 
                               (ub2 *)&colnamel,
                               (ub2 *)&coldty, (void **)&colval, 
                               (OCIInd *)&colind, (ub2 *)&collen, 
                               (ub1 *)&colcsf, (oraub8*)colflg, (ub2 *)colcsid,
                               lcrp, MAX_COLUMNS, mode);
  
  if (ret != OCI_SUCCESS)
  {
    ocierror(ocip, (char *)"OCILCRRowColumnInfoGet failed ", FALSE);
    return ret;
  }
  printf ("num_columns=%d \n", num_cols); 

  /* if num_colums = 0, we need to check if the data is unsupported type */
  if (num_cols == 0) return OCI_STILL_EXECUTING;

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

          printf (" value=");
          print_number(ocip, rawnum, seqlcr);
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
          sword         ret = OCI_SUCCESS;
          
          ret = OCIDateTimeToText(ocip->envp, ocip->errp,
                                  dtvalue, TS_FORMAT,
                                  (ub1) strlen((const char *)TS_FORMAT),
                                  (ub1) 6, (const oratext*)0, (size_t) 0,
                                  &bufsize, buf);
          if (!ret)
          {
            printf (" value=%.*s ", bufsize, buf);
          }
          else
          {
            /* tklmqa02t.sql was having issues printing C_TS for update 2.4.1.
             * The column data itself looks good, unclear why it failed. For
             * now, if we get an error converting, just print this out.
             */
            printf (" cannot convert timestamp ");
          }
          break;
        }
        case SQLT_TIMESTAMP_TZ:
        {
          OCIDateTime  *dtvalue = colval[idx];
          ub4           bufsize = sizeof(buf);

          OCICALL(ocip,
                  OCIDateTimeToText(ocip->envp, ocip->errp,
                                    dtvalue, TSZ_FORMAT,
                                    (ub1) strlen((const char *)TSZ_FORMAT),
                                    (ub1) 6, (const oratext*)0, (size_t) 0,
                                    &bufsize, buf)); 
          printf (" value=%.*s ", bufsize, buf);
          
          break;
        }
        case SQLT_INTERVAL_YM:
        case SQLT_INTERVAL_DS:
        {
          OCIInterval  *intv = colval[idx];
          size_t        reslen;

          OCICALL(ocip,
                  OCIIntervalToText(ocip->envp, ocip->errp,
                                    intv, 9, 6, buf, sizeof(buf), &reslen)); 
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
          printf (" dty=%d collen=%d\n", coldty[idx], collen[idx]);
          /* print out max of MAX_PRINT_BYTES bytes of data */
          if (collen[idx] > MAX_PRINT_BYTES)
            printlen = MAX_PRINT_BYTES;
          else
            printlen = collen[idx];
          for (idx2 = 0; idx2 < printlen; idx2++)
            printf("%02x ", byteptr[idx2]); 
          printf("\n");
          break;
        }
      }
    }
    printf ("\n");
  }
  return ret;
}

/*---------------------------------------------------------------------
 * print_ddl - Print ddl LCR
 *---------------------------------------------------------------------*/
static void print_ddl(oci_t *ocip, void *lcrp)
{
  sword     ret;
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

  ret = OCILCRDDLInfoGet(ocip->svcp, ocip->errp,
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
  printf("Table=%.*s.%.*s\n", base_table_ownerl, base_table_owner,
         base_table_namel, base_table_name);
  printf("Statement=%.*s\n", ddl_textl, ddl_text);
}

/*---------------------------------------------------------------------
 * print_raw - Print raw value
 *---------------------------------------------------------------------*/
static void print_raw(ub1 *rawval, ub2 rawval_len)
{
  ub2 i;

  for (i=0; i<rawval_len; i++)
  {
    printf("%1x", rawval[i] >> 4);
    printf("%1x", rawval[i] & 0xf);
  }
}

/*---------------------------------------------------------------------
 * print_pos - Print CSCN and SCN for this LCR
 *---------------------------------------------------------------------*/
static void print_pos(oci_t *ocip, ub1 *pos, ub2 poslen, char *comm)
{
  sword        ret;
  OCINumber    scn;
  OCINumber    cscn;
  ub4          numval;

  printf("%s=", comm);
  print_raw(pos, poslen);
  printf("\n");
  ret = OCILCRSCNsFromPosition(ocip->svcp, ocip->errp,
                               pos, poslen, &scn, &cscn, OCI_DEFAULT);
  if (ret != OCI_SUCCESS)
    printf ("OCILCRNumberFromPosition ERROR %d\n", ret);

  ret = OCINumberToInt(ocip->errp, (const OCINumber *)&cscn,
                       sizeof(numval), OCI_NUMBER_UNSIGNED, &numval);

  printf("  Position CSCN=%d ", numval);

  ret = OCINumberToInt(ocip->errp, (const OCINumber *)&scn,
                       sizeof(numval), OCI_NUMBER_UNSIGNED, &numval);

  printf("  SCN=%d\n", numval);
}

/*---------------------------------------------------------------------
 * process_lcr - Print this LCR and process the data in it
 *---------------------------------------------------------------------*/
static sb4 process_lcr(myctx_t *ctxp, void *lcrp, ub1 lcrtype,
                       boolean *idkey_lcr, boolean *stmt_lcr)
{
  oratext     *src_db_name;
  ub2          src_db_namel;
  oratext     *cmd_type;
  ub2          cmd_type_len;
  oratext     *owner;
  ub2          ownerl;
  oratext     *oname;
  ub2          onamel;
  ub1         *tag;
  ub2          tagl;
  oratext     *txid;
  ub2          txidl;
  OCIDate      src_time;
  oci_t       *ocip = ctxp->outbound_ocip;
  sword        ret;
  oratext      datebuf[DATE_BUF_LEN+1];
  ub4          datelen = DATE_BUF_LEN;
  ub1         *pos;
  ub2          pos_len;
  oraub8       flag = 0;
  boolean      seqlcr = FALSE;

  *idkey_lcr = FALSE;
  *stmt_lcr = FALSE;
  
  /* Get LCR Header information */  
  ret = OCILCRHeaderGet(ocip->svcp, ocip->errp,
                        &src_db_name, &src_db_namel,       /* source db */
                        &cmd_type, &cmd_type_len,       /* command type */
                        &owner, &ownerl,                  /* owner name */
                        &oname, &onamel,                 /* object name */
                        &tag, &tagl,                         /* lcr tag */
                        &txid, &txidl, &src_time, /* txn id  & src time */
                        (ub2 *)0, (ub2 *)0,         /* OLD/NEW col cnts */
                        &pos, &pos_len,                 /* LCR position */
                        &flag, lcrp, OCI_DEFAULT);

  printf("\n ----------- %s LCR Header  -----------------\n",
         lcrtype == OCI_LCR_XDDL ? "DDL" : "ROW");
  
  if (ret != OCI_SUCCESS)
    ocierror(ocip, (char *)"OCILCRHeaderGet failed", TRUE);
  else
  {
    /* check sequence lcr */
    if (lcrtype == OCI_LCR_XROW)
    {
      seqlcr = (lbit(flag, OCI_ROWLCR_SEQ_LCR) > 0);
    }
      
    printf("OCILCRHeaderGet, flag is 0x%x\n", flag);
    
    /* See if this is an idkey LCR */
    if (lbit(flag, OCI_ROWLCR_HAS_ID_KEY_ONLY))
    {
      printf("XXX ID KEY LCR\n");
      *idkey_lcr = TRUE;
    }

    memcpy(ctxp->last_lcrpos, pos, pos_len);
    ctxp->last_lcrpos_len = pos_len;

    print_pos(ocip, pos, pos_len, (char *)"LCR ID");
    printf("src_db_name=%.*s cmd_type=%.*s txid=%.*s\n",
           src_db_namel, src_db_name, cmd_type_len, cmd_type, txidl, txid );
    ctxp->is_commit = FALSE;
    if (samecmd(cmd_type, cmd_type_len,
                OCI_LCR_ROW_CMD_COMMIT, strlen(OCI_LCR_ROW_CMD_COMMIT)))
    {
      ctxp->is_commit = TRUE;
      return OCI_SUCCESS;
    }
     
    printf("owner=%.*s oname=%.*s \n", ownerl, owner, onamel, oname);

    ctxp->ownerlen = ownerl;
    ctxp->owner    = owner;
    ctxp->onamelen = onamel;
    ctxp->oname    = oname;
    ctxp->txid     = txid;
    ctxp->txidlen  = txidl;

    if (lcrtype == OCI_LCR_XDDL)
    {
      print_ddl(ocip, lcrp);
    }
    else if (lcrtype == OCI_LCR_XROW && ctxp->longform)
    {
      boolean  loblcr = FALSE;

      loblcr = 
         (samecmd(cmd_type, cmd_type_len,
              OCI_LCR_ROW_CMD_LOB_WRITE, strlen(OCI_LCR_ROW_CMD_LOB_WRITE)) ||
          samecmd(cmd_type, cmd_type_len,
              OCI_LCR_ROW_CMD_LOB_TRIM, strlen(OCI_LCR_ROW_CMD_LOB_TRIM)) ||
          samecmd(cmd_type, cmd_type_len,
              OCI_LCR_ROW_CMD_LOB_ERASE, strlen(OCI_LCR_ROW_CMD_LOB_ERASE)));

      if (loblcr)
        print_lob_info (ctxp);

      if (samecmd(cmd_type, cmd_type_len,
                  OCI_LCR_ROW_CMD_DELETE, strlen(OCI_LCR_ROW_CMD_DELETE)) ||
          samecmd(cmd_type, cmd_type_len,
                  OCI_LCR_ROW_CMD_UPDATE, strlen(OCI_LCR_ROW_CMD_UPDATE)))
      {
        ret = process_lcr_data(ctxp, lcrp, OCI_LCR_ROW_COLVAL_OLD, seqlcr);
      }

      if (samecmd(cmd_type, cmd_type_len,
                  OCI_LCR_ROW_CMD_INSERT, strlen(OCI_LCR_ROW_CMD_INSERT)) ||
          samecmd(cmd_type, cmd_type_len,
                  OCI_LCR_ROW_CMD_UPDATE, strlen(OCI_LCR_ROW_CMD_UPDATE)) ||
          loblcr)
      {
        ret = process_lcr_data(ctxp, lcrp, OCI_LCR_ROW_COLVAL_NEW, seqlcr);
      }
    }
  } 
  return ret;
}

/*---------------------------------------------------------------------
 * print_flags - Print the meaning of a flag.
 *---------------------------------------------------------------------*/
static void print_flags (oraub8 flags, ub2 csid)
{
  if (lbit(flags, OCI_LCR_COLUMN_XML_DATA | OCI_LCR_COLUMN_XML_DIFF))
    printf ("column csid=%d flags: ", csid);
  else
    printf ("column flags: ");
  if (lbit(flags, OCI_LCR_COLUMN_LOB_DATA))
    printf("LOB_DATA ");
  if (lbit(flags, OCI_LCR_COLUMN_EMPTY_LOB))
    printf("EMPTY ");
  if (lbit(flags, OCI_LCR_COLUMN_LAST_CHUNK))
    printf("LAST_CHUNK ");
  if (lbit(flags, OCI_LCR_COLUMN_AL16UTF16))
    printf("UTF16 ");
  if (lbit(flags, OCI_LCR_COLUMN_ENCRYPTED))
    printf("ENCRYPTED ");
  if (lbit(flags, OCI_LCR_COLUMN_NCLOB))
    printf("NCLOB ");
  if (lbit(flags, OCI_LCR_COLUMN_XML_DATA))
    printf("XMLDATA ");
  if (lbit(flags, OCI_LCR_COLUMN_XML_DIFF))
    printf("XMLDIFF ");
  printf("\n");
}

static void print_buffer (ub1 *bufp, ub4 buflen, ub2 dty)
{
  ub4  idx2;

  if (buflen == 0)
    return;

  if (buflen > MAX_PRINT_BYTES)
  {
    if (dty == SQLT_CHR) 
    {
      printf("Beg of buffer=%.*s\n", MAX_PRINT_BYTES, bufp);
      printf("End of buffer=%.*s\n", MAX_PRINT_BYTES, 
             &bufp[buflen-MAX_PRINT_BYTES]);
    }
    else
    {
      printf("Beg of buffer=");
      for (idx2 = 0; idx2 < MAX_PRINT_BYTES; idx2++)
        printf("%02x ", bufp[idx2]); 
      printf("\nEnd of buffer=");
      for (idx2 = buflen-MAX_PRINT_BYTES; idx2 < buflen; idx2++)
        printf("%02x ", bufp[idx2]); 
      printf("\n");
    }
  }
  else
  {
    printf("buffer=");
    if (dty == SQLT_CHR) 
      printf("%.*s\n", buflen, bufp);
    else
    {
      for (idx2 = 0; idx2 < buflen; idx2++)
        printf("%02x ", bufp[idx2]); 
    }
    printf("\n");
  }
}

/*---------------------------------------------------------------------
 * print_lob_info - Print the lob chunk of given lcr.
 *---------------------------------------------------------------------*/
static void print_lob_info (myctx_t *myctx)
{
  oci_t    *ocip = myctx->outbound_ocip;
  oratext  *colname;
  ub2       colname_len;
  ub2       coldty;
  oraub8    col_flags;
  ub4       op_offset = 0;
  ub4       op_size = 0;

  if (OCILCRLobInfoGet(ocip->svcp, ocip->errp, &colname, &colname_len,
                       &coldty, &col_flags, &op_offset, &op_size,
                       myctx->lcrp, OCI_DEFAULT) == OCI_SUCCESS)
  {
    printf("Chunked colname = %.*s  coldty=%d offset=%d size=%d\n",
            colname_len, colname, coldty, op_offset, op_size);
    print_flags(col_flags, 0);
  }
  else
  {
    ocierror(ocip, (char *)"Error from OCILCRLobInfoGet", FALSE);
  }
}

/*---------------------------------------------------------------------
 * getRowID - Get RowID of this ID Key LCR
 *---------------------------------------------------------------------*/
static sword getRowID (myctx_t* ctxp, void* lcrp, oratext* rowid, ub2* len)
{
  oci_t   *ocip = ctxp->outbound_ocip;
  ub2      i = 0;      
  ub2      num_attrs = 0;
  void    *attr_value[OCI_LCR_MAX_ATTRIBUTES];
  oratext *attr_name [OCI_LCR_MAX_ATTRIBUTES];
  ub2      attr_namel[OCI_LCR_MAX_ATTRIBUTES];
  ub2      attr_dty[OCI_LCR_MAX_ATTRIBUTES];
  OCIInd   attr_ind[OCI_LCR_MAX_ATTRIBUTES];
  ub2      attr_len[OCI_LCR_MAX_ATTRIBUTES];

  if (rowid == NULL)
    return OCI_ERROR;

  /* get the attributes for this lcr */
  OCICALL(ocip, OCILCRAttributesGet(ocip->svcp, ocip->errp,
                                &num_attrs, attr_name, attr_namel, attr_dty,
                                attr_value, attr_ind, attr_len, lcrp,
                                OCI_LCR_MAX_ATTRIBUTES, OCI_DEFAULT));

  if (num_attrs == 0)
    return OCI_ERROR;

  for (i = 0; i < num_attrs; i++)
  {
    /* filter out Row_ID for this LCR */
    if ((attr_ind[i] != OCI_IND_NULL) &&
        !strncmp ((const char*)attr_name[i], OCI_LCR_ATTR_ROW_ID,
                  attr_namel[i]) &&
        (attr_dty[i] == SQLT_RDD))
    {
          OCIRowid  *rid = attr_value[i];
          oratext    rid_buf[80];
          ub2        reslen = (ub2)sizeof(rid_buf);

          OCICALL(ocip,
                  OCIRowidToChar(rid, rid_buf, &reslen, ocip->errp));

          memcpy (rowid, rid_buf, reslen);
          *len = reslen;
          return OCI_SUCCESS;
    }
  }
  return OCI_ERROR;
}

/*---------------------------------------------------------------------
 * handleIDKeyLCR - Get RowID of this ID Key LCR, then query source
 *                  database with the RowID.
 *---------------------------------------------------------------------*/
static void handleIDKeyLCR (myctx_t *myctx, void *lcrp)
{
  oratext rowid[80];
  ub2 len = (ub2)sizeof(rowid);

  /* get rowid from LCR */
  getRowID(myctx, lcrp, rowid, &len);

  printf("ID Key LCR: RowID = %.*s\n", len, rowid);
  printf("myctx->owner=%.*s, len=%u\n", myctx->ownerlen, myctx->owner, myctx->ownerlen);
  printf("myctx->oname=%.*s, len=%u\n", myctx->onamelen, myctx->oname, myctx->onamelen);
  printf("myctx->txid =%.*s, len=%u\n", myctx->txidlen,  myctx->txid,  myctx->txidlen);

  /* query source DB with this row id and then print the result */
  query_withROWID(myctx->sql_sel_ocip, myctx, (char *)rowid, len, TRUE);
}

/*---------------------------------------------------------------------
 * process_IDKeyLCR - process lcrs, and if it is IDkey then print it,
 *               and query source DB with it's row id.
 *---------------------------------------------------------------------*/
static void process_IDKeyLCR(myctx_t *myctx, void *lcrp, ub1 lcrtyp, oraub8 flag)
{
  sword     ret;
  boolean   id_key_lcr = FALSE;
  boolean   stmt_lcr = FALSE;

  myctx->lcrcnt++;
  printf("\n ----------- Contents of LCR %d -----------------\n",
         myctx->lcrcnt);

  /* save lcr in the context for later use */
  myctx->lcrp = lcrp; 

  /* Process this LCR. We print the contents and also gather necessary
   * data for RowID process
   */
  ret = process_lcr(myctx, lcrp, lcrtyp, &id_key_lcr, &stmt_lcr);
  fflush (stdout);

  if (id_key_lcr)
  {
    /* print row id of this IDKey LCR and query source DB with the
     * row id.
     */
    handleIDKeyLCR(myctx, lcrp);
  }
}

/*---------------------------------------------------------------------
 * print_chunk_cb - Print chunked column information. Only print the first
 *                  50 bytes for each chunk. 
 *---------------------------------------------------------------------*/
static void print_chunk_cb(myctx_t *myctx, 
  oratext *colname, ub2 colname_len,
  ub2 coldty, oraub8 col_flags, ub2 col_csid,
  ub4 bytes_rd, ub1 *buffer, oraub8 row_flag)
{
  ub1       lsb_flag = (ub1)(row_flag & 0xff);               /* get lsb byte */

  if (colname_len == 0)
  {
    printf("print_chunk_cb: invalid zero colname_len.\n");
    exit (1);
  }

  printf("row_flag=%x colname = %.*s  coldty=%d bytes_rd=%d\n",
          lsb_flag, colname_len, colname, coldty, bytes_rd);

  print_flags(col_flags, col_csid);

  if (lbit(col_flags, OCI_LCR_COLUMN_XML_DATA))
    print_buffer(buffer, bytes_rd, SQLT_CHR);
  else
    print_buffer(buffer, bytes_rd, coldty);
}

/*---------------------------------------------------------------------
 * attach - Attach to XStream server specified in connection info
 *---------------------------------------------------------------------*/
static void attach_session(oci_t * ocip, conn_info_t *conn, boolean outbound)
{
  sword       err;
  ub4         mode = OCI_DEFAULT;
  ub4         ack_interval = 30;
  ub4         idle_tmout = 5;

  printf (">>> attach_session server=%.*s \n", conn->svrnmlen, conn->svrnm);

  OCICALL(ocip,
          OCIAttrSet((dvoid *) ocip->srvp, (ub4) OCI_HTYPE_SERVER,
                     (dvoid *) &ack_interval, (ub4) 0,
                     (ub4) OCI_ATTR_XSTREAM_ACK_INTERVAL, ocip->errp));

  if (outbound)
  {
    OCICALL(ocip,
            OCIAttrSet((dvoid *) ocip->srvp, (ub4) OCI_HTYPE_SERVER,
                       (dvoid *) &idle_tmout, (ub4) 0,
                       (ub4) OCI_ATTR_XSTREAM_IDLE_TIMEOUT, ocip->errp));

    OCICALL(ocip, 
            OCIXStreamOutAttach(ocip->svcp, ocip->errp, conn->svrnm,
                              (ub2)conn->svrnmlen, (ub1 *)0, 0, OCI_DEFAULT));
  }
  else
  {
    OCICALL(ocip, 
            OCIXStreamInAttach(ocip->svcp, ocip->errp, conn->svrnm,
                               (ub2)conn->svrnmlen, (oratext *)"From_XOUT", 9,
                               (ub1 *)0, 0, OCI_DEFAULT));
  }

  ocip->attached = TRUE;
  ocip->outbound = outbound;

  printf (">>> attach_session -- Done. \n");
}

/*---------------------------------------------------------------------
 * query_withROWID - query the source db with rowid
 *---------------------------------------------------------------------*/
static void query_withROWID(oci_t *ocip, myctx_t *myctx,
                               char* rowid, ub2 rowidl, boolean idkey)
{
  oratext    tmp [2048];
  oratext    *sqlstmt = (oratext *)
                        "select count(*) from %.*s where ROWID = '%.*s'";
  OCIDefine  *defnp1 = (OCIDefine *) NULL;
  OCIBind    *bindp1 = (OCIBind *)NULL;
  ub4         count = 0;

  sprintf((char *)tmp, (char *)sqlstmt, myctx->onamelen, myctx->oname, rowidl, rowid);
  printf(">>> query statement = %s\n", tmp);
  OCICALL(ocip, OCIStmtPrepare(ocip->stmtp, ocip->errp,
                               (CONST text *)tmp,
                               (ub4)strlen((char *)tmp),
                               (ub4)OCI_NTV_SYNTAX,
                               (ub4)OCI_DEFAULT));
  OCICALL(ocip,OCIDefineByPos(ocip->stmtp, &defnp1,
                              ocip->errp, (ub4) 1, (dvoid*) &count,
                              (sb4) sizeof(count), (ub2)SQLT_INT, (dvoid*) 0,
                              (ub2 *) 0, (ub2 *) 0, OCI_DEFAULT));
  OCICALL(ocip, OCIStmtExecute(ocip->svcp, ocip->stmtp, ocip->errp,
                               (ub4)1, (ub4)0, (const OCISnapshot *)0,
                               (OCISnapshot *)0, (ub4)OCI_DEFAULT));
  printf ("\n>>> query result: count = %d\n", count);
}

/*---------------------------------------------------------------------
 * detach - Detach from outbound server
 *---------------------------------------------------------------------*/
static void detach_session(oci_t * ocip, boolean from_errhandler)
{
  sword  err = OCI_SUCCESS;

  printf (">>> Detach from XStream %s server\n",
          ocip->outbound ? "outbound" : "inbound" );

  if (ocip->outbound)
  {
    err = OCIXStreamOutDetach(ocip->svcp, ocip->errp, OCI_DEFAULT);
  }
  else
  {
   
    /* end any in-progress XStream In call */
    err = OCIXStreamInFlush(ocip->svcp, ocip->errp, OCI_DEFAULT);

    err = OCIXStreamInDetach(ocip->svcp, ocip->errp, (ub1 *)0, (ub2 *)0,
                             OCI_DEFAULT);
  }

  if (err && !from_errhandler)
  {
    ocierror(ocip, (char *)"detach_session() failed", TRUE);
  }

  printf (">>> detach_session -- Done\n");
}

/*---------------------------------------------------------------------
 * get_chunks - Get all the chunks belonging to the current LCR.
 *---------------------------------------------------------------------*/
static void get_chunks(myctx_t *ctx)
{
  oratext *colname; 
  ub2      colname_len;
  ub2      coldty; 
  oraub8   col_flags; 
  ub2      col_csid;
  ub4      bytes_rd; 
  ub1     *buffer; 
  oraub8   row_flag;
  sword    err;
  sb4      rtncode;
  oci_t   *ocip = ctx->outbound_ocip;

  /* Loop to receive each chunk until there is no more data for the current
   * row change.
   */
  do
  {
    OCICALL(ocip, 
            OCIXStreamOutChunkReceive(ocip->svcp, ocip->errp, &colname, 
                                      &colname_len, &coldty, &col_flags, 
                                      &col_csid, &bytes_rd, &buffer, 
                                      &row_flag, OCI_DEFAULT));
    
    print_chunk_cb(ctx, colname, colname_len, coldty, col_flags, 
                   col_csid, bytes_rd, buffer, row_flag);

  } while (row_flag & OCI_XSTREAM_MORE_ROW_DATA);
}

/*---------------------------------------------------------------------
 * get_lcrs - Execute loop to get lcrs from outbound server.
 *---------------------------------------------------------------------*/
static void get_lcrs(myctx_t *ctx)
{
  sword       err = OCI_SUCCESS;
  ub4         startcnt = 0;
  void       *lcr;
  ub1         lcrtype;
  oraub8      flag;
  ub1         flwm[OCI_LCR_MAX_POSITION_LEN];
  ub2         flwm_len = 0;
  ub1         proclwm[OCI_LCR_MAX_POSITION_LEN];
  ub2         proclwm_len = 0;
  sb4         rtncode = 0;
  oci_t      *ocip = ctx->outbound_ocip;

  printf ("\n>>> get_lcrs -- Start\n");

  ctx->lcrcnt = 0;

  while (err == OCI_SUCCESS)
  {
    startcnt = ctx->lcrcnt;
    /* if unsupported LCRs are returned, we need to ignore them and
     * continue
     */
    while ((err = OCIXStreamOutLCRReceive(ocip->svcp, ocip->errp,
                    &lcr, &lcrtype, &flag, flwm, &flwm_len, OCI_DEFAULT))
               == OCI_STILL_EXECUTING)
    {
      /* print ID Key LCRs */
      process_IDKeyLCR(ctx, lcr, lcrtype, flag);
      
      /* If LCR has chunked columns (i.e, has LOB/Long/XMLType columns) */
      if (flag & OCI_XSTREAM_MORE_ROW_DATA)
      {
        /* Get all the chunks belonging to the current LCR. */
        get_chunks(ctx);
      }
    }

    /* print lwm */
    if (flwm_len > 0) 
    {
      time_t  tm;
      time(&tm);
      printf ("\n=== Current time = %s", ctime(&tm));
      print_pos(ocip, flwm, flwm_len, (char *)"Outbound lwm");
    }
 
  }
  
  if (err)
  {
    ocierror(ocip, (char *)"get_lcrs() encounters error", FALSE);
  }

  printf (">>> get_lcrs [DONE]\n\n");
}

/*---------------------------------------------------------------------
 * get_db_charsets - Get the database CHAR and NCHAR character set ids.
 *---------------------------------------------------------------------*/
static const oratext GET_DB_CHARSETS[] =  \
"select parameter, value from nls_database_parameters where parameter = \
 'NLS_CHARACTERSET' or parameter = 'NLS_NCHAR_CHARACTERSET'";

#define PARM_BUFLEN      (30)

static void get_db_charsets(conn_info_t *params_p, ub2 *char_csid, 
                            ub2 *nchar_csid)
{
  OCIDefine  *defnp1 = (OCIDefine *) NULL;
  OCIDefine  *defnp2 = (OCIDefine *) NULL;
  oratext     parm[PARM_BUFLEN];
  oratext     value[OCI_NLS_MAXBUFSZ];
  ub2         parm_len = 0;
  ub2         value_len = 0;
  oci_t       ocistruct; 
  oci_t      *ocip = &ocistruct;
   
  *char_csid = 0;
  *nchar_csid = 0;
  memset (ocip, 0, sizeof(ocistruct));

  if (OCIEnvCreate(&ocip->envp, OCI_OBJECT, (dvoid *)0,
                   (dvoid * (*)(dvoid *, size_t)) 0,
                   (dvoid * (*)(dvoid *, dvoid *, size_t))0,
                   (void (*)(dvoid *, dvoid *)) 0,
                   (size_t) 0, (dvoid **) 0))
  {
    ocierror(ocip, (char *)"OCIEnvCreate() failed", TRUE);
  }

  if (OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **) &ocip->errp,
                     (ub4) OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0))
  {
    ocierror(ocip, (char *)"OCIHandleAlloc(OCI_HTYPE_ERROR) failed", TRUE);
  }

  OCICALL(ocip, 
          OCILogon(ocip->envp, ocip->errp, &ocip->svcp,
                   params_p->user, params_p->userlen,
                   params_p->passw, params_p->passwlen,
                   params_p->dbname, params_p->dbnamelen));

  OCICALL(ocip, 
          OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **) &ocip->stmtp,
                         (ub4) OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0));

  /* Execute stmt to select the db nls char and nchar character set */ 
  OCICALL(ocip, 
          OCIStmtPrepare(ocip->stmtp, ocip->errp,
                         (CONST text *)GET_DB_CHARSETS,
                         (ub4)strlen((char *)GET_DB_CHARSETS),
                         (ub4)OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT));

  OCICALL(ocip,
          OCIDefineByPos(ocip->stmtp, &defnp1,
                         ocip->errp, (ub4) 1, parm,
                         PARM_BUFLEN, SQLT_CHR, (void*) 0,
                         &parm_len, (ub2 *)0, OCI_DEFAULT));

  OCICALL(ocip,
          OCIDefineByPos(ocip->stmtp, &defnp2,
                         ocip->errp, (ub4) 2, value,
                         OCI_NLS_MAXBUFSZ, SQLT_CHR, (void*) 0,
                         &value_len, (ub2 *)0, OCI_DEFAULT));

  OCICALL(ocip, 
          OCIStmtExecute(ocip->svcp, ocip->stmtp, 
                         ocip->errp, (ub4)0, (ub4)0, 
                         (const OCISnapshot *)0,
                         (OCISnapshot *)0, (ub4)OCI_DEFAULT));

  while (OCIStmtFetch(ocip->stmtp, ocip->errp, 1,
                      OCI_FETCH_NEXT, OCI_DEFAULT) == OCI_SUCCESS)
  {
    value[value_len] = '\0';
    if (parm_len == strlen("NLS_CHARACTERSET") &&
        !memcmp(parm, "NLS_CHARACTERSET", parm_len))
    {
      *char_csid = OCINlsCharSetNameToId(ocip->envp, value);
      printf("Outbound database NLS_CHARACTERSET = %.*s (csid = %d) \n",
             value_len, value, *char_csid);
    }
    else if (parm_len == strlen("NLS_NCHAR_CHARACTERSET") &&
             !memcmp(parm, "NLS_NCHAR_CHARACTERSET", parm_len))
    {
      *nchar_csid = OCINlsCharSetNameToId(ocip->envp, value);
      printf("Outbound database NLS_NCHAR_CHARACTERSET = %.*s (csid = %d) \n",
             value_len, value, *nchar_csid);
    }
  }

  disconnect_db(ocip);
}

/*---------------------------------------------------------------------
 *                M A I N   P R O G R A M
 *---------------------------------------------------------------------*/
int main(int argc, char **argv)
{
  params_t     oci_params;
  
  /* Outbound connection info */
  conn_info_t  xout_params = {XOUT_USER, XOUT_USER_LEN, 
                               XOUT_PSW, XOUT_PSW_LEN,
                               XOUT_DB, XOUT_DB_LEN, 
                               XOUT_APPLY, XOUT_APPLY_LEN};
  
  /* Database connection info */
  conn_info_t  db_params = {DB_USER, DB_USER_LEN, 
                             DB_PSW, DB_PSW_LEN,
                             XOUT_DB, XOUT_DB_LEN,
                             (oratext *) 0, 0};
  
  myctx_t       ctx;
  oci_t        *ocip = (oci_t *)NULL;
  ub2           obdb_char_csid = 0;                 /* outbound db char csid */
  ub2           obdb_nchar_csid = 0;               /* outbound db nchar csid */

  memset(&ctx, 0, sizeof(ctx));
  ctx.longform = TRUE;
 
  /* parse command line arguments */
  get_inputs(&ctx, &xout_params, &db_params, argc, argv);
  
  /* Get the outbound database CHAR and NCHAR character set info */
  get_db_charsets(&xout_params, &obdb_char_csid, &obdb_nchar_csid);
  
  /* connect to the source database */
  connect_db(&xout_params, &ocip, obdb_char_csid, obdb_nchar_csid);
  /* Attach to outbound server */
  attach_session(ocip, &xout_params, TRUE);
  ctx.outbound_ocip = ocip;
  
  /* connect to source database to do query and inserts on source tables */
  ocip = (oci_t *)NULL;
  connect_db(&db_params, &ocip, obdb_char_csid, obdb_nchar_csid);
  ctx.sql_sel_ocip = ocip;

  /* Get lcrs from outbound server and send to inbound server */
  get_lcrs(&ctx);

  /* Detach from XStream servers */
  detach_session(ctx.outbound_ocip, FALSE);

  /* Disconnect from outbound database */
  disconnect_db(ctx.outbound_ocip);
  free(ctx.outbound_ocip);
  
  /* Disconnect from sql database */  
  disconnect_db(ctx.sql_sel_ocip);
  free(ctx.sql_sel_ocip);
  return 0;
}

/* end of file xoidkey.c */
