/* Copyright (c) 2008, 2009, Oracle and/or its affiliates. 
All rights reserved. */

/*

   NAME
     XStreamOutClient.c - XStreams LCR Sql generation Client

   DESCRIPTION
     Connects to the XStreams Out server named 'DEMOOUT' that is
     on inst1 and receives LCRs. It generates sql using the received
     LCR and executes the sql on the same schema on inst2. 
    
     This client can attach to the xstreams out server in callback mode
     or non-callback mode.

     This can convert the lcr to sql with inlined values or bind values.

     The following parameters are allowed in the command line
      -bind      [use bind variables when generating LCR]
      -callback  [use XStreamsOUT callback mode to receive LCR]
      -printlcr  [Just print the generated SQL to the stdout]
      -debug     [prints the debug comments to the stdout ]

   
   NOTES
     Add a member to params_t structure when a new command line arg 
     has to be added.

     Add a member to myctx_t structure if something has to be globally
     accessed across the program.

     This program can be used to just print the lcrs in sql generated form
     or 
     drain the lcrs in the xstreams out so that fresh start is made
     or
     replicate the lcr using sql gen to a user in inst2
    

   MODIFIED   (MM/DD/YY)
   praghuna    03/13/08 - Creation

*/

#ifndef ORATYPES_ORACLE
#include <oratypes.h>
#endif

#ifndef STDIO_ORACLE
#include <stdio.h>
#endif

#ifndef STDLIB_ORACLE
#include <stdlib.h>
#endif

#ifndef STRING_ORACLE
#include <string.h>
#endif

#ifndef TYPES_ORACLE
#include <sys/types.h>
#endif

#ifndef TIME_ORACLE
#include <time.h>
#endif

#ifndef SIGNAL_ORACLE
#include <signal.h>
#endif

#ifndef OCIXSTREAM_ORACLE
#include <ocixstream.h>
#endif

/*---------------------------------------------------------------------- 
 *           Internal structures
 *----------------------------------------------------------------------*/ 
static void print_flags(oraub8 flags);
static ub1     *buffer = (ub1 *)0;
static ub4      bytes_rd;
static oraub8   flags;
static oratext *colname;
static ub2      colname_len;
static ub2      coldty;
static ub4      op_offset = 1;
static ub4      op_size = 0;
static ub4      g_timer = 0;
static const oratext DATE_FMT[] = "YYYY-MON-DD HH24:MI:SS";
static oratext *g_cmd_type;
static ub2      g_cmd_type_len;
static oratext *g_owner;
static  ub2     g_ownerl;
static  oratext *g_oname;
static  ub2      g_onamel;
static  ub1     *g_tag;
static  ub2      g_tagl;
static  ub1      g_temp_lob;
static ub1 g_oper;
static  ub1      g_debug = FALSE;

#define DEBUG if(g_debug) printf

#define DATE_FMT_LEN            sizeof(DATE_FMT)-1
#define DATE_BUF_LEN            80
#define MAX_COLUMNS             1000
#define MAX_PRINT_BYTES         50
#define SOURCE_CONNECT          1
#define DEST_CONNECT            2
#define DEFAULT_USER       (oratext *)"stradm"
#define DEFAULT_USER_LEN   6
#define DEFAULT_PSW        (oratext *)"stradm" 
#define DEFAULT_PSW_LEN    6
#define DEFAULT_SDB        (oratext *)"database"
#define DEFAULT_SDB_LEN    5
#define DEFAULT_DDB        (oratext *)"database"
#define DEFAULT_DDB_LEN    5
#define DEFAULT_APPLY      (oratext *)"DBNEXUS_OUT"      /* case insensitive name */
#define DEFAULT_APPLY_LEN  11

#define M_IDEN         30
#define INSERT_CMD     1
#define UPDATE_CMD     2
#define DELETE_CMD     3
#define LOB_WRITE_CMD  4
#define LOB_ERASE_CMD  5
#define LOB_TRIM_CMD   6

#define PIECEWISE_LOB(oper) ((oper) == LOB_WRITE_CMD || \
                             (oper) == LOB_ERASE_CMD || \
                             (oper) == LOB_TRIM_CMD)

#define lbit(x,y)  ((x) & (y))

#define samecmd(cmdstr1, cmdlen1, cmdstr2, cmdlen2) \
 (((cmdlen1) == (cmdlen2)) && !memcmp((cmdstr1), (cmdstr2), cmdlen1))

#define OCICALL(ocip, function) do {\
sword status=function;\
if (OCI_SUCCESS==status || OCI_NEED_DATA==status) break;\
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
else if (OCI_STILL_EXECUTING==status) \
{puts((char *)"Error: OCI_STILL_EXECUTING");\
exit(1);}\
else if (OCI_CONTINUE==status) \
{puts((char *)"Error: OCI_CONTINUE");\
exit(1);}\
else {printf("Error: unknown status %d\n", status);\
exit(1);}\
} while(0)

typedef struct params
{
  oratext * user; 
  ub4       userlen;
  oratext * passw;
  ub4       passwlen;
  oratext * dbsrc;
  ub4       dbsrclen;
  oratext * dbdest;
  ub4       dbdestlen;
  oratext * applynm;
  ub4       applynmlen;
  ub1       sysdba;
  ub1       callback_mode;
  ub1       bind_mode;
  ub1       print_lcr;
  ub1       drain_lcr;
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
} oci_t;

#define DML_SIZE 40000
#define ARR_SIZE 2000
typedef struct bindctx_t
{
   ub2      num_bind;
   ub2      bind_var_dtyp[ARR_SIZE];
   void    *bind_var_valuesp[ARR_SIZE];
   OCIInd   bind_var_indp[ARR_SIZE];
   ub2      bind_var_alensp[ARR_SIZE];
   ub2      bind_var_csetidp[ARR_SIZE];
   ub1      bind_var_csetfp[ARR_SIZE];
   oratext *lob_column_names[ARR_SIZE];
   ub2      lob_column_namesl[ARR_SIZE];
   oraub8   lob_column_flags[ARR_SIZE];
   ub2      array_size; 
} bindctx_t;

typedef struct myctx_t 
{
  time_t    starttime;
  time_t    endtime;
  ub1       callback_mode;
  ub1       bind_mode;
  ub1       print_lcr;
  ub1       drain_lcr;
  ub4       lcrcnt;
  oci_t    *outbound_ocip;
  oci_t    *inbound_ocip;
  void     *pinglcr;
  void     *lcrp;
  boolean   is_commit;
  boolean   debug;
  ub1       last_lcrpos[OCI_LCR_MAX_POSITION_LEN];
  ub2       last_lcrpos_len;
  /* Used for storing the generated dml */
  oratext  *dml;          
  ub4       dml_len;
  /* Used for storing the generated where_clause */
  oratext  *where_clause;
  ub4       where_clausel;
  /* maximum length available in dml or where_clause */
  ub4       max_len;
  /* maximum length available in dml or where_clause */
  oratext   colname[M_IDEN];
  /* used during chunk processing if a new column is being received */
  ub2       colname_len; 
  /* the pos for resetting the xsout server */
  ub1       pos[OCI_LCR_MAX_POSITION_LEN];
  ub2       poslen;
  OCILobLocator *lob;
  ub1            temp_lob_alloc;
  OCILobLocator *empty_lob;
  ub1       empty_temp_lob_alloc;
  ub4       op_offset;
  ub4       op_size;
  ub4       coldty;
  ub8       colflags;
} myctx_t;


static params_t      params = {DEFAULT_USER, DEFAULT_USER_LEN, 
                        DEFAULT_PSW, DEFAULT_PSW_LEN,
                        DEFAULT_SDB, DEFAULT_SDB_LEN, 
                        DEFAULT_DDB, DEFAULT_DDB_LEN, 
                        DEFAULT_APPLY, DEFAULT_APPLY_LEN, FALSE};
  

static boolean get_lob_stmt(myctx_t *ctxp);
static sb4 process_chunk(void *vctxp, oratext *colname, ub2 colname_len,
                   ub2 coldty, oraub8 col_flags, ub2 col_csid,
                   ub4 bytes_rd, ub1 *buffer, oraub8 row_flag);
static void prepare_env(myctx_t *ctxp);
static void get_where_clause(myctx_t *ctxp);
static void alloc_temp_lob(myctx_t *ctxp, ub8 col_flags);
static void free_temp_lob(myctx_t *ctxp);
static void bind_stmt(myctx_t *ctxp, bindctx_t *bcp);
static void drain_chunks(myctx_t *ctxp);
static void get_options(params_t *params, int argc, char ** argv);
static void print_usage(int exitcode);
static void oraconnect(params_t *opt_params_p, oci_t ** ocip, ub1);
static void oradisconnect(oci_t * ocip);
static void ocierror(oci_t * ocip, char * msg, boolean stop_on_error);
static void attach_session(oci_t *ocip, params_t *paramsp);
static void detach_session(oci_t *ocip, boolean from_errhandler);
static void get_lcrs(myctx_t *ctxp);
static sb4 execute_lcr_cb(void *ctxp, void *lcr, ub1 lcrtype, oraub8 flags);
static void drain_lcr_cb(void *vctxp, void *lcrp, ub1 lcrtype, oraub8 flags);
static void print_lcr_cb(void *vctxp, void *lcrp, ub1 lcrtype, oraub8 flags);
static void execute_lcr_ncb(void *ctxp, void *lcr, ub1 lcrtype, oraub8 flags);
static void GetOperation(oratext *cmd_type, ub2 cmd_type_len, ub1 *oper);

/*---------------------------------------------------------------------- 
 * static variables used for array inserts
 *----------------------------------------------------------------------*/ 
static sb2  execute_lcr(myctx_t *ctxp, void *lcrp, oratext *cmd_type, 
                           ub2 cmd_len);


static void ocierror(oci_t * ocip, char * msg, boolean stop_on_error)
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
  if (stop_on_error)
  {
    if (ocip->attached)
      detach_session(ocip, TRUE);
    exit(1);
  }
}

static void get_options(params_t *params, int argc, char ** argv)
{
  char * option;
  char * value;

  params->callback_mode = FALSE;
  params->print_lcr = FALSE;
  params->drain_lcr = FALSE;
  
  while(--argc)
  {
    /* get the option name */
    argv++;
    option = *argv;

    /* check that the option begins with a "-" */
    if (!strncmp(option, (char *)"-", 1))
    {
      /* Ignore empty arguments */
      if (option[1] == 0)
        continue;
      option ++;
    }
    else
    {
      printf("Error: bad argument %s\n", option);
      print_usage(1);
    }

    /* check if its a boolean option */
    if (!strncmp(option, (char *)"sysdba", 6))
    {
      params->sysdba = TRUE;
      continue;
    }

    if (!strncmp(option, (char *)"callback",8))
    {
      params->callback_mode = TRUE;
      continue;
    }

    if (!strncmp(option, (char *)"bind",4))
    {
      params->bind_mode = TRUE;
      continue;
    }

    if (!strncmp(option, (char *)"printlcr",8))
    {
      params->print_lcr = TRUE;
      continue;
    }

    if (!strncmp(option, (char *)"drainlcr",8))
    {
      params->drain_lcr = TRUE;
      continue;
    }

    if (!strncmp(option, (char *)"debug",8))
    {
      g_debug = TRUE;
      continue;
    }

    if (!strncmp(option, (char *)"help", 4))
    {
      print_usage(0);
    }

    /* get the value of the option */
    --argc;
    argv++;

    value = *argv;    

    if (!strncmp(option, (char *)"dbsrc", 8))
    {
      params->dbsrc = (oratext *)value;
      params->dbsrclen = strlen(value);
    }
    else if (!strncmp(option, (char *)"dbdest", 6))
    {
      params->dbdest = (oratext *)value;
      params->dbdestlen = strlen(value);
    }
    else if (!strncmp(option, (char *)"user", 4))
    {
      params->user = (oratext *)value;
      params->userlen = strlen(value);
    }
    else if (!strncmp(option, (char *)"passw", 5))
    {
      params->passw = (oratext *)value;
      params->passwlen = strlen(value);
    }
    else if (!strncmp(option, (char *)"apply", 5))
    {
      params->applynm = (oratext *)value;
      params->applynmlen = strlen(value);
    }
    else
    {
      printf("Error: unknown option %s\n", option);
      print_usage(1);
    }
  }
}

static void print_usage(int exitcode)
{
  puts((char *)"Usage: XStreamOutClient [args]"
       "\n      -user  <username>     - Streams admin user name. "
                                        "Default is STRADM"
       "\n      -passw <password>     - Streams admin user password. "
                                        "Default is STRADM"
       "\n      -dbsrc  <tnsname>     - Source DB tnsname. "
                                        "Default is INST1"
       "\n      -dbdest <tnsname>     - Destination DB tnsname"
                                        "Default is INST2"
       "\n      -apply  <apply name>  - XStreams OUT apply name to connect to."
       "\n                              Default is DEMOOUT"
       "\n      -callback             - Use XStreams callback mode API."
       "\n                              Default is no callback mode"
       "\n      -bind                 - Do SQL Generation with binds."
                                      " Default is Inline "
       "\n      -printlcr             - Just dump the SQL generated "
       "\n      -debug                - Print Debug statements "    
       "\n      -help                 - Print the options "    
       "\n"
       "\n");

  exit(exitcode);
}

static void oraconnect(params_t *params_p, oci_t ** ociptr, ub1 who)
{
  oci_t        *ocip;

  printf ("Connecting to Oracle as %.*s@%.*s\n",
          params_p->userlen, params_p->user, 
          params_p->dbsrclen, params_p->dbsrc);
       
  ocip = (oci_t *)malloc(sizeof(oci_t));

  if (OCIEnvCreate(&ocip->envp, OCI_OBJECT, (dvoid *)0,
                   (dvoid * (*)(dvoid *, size_t)) 0,
                   (dvoid * (*)(dvoid *, dvoid *, size_t))0,
                   (void (*)(dvoid *, dvoid *)) 0,
                   (size_t) 0, (dvoid **) 0 ))
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
  if (who == SOURCE_CONNECT)
  {
  OCICALL(ocip,
          OCIServerAttach (ocip->srvp, ocip->errp, 
                           (oratext *)params_p->dbsrc, 
                           (sb4)params_p->dbsrclen, OCI_DEFAULT));
  }
  else
  {
  OCICALL(ocip,
          OCIServerAttach (ocip->srvp, ocip->errp, 
                           (oratext *)params_p->dbdest, 
                           (sb4)params_p->dbdestlen, OCI_DEFAULT));
  }
 

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
                          OCI_CRED_RDBMS, (ub4) params_p->sysdba ?
                          OCI_SYSDBA : OCI_DEFAULT));

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

static void oradisconnect(oci_t * ocip)
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


static void get_where_clause(myctx_t *ctxp)
{
  ub2 ret;
  void *lcrp = ctxp->lcrp;
  oci_t *ocip = ctxp->outbound_ocip;

  if (samecmd(g_cmd_type, g_cmd_type_len,
               OCI_LCR_ROW_CMD_COMMIT, strlen(OCI_LCR_ROW_CMD_COMMIT)))
  {
    return;
  }

  ret = (ub2) OCILCRWhereClauseGet(ocip->svcp, ocip->errp, ctxp->where_clause, 
                         &ctxp->where_clausel , 
                         lcrp, OCI_DEFAULT);
  if (ret) 
  {
    ocierror(ocip, (char *)"WhereClause Generation failed", FALSE);

    printf ("Partial WhereClause (%d):%.*s\n",ctxp->where_clausel,
                                              ctxp->where_clausel, 
                                              ctxp->where_clause);
  }
}



/* process_lcr:
 * for each lcr generate the sql stmt and execute them on the remote side
 * can be used in both callback and non-callback mode.
 */
static void process_lcr(myctx_t *ctxp, void *lcrp, ub1 lcrtype, oraub8 flag)
{
  oratext     *src_db_name;
  ub2          src_db_namel;
  oratext     *txid;
  ub2          txidl;
  OCIDate      src_time;
  oci_t       *ociout = ctxp->outbound_ocip;
  sword        ret;
  oratext      datebuf[DATE_BUF_LEN+1];
  ub4          datelen = DATE_BUF_LEN;
  OCINumber    cscn;
  ub4 i;
  ub2        collist_type;
  ub2        num_cols;
  oratext   *colname[MAX_COLUMNS]; 
  ub2        colnamel[MAX_COLUMNS]; 
  ub2        coldty[MAX_COLUMNS]; 
  void      *colval[MAX_COLUMNS]; 
  ub2        collen[MAX_COLUMNS]; 
  OCIInd     colind[MAX_COLUMNS]; 
  ub1        colcsf[MAX_COLUMNS]; 
  ub2        idx;
  OCIBind   *bndhp[2000];
  oci_t     *ociin = ctxp->inbound_ocip;
  ub1       *pos = (ub1*)0;
  ub2        posl = 0;

  /* Note the starting time */
  if (!ctxp->lcrcnt)
    time(&ctxp->starttime);

  /* Count the top level lcr */
  ctxp->lcrcnt++;

  DEBUG("\n ----------- Contents of LCR %d -----------------\n",
         ctxp->lcrcnt);
  /* reset time */
  g_timer = 0;

  if (lcrtype == OCI_LCR_XDDL)
  {
     printf("Received DDL LCR\n");
     return;
  }

  if (!lcrp)
  {
    printf("Null LCR type (%d)\n", lcrtype);
    return;
  }

  ctxp->lcrp = lcrp;
  /* get command type , owner, object name */
  OCICALL(ociout, 
          OCILCRHeaderGet(ociout->svcp, ociout->errp, &src_db_name, 
                  &src_db_namel,
                  &g_cmd_type, &g_cmd_type_len,
                  &g_owner, &g_ownerl, &g_oname, &g_onamel, &g_tag, &g_tagl,
                  &txid, &txidl, &src_time, (ub2 *)0, (ub2 *)0,(ub1**)&pos, 
                  (ub2*)&posl, (oraub8 *)0, lcrp, OCI_DEFAULT));

  ociin = ctxp->inbound_ocip ;

  /* Generate  and execute inline sql generation */
  ret = execute_lcr(ctxp, lcrp, g_cmd_type, g_cmd_type_len);

  memcpy(ctxp->pos, pos, posl);
  ctxp->poslen = posl;
  OCICALL(ociout,
          OCIXStreamOutProcessedLWMSet(ociout->svcp, ociout->errp,
                      ctxp->pos, ctxp->poslen, OCI_DEFAULT));
}

/* prepare_env:
 * Resets all the ctx variables that are required to be reset and frees 
 * any descriptor or memory that needs to be freed. To be called before 
 * processing the lcr.
 */
static void prepare_env(myctx_t *ctxp)
{
  /* Reset global environment */
  if (ctxp->lob && ctxp->temp_lob_alloc)
  {
     OCICALL(ctxp->inbound_ocip,
            OCILobFreeTemporary(ctxp->inbound_ocip->svcp, 
                                ctxp->inbound_ocip->errp,
                                ctxp->lob));
     ctxp->temp_lob_alloc = 0;
  }

  if (ctxp->lob)
  {
    OCIDescriptorFree((dvoid *)ctxp->lob, (ub4) OCI_DTYPE_LOB);
  }

  g_timer = 0;
  g_oper = 0;

  ctxp->max_len = DML_SIZE;
  ctxp->colname_len = 0;
  ctxp->lob = (OCILobLocator*) 0;
  ctxp->op_offset = 1;
  ctxp->op_size = 0;

  /* Connect to the destination if not already connected */
  if (!ctxp->inbound_ocip)
  {
    oraconnect(&params, &ctxp->inbound_ocip, DEST_CONNECT);
  }

  /* Allocate buffer to hold the generated statement */
  if (!ctxp->dml)
  {
    ctxp->dml = (oratext *)malloc (sizeof(oratext) * ctxp->max_len);
  }

  if (!ctxp->where_clause)
  {
    ctxp->where_clause = (oratext *)malloc (sizeof(oratext) * ctxp->max_len);
  }

  ctxp->dml_len = ctxp->max_len;
  ctxp->where_clausel = ctxp->max_len;

  /* Allocate lob descriptor. Not sure if this needs to be freed at the
   * begining of the function and allocated here. Instead can we reuse it ?
   * Not sure of the effects of reusing so reallocating to avoid any confusion.
   */
  if (!ctxp->lob)
     OCICALL(ctxp->inbound_ocip, 
        OCIDescriptorAlloc((dvoid *) ctxp->inbound_ocip->envp, 
                         (dvoid **) &ctxp->lob,
                         (ub4)OCI_DTYPE_LOB, (size_t) 0, (dvoid **) 0));
}

/* get_chunks: Used in non-callback mode. It invokes process_chunk after
 * receiving each chunk. 
 */
static void get_chunks(myctx_t *ctxp)
{
  oratext *colname; 
  ub2      colname_len;
  ub2      coldty; 
  oraub8   col_flags; 
  ub2      col_csid;
  ub4      bytes_rd; 
  ub1     *buffer; 
  oraub8   row_flag;
  oci_t   *ociout = ctxp->outbound_ocip;
  oci_t   *ociin = ctxp->inbound_ocip;


  do
  {
    OCICALL(ociout, 
            OCIXStreamOutChunkReceive(ociout->svcp, ociout->errp, &colname, 
                                      &colname_len, &coldty, &col_flags, 
                                      &col_csid, &bytes_rd, &buffer, 
                                      &row_flag, OCI_DEFAULT));
    
    process_chunk(ctxp, colname, colname_len, coldty, col_flags, col_csid,
                bytes_rd, buffer, row_flag);
   
  } while (row_flag & OCI_XSTREAM_MORE_ROW_DATA);

}

/* drain_chunk: Receives the chunks and does nothing . Used when the
 * xstreams out server needs to be drained 
 */
static void drain_chunks(myctx_t *ctxp)
{
  oratext *colname; 
  ub2      colname_len;
  ub2      coldty; 
  oraub8   col_flags; 
  ub2      col_csid;
  ub4      bytes_rd; 
  ub1     *buffer; 
  oraub8   row_flag;
  oci_t   *ociout = ctxp->outbound_ocip;
  oci_t   *ociin = ctxp->inbound_ocip;


  do
  {
    OCICALL(ociout, 
            OCIXStreamOutChunkReceive(ociout->svcp, ociout->errp, &colname, 
                                      &colname_len, &coldty, &col_flags, 
                                      &col_csid, &bytes_rd, &buffer, 
                                      &row_flag, OCI_DEFAULT));
   /* do nothing */ 
   
  } while (row_flag & OCI_XSTREAM_MORE_ROW_DATA);

}

static void process_lob_write_chunk(myctx_t *ctxp, oratext *colname, 
                   ub2 colname_len,
                   ub2 coldty, oraub8 col_flags, ub2 col_csid,
                   ub4 bytes_rd, ub1 *buffer, oraub8 row_flag)
{
  ub2      defn ;
  ub2      ret;
  ub4      amt;
  sword    err;
  oci_t   *ociout = ctxp->outbound_ocip;
  oci_t   *ociin = ctxp->inbound_ocip;
  ub2     csform;
  ub1     piece;

  coldty = (coldty == SQLT_CHR) || lbit(col_flags,OCI_LCR_COLUMN_AL16UTF16)
            ? SQLT_CLOB : SQLT_BLOB;

  csform = !lbit(col_flags, OCI_LCR_COLUMN_NCLOB) ? SQLCS_IMPLICIT:
           SQLCS_NCHAR;
  col_csid = lbit(col_flags,OCI_LCR_COLUMN_AL16UTF16)? OCI_UTF16ID: 0;

  /* Set the piece info for the lob operation */
  piece = OCI_ONE_PIECE;
  if (col_csid == OCI_UTF16ID)
    amt = bytes_rd/2;
  else
    amt = bytes_rd;
  DEBUG("INFO: amt=%d, col_csid=%d, piece=%d,frm=%d offset=%d\n",
          amt, col_csid, piece, csform, ctxp->op_offset);
  /* Doesnt work for NCLOB. Look into it */
  OCICALL(ociin, 
          OCILobWrite(ociin->svcp, ociin->errp, ctxp->lob, &amt, 
           ctxp->op_offset,
           buffer, bytes_rd, piece, (void*)0,(void*)0,
           col_csid, (ub1)csform));

  ctxp->op_offset += amt;
}

static void exec_lob_chunk(myctx_t *ctxp)
{
  ub2        dty ;
  OCIBind   *bndhp;
  sb4        len = -1;
  ub2        defn ;
  ub2        ret;
  ub4        amt;
  sword      err;
  oci_t     *ociout = ctxp->outbound_ocip;
  oci_t     *ociin = ctxp->inbound_ocip;
  ub2     csform;
  ub1     piece;
  OCIInd  ind;

   if (lbit(ctxp->colflags, OCI_LCR_COLUMN_NCLOB) || 
       (lbit(ctxp->colflags, OCI_LCR_COLUMN_LOB_DATA) && 
           ctxp->coldty == SQLT_CHR) ||
       lbit(ctxp->colflags, OCI_LCR_COLUMN_XML_DATA))
      dty = SQLT_CLOB;
   else if (lbit(ctxp->colflags, OCI_LCR_COLUMN_LONG_DATA))
   {
      dty = SQLT_CLOB;
      len = -1; /*ctxp->op_offset;*/
   }
   else if (lbit(ctxp->colflags, OCI_LCR_COLUMN_LOB_DATA) &&
           ctxp->coldty == SQLT_BIN)
      dty = SQLT_BLOB; 

      ctxp->dml_len = ctxp->max_len;
      if (!get_lob_stmt(ctxp))
      { 
        printf("Error generating lob statement \n");
        return ;
      }

      OCICALL(ociin,
           OCIStmtPrepare(ociin->stmtp, ociin->errp, ctxp->dml, 
                         (ub4) ctxp->dml_len,
                         (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT));
    
      if (ctxp->op_offset == 1 )
         ind = OCI_IND_NULL;
      else
         ind = OCI_IND_NOTNULL;

      
      OCICALL(ociin, 
        OCIBindByPos(ociin->stmtp,
                     &bndhp,
                     ociin->errp,
                     1,
                     &ctxp->lob,
                     len,
                     dty,
                     &ind,
                     (ub2 *) 0,
                     (ub2 *)0,
                     (ub4) 0,
                     (ub4 *) 0,
                     (ub4) OCI_DEFAULT));

      if ((ret = (ub2)OCIStmtExecute(ociin->svcp, ociin->stmtp, ociin->errp, 
                    (ub4) 1, 
                    (ub4) 0, (CONST OCISnapshot *) 0, (OCISnapshot *) 0,
                    (ub4) OCI_DEFAULT)) != OCI_SUCCESS)
      {
        ocierror(ociin, (char *)"StmtExecute", FALSE);
        return ;
      }

      free_temp_lob(ctxp);
}

static void free_temp_lob(myctx_t *ctxp)
{
    if (ctxp->temp_lob_alloc)
    {
      OCICALL(ctxp->inbound_ocip,
            OCILobFreeTemporary(ctxp->inbound_ocip->svcp, 
                                ctxp->inbound_ocip->errp,
                                ctxp->lob));
      ctxp->temp_lob_alloc = 0;
    }
}
static void process_dml_chunk(myctx_t *ctxp, oratext *colname, 
                   ub2 colname_len,
                   ub2 coldty, oraub8 col_flags, ub2 col_csid,
                   ub4 bytes_rd, ub1 *buffer, oraub8 row_flag)
{
  OCIBind   *bndhp;
  ub2      defn ;
  ub2      ret;
  ub4      amt;
  sword    err;
  oci_t   *ociout = ctxp->outbound_ocip;
  oci_t   *ociin = ctxp->inbound_ocip;
  ub2     csform;
  ub1     piece;


  /* If the chunk is for a different column than the one previously
   * received, then materialize the chunks for the old column 
   */
  if ((colname_len != ctxp->colname_len ||
      strncmp((char*)colname,(char*) ctxp->colname, ctxp->colname_len)))
  {
    /* If this is not the first column then materialize the previous chunks */
    if (ctxp->colname_len)
    {
       exec_lob_chunk(ctxp);
    }

    alloc_temp_lob(ctxp, col_flags);

    strncpy((char*)ctxp->colname, (char*)colname, colname_len);
    ctxp->colname_len = colname_len;
    ctxp->op_offset = 1;
    ctxp->coldty = coldty;
    ctxp->colflags = col_flags;;
  }
  
  /* Add the chunk to the lob */

  csform = !lbit(col_flags, OCI_LCR_COLUMN_NCLOB) ? SQLCS_IMPLICIT:
           SQLCS_NCHAR;

  col_csid = lbit(col_flags,OCI_LCR_COLUMN_AL16UTF16)? OCI_UTF16ID: 0;

  /* Set the piece info for the lob operation */
  piece = OCI_ONE_PIECE;
  if (col_csid == OCI_UTF16ID)
    amt = bytes_rd/2;
  else
    amt = bytes_rd;

  DEBUG("INFO: amt=%d, col_csid=%d, piece=%d, frm=%d offset=%d\n",
          amt, col_csid, piece, csform, ctxp->op_offset);

  /* Doesnt work for NCLOB. Look into it */
  if (amt)
  {
    OCICALL(ociin, 
          OCILobWrite(ociin->svcp, ociin->errp, ctxp->lob, &amt, 
           ctxp->op_offset,
           buffer, bytes_rd, piece, (void*)0,(void*)0,
           col_csid, (ub1)csform));

    ctxp->op_offset += amt;
  }

  /* If this is the last chunk then we wont come here again 
   * so materialize whatever we have received so far
   */
  if (!(row_flag & OCI_XSTREAM_MORE_ROW_DATA))
  {
    exec_lob_chunk(ctxp);
  }
}

static sb4 process_chunk(void *vctxp, oratext *colname, ub2 colname_len,
                   ub2 coldty, oraub8 col_flags, ub2 col_csid,
                   ub4 bytes_rd, ub1 *buffer, oraub8 row_flag)
{
  myctx_t  *ctxp = (myctx_t*)vctxp;
  ub2      defn ;
  ub2      ret;
  ub4      amt;
  sword    err;
  sb4      rtncode;
  oci_t   *ociout = ctxp->outbound_ocip;
  oci_t   *ociin = ctxp->inbound_ocip;
  ub2     csform;
  ub1     piece;

  amt = 0;

  /* Only LOB_WRITE piecewise operation will have any chunks. Other 
   * piecewise operations wont have any chunks (lob_erase, lob_trim)
   * Other operations that will have chunks are INSERT and UPDATE
   * DELETE operation wont have any chunks 
   */
  if (g_oper == LOB_WRITE_CMD)
  {
     process_lob_write_chunk(ctxp, colname, colname_len, coldty, col_flags, 
                         col_csid, bytes_rd, buffer, row_flag);
  }
  else 
  {

    process_dml_chunk(ctxp, colname, colname_len, coldty, col_flags, 
                         col_csid, bytes_rd, buffer, row_flag);
    
  }

  return OCI_CONTINUE;
}

static void alloc_temp_lob(myctx_t *ctxp, ub8 col_flags)
{
   ub4 csform;
   ub2 col_csid;
   oci_t *ociin = ctxp->inbound_ocip;

    csform = !lbit(col_flags, OCI_LCR_COLUMN_NCLOB) ? SQLCS_IMPLICIT:
             SQLCS_NCHAR;
    col_csid = lbit(col_flags,OCI_LCR_COLUMN_AL16UTF16)? OCI_UTF16ID: 0;

    /* Allocate a lob descriptor and a temporary lob for holding the
     * chunks. The lob locator need not be allocated only in one case 
     * that is for LOB_WRITE case
     */
    if (ctxp->temp_lob_alloc == 0)
    {
      OCICALL(ociin, 
          OCILobCreateTemporary(ociin->svcp,
                            ociin->errp,
                            ctxp->lob,
                            col_csid,
                            (ub1)csform,
                          coldty == SQLT_BLOB ? OCI_TEMP_BLOB : OCI_TEMP_CLOB,
                            TRUE,
                            OCI_DURATION_SESSION));
      ctxp->temp_lob_alloc = 1;;
    }
}

static boolean get_lob_stmt(myctx_t *ctxp)
{
  char *upd_stmt = (char*) "UPDATE \"%.*s\".\"%.*s\" SET \"%.*s\"=:1 %.*s";
  char *xml_upd_stmt = 
   (char*) "UPDATE \"%.*s\".\"%.*s\" SET \"%.*s\"=xmltype.createxml(:1) %.*s";

  if (lbit(ctxp->colflags, OCI_LCR_COLUMN_XML_DATA))
    upd_stmt = xml_upd_stmt;

  if (strlen(upd_stmt) + ctxp->colname_len + g_ownerl + 
                              ctxp->where_clausel > ctxp->dml_len)
    return FALSE;

  sprintf((char*)ctxp->dml, upd_stmt, 
          g_ownerl, g_owner,
          g_onamel, g_oname,
          ctxp->colname_len, ctxp->colname, 
          ctxp->where_clausel, ctxp->where_clause);

  ctxp->dml_len = strlen((char*)ctxp->dml);

  return TRUE;
}

/* Top level function to start receiving lcrs from xstreams out server */
static void get_lcrs(myctx_t *ctxp)
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
  oci_t      *ociout = ctxp->outbound_ocip;

  DEBUG ("\n>>> get_lcrs -- Start\n");

  ctxp->lcrcnt = 0;

  while (err == OCI_SUCCESS)
  {
    startcnt = ctxp->lcrcnt;

    /* If callback mode then invoke appropriate API */
    if (ctxp->callback_mode)
    {
      if ((err = OCIXStreamOutLCRCallbackReceive(ociout->svcp, ociout->errp, 
                                execute_lcr_cb, process_chunk,
                                (void *) ctxp, (ub1 *)0, 0, 0)) != OCI_SUCCESS)
        break;

    }
    else
    {
      /* All the other modes are non-callback mode .
       * -printlcr, -drainlcr, -errortest etc. Though all of them can
       * appear in the commandline together onely one of them
       * will be honoured in this order in below code
       */
      while ((err = OCIXStreamOutLCRReceive(ociout->svcp, ociout->errp,
                      &lcr, &lcrtype, &flag, flwm, &flwm_len, OCI_DEFAULT))
                 == OCI_STILL_EXECUTING)
      {
        g_timer = 0;
        ctxp->lcrp = lcr;
        if(ctxp->print_lcr)
          print_lcr_cb(ctxp, lcr, lcrtype, flag);
        else if(ctxp->drain_lcr)
          drain_lcr_cb(ctxp, lcr, lcrtype, flag);
        else 


          execute_lcr_ncb(ctxp, lcr, lcrtype, flag);
      }
    }


    /* print fetch lwm */
    if (flwm_len > 0 && ctxp->debug)
    {
      time_t  tm;
      time(&tm);
      DEBUG("\n=== Current time = %s", ctime(&tm));
    }

    if (ctxp->lcrcnt - startcnt > 0)
    {
      /* end any in-progress XStream In call */
    }
    else
    {
      ub2  cmplen = ctxp->last_lcrpos_len < flwm_len ? ctxp->last_lcrpos_len
                                                    : flwm_len;
      int  cmpres = memcmp(ctxp->last_lcrpos, flwm, cmplen);

      if (cmpres == 0)
        cmpres = ctxp->last_lcrpos_len - flwm_len;

      /* end any in-progress XStream In call */
    }

    /* get processed lwm from inbound server */   

    time(&ctxp->endtime); 

    /* When there are no lcrs coming in for 3 minutes (180 secs)
     * then exit the loop and end the execution. timer is reset everytime
     * lcr callback is called 
     */
    if (g_timer < 180)
      g_timer++;
    else
      break;
  }
  
  if (err)
  {
    ocierror(ociout, (char *)"get_lcrs() encounters error", FALSE);
  }

  DEBUG(">>> get_lcrs [DONE]\n\n");
}


static sb4 execute_lcr_cb(void *vctxp, void *lcr, ub1 lcrtype, oraub8 flags)
{
    myctx_t *ctxp = (myctx_t*)vctxp;

    prepare_env(ctxp);

    process_lcr(ctxp, lcr, lcrtype, flags);

    /* Process chunks associated with the top lcr */
    if (flags & OCI_XSTREAM_MORE_ROW_DATA)
    {
      /* Caches the where-clause of the current lcr in context */
      get_where_clause(ctxp);
    }

    return OCI_CONTINUE;
}

static void execute_lcr_ncb(void *vctxp, void *lcr, ub1 lcrtype, oraub8 flags)
{
   execute_lcr_cb(vctxp, lcr, lcrtype, flags);

   if (flags & OCI_XSTREAM_MORE_ROW_DATA)
   {
     get_chunks((myctx_t*)vctxp);
   }
}

static void print_lcr_cb(void *vctxp, void *lcrp, ub1 lcrtype, oraub8 flags)
{
   myctx_t *ctxp = (myctx_t*)vctxp;
   sword      ret;
   oci_t   *ociout = ctxp->outbound_ocip;
   ub2      num_bind = 0;
   ub2      bind_var_dtyp[ARR_SIZE];
   void    *bind_var_valuesp[ARR_SIZE];
   OCIInd   bind_var_indp[ARR_SIZE];
   ub2      bind_var_alensp[ARR_SIZE];
   ub2      bind_var_csetidp[ARR_SIZE];
   ub1      bind_var_csetfp[ARR_SIZE];
   oratext *lob_column_names[ARR_SIZE];
   ub2      lob_column_namesl[ARR_SIZE];
   oraub8   lob_column_flags[ARR_SIZE];
   ub2      array_size = ARR_SIZE;
   oratext  bind_var_syntax[] = ":";

   prepare_env(ctxp);

    
   ctxp->lcrcnt++;
   ret = OCILCRRowStmtGet(ociout->svcp, ociout->errp, ctxp->dml, 
                          &ctxp->dml_len , lcrp, OCI_DEFAULT);

   /* Dont print COMMIT lcr */
   if (!memcmp(ctxp->dml, " COMMIT ", 8))
     return;

   printf("------------------ LCR %d --------------------\n", ctxp->lcrcnt);
   if (!ret)
     printf("\nINLINE SQL\n------\nDML: [LEN=%d]\n %.*s\n\n",ctxp->dml_len, 
                                          ctxp->dml_len, ctxp->dml);
   else
     printf("INLINE DML FAILED (%d)\n", ret);


   ctxp->dml_len = ctxp->max_len;
   ret = OCILCRWhereClauseGet(ociout->svcp, ociout->errp, ctxp->dml, 
                             &ctxp->dml_len , lcrp, OCI_DEFAULT);
   if (!ret)
   {
     printf ("WHERE CLAUSE:[LEN=%d]\n%.*s\n\n",ctxp->dml_len,
                                             ctxp->dml_len, ctxp->dml);
   }
   else
   {
     printf("WHERE CLAUSE FAILED (%d)", ret);
   }

  ctxp->dml_len = ctxp->max_len;
  num_bind = 0;
  ret = OCILCRRowStmtWithBindVarGet(
                        ociout->svcp, ociout->errp, ctxp->dml, &ctxp->dml_len,
                        &num_bind,
                        bind_var_dtyp, bind_var_valuesp, bind_var_indp,
                        bind_var_alensp, bind_var_csetidp, 
                        bind_var_csetfp, lcrp, 
                        lob_column_names, lob_column_namesl, lob_column_flags,
                        array_size, bind_var_syntax,
                        OCI_DEFAULT);

   if (!ret)
     printf("\nBINDS SQL\n-----\nDML [LEN=%d] [BINDS=%d]\n %.*s\n\n",
                                     ctxp->dml_len, num_bind,
                                     ctxp->dml_len, ctxp->dml);
   else
     printf("BINDS DML FAILED (%d)\n", ret);

   ctxp->dml_len = ctxp->max_len;

   num_bind = 0;
   ret = OCILCRWhereClauseWithBindVarGet(
                        ociout->svcp, ociout->errp, ctxp->dml, &ctxp->dml_len,
                        &num_bind,
                        bind_var_dtyp, bind_var_valuesp, bind_var_indp,
                        bind_var_alensp, bind_var_csetidp, 
                        bind_var_csetfp, lcrp, 
                        array_size, bind_var_syntax,
                        OCI_DEFAULT);

   if (!ret)
     printf("WHERE CLAUSE [LEN=%d] [BINDS=%d]\n%.*s\n\n",
                                     ctxp->dml_len, num_bind,
                                     ctxp->dml_len, ctxp->dml);
   else
     printf("WHERE CLAUSE FAILED (%d)\n", ret);


   if (flags & OCI_XSTREAM_MORE_ROW_DATA)
   {
     drain_chunks((myctx_t*)vctxp);
   }
}

static void drain_lcr_cb(void *vctxp, void *lcrp, ub1 lcrtype, oraub8 flags)
{
  myctx_t  *ctxp = (myctx_t*)vctxp;
  oci_t    *ociout = ctxp->outbound_ocip;
  oci_t    *ociin = ctxp->inbound_ocip;
  oratext     *txid;
  ub2          txidl;
  oratext     *src_db_name;
  ub2          src_db_namel;
  oratext     *oname;
  ub2          onamel;
  ub1          *pos = (ub1*)0;
  ub2          posl = 0;
  OCIDate      src_time;

  ctxp->lcrcnt++;
  OCICALL(ociout, 
          OCILCRHeaderGet(ociout->svcp, ociout->errp, &src_db_name, 
                  &src_db_namel,
                  &g_cmd_type, &g_cmd_type_len,
                  &g_owner, &g_ownerl, &g_oname, &g_onamel, &g_tag, &g_tagl,
                  &txid, &txidl, &src_time, (ub2 *)0, (ub2 *)0,(ub1**)&pos, 
                  (ub2*)&posl, (oraub8 *)0, lcrp, OCI_DEFAULT));

  memcpy(ctxp->pos, pos, posl);
  ctxp->poslen = posl;
  /* Increase LWM so that xstreams server forgets the lcr */
  OCICALL(ociout,
          OCIXStreamOutProcessedLWMSet(ociout->svcp, ociout->errp,
                      ctxp->pos, ctxp->poslen, OCI_DEFAULT));

  if (flags & OCI_XSTREAM_MORE_ROW_DATA)
  {
    drain_chunks((myctx_t*)vctxp);
  }
}

static void attach_session(oci_t * ociout, params_t *paramsp)
{
  sword       err;

  printf ("\n>>> attach_session [START]\n");

  err = OCIXStreamOutAttach(ociout->svcp, ociout->errp, paramsp->applynm, 
                       (ub2)paramsp->applynmlen, (ub1 *)0, 0, OCI_DEFAULT);
  
  if (err)
    ocierror(ociout, (char *)"OCIXStreamOutAttach failed", TRUE);
  else
    printf ("OCIXStreamOutAttach to %.*s successful \n", paramsp->applynmlen,
                                                         paramsp->applynm);

  printf (">>> attach_session [DONE]\n");
}

static void detach_session(oci_t * ociout, boolean from_errhandler)
{
  sword  err;

  printf (">>> detach_session [START]\n");

  err = OCIXStreamOutDetach(ociout->svcp, ociout->errp, OCI_DEFAULT);

  if (err && !from_errhandler)
  {
    ocierror(ociout, (char *)"detach_session() failed", TRUE);
  }
  printf (">>> detach_session [DONE]\n");
}


int main(int argc, char **argv)
{
  oci_t        *oci = (oci_t *)NULL;
  oci_t        *ociout;

  myctx_t ctx;

  /* initialize ctx */
  memset(&ctx, 0 , sizeof(ctx));

  /* store the command line arguments */
  get_options(&params, argc, argv);

  /* connect to the database */
  oraconnect(&params, &oci, SOURCE_CONNECT);

  ociout = oci;

  ociout->attached = FALSE;
  attach_session(ociout, &params);
  ociout->attached = TRUE;
  ctx.outbound_ocip = ociout; 
  ctx.callback_mode = params.callback_mode;
  ctx.bind_mode    = params.bind_mode;
  ctx.print_lcr = params.print_lcr;
  ctx.drain_lcr = params.drain_lcr;

  get_lcrs(&ctx);

  detach_session(ociout, FALSE);
  oradisconnect(ociout);

  return 0;
}

static void print_flags(oraub8 flags)
{
  printf ("column flags: ");
  if (lbit(flags, OCI_LCR_COLUMN_LOB_DATA))
    printf("LOB_DATA ");
  if (lbit(flags, OCI_LCR_COLUMN_LONG_DATA))
    printf("LONG_DATA ");
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

sb2 execute_lcr(myctx_t *ctxp, void *lcrp, oratext *cmd_type, 
                   ub2 cmd_type_len)
{
  ub1 piece;
  sword ret;
  ub4 amtp = 0;
  ub2 sqlt;
  static OCIDefine     *defnp2 = (OCIDefine *) 0;
  bindctx_t    bc;
  oci_t *ociout = ctxp->outbound_ocip;
  oci_t *ociin  = ctxp->inbound_ocip;
  oratext *dml = ctxp->dml;
  ub4    dml_len = ctxp->dml_len;

  memset(&bc, 0, sizeof(bc));
  bc.array_size = ARR_SIZE;

  /* What is the top level operation */
  GetOperation(cmd_type, cmd_type_len, &g_oper);

  /* Get the statement to be executed. In in-line mode it will be
   * ready to execute 
   */
  if (ctxp->bind_mode)
     ret = OCILCRRowStmtWithBindVarGet(
                        ociout->svcp, ociout->errp, dml, &dml_len, &bc.num_bind,
                        bc.bind_var_dtyp, bc.bind_var_valuesp, bc.bind_var_indp,
                        bc.bind_var_alensp, bc.bind_var_csetidp,
                        bc.bind_var_csetfp, lcrp,
                        bc.lob_column_names, bc.lob_column_namesl, 
                        bc.lob_column_flags,
                        bc.array_size, (oratext*)":",
                        OCI_DEFAULT);
  else
    ret = OCILCRRowStmtGet(ociout->svcp, ociout->errp, dml, &dml_len , 
                           lcrp, OCI_DEFAULT);

  if (!ret)
  {
    DEBUG ("Preparing Dml (%d):%.*s\n\n",dml_len,  dml_len, dml);
    if (OCIStmtPrepare(ociin->stmtp, ociin->errp, dml, (ub4) dml_len,
                   (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT))
    {
      (void) printf("FAILED: OCIStmtPrepare() dml\n");
      return OCI_ERROR;
    }

    /* Piecewise lob lcrs generate select-for-update statements. The selected
     * column is the lob being piecewise operated on. So they
     * need to be defined before executing 
     * They will be of the corm SELECT COL FROM TAB WHERE N1 = :1 AND N2 = :2
     * There will definitely be only one column as there is only one lob
     * being operated on.
     */
    if (PIECEWISE_LOB(g_oper))
    {
      oraub8    col_flags;
      if (OCILCRLobInfoGet(ociout->svcp, ociout->errp, &colname, &colname_len,
                           &coldty, &col_flags, &op_offset, &op_size,
                           lcrp, OCI_DEFAULT) == OCI_SUCCESS)
      {
        ub4  printlen;

        DEBUG("retval=%d colname = %.*s  coldty=%d bytes_rd=%d\n",
                ret, colname_len, colname, coldty, bytes_rd);
    
        DEBUG("op_offset=%d op_size=%d\n", op_offset, op_size);

        /* Set the datatype for CLOB, BLOB. NCLOB is also SQLT_CLOB */
        sqlt = coldty == SQLT_CHR ? SQLT_CLOB : 
               lbit(col_flags,OCI_LCR_COLUMN_AL16UTF16) ? SQLT_CLOB :
               SQLT_BLOB;
      }
      else
      {
        ocierror(ociout, (char *)"LOB info failed", FALSE);
        return OCI_ERROR;
      }


      if (OCIDefineByPos (ociin->stmtp, &defnp2, ociin->errp, 1,
                 (dvoid *)&ctxp->lob, 0 , sqlt,
                 (dvoid *)0, (ub2 *)0, (ub2 *)0, OCI_DEFAULT))
      {
        ocierror(ociin, (char *)"LOB define failed", FALSE);
        return OCI_ERROR;
      }
    }

    if (ctxp->bind_mode)
      bind_stmt(ctxp, &bc);

    /* Execute the sql statement generated by the top level lcr */
    if (OCIStmtExecute(ociin->svcp, ociin->stmtp, ociin->errp, (ub4) 1, (ub4) 0,
                    (CONST OCISnapshot *) 0, (OCISnapshot *) 0,
                    (ub4) OCI_DEFAULT))
    {
      ocierror(ociin, (char *)"Dml execution failed", FALSE);
      return OCI_ERROR;
    }
    
    /* This is not a piecewise lob operation so nothing to be done */
    if (!PIECEWISE_LOB(g_oper))
      return OCI_SUCCESS;

    /* lob_erase and lob_trim doesnt have any follow up chunks so they
     * can be completed here. Lob_write will have follow up lob chunk
     * lcrs. So prepare the info for executing lob write
     */
    switch(g_oper)
    {
    case LOB_ERASE_CMD:
      if (ret = OCILobErase(ociin->svcp, ociin->errp, ctxp->lob, 
                            &op_size, op_offset))
      {
        (void) printf("ERROR: OCILobErase().\n");
        return OCI_ERROR;
      }
      break;
    case LOB_TRIM_CMD:
      if (ret = OCILobTrim(ociin->svcp, ociin->errp, ctxp->lob, op_size))
      {
        (void) printf("ERROR: OCILobTrim().\n");
        return OCI_ERROR;
      }
      break; 
    case LOB_WRITE_CMD:
      ctxp->op_offset = op_offset;
      ctxp->op_size = op_size;
      break;
    default:
      printf("ERROR: Unexpected operation \n");
      return OCI_ERROR;
    }
  }
  else
  {
    ocierror(ociout, (char *)"Dml Generation failed", FALSE);

    DEBUG ("Partial Dml (%d):%.*s\n",dml_len,  dml_len, dml);
  }

  return OCI_SUCCESS;
}


static void bind_stmt(myctx_t *ctxp, bindctx_t *bcp)
{
  ub4        i;
  OCIBind   *bndhp[2000];
  OCILobLocator *lob=0;
  ub4  lobEmpty = 0;
  oci_t * ocip = ctxp->inbound_ocip;
  oratext *dml = ctxp->dml;
  ub4 dml_len = ctxp->dml_len;
  OCIInd ind = OCI_IND_NULL;
  ub1 dty = 0;
  sb4 len = -1;
      void *buf = (void*)&ctxp->empty_lob;

  if (!ctxp->empty_lob)
  {
     OCICALL(ocip, 
          OCIDescriptorAlloc((dvoid *) ocip->envp, (dvoid **) &ctxp->empty_lob,
                         (ub4)OCI_DTYPE_LOB, (size_t) 0, (dvoid **) 0));

     OCICALL(ocip,
         OCIAttrSet(ctxp->empty_lob, OCI_DTYPE_LOB, &lobEmpty, 0
                   , OCI_ATTR_LOBEMPTY,
                    ocip->errp));
  }

  for (i = 0; i < bcp->num_bind; i++)
  {
    dty = (ub1)bcp->bind_var_dtyp[i];
    /* Bind for non lob columns */
    if (!bcp->lob_column_namesl[i] || 1)
    {
      if (lbit(bcp->lob_column_flags[i], OCI_LCR_COLUMN_NCLOB))
        dty = SQLT_CLOB;
      else if (lbit(bcp->lob_column_flags[i], OCI_LCR_COLUMN_LOB_DATA) && 
               bcp->bind_var_dtyp[i] == SQLT_CHR)
        dty = SQLT_CLOB;
      else if (lbit(bcp->lob_column_flags[i], OCI_LCR_COLUMN_LOB_DATA) &&
               bcp->bind_var_dtyp[i] == SQLT_BIN)
        dty = SQLT_BLOB;
      else if (lbit(bcp->lob_column_flags[i], OCI_LCR_COLUMN_LONG_DATA) &&
                bcp->bind_var_dtyp[i] == SQLT_CHR)
      {
        dty = SQLT_CLOB;
/*        buf = (void*)"\'0'";*/
        len = -1;
      }
      else if (lbit(bcp->lob_column_flags[i], OCI_LCR_COLUMN_LONG_DATA) &&
                bcp->bind_var_dtyp[i] == SQLT_BIN)
      {
/*        dty = SQLT_LBI;*/
        dty = SQLT_BLOB;
/*        buf = (void*)"\'0'";*/
        len = -1;
      }
      else if (lbit(bcp->lob_column_flags[i], OCI_LCR_COLUMN_XML_DATA))
      {
        dty = SQLT_CHR;
        buf = (void*)"\'<xml />\'";
        len = (sb4) strlen((char*)buf);
      }
     
      OCICALL(ocip, 
        OCIBindByPos(ocip->stmtp,
                    &bndhp[i],
                     ocip->errp,
                     i+1,
                     bcp->bind_var_valuesp[i],
                     bcp->bind_var_alensp[i],
                     dty,
                     &bcp->bind_var_indp[i],
                     (ub2 *) 0,
                     (ub2 *)0,
                     (ub4) 0,
                     (ub4 *) 0,
                     (ub4) OCI_DEFAULT));
    }
    else 
    {

      if (lbit(bcp->lob_column_flags[i], OCI_LCR_COLUMN_NCLOB))
        dty = SQLT_CLOB;
      else if (lbit(bcp->lob_column_flags[i], OCI_LCR_COLUMN_LOB_DATA) && 
               bcp->bind_var_dtyp[i] == SQLT_CHR)
        dty = SQLT_CLOB;
      else if (lbit(bcp->lob_column_flags[i], OCI_LCR_COLUMN_LOB_DATA) &&
               bcp->bind_var_dtyp[i] == SQLT_BIN)
        dty = SQLT_BLOB;
      else if (lbit(bcp->lob_column_flags[i], OCI_LCR_COLUMN_LONG_DATA) &&
                bcp->bind_var_dtyp[i] == SQLT_CHR)
      {
        dty = SQLT_CLOB;
/*        buf = (void*)"\'0'";*/
        len = -1;
      }
      else if (lbit(bcp->lob_column_flags[i], OCI_LCR_COLUMN_LONG_DATA) &&
                bcp->bind_var_dtyp[i] == SQLT_BIN)
      {
/*        dty = SQLT_LBI;*/
        dty = SQLT_BLOB;
/*        buf = (void*)"\'0'";*/
        len = -1;
      }
      else if (lbit(bcp->lob_column_flags[i], OCI_LCR_COLUMN_XML_DATA))
      {
        dty = SQLT_CHR;
        buf = (void*)"\'<xml />\'";
        len = (sb4)strlen((char*)buf);
      }

      OCICALL(ocip, 
        OCIBindByPos(ocip->stmtp,
                    &bndhp[i],
                     ocip->errp,
                     i+1,
                     &buf,
                     len,
                     dty,
                     &ind,
                     (ub2 *) 0,
                     (ub2 *)0,
                     (ub4) 0,
                     (ub4 *) 0,
                     (ub4) OCI_DEFAULT));
     
    }
  }

/*
  if (lob)
    OCIDescriptorFree(lob, OCI_DTYPE_LOB);
*/
}

static void GetOperation(oratext *cmd_type, ub2 cmd_type_len, ub1 *oper)
{
  if (samecmd(cmd_type, cmd_type_len,
              OCI_LCR_ROW_CMD_LOB_ERASE, strlen(OCI_LCR_ROW_CMD_LOB_ERASE)))
    *oper = LOB_ERASE_CMD;
  else if(samecmd(cmd_type, cmd_type_len,
              OCI_LCR_ROW_CMD_LOB_TRIM, strlen(OCI_LCR_ROW_CMD_LOB_TRIM)))
    *oper = LOB_TRIM_CMD;
  else if(samecmd(cmd_type, cmd_type_len,
              OCI_LCR_ROW_CMD_LOB_WRITE, strlen(OCI_LCR_ROW_CMD_LOB_WRITE)))
    *oper = LOB_WRITE_CMD;
  else if(samecmd(cmd_type, cmd_type_len,
              OCI_LCR_ROW_CMD_INSERT, strlen(OCI_LCR_ROW_CMD_INSERT)))
    *oper = INSERT_CMD;
  else if(samecmd(cmd_type, cmd_type_len,
              OCI_LCR_ROW_CMD_UPDATE, strlen(OCI_LCR_ROW_CMD_UPDATE)))
    *oper = UPDATE_CMD;
  else if(samecmd(cmd_type, cmd_type_len,
              OCI_LCR_ROW_CMD_DELETE, strlen(OCI_LCR_ROW_CMD_DELETE)))
    *oper = DELETE_CMD;
}

/* end of file XStreamOutClient.c */

