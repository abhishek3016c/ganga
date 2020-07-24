/* Copyright (c) 2009, 2013, Oracle and/or its affiliates. 
All rights reserved.*/

/*

   NAME
     xin.c - XStream Inbound Demo Program

   DESCRIPTION
     This program demostrates how to construct DDL and DML LCRs and send
     to an XStream Inbound server. 
     
     After connecting to the database where the inbound server is created,
     it attaches to the inbound server and sends two transactions to this
     server. The first transaction contains only a DDL statement. The 
     second consists of a series of inserts followed by an update. Finally, it
     detaches from the inbound server. 
  
     The workload sent by this demo program is 

      create table scott.t1 (f1 number primary key, f2 varchar2(10), f3 date);
      begin
        for i in 1 .. 10 loop
          insert into scott.t1 (f2,f1,f3) values ('row_'||i, i, sysdate);
        end loop;
      end;
      /
      update scott.t1 set f2 = 'ORACLE_11' where f1 = 10;


     Usage: xin [-usr <user name>] [-pwd <password>]
                [-db <db name>] [-svr <svr name>]

     svr  : inbound server name
     db   : database name where inbound server is created
     usr  : connect user to inbound server
     pwd  : connect user's password
   
     Default values for above parameters are:
      svr    = xin2
      db     = inst1 
      usr    = stradm
      pwd    = stradm 

    To create an inbound server, execute dbms_xstream.create_inbound procedure,
    for example, 

        begin
          dbms_xstream_adm.create_inbound(
            server_name => 'xin2',
            queue_name  => 'xin2_queue',
            comment     => 'XStreams In');
        end;
        /

    To start the inbound server, execute the following statement: 
       execute dbms_apply_adm.start_apply('xin2');

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

/* for WINDOWS compatibility of 'sleep' call */
#if defined(WIN32COMMON) || defined(WIN32) || defined(_WIN32)
#include <windows.h>
#define sleep(x) Sleep(1000*(x))
#endif

#if !(defined(WIN32COMMON) || defined(WIN32) || defined(_WIN32))
#ifndef _UNISTD_H
#include <unistd.h>
#endif
#endif

/*---------------------------------------------------------------------- 
 *           Internal structures
 *----------------------------------------------------------------------*/
typedef struct params
{
  oratext *user; 
  ub4      userlen;
  oratext *passw;
  ub4      passwlen;
  oratext *dbname;
  ub4      dbnamelen;
  oratext *svrnm;
  ub4      svrnmlen;
} params_t;

typedef struct oci
{
  OCIEnv     *envp;
  OCIError   *errp;
  OCIServer  *srvp;
  OCISvcCtx  *svcp;
  OCISession *authp;
  OCIStmt    *stmtp;
  OCIStmt    *stmt2p;
} oci_t;

typedef struct myctx_t 
{
  oci_t    *ocip;
  void     *dml_lcr;
  void     *ddl_lcr;
  ub4       lcr_pos;                                       /* lcr sequence # */
} myctx_t;

#define OCICALL(ocip, function) do {\
sword status=function;\
if (OCI_SUCCESS==status || OCI_STILL_EXECUTING==status) break;\
else if (OCI_ERROR==status) \
{ocierror(ocip, (char *)"OCI_ERROR");\
exit(1);}\
else {printf("Error encountered %d\n", status);\
exit(1);}\
} while(0)

/* lcvm4by = move a ub4 value from ub4ptr to a byte array, b. */ 
# define lcvm4by(ub4ptr, b) (((ub1*)(b))[0]  = ((CONST ub1*)(ub4ptr))[3], \
                       ((ub1*)(b))[1]  = ((CONST ub1*)(ub4ptr))[2], \
                       ((ub1*)(b))[2]  = ((CONST ub1*)(ub4ptr))[1], \
                       ((ub1*)(b))[3]  = ((CONST ub1*)(ub4ptr))[0] )

/* lcvmby4 = move 4 bytes from a ub1 array, b, to a ub4 value. */ 
# define lcvmby4(b, ub4ptr) (((ub1*)(ub4ptr))[0] = ((CONST ub1*)(b))[3],  \
                       ((ub1*)(ub4ptr))[1] = ((CONST ub1*)(b))[2],  \
                       ((ub1*)(ub4ptr))[2] = ((CONST ub1*)(b))[1],  \
                       ((ub1*)(ub4ptr))[3] = ((CONST ub1*)(b))[0]  )

static void get_inputs(params_t *params, int argc, char ** argv);
static void print_usage(int exitcode);
static void connect_db(params_t *params_p, oci_t ** ociptr, ub2 char_csid,
                       ub2 nchar_csid);
static void disconnect_db(oci_t * ocip);
static void ocierror(oci_t * ocip, char * msg);
static void print_pos (oratext *posnm, ub1 *pos, ub2 poslen);
static void attach(myctx_t *ctx, params_t *paramsp);
static void detach(oci_t * ocip);
static void send_lcr_stream(myctx_t *ctx);
static void send_dml_lcrs(myctx_t  *ctx);
static void send_ddl_lcr(myctx_t  *ctx);
static void get_db_charsets(params_t *params_p, ub2 *char_csid,
                            ub2 *nchar_csid);

/*---------------------------------------------------------------------- 
 * Constants
 *----------------------------------------------------------------------*/ 

#define DEFAULT_USER        (oratext *)"stradm"
#define DEFAULT_USER_LEN    (strlen((char *)DEFAULT_USER))
#define DEFAULT_PSW         (oratext *)"stradm" 
#define DEFAULT_PSW_LEN     (strlen((char *)DEFAULT_PSW))
#define DEFAULT_DB          (oratext *)"inst1" 
#define DEFAULT_DB_LEN      (strlen((char *)DEFAULT_DB))
#define DEFAULT_SVR         (oratext *)"xin2"  
#define DEFAULT_SVR_LEN     (strlen((char *)DEFAULT_SVR))
#define DEFAULT_CAPTURE     (oratext *)"EXTERNAL_CAPTURE"
#define DEFAULT_CAPTURE_LEN (strlen((char *)DEFAULT_CAPTURE))
#define DEFAULT_DBNM        ((oratext *)"DBS1.REGRESS.RDBMS.DEV.US.ORACLE.COM")
#define DEFAULT_DBNM_LEN    (strlen((char *)DEFAULT_DBNM))

#define CREATE_T1       \
   "create table t1 (f1 number primary key, f2 varchar2(10), f3 date)"
#define TRUNCATE_T1     "truncate table t1"
#define T1_NUM_COLS     (3)

/*---------------------------------------------------------------------
 *                M A I N   P R O G R A M
 *---------------------------------------------------------------------*/
main(int argc, char **argv)
{
  myctx_t       ctx;
  oci_t        *ocip = (oci_t *)NULL;
  params_t      params; 
  ub2           ibdb_char_csid = 0;                  /* inbound db char csid */
  ub2           ibdb_nchar_csid = 0;                /* inbound db nchar csid */

  params.user = DEFAULT_USER;
  params.userlen = (ub4)DEFAULT_USER_LEN;
  params.passw = DEFAULT_PSW;
  params.passwlen = (ub4)DEFAULT_PSW_LEN;
  params.dbname = DEFAULT_DB;
  params.dbnamelen = (ub4)DEFAULT_DB_LEN;
  params.svrnm = DEFAULT_SVR;
  params.svrnmlen = (ub4)DEFAULT_SVR_LEN;

  get_inputs(&params, argc, argv);           /* parse command line arguments */

  /* Get the inbound database's CHAR and NCHAR character set info */
  get_db_charsets(&params, &ibdb_char_csid, &ibdb_nchar_csid);

  /* Connect to the inbound db and set the client env to the inbound database's
   * charsets.
   */
  connect_db(&params, &ocip, ibdb_char_csid, ibdb_nchar_csid);

  /* set up user context */
  memset(&ctx, 0, sizeof(ctx));
  ctx.ocip = ocip;
  ctx.lcr_pos = 1;  

  /* Attach to outbound server */
  attach(&ctx, &params);

  /* Send lcrs to inbound server */
  send_lcr_stream(&ctx);
    
  /* Detach from outbound server */
  detach(ocip);

  /* Disconnect from database */
  disconnect_db(ocip);

  free(ocip);
  exit(0);
}

/*---------------------------------------------------------------------
 * send_lcr_stream - send lcr stream to inbound server.
 *---------------------------------------------------------------------*/
static void send_lcr_stream(myctx_t *ctx)
{
  sword       err;
  oci_t      *ocip = ctx->ocip;
  OCISvcCtx  *svchp    = ocip->svcp;
  OCIError   *errhp    = ocip->errp;
  ub4         ub4_pos;                         
  
  /* create a DML and DDL LCR */
  OCICALL(ocip, OCILCRNew(svchp, errhp, OCI_DURATION_SESSION,
                          OCI_LCR_XROW, &ctx->dml_lcr, OCI_DEFAULT));

  OCICALL(ocip, OCILCRNew(svchp, errhp, OCI_DURATION_SESSION,
                          OCI_LCR_XDDL, &ctx->ddl_lcr, OCI_DEFAULT));

  /* Generate a DDL LCR to create or truncate table scott.t1 */
  send_ddl_lcr(ctx);
  
  /* Generate some DML LCRs to insert and update scott.t1 */
  send_dml_lcrs(ctx);

  /* free LCRs */
  OCICALL(ocip, OCILCRFree(svchp, errhp, ctx->dml_lcr, OCI_DEFAULT));  
  OCICALL(ocip, OCILCRFree(svchp, errhp, ctx->ddl_lcr, OCI_DEFAULT));  
         
}

/*---------------------------------------------------------------------
 * send_lcr - Send the given lcr and bump up lcr position.
 *---------------------------------------------------------------------*/
static void send_lcr(myctx_t  *ctx, void *lcr, ub1 lcrtype)
{
  oci_t      *ocip = ctx->ocip;

  OCICALL(ocip, 
          OCIXStreamInLCRSend(ocip->svcp, ocip->errp, lcr, lcrtype,
                              0, OCI_DEFAULT));
  ctx->lcr_pos++;             
}

/*---------------------------------------------------------------------
 * send_ddl_lcr - Send a DDL LCR
 *---------------------------------------------------------------------*/
static void send_ddl_lcr(myctx_t  *ctx)
{
  oci_t      *ocip = ctx->ocip;
  OCIDate     src_time;
  oratext     txid[OCI_LCR_MAX_TXID_LEN];
  ub1         position[OCI_LCR_MAX_POSITION_LEN];
  ub2         poslen = sizeof(ctx->lcr_pos);

  sprintf((char *)txid, (oratext *)"%d.0.0", ctx->lcr_pos);
  printf("Sending DDL LCR (txid = %s)\n", txid);
  OCICALL(ocip, OCIDateSysDate(ocip->errp, &src_time));

  /* convert a ub4 lcr_pos to ub1 array */
  lcvm4by(&ctx->lcr_pos, position);
  if (ctx->lcr_pos == 1)
  {
    /* Generate a DDL LCR to create table scott.t1 */
    OCICALL(ocip, 
            OCILCRHeaderSet(ocip->svcp, ocip->errp,
                            (oratext *)DEFAULT_DBNM,       /* source db name */
                            (ub2)DEFAULT_DBNM_LEN,
                            (oratext *)"CREATE TABLE",           /* cmd type */
                            (ub2)strlen("CREATE TABLE"),
                            (oratext *)"SCOTT",         /* schema name & len */
                            (ub2)strlen("SCOTT"),            
                            (oratext *)"T1",            /* object name & len */
                            (ub2)strlen("T1"),
                            (ub1 *)0, 0,                          /* lcr tag */
                            txid, (ub2)strlen((char *)txid),       /* txn id */
                            &src_time,                    /* lcr source time */
                            position,                        /* lcr position */
                            poslen,                     
                            0,                                   /* LCR flag */
                            ctx->ddl_lcr,                       /* input lcr */
                            OCI_DEFAULT));

    /* Fill in DDL info */
   OCICALL(ocip, 
           OCILCRDDLInfoSet(ocip->svcp, ocip->errp,
                            (oratext *)"TABLE",               /* object_type */
                            (ub2)strlen("TABLE"),
                            (oratext *)CREATE_T1,                /* DDL text */
                            (ub2)strlen(CREATE_T1),
                            (oratext *)"SCOTT",                /* logon_user */
                            (ub2)strlen("SCOTT"),            
                            (oratext *)"SCOTT",            /* current_schema */
                            (ub2)strlen("SCOTT"),
                            (oratext *)"SCOTT",  /*base_table_owner(optional)*/
                            (ub2)strlen("SCOTT"),
                            (oratext *)"T1",    /* base_table_name (optional)*/
                            (ub2)strlen("T1"),
                            0,                                   /* LCR flag */
                            (void *)ctx->ddl_lcr,               /* input lcr */
                            OCI_DEFAULT));
  }
  else
  {
    /* Generate a DDL LCR to create table scott.t1 */
    OCICALL(ocip, 
            OCILCRHeaderSet(ocip->svcp, ocip->errp,
                            (oratext *)DEFAULT_DBNM,       /* source db name */
                            (ub2)DEFAULT_DBNM_LEN,
                            (oratext *)"TRUNCATE TABLE",         /* cmd type */
                            (ub2)strlen("TRUNCATE TABLE"),
                            (oratext *)"SCOTT",         /* schema name & len */
                            (ub2)strlen("SCOTT"),            
                            (oratext *)"T1",            /* object name & len */
                            (ub2)strlen("T1"),
                            (ub1 *)0, 0,                          /* lcr tag */
                            txid, (ub2)strlen((char *)txid),       /* txn id */
                            &src_time,                    /* lcr source time */
                            position,                        /* lcr position */
                            poslen,                     
                            0,                                   /* LCR flag */
                            ctx->ddl_lcr,                       /* input lcr */
                            OCI_DEFAULT));
    /* Fill in DDL info */
   OCICALL(ocip, 
           OCILCRDDLInfoSet(ocip->svcp, ocip->errp,
                            (oratext *)"TABLE",               /* object_type */
                            (ub2)strlen("TABLE"),
                            (oratext *)TRUNCATE_T1,              /* DDL text */
                            (ub2)strlen(TRUNCATE_T1),
                            (oratext *)"SCOTT",                /* logon_user */
                            (ub2)strlen("SCOTT"),            
                            (oratext *)"SCOTT",            /* current_schema */
                            (ub2)strlen("SCOTT"),
                            (oratext *)"SCOTT",  /*base_table_owner(optional)*/
                            (ub2)strlen("SCOTT"),
                            (oratext *)"T1",    /* base_table_name (optional)*/
                            (ub2)strlen("T1"),
                            0,                                   /* LCR flag */
                            ctx->ddl_lcr,                       /* input lcr */
                            OCI_DEFAULT));
  }

  /* send DDL lcr */
  send_lcr(ctx, ctx->ddl_lcr, OCI_LCR_XDDL);

  /* generate and send a commit lcr */
  lcvm4by(&ctx->lcr_pos, position);
  OCICALL(ocip, 
          OCILCRHeaderSet(ocip->svcp, ocip->errp,
                          (oratext *)DEFAULT_DBNM,         /* source db name */
                          (ub2)DEFAULT_DBNM_LEN,
                          (oratext *)OCI_LCR_ROW_CMD_COMMIT,     /* cmd_type */
                          (ub2)strlen(OCI_LCR_ROW_CMD_COMMIT),
                          (oratext *)0, 0,              /* schema name & len */
                          (oratext *)0, 0,              /* object name & len */
                          (ub1 *)0, 0,                            /* lcr tag */
                          txid, (ub2)strlen((char *)txid),         /* txn id */
                          &src_time,                      /* lcr source time */
                          position,                          /* lcr position */
                          poslen,                     
                          0,                                     /* LCR flag */
                          ctx->dml_lcr,                         /* input lcr */
                          OCI_DEFAULT));

  /* send lcr */
  send_lcr(ctx, ctx->dml_lcr, OCI_LCR_XROW);
}

/*---------------------------------------------------------------------
 * send_dml_lcrs - Send DML LCRs
 * This procedure sends a series of insert LCRs and an update LCR.
 *---------------------------------------------------------------------*/
#define NUM_INSERTS            (10)                       /* # of insert LCR */
static void send_dml_lcrs(myctx_t  *ctx)
{
  oci_t      *ocip = ctx->ocip;
  OCIDate     src_time;
  oratext     txid[OCI_LCR_MAX_TXID_LEN];
  ub4         f1_value = 0;             
  oratext     f2_value[10]; 
  ub2         cnt = 0;
  ub1         position[OCI_LCR_MAX_POSITION_LEN];
  ub2         poslen = sizeof(ctx->lcr_pos);

  /* The following arrays defined the properties of the columns in the LCR. 
   *  col_names     : list of column names 
   *  col_name_lens : list of column name lengths 
   *  col_dtys      : list of column DTYs 
   *  col_inds      : list of column indicators 
   *  col_csfs      : list of column character set forms 
   *  col_data      : list of column data 
   *  col_data_lens : list of column data lengths 
   *
   * The order of the columns in col_names can be different from the order
   * defined for the table. However, the index to each column for each array 
   * must be the same, for example, the first item in col_names is "F2" then
   * the first entry for all the other arrays must be for column "F2".
   */

  oratext    *col_names[T1_NUM_COLS] = {(oratext *)"F2", (oratext *)"F1", 
                                        (oratext *)"F3"};    
  ub2         col_name_lens[T1_NUM_COLS] = {2, 2, 2};    
  ub2         col_dtys[T1_NUM_COLS] = {SQLT_CHR, SQLT_INT, SQLT_ODT};    
  OCIInd      col_inds[T1_NUM_COLS] = {OCI_IND_NOTNULL, OCI_IND_NOTNULL, 
                                       OCI_IND_NOTNULL};    
  ub1         col_csfs[T1_NUM_COLS] = {SQLCS_IMPLICIT, 0, 0};    
  void       *col_data[T1_NUM_COLS];    
  ub2         col_data_lens[T1_NUM_COLS];    

  sprintf((char *)txid, (oratext *)"%d.0.0", ctx->lcr_pos);
  printf("Sending DML LCRs (txid = %s)\n", txid);

  /* Generate 10 insert LCRs:
   * insert into scott.t1 (f2, f1, f3) values ('row_#', f1_value, src_time); 
   */

  /* Fill in NEW column properties that remain fixed */
  col_data[0] = f2_value;                /* value and length for column "F2" */
  col_data[1] = &f1_value;               /* value and length for column "F1" */
  col_data_lens[1] = sizeof(f1_value); 
  col_data[2] = &src_time;               /* value and length for column "F3" */
  col_data_lens[2] = sizeof(src_time); 

  for (cnt  = 0; cnt < NUM_INSERTS; cnt++)
  { 
    f1_value = cnt + 1;                         /* new f1_value for each lcr */
    sprintf((char *)f2_value, "row_%d", cnt+1); /* new f2_value for each lcr */
    col_data_lens[0] = (ub2)strlen((char *)f2_value);        

    lcvm4by(&ctx->lcr_pos, position);
    OCICALL(ocip, 
            OCILCRHeaderSet(ocip->svcp, ocip->errp,
                            (oratext *)DEFAULT_DBNM,       /* source db name */
                            (ub2)DEFAULT_DBNM_LEN,
                            (oratext *)OCI_LCR_ROW_CMD_INSERT,   /* cmd type */
                            (ub2)strlen(OCI_LCR_ROW_CMD_INSERT),
                            (oratext *)"SCOTT", 5,      /* schema name & len */
                            (oratext *)"T1", 2,         /* object name & len */
                            (ub1 *)0, 0,                          /* lcr tag */
                            txid, (ub2)strlen((char *)txid),       /* txn id */
                            &src_time,                    /* lcr source time */
                            position,                        /* lcr position */
                            poslen,                     
                            0,                                   /* LCR flag */
                            ctx->dml_lcr,                       /* input lcr */
                            OCI_DEFAULT));

    OCICALL(ocip, OCIDateSysDate(ocip->errp, &src_time));
    OCICALL(ocip, 
            OCILCRRowColumnInfoSet(ocip->svcp, ocip->errp,
                            OCI_LCR_ROW_COLVAL_NEW,     /* column_value_type */
                            T1_NUM_COLS,                      /* num columns */
                            col_names,                    
                            col_name_lens,              
                            col_dtys,                    
                            col_data,                   
                            col_inds,                        
                            col_data_lens,             
                            col_csfs,               
                            (oraub8 *)0,                     /* column flags */
                            (ub2 *)0,                         /* column csid */
                            (void *)ctx->dml_lcr,               /* input lcr */
                            OCI_DEFAULT));

    /* send lcr */
    send_lcr(ctx, ctx->dml_lcr, OCI_LCR_XROW);
  }

  /* Generate an update LCR to update the last insert lcr:
   *  update scott.t1 set f2 = 'ORACLE_11' where f1 = f1_val;
   */
  lcvm4by(&ctx->lcr_pos, position);
  OCICALL(ocip, 
          OCILCRHeaderSet(ocip->svcp, ocip->errp,
                          (oratext *)DEFAULT_DBNM,         /* source db name */
                          (ub2)DEFAULT_DBNM_LEN,
                          (oratext *)OCI_LCR_ROW_CMD_UPDATE,     /* cmd type */
                          (ub2)strlen(OCI_LCR_ROW_CMD_UPDATE),
                          (oratext *)"SCOTT", 5,        /* schema name & len */
                          (oratext *)"T1", 2,           /* object name & len */
                          (ub1 *)0, 0,                            /* lcr tag */
                          txid, (ub2)strlen((char *)txid),         /* txn id */
                          &src_time,                      /* lcr source time */
                          position,                          /* lcr position */
                          poslen,                     
                          0,                                     /* LCR flag */
                          ctx->dml_lcr,                         /* input lcr */
                          OCI_DEFAULT));

  /* Fill in NEW column list for update LCR.  */
  col_data[0] = (oratext *)"ORACLE_11";  /* value and length for column "F2" */
  col_data_lens[0] = 9;           

  OCICALL(ocip, 
          OCILCRRowColumnInfoSet(ocip->svcp, ocip->errp,
                            OCI_LCR_ROW_COLVAL_NEW,     /* column_value_type */
                            1,             /* num columns in col prop arrays */
                            col_names,                    
                            col_name_lens,              
                            col_dtys,                    
                            col_data,                   
                            col_inds,                        
                            col_data_lens,             
                            col_csfs,               
                            (oraub8 *)0,                     /* column flags */
                            (ub2 *)0,                         /* column csid */
                            (void *)ctx->dml_lcr,               /* input lcr */
                            OCI_DEFAULT));

  /* Fill in OLD column list for update LCR.  Must include PK or unique 
   * key column to identify the row to be updated. For this demo, column
   * "F1" must be included. Column "F2" is not required. Include this column
   * if want to compare this column for update conflict detection.
   */
  col_data[0] = f2_value;                /* value and length for column "F2" */
  col_data_lens[0] = (ub2)strlen((char *)f2_value);           
  col_data[1] = &f1_value;               /* value and length for column "F1" */
  col_data_lens[1] = sizeof(f1_value); 

  OCICALL(ocip, 
          OCILCRRowColumnInfoSet(ocip->svcp, ocip->errp,
                            OCI_LCR_ROW_COLVAL_OLD,     /* column_value_type */
                            2,             /* num columns in col prop arrays */
                            col_names,                    
                            col_name_lens,              
                            col_dtys,                    
                            col_data,                   
                            col_inds,                        
                            col_data_lens,             
                            col_csfs,               
                            (oraub8 *)0,                     /* column flags */
                            (ub2 *)0,                         /* column csid */
                            (void *)ctx->dml_lcr,               /* input lcr */
                            OCI_DEFAULT));

  /* send lcr */
  send_lcr(ctx, ctx->dml_lcr, OCI_LCR_XROW);

  /* send commit lcr */
  lcvm4by(&ctx->lcr_pos, position);
  OCICALL(ocip, 
          OCILCRHeaderSet(ocip->svcp, ocip->errp,
                          (oratext *)DEFAULT_DBNM,         /* source db name */
                          (ub2)DEFAULT_DBNM_LEN,
                          (oratext *)OCI_LCR_ROW_CMD_COMMIT,     /* cmd_type */
                          (ub2)strlen(OCI_LCR_ROW_CMD_COMMIT),
                          (oratext *)0, 0,              /* schema name & len */
                          (oratext *)0, 0,              /* object name & len */
                          (ub1 *)0, 0,                            /* lcr tag */
                          txid, (ub2)strlen((char *)txid),         /* txn id */
                          &src_time,                      /* lcr source time */
                          position,                          /* lcr position */
                          poslen,                     
                          0,                                     /* LCR flag */
                          ctx->dml_lcr,                         /* input lcr */
                          OCI_DEFAULT));

  /* send lcr */
  send_lcr(ctx, ctx->dml_lcr, OCI_LCR_XROW);
}

/*---------------------------------------------------------------------
 * print_pos - Print position
 *---------------------------------------------------------------------*/
static void print_pos (oratext *posnm, ub1 *pos, ub2 poslen)
{
  if (poslen)
  {
    ub2  idx;

    printf ("%s = ", posnm);
    for (idx = 0; idx < poslen; idx++)
      printf("%02x", pos[idx]);
    printf("\n");
  }
}

/*---------------------------------------------------------------------
 * attach - Attach to inbound server
 *---------------------------------------------------------------------*/
static void attach(myctx_t *ctx, params_t *paramsp)
{
  oci_t *ocip = ctx->ocip;
  ub1    last_pos[OCI_LCR_MAX_POSITION_LEN];
  ub2    last_pos_len = 0;
  sword  err;

  printf ("Attach to XStream inbound '%.*s'\n",
          paramsp->svrnmlen, paramsp->svrnm);

  err = OCIXStreamInAttach(ocip->svcp, ocip->errp, 
                           paramsp->svrnm, (ub2)paramsp->svrnmlen, 
                           DEFAULT_CAPTURE, (ub2)DEFAULT_CAPTURE_LEN,
                           (ub1 *)last_pos, &last_pos_len, OCI_DEFAULT); 
  if (err)
    ocierror(ocip, (char *)"OCIXStreamInAttach failed");

  if (last_pos_len)
  {
    /* convert from ub1 array to ub4 value */
    lcvmby4(last_pos, &ctx->lcr_pos);
    ctx->lcr_pos++;

    print_pos((oratext *)"Last Position processed by inbound server", 
              last_pos, last_pos_len);
    printf ("Starting LCR sequence = %d\n", ctx->lcr_pos);
  }
}

/*---------------------------------------------------------------------
 * detach - Detach from inbound server
 *---------------------------------------------------------------------*/
static void detach(oci_t * ocip)
{
  ub1    proc_pos[OCI_LCR_MAX_POSITION_LEN];
  ub2    proc_pos_len;
  sword  err;
  
  printf ("Detach from XStream inbound server\n");

  err = OCIXStreamInDetach(ocip->svcp, ocip->errp, proc_pos, &proc_pos_len,
                           OCI_DEFAULT);

  if (err)
    ocierror(ocip, (char *)"detach_session() failed");
}

/*---------------------------------------------------------------------
 * connect_db - Connect to the database
 *---------------------------------------------------------------------*/
static void connect_db(params_t *params_p, oci_t ** ociptr, ub2 char_csid,
                       ub2 nchar_csid)
{
  oci_t        *ocip;

  printf ("Connect to Oracle as %.*s@%.*s ",
          params_p->userlen, params_p->user,
          params_p->dbnamelen, params_p->dbname);

  if (char_csid && nchar_csid)
    printf ("using char csid=%d and nchar csid=%d", char_csid, nchar_csid);

  printf("\n");

  ocip = (oci_t *)malloc(sizeof(oci_t));

  if (OCIEnvNlsCreate(&ocip->envp, OCI_OBJECT, (dvoid *)0,
                     (dvoid * (*)(dvoid *, size_t)) 0,
                     (dvoid * (*)(dvoid *, dvoid *, size_t))0,
                     (void (*)(dvoid *, dvoid *)) 0,
                     (size_t) 0, (dvoid **) 0, char_csid, nchar_csid))
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
 * disconnect_db  - Logoff from the database
 *---------------------------------------------------------------------*/
static void disconnect_db(oci_t * ocip)
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

/*--------------------------------------------------------------------
 * print_usage - Print command usage
 *---------------------------------------------------------------------*/
static void print_usage(int exitcode)
{
  puts((char *)"\nUsage: xin -svr <svr_name> -db <db_name> "
               "-usr <conn_user> -pwd <password>\n");

  puts("  svr  : inbound server name\n"
       "  db   : database name of inbound server\n"
       "  usr  : connect user to inbound server\n"
       "  pwd  : password of inbound's connect user\n");

  exit(exitcode);
}

/*--------------------------------------------------------------------
 * get_inputs - Get user inputs from command line
 *---------------------------------------------------------------------*/
static void get_inputs(params_t *params, int argc, char ** argv)
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
      printf("Error: bad argument %s\n", option);
      print_usage(1);
    }

    /* get the value of the option */
    --argc;
    argv++;

    if (!strncmp(option, (char *)"h", 1))
    {
      print_usage(0);
    }

    value = *argv;    

    if (!strncmp(option, (char *)"db", 2))
    {
      params->dbname = (oratext *)value;
      params->dbnamelen = (ub4)strlen(value);
    }
    else if (!strncmp(option, (char *)"usr", 3))
    {
      params->user = (oratext *)value;
      params->userlen = (ub4)strlen(value);
    }
    else if (!strncmp(option, (char *)"pwd", 3))
    {
      params->passw = (oratext *)value;
      params->passwlen = (ub4)strlen(value);
    }
    else if (!strncmp(option, (char *)"svr", 3))
    {
      params->svrnm = (oratext *)value;
      params->svrnmlen = (ub4)strlen(value);
    }
    else
    {
      printf("Error: unknown option %s\n", option);
      print_usage(1);
    }
  }
}

/*---------------------------------------------------------------------
 * get_db_charsets - Get the database CHAR and NCHAR character set ids.
 *---------------------------------------------------------------------*/
static const oratext GET_DB_CHARSETS[] =  \
 "select parameter, value from nls_database_parameters where parameter = \
 'NLS_CHARACTERSET' or parameter = 'NLS_NCHAR_CHARACTERSET' order by parameter";

#define PARM_BUFLEN      (30)

static void get_db_charsets(params_t *params_p, ub2 *char_csid,
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
    ocierror(ocip, (char *)"OCIEnvCreate() failed");
  }

  if (OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **) &ocip->errp,
                     (ub4) OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0))
  {
    ocierror(ocip, (char *)"OCIHandleAlloc(OCI_HTYPE_ERROR) failed");
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

/* end of file xin.c */

