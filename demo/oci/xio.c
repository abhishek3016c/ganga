/* Copyright (c) 2008, 2011, Oracle and/or its affiliates. 
All rights reserved. */

/*
   NAME   
      xio - XStream Out -> In Demo

   DESCRIPTION  
   This program first attaches to an XStream outbound server and inbound
   server. Next it goes into an infinite loop waiting for lcrs from the
   outbound server. Each lcr received is immediately sent to the inbound
   server. While transferring data, it periodically gets the processed 
   low position from the inbound server and sends that value to the 
   outbound server.
   
   During periods when no lcr is received from the outbound server, this 
   program periodically sends a commit lcr to ping the inbound server with the 
   new position so that the inbound server can advance its processed low
   position.

   This program waits indefinitely for transactions from the outbound server.
   Hit control-C to interrupt the program.

   If the program is restarted, the outbound server will start sending
   from the processed low position which was set during the previous run.
   
   Usage: xio -ob_svr <outbound_svr> -ob_db <outbound_db>
              -ob_usr <conn_user> -ob_pwd <conn_user_pwd>
              -ib_svr <inbound_svr> -ib_db <inbound_db>
              -ib_usr <apply_user> -ib_pwd <apply_user_pwd>

     ob_svr  : outbound server name
     ob_db   : database name of outbound server
     ob_usr  : connect user to outbound server
     ob_pwd  : password of outbound's connect user
     ib_svr  : inbound server name
     ib_db   : database name of inbound server
     ib_usr  : apply user for inbound server
     ib_pwd  : password of inbound's apply user

   Notes:
   To mimimize character set conversion when transferring LCRs from outbound
   to inbound server, this program sets the client CHAR and NCHAR
   character sets to the outbound database character sets.

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

/*---------------------------------------------------------------------- 
 *           Internal structures
 *----------------------------------------------------------------------*/ 

#define M_DBNAME_LEN    (128)

typedef struct conn_info                                     /* connect info */
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
  conn_info_t  xout;                                        /* outbound info */
  conn_info_t  xin;                                          /* inbound info */
} params_t;

typedef struct oci                                            /* OCI handles */
{
  OCIEnv      *envp;                                   /* Environment handle */
  OCIError    *errp;                                         /* Error handle */
  OCIServer   *srvp;                                        /* Server handle */
  OCISvcCtx   *svcp;                                       /* Service handle */
  OCISession  *authp;
  OCIStmt    *stmtp;        
  boolean     attached;
  boolean     outbound;
} oci_t;

static void connect_db(conn_info_t *opt_params_p, oci_t ** ocip, ub2 char_csid,
                       ub2 nchar_csid);
static void disconnect_db(oci_t * ocip);
static void ocierror(oci_t * ocip, char * msg);
static void attach(oci_t * ocip, conn_info_t *conn, boolean outbound);
static void detach(oci_t *ocip);
static void get_lcrs(oci_t *xin_ocip, oci_t *xout_ocip);
static void get_chunks(oci_t *xin_ocip, oci_t *xout_ocip);
static void print_lcr(oci_t *ocip, void *lcrp, ub1 lcrtype, 
                      oratext **src_db_name, ub2  *src_db_namel);
static void print_chunk (ub1 *chunk_ptr, ub4 chunk_len, ub2 dty);
static void get_inputs(conn_info_t *xout_params, conn_info_t *xin_params, 
                       int argc, char ** argv);
static void get_db_charsets(conn_info_t *params_p, ub2 *char_csid, 
                            ub2 *nchar_csid);
static void set_client_charset(oci_t *outbound_ocip);

#define OCICALL(ocip, function) do {\
sword status=function;\
if (OCI_SUCCESS==status) break;\
else if (OCI_ERROR==status) \
{ocierror(ocip, (char *)"OCI_ERROR");\
exit(1);}\
else {printf("Error encountered %d\n", status);\
exit(1);}\
} while(0)

/*---------------------------------------------------------------------
 *                M A I N   P R O G R A M
 *---------------------------------------------------------------------*/
main(int argc, char **argv)
{
  /* Outbound and inbound connection info */
  conn_info_t   xout_params;
  conn_info_t   xin_params;
  oci_t        *xout_ocip = (oci_t *)NULL;
  oci_t        *xin_ocip = (oci_t *)NULL;
  ub2           obdb_char_csid = 0;                 /* outbound db char csid */
  ub2           obdb_nchar_csid = 0;               /* outbound db nchar csid */

  /* parse command line arguments */
  get_inputs(&xout_params, &xin_params, argc, argv); 

  /* Get the outbound database CHAR and NCHAR character set info */
  get_db_charsets(&xout_params, &obdb_char_csid, &obdb_nchar_csid);

  /* Connect to the outbound db and set the client env to the outbound charsets
   * to minimize character conversion when transferring LCRs from outbound 
   * directly to inbound server. 
   */
  connect_db(&xout_params, &xout_ocip, obdb_char_csid, obdb_nchar_csid);

  /* Attach to outbound server */
  attach(xout_ocip, &xout_params, TRUE);

  /* connect to inbound db and set the client charsets the same as the 
   * outbound db charsets.
   */
  connect_db(&xin_params, &xin_ocip, obdb_char_csid, obdb_nchar_csid);

  /* Attach to inbound server */
  attach(xin_ocip, &xin_params, FALSE);

  /* Get lcrs from outbound server and send to inbound server */
  get_lcrs(xin_ocip, xout_ocip);

  /* Detach from XStream servers */
  detach(xout_ocip);
  detach(xin_ocip);

  /* Disconnect from both databases */
  disconnect_db(xout_ocip);
  disconnect_db(xin_ocip);

  free(xout_ocip);
  free(xin_ocip);
  exit (0);
}

/*---------------------------------------------------------------------
 * connect_db - Connect to the database and set the env to the given
 * char and nchar character set ids. 
 *---------------------------------------------------------------------*/
static void connect_db(conn_info_t *params_p, oci_t **ociptr, ub2 char_csid,
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

  /* Logon to database */
  OCICALL(ocip,
          OCILogon(ocip->envp, ocip->errp, &ocip->svcp,
                   params_p->user, params_p->userlen,
                   params_p->passw, params_p->passwlen,
                   params_p->dbname, params_p->dbnamelen));

  /* allocate the server handle */
  OCICALL(ocip,
          OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **) &ocip->srvp,
                         OCI_HTYPE_SERVER, (size_t) 0, (dvoid **) 0));

  OCICALL(ocip, 
          OCIHandleAlloc((dvoid *) ocip->envp, (dvoid **) &ocip->stmtp,
                     (ub4) OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0));

  if (*ociptr == (oci_t *)NULL)
  {
    *ociptr = ocip;
  }
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

/*---------------------------------------------------------------------
 * attach - Attach to XStream server specified in connection info
 *---------------------------------------------------------------------*/
static void attach(oci_t * ocip, conn_info_t *conn, boolean outbound)
{
  sword       err;

  printf ("Attach to XStream %s server '%.*s'\n", 
          outbound ? "outbound" : "inbound",
          conn->svrnmlen, conn->svrnm);

  if (outbound)
  {
    OCICALL(ocip, 
            OCIXStreamOutAttach(ocip->svcp, ocip->errp, conn->svrnm,
                              (ub2)conn->svrnmlen, (ub1 *)0, 0, OCI_DEFAULT));
  }
  else
  {
    OCICALL(ocip, 
            OCIXStreamInAttach(ocip->svcp, ocip->errp, conn->svrnm,
                               (ub2)conn->svrnmlen, 
                               (oratext *)"From_XOUT", 9,
                               (ub1 *)0, 0, OCI_DEFAULT));
  }

  ocip->attached = TRUE;
  ocip->outbound = outbound;
}

/*---------------------------------------------------------------------
 * ping_svr - Ping inbound server by sending a commit LCR.
 *---------------------------------------------------------------------*/
static void ping_svr(oci_t *xin_ocip, void *commit_lcr,
                     ub1 *cmtpos, ub2 cmtpos_len, 
                     oratext *source_db, ub2 source_db_len)
{
  OCIDate     src_time;
  oratext     txid[128];

  OCICALL(xin_ocip, OCIDateSysDate(xin_ocip->errp, &src_time));
  sprintf((char *)txid, "Ping %2d:%2d:%2d",
          src_time.OCIDateTime.OCITimeHH,
          src_time.OCIDateTime.OCITimeMI,
          src_time.OCIDateTime.OCITimeSS);

  /* Initialize LCR with new txid and commit position */
  OCICALL(xin_ocip,
          OCILCRHeaderSet(xin_ocip->svcp, xin_ocip->errp,
                          source_db, source_db_len,
                          (oratext *)OCI_LCR_ROW_CMD_COMMIT,
                          (ub2)strlen(OCI_LCR_ROW_CMD_COMMIT),
                          (oratext *)0, 0,                     /* null owner */
                          (oratext *)0, 0,                    /* null object */
                          (ub1 *)0, 0,                           /* null tag */
                          txid, (ub2)strlen((char *)txid),
                          &src_time, cmtpos, cmtpos_len,
                          0, commit_lcr, OCI_DEFAULT));

  /* Send commit lcr to inbound server. */
  if (OCIXStreamInLCRSend(xin_ocip->svcp, xin_ocip->errp, commit_lcr,
                          OCI_LCR_XROW, 0, OCI_DEFAULT) == OCI_ERROR)
  {
    ocierror(xin_ocip, (char *)"OCIXStreamInLCRSend failed in ping_svr()");
  }
}

/*---------------------------------------------------------------------
 * get_lcrs - Get LCRs from outbound server and send to inbound server.
 *---------------------------------------------------------------------*/
static void get_lcrs(oci_t *xin_ocip, oci_t *xout_ocip)
{
  sword       status = OCI_SUCCESS;
  void       *lcr;
  ub1         lcrtype;
  oraub8      flag;
  ub1         proclwm[OCI_LCR_MAX_POSITION_LEN];
  ub2         proclwm_len = 0;
  ub1         sv_pingpos[OCI_LCR_MAX_POSITION_LEN];
  ub2         sv_pingpos_len = 0;
  ub1         fetchlwm[OCI_LCR_MAX_POSITION_LEN];
  ub2         fetchlwm_len = 0;
  void       *commit_lcr = (void *)0;
  oratext    *lcr_srcdb = (oratext *)0;
  ub2         lcr_srcdb_len = 0;
  oratext     source_db[M_DBNAME_LEN];
  ub2         source_db_len = 0;
  ub4         lcrcnt = 0;

  /* create an lcr to ping the inbound server periodically by sending a
   * commit lcr.
   */
  commit_lcr = (void*)0;
  OCICALL(xin_ocip,
          OCILCRNew(xin_ocip->svcp, xin_ocip->errp, OCI_DURATION_SESSION,
                    OCI_LCR_XROW, &commit_lcr, OCI_DEFAULT));

  while (status == OCI_SUCCESS)
  {
    lcrcnt = 0;                         /* reset lcr count before each batch */

    while ((status = 
                OCIXStreamOutLCRReceive(xout_ocip->svcp, xout_ocip->errp,
                                        &lcr, &lcrtype, &flag, 
                                        fetchlwm, &fetchlwm_len, OCI_DEFAULT))
                                               == OCI_STILL_EXECUTING)
    {
      lcrcnt++;

      /* print header of LCR just received */
      print_lcr(xout_ocip, lcr, lcrtype, &lcr_srcdb, &lcr_srcdb_len);

      /* save the source db to construct ping lcr later */
      if (!source_db_len && lcr_srcdb_len)
      {
        memcpy(source_db, lcr_srcdb, lcr_srcdb_len);
        source_db_len = lcr_srcdb_len;
      }
      
      /* send the LCR just received */
      if (OCIXStreamInLCRSend(xin_ocip->svcp, xin_ocip->errp, 
                              lcr, lcrtype, flag, OCI_DEFAULT) == OCI_ERROR)
      {
        ocierror(xin_ocip, (char *)"OCIXStreamInLCRSend failed");
      }

      /* If LCR has chunked columns (i.e, has LOB/Long/XMLType columns) */
      if (flag & OCI_XSTREAM_MORE_ROW_DATA)
      {
        /* receive and send chunked columns */
        get_chunks(xin_ocip, xout_ocip); 
      }
    }

    if (status == OCI_ERROR)
      ocierror(xout_ocip, (char *)"OCIXStreamOutLCRReceive failed");

    /* clear the saved ping position if we just received some new lcrs */
    if (lcrcnt)
    {
      sv_pingpos_len = 0;
    }

    /* If no lcrs received during previous WHILE loop and got a new fetch 
     * LWM then send a commit lcr to ping the inbound server with the new
     * fetch LWM position.
     */
    else if (fetchlwm_len > 0 && source_db_len > 0 &&
        (fetchlwm_len != sv_pingpos_len ||
         memcmp(sv_pingpos, fetchlwm, fetchlwm_len))) 
    {
      /* To ensure we don't send multiple lcrs with duplicate position, send
       * a new ping only if we have saved the last ping position.
       */
      if (sv_pingpos_len > 0)
      {     
        ping_svr(xin_ocip, commit_lcr, fetchlwm, fetchlwm_len,
                 source_db, source_db_len); 
      }

      /* save the position just sent to inbound server */
      memcpy(sv_pingpos, fetchlwm, fetchlwm_len);
      sv_pingpos_len = fetchlwm_len;
    }

    /* flush inbound network to flush all lcrs to inbound server */
    OCICALL(xin_ocip,
            OCIXStreamInFlush(xin_ocip->svcp, xin_ocip->errp, OCI_DEFAULT));

    
    /* get processed LWM of inbound server */   
    OCICALL(xin_ocip, 
            OCIXStreamInProcessedLWMGet(xin_ocip->svcp, xin_ocip->errp,
                                        proclwm, &proclwm_len, OCI_DEFAULT));
 
    if (proclwm_len > 0)
    {
      /* Set processed LWM for outbound server */
      OCICALL(xout_ocip, 
              OCIXStreamOutProcessedLWMSet(xout_ocip->svcp, xout_ocip->errp, 
                                           proclwm, proclwm_len, OCI_DEFAULT));
    }
  }
  
  if (status != OCI_SUCCESS)
    ocierror(xout_ocip, (char *)"get_lcrs() encounters error");
}

/*---------------------------------------------------------------------
 * get_chunks - Get each chunk for the current LCR and send it to 
 *              the inbound server.
 *---------------------------------------------------------------------*/
static void get_chunks(oci_t *xin_ocip, oci_t *xout_ocip)
{
  oratext *colname;
  ub2      colname_len;
  ub2      coldty;
  oraub8   col_flags;
  ub2      col_csid;
  ub4      chunk_len;
  ub1     *chunk_ptr;
  oraub8   row_flag;
  sword    err;
  sb4      rtncode;

  do
  {
    /* Get a chunk from outbound server */
    OCICALL(xout_ocip,
            OCIXStreamOutChunkReceive(xout_ocip->svcp, xout_ocip->errp, 
                                      &colname, &colname_len, &coldty, 
                                      &col_flags, &col_csid, &chunk_len, 
                                      &chunk_ptr, &row_flag, OCI_DEFAULT));
   
    /* print chunked column info */
    printf(
     "  Chunked column name=%.*s DTY=%d  chunk len=%d csid=%d col_flag=0x%lx\n",
      colname_len, colname, coldty, chunk_len, col_csid, col_flags);

    /* print chunk data */
    print_chunk(chunk_ptr, chunk_len, coldty);

    /* Send the chunk just received to inbound server */
    OCICALL(xin_ocip,
            OCIXStreamInChunkSend(xin_ocip->svcp, xin_ocip->errp, colname,
                                  colname_len, coldty, col_flags,
                                  col_csid, chunk_len, chunk_ptr,
                                  row_flag, OCI_DEFAULT));

  } while (row_flag & OCI_XSTREAM_MORE_ROW_DATA);
}

/*---------------------------------------------------------------------
 * print_chunk - Print chunked column information. Only print the first
 *               50 bytes for each chunk.
 *---------------------------------------------------------------------*/
static void print_chunk (ub1 *chunk_ptr, ub4 chunk_len, ub2 dty)
{
#define MAX_PRINT_BYTES     (50)          /* print max of 50 bytes per chunk */

  ub4  print_bytes;

  if (chunk_len == 0)
    return;

  print_bytes = chunk_len > MAX_PRINT_BYTES ? MAX_PRINT_BYTES : chunk_len;

  printf("  Data = ");
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
 * print_lcr - Print header information of given lcr.
 *---------------------------------------------------------------------*/
static void print_lcr(oci_t *ocip, void *lcrp, ub1 lcrtype, 
                      oratext **src_db_name, ub2  *src_db_namel)
{
  oratext     *cmd_type;
  ub2          cmd_type_len;
  oratext     *owner;
  ub2          ownerl;
  oratext     *oname;
  ub2          onamel;
  oratext     *txid;
  ub2          txidl;
  sword        ret;

  printf("\n ----------- %s LCR Header  -----------------\n",
         lcrtype == OCI_LCR_XDDL ? "DDL" : "ROW");

  /* Get LCR Header information */
  ret = OCILCRHeaderGet(ocip->svcp, ocip->errp, 
                        src_db_name, src_db_namel,              /* source db */
                        &cmd_type, &cmd_type_len,            /* command type */
                        &owner, &ownerl,                       /* owner name */
                        &oname, &onamel,                      /* object name */
                        (ub1 **)0, (ub2 *)0,                      /* lcr tag */
                        &txid, &txidl, (OCIDate *)0,   /* txn id  & src time */
                        (ub2 *)0, (ub2 *)0,              /* OLD/NEW col cnts */
                        (ub1 **)0, (ub2 *)0,                 /* LCR position */
                        (oraub8*)0, lcrp, OCI_DEFAULT);

  if (ret != OCI_SUCCESS)
    ocierror(ocip, (char *)"OCILCRHeaderGet failed");
  else
  {
    printf("  src_db_name=%.*s\n  cmd_type=%.*s txid=%.*s\n",
           *src_db_namel, *src_db_name, cmd_type_len, cmd_type, txidl, txid );

    if (ownerl > 0)
      printf("  owner=%.*s oname=%.*s \n", ownerl, owner, onamel, oname);
  } 
}

/*---------------------------------------------------------------------
 * detach - Detach from XStream server
 *---------------------------------------------------------------------*/
static void detach(oci_t * ocip)
{
  sword  err = OCI_SUCCESS;

  printf ("Detach from XStream %s server\n",
          ocip->outbound ? "outbound" : "inbound" );

  if (ocip->outbound)
  {
    OCICALL(ocip, OCIXStreamOutDetach(ocip->svcp, ocip->errp, OCI_DEFAULT));
  }
  else
  {
    OCICALL(ocip, OCIXStreamInDetach(ocip->svcp, ocip->errp, 
                                     (ub1 *)0, (ub2 *)0,    /* processed LWM */
                                     OCI_DEFAULT));
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
  puts("\nUsage: xio -ob_svr <outbound_svr> -ob_db <outbound_db>\n" 
         "           -ob_usr <conn_user> -ob_pwd <conn_user_pwd>\n" 
         "           -ib_svr <inbound_svr> -ib_db <inbound_db>\n"
         "           -ib_usr <apply_user> -ib_pwd <apply_user_pwd>\n");
  puts("  ob_svr  : outbound server name\n"
       "  ob_db   : database name of outbound server\n"
       "  ob_usr  : connect user to outbound server\n"
       "  ob_pwd  : password of outbound's connect user\n"
       "  ib_svr  : inbound server name\n"
       "  ib_db   : database name of inbound server\n"
       "  ib_usr  : apply user for inbound server\n"
       "  ib_pwd  : password of inbound's apply user\n");

  exit(exitcode);
}

/*--------------------------------------------------------------------
 * get_inputs - Get user inputs from command line
 *---------------------------------------------------------------------*/
static void get_inputs(conn_info_t *xout_params, conn_info_t *xin_params, 
                       int argc, char ** argv)
{
  char * option;
  char * value;

  memset (xout_params, 0, sizeof(*xout_params));
  memset (xin_params, 0, sizeof(*xin_params));
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

    if (!strncmp(option, (char *)"ob_db", 5))
    {
      xout_params->dbname = (oratext *)value;
      xout_params->dbnamelen = (ub4)strlen(value);
    }
    else if (!strncmp(option, (char *)"ob_usr", 6))
    {
      xout_params->user = (oratext *)value;
      xout_params->userlen = (ub4)strlen(value);
    }
    else if (!strncmp(option, (char *)"ob_pwd", 6))
    {
      xout_params->passw = (oratext *)value;
      xout_params->passwlen = (ub4)strlen(value);
    }
    else if (!strncmp(option, (char *)"ob_svr", 6))
    {
      xout_params->svrnm = (oratext *)value;
      xout_params->svrnmlen = (ub4)strlen(value);
    }
    else if (!strncmp(option, (char *)"ib_db", 5))
    {
      xin_params->dbname = (oratext *)value;
      xin_params->dbnamelen = (ub4)strlen(value);
    }
    else if (!strncmp(option, (char *)"ib_usr", 6))
    {
      xin_params->user = (oratext *)value;
      xin_params->userlen = (ub4)strlen(value);
    }
    else if (!strncmp(option, (char *)"ib_pwd", 6))
    {
      xin_params->passw = (oratext *)value;
      xin_params->passwlen = (ub4)strlen(value);
    }
    else if (!strncmp(option, (char *)"ib_svr", 6))
    {
      xin_params->svrnm = (oratext *)value;
      xin_params->svrnmlen = (ub4)strlen(value);
    }
    else
    {
      printf("Error: unknown option '%s'.\n", option);
      print_usage(1);
    }
  }

  /* print usage and exit if any argument is not specified */
  if (!xout_params->svrnmlen || !xout_params->passwlen || 
      !xout_params->userlen || !xout_params->dbnamelen ||
      !xin_params->svrnmlen || !xin_params->passwlen || 
      !xin_params->userlen || !xin_params->dbnamelen)
  {
    printf("Error: missing command arguments. \n");
    print_usage(1);
  }
}
