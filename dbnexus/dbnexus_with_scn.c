/* Copyright (c) 2008, 2011, Oracle and/or its affiliates. 
All rights reserved. */

/*

   NAME         xout - XStream Out Client Using Non-Callback Method.

   DESCRIPTION  
   This program attaches to XStream outbound server and waits for 
   transactions from that server. For each LCR received it prints the 
   contents of each LCR. If the LCR contains chunked columns, it
   prints the first 50 bytes of each chunk. 

   This program waits indefinitely for transactions from the outbound server.
   Hit control-C to interrupt the program. 

   This program does not set the processed low position for the outbound
   server. This was not done to demonstrate that if the processed low position
   is not set then each time the program re-attaches to the outbound server, 
   the same set of LCRs are resent from the server.

   Usage: xout -svr <svr_name> -db <db_name> -usr <conn_user> -pwd <password>

     svr  : outbound server name
     db   : database name of outbound server
     usr  : connect user to outbound server
     pwd  : password of outbound's connect user
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

#define MAX_COLUMNS        1000                       /* max cols in a table */
#define MAX_PRINT_BYTES    (50) /* print max of 50 bytes per col/chunk value */
#define BUF_SIZE           80
#define DEFAULT_FS_PREC    6             /* default fractional sec precision */
#define DEFAULT_LF_PREC    9              /* default leading field precision */
#define TS_FORMAT          (oratext *)"DD-MON-YYYY HH24:MI:SS.FF"
#define TSZ_FORMAT         (oratext *)"DD-MON-YYYY HH24:MI:SS.FF TZH:TZM"

#define samecmd(cmdstr1, cmdlen1, cmdstr2, cmdlen2) \
 (((cmdlen1) == (cmdlen2)) && !memcmp((cmdstr1), (cmdstr2), cmdlen1))

#define IS_COMMIT_CMD(cmd_type, cmd_type_len) \
   (samecmd((cmd_type), (cmd_type_len), \
             OCI_LCR_ROW_CMD_COMMIT, strlen(OCI_LCR_ROW_CMD_COMMIT)))

#define IS_INSERT_CMD(cmd_type, cmd_type_len) \
   (samecmd((cmd_type), (cmd_type_len), \
             OCI_LCR_ROW_CMD_INSERT, strlen(OCI_LCR_ROW_CMD_INSERT)))

#define IS_UPDATE_CMD(cmd_type, cmd_type_len) \
   (samecmd((cmd_type), (cmd_type_len), \
             OCI_LCR_ROW_CMD_UPDATE, strlen(OCI_LCR_ROW_CMD_UPDATE)))

#define IS_DELETE_CMD(cmd_type, cmd_type_len) \
   (samecmd((cmd_type), (cmd_type_len), \
             OCI_LCR_ROW_CMD_DELETE, strlen(OCI_LCR_ROW_CMD_DELETE)))

#define IS_LOBOP_CMD(cmd_type, cmd_type_len) \
  (samecmd((cmd_type), (cmd_type_len), \
           OCI_LCR_ROW_CMD_LOB_WRITE, strlen(OCI_LCR_ROW_CMD_LOB_WRITE)) || \
   samecmd((cmd_type), (cmd_type_len), \
           OCI_LCR_ROW_CMD_LOB_TRIM, strlen(OCI_LCR_ROW_CMD_LOB_TRIM)) || \
   samecmd((cmd_type), (cmd_type_len), \
           OCI_LCR_ROW_CMD_LOB_ERASE, strlen(OCI_LCR_ROW_CMD_LOB_ERASE)))

typedef struct params
{
  oratext * user; 
  ub4       userlen;
  oratext * passw;
  ub4       passwlen;
  oratext * dbname;
  ub4       dbnamelen;
  oratext * applynm;
  ub4       applynmlen;
} params_t;

typedef struct oci                                            /* OCI handles */
{
  OCIEnv      *envp;                                   /* Environment handle */
  OCIError    *errp;                                         /* Error handle */
  OCIServer   *srvp;                                        /* Server handle */
  OCISvcCtx   *svcp;                                       /* Service handle */
  OCISession  *authp;
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

static void connect_db(params_t *opt_params_p, oci_t ** ocip);
static void disconnect_db(oci_t * ocip);
static void ocierror(oci_t * ocip, char * msg);
static void attach(oci_t *ocip, params_t *paramsp);
static void detach(oci_t *ocip);
static void get_lcrs(oci_t *ocip);
static void print_lcr(oci_t *ocip, void *lcrp, ub1 lcrtype);
static void princt_lcr_cb(void *ocip, void *lcrp, ub1 lcrtype, oraub8 flags);
static void get_chunks(oci_t * ocip);
static void print_chunk (ub1 *chunk_ptr, ub4 chunk_len, ub2 dty);
static void print_col_data(oci_t *ocip, void *lcrp, ub2 col_value_type);
static void print_ddl(oci_t *ocip, void *lcrp);      
static void get_inputs(params_t *params, int argc, char ** argv);

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
  oci_t        *ocip = (oci_t *)NULL;
  params_t      params;

  get_inputs(&params, argc, argv);           /* parse command line arguments */

  /* connect to the database */
  connect_db(&params, &ocip);

  /* Attach to outbound server */
  attach(ocip, &params);

  /* Get LCR loop */
  get_lcrs(ocip);

  /* Detach from outbound server */
  detach(ocip);

  /* Disconnect from database */
  disconnect_db(ocip);

  free(ocip);
  exit(0);
}

/*---------------------------------------------------------------------
 * connect_db - Connect to the database
 *---------------------------------------------------------------------*/
static void connect_db(params_t *params_p, oci_t ** ociptr)
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
                            (ub2)paramsp->applynmlen, (ub1 *)0, 0, OCI_DEFAULT);
  if (err)
    ocierror(ocip, (char *)"OCIXStreamOutAttach failed");
}

/*---------------------------------------------------------------------
 * get_lcrs - Execute loop to get lcrs from outbound server.
 *---------------------------------------------------------------------*/
static void get_lcrs(myctx_t *ctx)
{
  sword       status = OCI_SUCCESS;
  void       *lcr;
  ub1         lcrtype;
  oraub8      flag;
  oraub8      flag;
  ub1         flwm[OCI_LCR_MAX_POSITION_LEN];
  ub2         flwm_len = 0;
  ub1         proclwm[OCI_LCR_MAX_POSITION_LEN];
  ub2         proclwm_len = 0;
  sb4         rtncode = 0;
  oci_t      *ocip = ctx->outbound_ocip;

  printf ("\n>>> get_lcrs -- Start\n");

  ctx->lcrcnt = 0;

  while (status == OCI_SUCCESS)
  {
    startcnt = ctx->lcrcnt;
    /* ignore unsupported LCRs and continue */
    /* Execute non-callback call to receive LCRs */
    while ((status = OCIXStreamOutLCRReceive(ocip->svcp, ocip->errp,
                                             &lcr, &lcrtype, &flag, 
                                             flwm, &flwm_len, OCI_DEFAULT))
                          == OCI_STILL_EXECUTING)
    {
      /*print_lcr(ocip, lcr, lcrtype); */
      //print_lcr_cb(ocip, lcr, lcrtype, flag);
      process_IDKeyLCR(ctx, lcr, lcrtype, flag);

      /* If LCR has chunked columns (i.e, has LOB/Long/XMLType columns) */
      if (flag & OCI_XSTREAM_MORE_ROW_DATA)
      {
        /* Get all the chunks belonging to the current LCR. */
        get_chunks(ocip);  
      }
    }

    if (status == OCI_ERROR)
      ocierror(ocip, (char *)"get_lcrs() failed");
  }
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
 * get_chunks - Get all the chunks belonging to the current LCR.
 *---------------------------------------------------------------------*/
static void get_chunks(oci_t * ocip)
{
  sword       status = OCI_SUCCESS;
  oratext    *colname;
  ub2         colname_len;
  ub2         coldty;
  oraub8      col_flags;
  ub2         col_csid;
  ub4         chunk_len;                            /* chunk length in bytes */
  ub1        *chunk_data;                           /* Ptr to the chunk data */
  oraub8      row_flag;

  /* Loop to receive each chunk until there is no more data for the current
   * row change.
   */
  do 
  {
    /* Execute non-callback call to receive each chunk */
    status = OCIXStreamOutChunkReceive(ocip->svcp, ocip->errp, &colname,
                                       &colname_len, &coldty, &col_flags, 
                                       &col_csid, &chunk_len, &chunk_data, 
                                       &row_flag, OCI_DEFAULT);
    if (status != OCI_SUCCESS)
      ocierror(ocip, (char *)"ReceiveChunk() failed");

    /* print chunked column info */
    printf(
      "Chunked column name=%.*s DTY=%d  chunk len=%d csid=%d col_flag=0x%lx\n",
      colname_len, colname, coldty, chunk_len, col_csid, 
      (unsigned long)col_flags);

    /* print each chunk received */
    print_chunk(chunk_data, chunk_len, coldty);

    if (status == OCI_ERROR)
      ocierror(ocip, (char *)"get_lcrs() failed");

  } while (row_flag & OCI_XSTREAM_MORE_ROW_DATA);
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
static void print_col_data(oci_t *ocip, void *lcrp, ub2 col_value_type)
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
  ub2        idx;
  oratext    buf[BUF_SIZE];

  OCICALL(ocip, 
          OCILCRRowColumnInfoGet (ocip->svcp, ocip->errp, col_value_type, 
                                  &num_cols, (oratext **)&colname, 
                                  (ub2 *)&colnamel, (ub2 *)&coldty, 
                                  (void **)&colval, (OCIInd *)&colind, 
                                  (ub2 *)&collen, (ub1 *)&colcsf, 
                                  (oraub8*)colflg, (ub2 *)colcsid,
                                  lcrp, MAX_COLUMNS, OCI_DEFAULT));
  
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
          float       float_val;

          OCICALL(ocip, 
                  OCINumberToReal(ocip->errp, (const OCINumber *)rawnum,
                                 sizeof(float_val), &float_val));

          printf (" value=%f", float_val);
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
                  OCIDateTimeToText(ocip->envp, ocip->errp, dtvalue, TSZ_FORMAT,
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
          ub2           intv_len;                  /* result interval length */

          OCICALL(ocip,
                  OCIIntervalToText(ocip->envp, ocip->errp, intv, 
                                    DEFAULT_LF_PREC, DEFAULT_FS_PREC, 
                                    buf, sizeof(buf), &reslen)); 
          intv_len = (ub2)reslen;
          printf (" value=%.*s ", intv_len, buf);
          
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

  printf("Data = ");
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
  puts((char *)"\nUsage: xout -svr <svr_name> -db <db_name> "
               "-usr <conn_user> -pwd <password>\n");

  puts("  svr  : outbound server name\n"
       "  db   : database name of outbound server\n"
       "  usr  : connect user to outbound server\n"
       "  pwd  : password of outbound's connect user\n");

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
      params->applynm = (oratext *)value;
      params->applynmlen = (ub4)strlen(value);
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
}

