/*-------------------------------------------------------------------------
 *
 * pg_controldata.c
 *		Expose output of pg_controldata as a system view.
 *
 * Copyright (c) 2010, PostgreSQL Global Development Group
 * ALL RIGHTS RESERVED;
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE AUTHOR OR DISTRIBUTORS HAVE BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR AND DISTRIBUTORS SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE AUTHOR AND DISTRIBUTORS HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 */

#include "postgres.h"

#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "catalog/pg_control.h"
#include "catalog/pg_type.h"


PG_MODULE_MAGIC;


#if !defined(PG_VERSION_NUM) || PG_VERSION_NUM < 80200
#error "pg_controldata only builds with PostgreSQL 8.2 or later"
#elif PG_VERSION_NUM < 80300
#define PG_VERSION_82_COMPAT
#elif PG_VERSION_NUM < 80400
#define PG_VERSION_83_COMPAT
#elif PG_VERSION_NUM < 80400
#endif


struct controldata
{
	char	   *name;
	char	   *setting;
};

static struct controldata ControlData[] =
{
	{"pg_control version number", NULL},
	{"Catalog version number", NULL},
	{"Database system identifier", NULL},
	{"Database cluster state", NULL},
	{"pg_control last modified", NULL},
	{"Latest checkpoint location", NULL},
	{"Prior checkpoint location", NULL},
	{"Latest checkpoint's REDO location", NULL},
	{"Latest checkpoint's TimeLineID", NULL},
	{"Latest checkpoint's NextXID", NULL},
	{"Latest checkpoint's NextOID", NULL},
	{"Latest checkpoint's NextMultiXactId", NULL},
	{"Latest checkpoint's NextMultiOffset", NULL},
	{"Latest checkpoint's oldestXID", NULL},
	{"Latest checkpoint's oldestXID's DB", NULL},
	{"Latest checkpoint's oldestActiveXID", NULL},
	{"Time of latest checkpoint", NULL},
	{"Minimum recovery ending location", NULL},
	{"Backup start location", NULL},
	{"Maximum data alignment", NULL},
	{"Database block size", NULL},
	{"Blocks per segment of large relation", NULL},
	{"WAL block size", NULL},
	{"Bytes per WAL segment", NULL},
	{"Maximum length of identifiers", NULL},
	{"Maximum columns in an index", NULL},
	{"Maximum size of a TOAST chunk", NULL},
	{"Date/time type storage", NULL},
	{"Float4 argument passing", NULL},
	{"Float8 argument passing", NULL},
	{NULL, NULL}
};


static const char *dbState(DBState state);
static void get_controldata(void);

Datum pg_controldata(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_controldata);
Datum
pg_controldata(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate	   *tupstore;
	HeapTuple			tuple;
	TupleDesc			tupdesc;
	AttInMetadata	   *attinmeta;
	MemoryContext		per_query_ctx;
	MemoryContext		oldcontext;
	char			   *values[2];
	int					i = 0;

	/* check to see if caller supports us returning a tuplestore */
	if (!rsinfo || !(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("materialize mode required, but it is not "
						"allowed in this context")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* get the requested return tuple description */
	tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);

	/*
	 * Check to make sure we have a reasonable tuple descriptor
	 */
	if (tupdesc->natts != 2 ||
		tupdesc->attrs[0]->atttypid != TEXTOID ||
		tupdesc->attrs[1]->atttypid != TEXTOID)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("query-specified return tuple and "
						"function return type are not compatible")));

	/* OK to use it */
	attinmeta = TupleDescGetAttInMetadata(tupdesc);

	/* let the caller know we're sending back a tuplestore */
	rsinfo->returnMode = SFRM_Materialize;

	/* initialize our tuplestore */
	tupstore = tuplestore_begin_heap(true, false, work_mem);

	get_controldata();
	while (ControlData[i].name)
	{
		values[0] = ControlData[i].name;
		values[1] = ControlData[i].setting;

		tuple = BuildTupleFromCStrings(attinmeta, values);
		tuplestore_puttuple(tupstore, tuple);
		++i;
	}
	
	/*
	 * no longer need the tuple descriptor reference created by
	 * TupleDescGetAttInMetadata()
	 */
	ReleaseTupleDesc(tupdesc);

	tuplestore_donestoring(tupstore);
	rsinfo->setResult = tupstore;

	/*
	 * SFRM_Materialize mode expects us to return a NULL Datum. The actual
	 * tuples are in our tuplestore and passed back through
	 * rsinfo->setResult. rsinfo->setDesc is set to the tuple description
	 * that we actually used to build our tuples with, so the caller can
	 * verify we did what it was expecting.
	 */
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	return (Datum) 0;
}

static const char *
dbState(DBState state)
{
	switch (state)
	{
		case DB_STARTUP:
			return _("starting up");
		case DB_SHUTDOWNED:
			return _("shut down");
		case DB_SHUTDOWNING:
			return _("shutting down");
		case DB_IN_CRASH_RECOVERY:
			return _("in crash recovery");
		case DB_IN_ARCHIVE_RECOVERY:
			return _("in archive recovery");
		case DB_IN_PRODUCTION:
			return _("in production");
	}
	return _("unrecognized status code");
}


static void
get_controldata(void)
{
	ControlFileData ControlFile;
	int				fd;
	char			ControlFilePath[MAXPGPATH];
	pg_crc32		crc;
	time_t			time_tmp;
	char			pgctime_str[128];
	char			ckpttime_str[128];
	char			sysident_str[32];
	const char	   *strftime_fmt = "%c";
	StringInfoData	buf;

	snprintf(ControlFilePath, MAXPGPATH, "%s/global/pg_control", DataDir);

	if ((fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0)) == -1)
		elog(ERROR, "could not open file \"%s\" for reading: %s",
					 ControlFilePath, strerror(errno));

	if (read(fd, &ControlFile, sizeof(ControlFileData)) != sizeof(ControlFileData))
		elog(ERROR, "could not read file \"%s\": %s",
					 ControlFilePath, strerror(errno));

	close(fd);

	/* Check the CRC. */
	INIT_CRC32(crc);
	COMP_CRC32(crc,
			   (char *) &ControlFile,
			   offsetof(ControlFileData, crc));
	FIN_CRC32(crc);

	if (!EQ_CRC32(crc, ControlFile.crc))
		elog(ERROR, "calculated CRC checksum does not match value stored in file");

	/*
	 * This slightly-chintzy coding will work as long as the control file
	 * timestamps are within the range of time_t; that should be the case in
	 * all foreseeable circumstances, so we don't bother importing the
	 * backend's timezone library.
	 *
	 * Use variable for format to suppress overly-anal-retentive gcc warning
	 * about %c
	 */
	time_tmp = (time_t) ControlFile.time;
	strftime(pgctime_str, sizeof(pgctime_str), strftime_fmt,
			 localtime(&time_tmp));
	time_tmp = (time_t) ControlFile.checkPointCopy.time;
	strftime(ckpttime_str, sizeof(ckpttime_str), strftime_fmt,
			 localtime(&time_tmp));

	/*
	 * Format system_identifier separately to keep platform-dependent format
	 * code out of the message string.
	 */
	snprintf(sysident_str, sizeof(sysident_str), UINT64_FORMAT,
			 ControlFile.system_identifier);


	initStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.pg_control_version);
	ControlData[0].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.catalog_version_no);
	ControlData[1].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%s", sysident_str);
	ControlData[2].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%s", dbState(ControlFile.state));
	ControlData[3].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%s", pgctime_str);
	ControlData[4].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%X/%X", ControlFile.checkPoint.xlogid, ControlFile.checkPoint.xrecoff);
	ControlData[5].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%X/%X", ControlFile.prevCheckPoint.xlogid, ControlFile.prevCheckPoint.xrecoff);
	ControlData[6].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%X/%X", ControlFile.checkPointCopy.redo.xlogid, ControlFile.checkPointCopy.redo.xrecoff);
	ControlData[7].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.checkPointCopy.ThisTimeLineID);
	ControlData[8].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u/%u", ControlFile.checkPointCopy.nextXidEpoch, ControlFile.checkPointCopy.nextXid);
	ControlData[9].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.checkPointCopy.nextOid);
	ControlData[10].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.checkPointCopy.nextMulti);
	ControlData[11].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.checkPointCopy.nextMultiOffset);
	ControlData[12].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.checkPointCopy.oldestXid);
	ControlData[13].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.checkPointCopy.oldestXidDB);
	ControlData[14].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.checkPointCopy.oldestActiveXid);
	ControlData[15].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%s", ckpttime_str);
	ControlData[16].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%X/%X", ControlFile.minRecoveryPoint.xlogid, ControlFile.minRecoveryPoint.xrecoff);
	ControlData[17].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%X/%X", ControlFile.backupStartPoint.xlogid, ControlFile.backupStartPoint.xrecoff);
	ControlData[18].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.maxAlign);
	ControlData[19].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.blcksz);
	ControlData[20].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.relseg_size);
	ControlData[21].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.xlog_blcksz);
	ControlData[22].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.xlog_seg_size);
	ControlData[23].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.nameDataLen);
	ControlData[24].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.indexMaxKeys);
	ControlData[25].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%u", ControlFile.toast_max_chunk_size);
	ControlData[26].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%s", (ControlFile.enableIntTimes ? "64-bit integers" : "floating-point numbers"));
	ControlData[27].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%s", (ControlFile.float4ByVal ? "by value" : "by reference"));
	ControlData[28].setting = pstrdup(buf.data);
	resetStringInfo(&buf);

	appendStringInfo(&buf, "%s", (ControlFile.float8ByVal ? "by value" : "by reference"));
	ControlData[29].setting = pstrdup(buf.data);
	resetStringInfo(&buf);
}
