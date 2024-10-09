/*-------------------------------------------------------------------------
 *
 * mcxtfuncs.c
 *	  Functions to show backend memory context.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/mcxtfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/wait_event_types.h"
#include "common/file_utils.h"

/* ----------
 * The max bytes for showing identifiers of MemoryContext.
 * ----------
 */

struct MemoryContextState *memCtxState = NULL;

/*
 * int_list_to_array
 *		Convert an IntList to an array of INT4OIDs.
 */
static Datum
int_list_to_array(const List *list)
{
	Datum	   *datum_array;
	int			length;
	ArrayType  *result_array;

	length = list_length(list);
	datum_array = (Datum *) palloc(length * sizeof(Datum));

	foreach_int(i, list)
		datum_array[foreach_current_index(i)] = Int32GetDatum(i);

	result_array = construct_array_builtin(datum_array, length, INT4OID);

	return PointerGetDatum(result_array);
}

/*
 * PutMemoryContextsStatsTupleStore
 *		Add details for the given MemoryContext to 'tupstore'.
 */
static void
PutMemoryContextsStatsTupleStore(Tuplestorestate *tupstore,
								 TupleDesc tupdesc, MemoryContext context,
								 HTAB *context_id_lookup)
{
#define PG_GET_BACKEND_MEMORY_CONTEXTS_COLS	10

	Datum		values[PG_GET_BACKEND_MEMORY_CONTEXTS_COLS];
	bool		nulls[PG_GET_BACKEND_MEMORY_CONTEXTS_COLS];
	MemoryContextCounters stat;
	List	   *path = NIL;
	const char *name;
	const char *ident;
	const char *type;

	Assert(MemoryContextIsValid(context));

	/*
	 * Figure out the transient context_id of this context and each of its
	 * ancestors.
	 */
	for (MemoryContext cur = context; cur != NULL; cur = cur->parent)
	{
		MemoryContextId *entry;
		bool		found;

		entry = hash_search(context_id_lookup, &cur, HASH_FIND, &found);

		if (!found)
			elog(ERROR, "hash table corrupted");
		path = lcons_int(entry->context_id, path);
	}

	/* Examine the context itself */
	memset(&stat, 0, sizeof(stat));
	(*context->methods->stats) (context, NULL, NULL, &stat, true);

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	name = context->name;
	ident = context->ident;

	/*
	 * To be consistent with logging output, we label dynahash contexts with
	 * just the hash table name as with MemoryContextStatsPrint().
	 */
	if (ident && strcmp(name, "dynahash") == 0)
	{
		name = ident;
		ident = NULL;
	}

	if (name)
		values[0] = CStringGetTextDatum(name);
	else
		nulls[0] = true;

	if (ident)
	{
		int			idlen = strlen(ident);
		char		clipped_ident[MEMORY_CONTEXT_IDENT_DISPLAY_SIZE];

		/*
		 * Some identifiers such as SQL query string can be very long,
		 * truncate oversize identifiers.
		 */
		if (idlen >= MEMORY_CONTEXT_IDENT_DISPLAY_SIZE)
			idlen = pg_mbcliplen(ident, idlen, MEMORY_CONTEXT_IDENT_DISPLAY_SIZE - 1);

		memcpy(clipped_ident, ident, idlen);
		clipped_ident[idlen] = '\0';
		values[1] = CStringGetTextDatum(clipped_ident);
	}
	else
		nulls[1] = true;

	switch (context->type)
	{
		case T_AllocSetContext:
			type = "AllocSet";
			break;
		case T_GenerationContext:
			type = "Generation";
			break;
		case T_SlabContext:
			type = "Slab";
			break;
		case T_BumpContext:
			type = "Bump";
			break;
		default:
			type = "???";
			break;
	}

	values[2] = CStringGetTextDatum(type);
	values[3] = Int32GetDatum(list_length(path));	/* level */
	values[4] = int_list_to_array(path);
	values[5] = Int64GetDatum(stat.totalspace);
	values[6] = Int64GetDatum(stat.nblocks);
	values[7] = Int64GetDatum(stat.freespace);
	values[8] = Int64GetDatum(stat.freechunks);
	values[9] = Int64GetDatum(stat.totalspace - stat.freespace);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	list_free(path);
}

/*
 * pg_get_backend_memory_contexts
 *		SQL SRF showing backend memory context.
 */
Datum
pg_get_backend_memory_contexts(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			context_id;
	List	   *contexts;
	HASHCTL		ctl;
	HTAB	   *context_id_lookup;

	ctl.keysize = sizeof(MemoryContext);
	ctl.entrysize = sizeof(MemoryContextId);
	ctl.hcxt = CurrentMemoryContext;

	context_id_lookup = hash_create("pg_get_backend_memory_contexts",
									256,
									&ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	InitMaterializedSRF(fcinfo, 0);

	/*
	 * Here we use a non-recursive algorithm to visit all MemoryContexts
	 * starting with TopMemoryContext.  The reason we avoid using a recursive
	 * algorithm is because we want to assign the context_id breadth-first.
	 * I.e. all contexts at level 1 are assigned IDs before contexts at level
	 * 2.  Because contexts closer to TopMemoryContext are less likely to
	 * change, this makes the assigned context_id more stable.  Otherwise, if
	 * the first child of TopMemoryContext obtained an additional grandchild,
	 * the context_id for the second child of TopMemoryContext would change.
	 */
	contexts = list_make1(TopMemoryContext);

	/* TopMemoryContext will always have a context_id of 1 */
	context_id = 1;

	foreach_ptr(MemoryContextData, cur, contexts)
	{
		MemoryContextId *entry;
		bool		found;

		/*
		 * Record the context_id that we've assigned to each MemoryContext.
		 * PutMemoryContextsStatsTupleStore needs this to populate the "path"
		 * column with the parent context_ids.
		 */
		entry = (MemoryContextId *) hash_search(context_id_lookup, &cur,
												HASH_ENTER, &found);
		entry->context_id = context_id++;
		Assert(!found);

		PutMemoryContextsStatsTupleStore(rsinfo->setResult,
										 rsinfo->setDesc,
										 cur,
										 context_id_lookup);

		/*
		 * Append all children onto the contexts list so they're processed by
		 * subsequent iterations.
		 */
		for (MemoryContext c = cur->firstchild; c != NULL; c = c->nextchild)
			contexts = lappend(contexts, c);
	}

	hash_destroy(context_id_lookup);

	return (Datum) 0;
}

/*
 * pg_log_backend_memory_contexts
 *		Signal a backend or an auxiliary process to log its memory contexts.
 *
 * By default, only superusers are allowed to signal to log the memory
 * contexts because allowing any users to issue this request at an unbounded
 * rate would cause lots of log messages and which can lead to denial of
 * service. Additional roles can be permitted with GRANT.
 *
 * On receipt of this signal, a backend or an auxiliary process sets the flag
 * in the signal handler, which causes the next CHECK_FOR_INTERRUPTS()
 * or process-specific interrupt handler to log the memory contexts.
 */
Datum
pg_log_backend_memory_contexts(PG_FUNCTION_ARGS)
{
	int			pid = PG_GETARG_INT32(0);
	PGPROC	   *proc;
	ProcNumber	procNumber = INVALID_PROC_NUMBER;

	/*
	 * See if the process with given pid is a backend or an auxiliary process.
	 */
	proc = BackendPidGetProc(pid);
	if (proc == NULL)
		proc = AuxiliaryPidGetProc(pid);

	/*
	 * BackendPidGetProc() and AuxiliaryPidGetProc() return NULL if the pid
	 * isn't valid; but by the time we reach kill(), a process for which we
	 * get a valid proc here might have terminated on its own.  There's no way
	 * to acquire a lock on an arbitrary process to prevent that. But since
	 * this mechanism is usually used to debug a backend or an auxiliary
	 * process running and consuming lots of memory, that it might end on its
	 * own first and its memory contexts are not logged is not a problem.
	 */
	if (proc == NULL)
	{
		/*
		 * This is just a warning so a loop-through-resultset will not abort
		 * if one backend terminated on its own during the run.
		 */
		ereport(WARNING,
				(errmsg("PID %d is not a PostgreSQL server process", pid)));
		PG_RETURN_BOOL(false);
	}

	procNumber = GetNumberFromPGProc(proc);
	if (SendProcSignal(pid, PROCSIG_LOG_MEMORY_CONTEXT, procNumber) < 0)
	{
		/* Again, just a warning to allow loops */
		ereport(WARNING,
				(errmsg("could not send signal to process %d: %m", pid)));
		PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
}

Datum
pg_get_backends_memory_contexts(PG_FUNCTION_ARGS)
{
	int			pid = PG_GETARG_INT32(0);
	PGPROC	   *proc;
	ProcNumber	procNumber = INVALID_PROC_NUMBER;
	int			i;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContextParams *mem_stat = NULL;
	char		tmpfilename[MAXPGPATH];
	FILE	   *fp = NULL;

	InitMaterializedSRF(fcinfo, 0);

	/*
	 * See if the process with given pid is a backend or an auxiliary process.
	 */
	proc = BackendPidGetProc(pid);
	if (proc == NULL)
		proc = AuxiliaryPidGetProc(pid);

	/*
	 * BackendPidGetProc() and AuxiliaryPidGetProc() return NULL if the pid
	 * isn't valid; but by the time we reach kill(), a process for which we
	 * get a valid proc here might have terminated on its own.  There's no way
	 * to acquire a lock on an arbitrary process to prevent that. But since
	 * this mechanism is usually used to debug a backend or an auxiliary
	 * process running and consuming lots of memory, that it might end on its
	 * own first and its memory contexts are not logged is not a problem.
	 */
	if (proc == NULL)
	{
		/*
		 * This is just a warning so a loop-through-resultset will not abort
		 * if one backend terminated on its own during the run.
		 */
		ereport(WARNING,
				(errmsg("PID %d is not a PostgreSQL server process", pid)));
		PG_RETURN_BOOL(false);
	}

	procNumber = GetNumberFromPGProc(proc);
	if (SendProcSignal(pid, PROCSIG_GET_MEMORY_CONTEXT, procNumber) < 0)
	{
		/* Again, just a warning to allow loops */
		ereport(WARNING,
				(errmsg("could not send signal to process %d: %m", pid)));
		PG_RETURN_BOOL(false);
	}
	/*
	 * Wait for a backend to publish stats, indicated when in_use is set true
	 * by the backend
	 */
	while (1)
	{
		/*
		 * We expect to come out of sleep only when atleast one backend has
		 * published some memcontext information
		 */
		SpinLockAcquire(&memCtxState->mutex);

		/* Make sure that all the stats has been published
		 * and the information belongs to pid we requested information
		 * for, Otherwise loop back and wait for the correct backend to
		 * publish the information
		 */
		if (memCtxState->in_use == true && memCtxState->proc_id == pid)
			break;
		else
			SpinLockRelease(&memCtxState->mutex);

		if (ConditionVariableTimedSleep(&memCtxState->memctx_cv, 120000,
							   WAIT_EVENT_MEM_CTX_PUBLISH))
		{
			ereport(WARNING,
					(errmsg("Wait for %d process to publish stats timed out, try again", pid)));
			PG_RETURN_BOOL(false);
		}
	}
	/* Backend has finished publishing the stats, read them */
	for (i = 0; i < 29; i++)
	{
		ArrayType  *path_array;
		int			path_length;
		Datum		values[10];
		bool		nulls[10];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		if (memCtxState->memctx_infos[i].name)
			values[0] = CStringGetTextDatum(memCtxState->memctx_infos[i].name);
		else
			nulls[0] = true;
		if (memCtxState->memctx_infos[i].ident)
			values[1] = CStringGetTextDatum(memCtxState->memctx_infos[i].ident);
		else
			nulls[1] = true;

		values[2] = CStringGetTextDatum(memCtxState->memctx_infos[i].type);
		path_length = memCtxState->memctx_infos[i].path_length;
		path_array = construct_array_builtin(memCtxState->memctx_infos[i].path, path_length, INT4OID);
		values[3] = PointerGetDatum(path_array);
		values[4] = Int64GetDatum(memCtxState->memctx_infos[i].totalspace);
		values[5] = Int64GetDatum(memCtxState->memctx_infos[i].nblocks);
		values[6] = Int64GetDatum(memCtxState->memctx_infos[i].freespace);
		values[7] = Int64GetDatum(memCtxState->memctx_infos[i].freechunks);
		values[8] = Int64GetDatum(memCtxState->memctx_infos[i].totalspace - memCtxState->memctx_infos[i].freespace);
		values[9] = Int32GetDatum(memCtxState->proc_id);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	/* Compute name for temp mem stat file */
	snprintf(tmpfilename, MAXPGPATH, "%s/%s.memstats.%d",
			 PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX,
			 memCtxState->proc_id);
	SpinLockRelease(&memCtxState->mutex);

	ConditionVariableCancelSleep();

	/* Open file */
	fp = AllocateFile(tmpfilename, PG_BINARY_R);
	if (!fp)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not read from the file")));
		SpinLockAcquire(&memCtxState->mutex);
		memCtxState->in_use = false;
		SpinLockRelease(&memCtxState->mutex);
		PG_RETURN_BOOL(false);
	}
	mem_stat = palloc0(sizeof(MemoryContextParams));
	while (!feof(fp))
	{
		int			path_length;
		ArrayType  *path_array;
		Datum		values[10];
		bool		nulls[10];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		/* Read stats from file */
		fread(mem_stat, sizeof(MemoryContextParams), 1, fp);
		if (ferror(fp))
		{
			elog(WARNING, "File read error");
			break;
		}

		path_length = mem_stat->path_length;
		if (mem_stat->name)
			values[0] = CStringGetTextDatum(mem_stat->name);
		else
			nulls[0] = true;

		if (mem_stat->ident)
			values[1] = CStringGetTextDatum(mem_stat->ident);
		else
			nulls[1] = true;

		values[2] = CStringGetTextDatum(mem_stat->type);

		path_array = construct_array_builtin(mem_stat->path, path_length, INT4OID);
		values[3] = PointerGetDatum(path_array);
		values[4] = Int64GetDatum(mem_stat->totalspace);
		values[5] = Int64GetDatum(mem_stat->nblocks);
		values[6] = Int64GetDatum(mem_stat->freespace);
		values[7] = Int64GetDatum(mem_stat->freechunks);
		values[8] = Int64GetDatum(mem_stat->totalspace - mem_stat->freespace);
		SpinLockAcquire(&memCtxState->mutex);
		values[9] = Int32GetDatum(memCtxState->proc_id);
		SpinLockRelease(&memCtxState->mutex);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	pfree(mem_stat);
	FreeFile(fp);
	/* Delete the temp file that stores memory stats */
	unlink(tmpfilename);
	SpinLockAcquire(&memCtxState->mutex);
	memCtxState->in_use = false;
	SpinLockRelease(&memCtxState->mutex);

	return (Datum) 0;
}

static Size
MemCtxShmemSize(void)
{
	Size		size;

	size = offsetof(MemoryContextState, memctx_infos);
	size = add_size(size, mul_size(30, sizeof(MemoryContextInfo)));
	return size;
}

void
MemCtxShmemInit(void)
{
	bool		found;

	memCtxState = (MemoryContextState *) ShmemInitStruct("MemoryContextState",
														 MemCtxShmemSize(),
														 &found);
	if (!found)
	{
		ConditionVariableInit(&memCtxState->memctx_cv);
		SpinLockInit(&memCtxState->mutex);
		memCtxState->in_use = false;
		memset(&memCtxState->memctx_infos, 0, 30 * sizeof(MemoryContextInfo));
	}
}
