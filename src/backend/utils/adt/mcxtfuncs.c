/*-------------------------------------------------------------------------
 *
 * mcxtfuncs.c
 *	  Functions to show backend memory context.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/mcxtfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/twophase.h"
#include "catalog/pg_authid_d.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/dsm_registry.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/wait_event_types.h"

#define CLIENT_KEY_SIZE 64

static LWLock *client_keys_lock = NULL;
static int *client_keys = NULL;
static dshash_table *MemoryStatsDsHash = NULL;
static dsa_area *MemoryStatsDsaArea = NULL;

static void memstats_dsa_cleanup(MemoryStatsDSHashEntry *entry);
static const char *ContextTypeToString(NodeTag type);
static void PublishMemoryContext(MemoryStatsEntry *memcxt_info,
								 int curr_id, MemoryContext context,
								 List *path,
								 MemoryContextCounters stat,
								 int num_contexts, int max_levels);
static List *compute_context_path(MemoryContext c, HTAB *context_id_lookup);
static void end_memorycontext_reporting(MemoryStatsDSHashEntry *entry, MemoryContext oldcontext,
										HTAB *context_id_lookup);

/* ----------
 * The max bytes for showing identifiers of MemoryContext.
 * ----------
 */
#define MEMORY_CONTEXT_IDENT_DISPLAY_SIZE	1024

/*
 * MemoryContextId
 *		Used for storage of transient identifiers for
 *		pg_get_backend_memory_contexts.
 */
typedef struct MemoryContextId
{
	MemoryContext context;
	int			context_id;
} MemoryContextId;

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
		MemoryStatsContextId *entry;
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

	type = ContextTypeToString(context->type);

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
 * ContextTypeToString
 *		Returns a textual representation of a context type
 *
 * This should cover the same types as MemoryContextIsValid.
 */
const char *
ContextTypeToString(NodeTag type)
{
	const char *context_type;

	switch (type)
	{
		case T_AllocSetContext:
			context_type = "AllocSet";
			break;
		case T_GenerationContext:
			context_type = "Generation";
			break;
		case T_SlabContext:
			context_type = "Slab";
			break;
		case T_BumpContext:
			context_type = "Bump";
			break;
		default:
			context_type = "???";
			break;
	}
	return context_type;
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
	ctl.entrysize = sizeof(MemoryStatsContextId);
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
		MemoryStatsContextId *entry;
		bool		found;

		/*
		 * Record the context_id that we've assigned to each MemoryContext.
		 * PutMemoryContextsStatsTupleStore needs this to populate the "path"
		 * column with the parent context_ids.
		 */
		entry = (MemoryStatsContextId *) hash_search(context_id_lookup, &cur,
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

/*
 * pg_get_process_memory_contexts
 *		Signal a backend or an auxiliary process to send its memory contexts,
 *		wait for the results and display them.
 *
 * By default, only superusers or users with ROLE_PG_READ_ALL_STATS are allowed
 * to signal a process to return the memory contexts. This is because allowing
 * any users to issue this request at an unbounded rate would cause lots of
 * requests to be sent, which can lead to denial of service. Additional roles
 * can be permitted with GRANT.
 *
 * On receipt of this signal, a backend or an auxiliary process sets the flag
 * in the signal handler, which causes the next CHECK_FOR_INTERRUPTS()
 * or process-specific interrupt handler to copy the memory context details
 * to a dynamic shared memory space.
 *
 * We have defined a limit on DSA memory that could be allocated per process -
 * if the process has more memory contexts than what can fit in the allocated
 * size, the excess contexts are summarized and represented as cumulative total
 * at the end of the buffer.
 *
 * After sending the signal, wait on a condition variable. The publishing
 * backend, after copying the data to shared memory, sends signal on that
 * condition variable. There is one condition variable per client process.
 * Once the condition variable is signalled, check if the latest memory context
 * information is available and display.
 *
 * If the publishing backend does not respond before the condition variable
 * times out, which is set to MEMSTATS_WAIT_TIMEOUT, retry given that there is
 * time left within the timeout specified by the user, before giving up and
 * returning previously published statistics, if any. If no previous statistics
 * exist, return NULL.
 */
#define MEMSTATS_WAIT_TIMEOUT 100
Datum
pg_get_process_memory_contexts(PG_FUNCTION_ARGS)
{
	int			pid = PG_GETARG_INT32(0);
	bool		summary = PG_GETARG_BOOL(1);
	double		timeout = PG_GETARG_FLOAT8(2);
	PGPROC	   *proc;
	ProcNumber	procNumber = INVALID_PROC_NUMBER;
	bool		proc_is_aux = false;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryStatsEntry *memcxt_info;
	MemoryStatsDSHashEntry *entry;
	bool		found;
	char		key[CLIENT_KEY_SIZE];

	/*
	 * See if the process with given pid is a backend or an auxiliary process
	 * and remember the type for when we requery the process later.
	 */
	proc = BackendPidGetProc(pid);
	if (proc == NULL)
	{
		proc = AuxiliaryPidGetProc(pid);
		proc_is_aux = true;
	}

	/*
	 * BackendPidGetProc() and AuxiliaryPidGetProc() return NULL if the pid
	 * isn't valid; this is however not a problem and leave with a WARNING.
	 * See comment in pg_log_backend_memory_contexts for a discussion on this.
	 */
	if (proc == NULL)
	{
		/*
		 * This is just a warning so a loop-through-resultset will not abort
		 * if one backend terminated on its own during the run.
		 */
		ereport(WARNING,
				errmsg("PID %d is not a PostgreSQL server process", pid));
		PG_RETURN_NULL();
	}

	InitMaterializedSRF(fcinfo, 0);

	procNumber = GetNumberFromPGProc(proc);

	/*
	 * Create a DSA to allocate memory for copying memory contexts statistics.
	 * Allocate the memory in the DSA and send dsa pointer to the server
	 * process for	storing the context statistics. If number of contexts
	 * exceed a predefined limit(1MB), a cumulative total is stored for such
	 * contexts.
	 *
	 * The DSA is created once for the lifetime of the server, and only
	 * attached in subsequent calls.
	 */
	if (MemoryStatsDsaArea == NULL)
		MemoryStatsDsaArea = GetNamedDSA("memory_context_statistics_dsa", &found);

	/*
	 * The dsa pointers containing statistics for each client are stored in a
	 * dshash table. In addition to dsa pointer, each entry in this table also
	 * contains information about the statistics, condition variable for
	 * signalling between client and the server and miscellaneous data
	 * specific to a request. There is one entry per client request in the
	 * hash table.
	 */
	if (MemoryStatsDsHash == NULL)
		MemoryStatsDsHash = GetNamedDSHash("memory_context_statistics_dshash", &memctx_dsh_params, &found);

	snprintf(key, sizeof(key), "%d", MyProcNumber);

	/*
	 * Check if the publishing process slot is empty and store this clients
	 * key i.e its procNumber. This informs the publishing process that it is
	 * supposed to write statistics in the hash entry corresponding to this
	 * client.
	 */
	LWLockAcquire(client_keys_lock, LW_EXCLUSIVE);
	if (client_keys[procNumber] == -1)
		client_keys[procNumber] = MyProcNumber;
	else
	{
		ereport(WARNING,
				errmsg("server process %d is processing previous request", pid));
		LWLockRelease(client_keys_lock);
		PG_RETURN_NULL();
	}
	LWLockRelease(client_keys_lock);

	elog(LOG, "Client: Server pid %d, server proc number %d", pid, procNumber);
	/*
	 * Insert an entry for this client in DSHASH table the first time this
	 * function is called. This entry is deleted when the process exits in
	 * before_shmem_exit call.
	 *
	 * dshash_find_or_insert locks the entry to prevent the publisher from
	 * reading before client has updated the entry.
	 */
	entry = dshash_find_or_insert(MemoryStatsDsHash, key, &found);
	if (!found)
		ConditionVariableInit(&entry->memcxt_cv);

	/*
	 * Allocate 1MB of memory for the backend to publish its statistics on
	 * every call to this function. The memory is freed at the end of the
	 * function.
	 */
	entry->memstats_dsa_pointer =
		dsa_allocate0(MemoryStatsDsaArea, MEMORY_CONTEXT_REPORT_MAX_PER_BACKEND);
	entry->summary = summary;
	entry->server_id = pid;
	dshash_release_lock(MemoryStatsDsHash, entry);
	
	PG_TRY();
	{
		/*
	 	 * Send a signal to a PostgreSQL process, informing it we want it to
	 	 * produce information about its memory contexts.
	 	 */
		if (SendProcSignal(pid, PROCSIG_GET_MEMORY_CONTEXT, procNumber) < 0)
		{
			ereport(WARNING,
				errmsg("could not send signal to process %d: %m", pid));
			PG_RETURN_NULL();
		}

		while (1)
		{
			entry = dshash_find_or_insert(MemoryStatsDsHash, key, &found);
			Assert(found);

			/*
		 	 * We expect to come out of sleep when the requested process has
		 	 * finished publishing the statistics, verified using a boolean
		 	 * stats_written.
		 	 */
			if (entry->stats_written)
				break;

			dshash_release_lock(MemoryStatsDsHash, entry);

			/*
		 	 * Recheck the state of the backend before sleeping on the condition
		 	 * variable to ensure the process is still alive.  Only check the
		 	 * relevant process type based on the earlier PID check.
		 	 */
			if (proc_is_aux)
				proc = AuxiliaryPidGetProc(pid);
			else
				proc = BackendPidGetProc(pid);

			/*
		 	 * The process ending during memory context processing is not an
		 	 * error.
		 	 */
			if (proc == NULL)
			{
				ereport(WARNING,
						errmsg("PID %d is no longer a PostgreSQL server process",
							   pid));
				memstats_dsa_cleanup(entry);
				PG_RETURN_NULL();
			}


			/*
		 	 * Wait for the timeout as defined by the user. If no statistics are
		 	 * available within the allowed time then return NULL. The timer is
		 	 * defined in milliseconds since that's what the condition variable
		 	 * sleep uses.
		 	 */
			if (ConditionVariableTimedSleep(&entry->memcxt_cv,
											(timeout * 1000), WAIT_EVENT_MEM_CXT_PUBLISH))
			{
				/* Timeout has expired, return NULL */
				memstats_dsa_cleanup(entry);
				PG_RETURN_NULL();
			}
		}

		/*
	 	 * Backend has finished publishing the stats, project them.
	 	 */
		memcxt_info = (MemoryStatsEntry *)
			dsa_get_address(MemoryStatsDsaArea, entry->memstats_dsa_pointer);

#define PG_GET_PROCESS_MEMORY_CONTEXTS_COLS	11
		for (int i = 0; i < entry->total_stats; i++)
		{
			ArrayType  *path_array;
			int			path_length;
			Datum		values[PG_GET_PROCESS_MEMORY_CONTEXTS_COLS];
			bool		nulls[PG_GET_PROCESS_MEMORY_CONTEXTS_COLS];
			Datum	   *path_datum = NULL;

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));

			if (memcxt_info[i].name[0] != '\0')
			{
				values[0] = CStringGetTextDatum(memcxt_info[i].name);
			}
			else
				nulls[0] = true;

			if (memcxt_info[i].ident[0] != '\0')
			{
				values[1] = CStringGetTextDatum(memcxt_info[i].ident);
			}
			else
				nulls[1] = true;

			values[2] = CStringGetTextDatum(ContextTypeToString(memcxt_info[i].type));

			path_length = memcxt_info[i].path_length;
			path_datum = (Datum *) palloc(path_length * sizeof(Datum));
			if (memcxt_info[i].path[0] != 0)
			{
				for (int j = 0; j < path_length; j++)
					path_datum[j] = Int32GetDatum(memcxt_info[i].path[j]);
				path_array = construct_array_builtin(path_datum, path_length, INT4OID);
				values[3] = PointerGetDatum(path_array);
			}
			else
				nulls[3] = true;

			values[4] = Int32GetDatum(memcxt_info[i].levels);
			values[5] = Int64GetDatum(memcxt_info[i].totalspace);
			values[6] = Int64GetDatum(memcxt_info[i].nblocks);
			values[7] = Int64GetDatum(memcxt_info[i].freespace);
			values[8] = Int64GetDatum(memcxt_info[i].freechunks);
			values[9] = Int64GetDatum(memcxt_info[i].totalspace -
									  memcxt_info[i].freespace);
			values[10] = Int32GetDatum(memcxt_info[i].num_agg_stats);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								 values, nulls);
		}
		memstats_dsa_cleanup(entry);
		dshash_release_lock(MemoryStatsDsHash, entry);

		ConditionVariableCancelSleep();

	}
	PG_CATCH();
	{
		memstats_dsa_cleanup(entry);
	}
	PG_END_TRY();
	
	PG_RETURN_NULL();
}

static void
memstats_dsa_cleanup(MemoryStatsDSHashEntry *entry)
{
	Assert(MemoryStatsDsaArea != NULL);
	dsa_free(MemoryStatsDsaArea, entry->memstats_dsa_pointer);
	entry->memstats_dsa_pointer = InvalidDsaPointer;
	entry->stats_written = false;
	entry->server_id = 0;
}
void
MemoryContextKeysShmemInit(void)
{
	bool		found;

	client_keys = (int *)
		ShmemInitStruct("MemoryContextKeys",
						MemoryContextKeysShmemSize() + sizeof(LWLockPadded), &found);
	client_keys_lock = (LWLock *) ((char *) client_keys + MemoryContextKeysShmemSize());

	if (!found)
	{
		MemSet(client_keys, -1, MemoryContextKeysShmemSize());
		LWLockInitialize(client_keys_lock, LWTRANCHE_MEMORY_CONTEXT_KEYS);
	}
}

Size
MemoryContextKeysShmemSize(void)
{
	Size		sz = 0;
	Size		TotalProcs = 0;

	TotalProcs = add_size(TotalProcs, NUM_AUXILIARY_PROCS);
	TotalProcs = add_size(TotalProcs, MaxBackends);
	sz = add_size(sz, mul_size(TotalProcs, sizeof(int)));

	return sz;
}

/*
 * HandleGetMemoryContextInterrupt
 *		Handle receipt of an interrupt indicating a request to publish memory
 *		contexts statistics.
 *
 * All the actual work is deferred to ProcessGetMemoryContextInterrupt() as
 * this cannot be performed in a signal handler.
 */
void
HandleGetMemoryContextInterrupt(void)
{
	InterruptPending = true;
	PublishMemoryContextPending = true;
	/* latch will be set by procsignal_sigusr1_handler */
}

/*
 * ProcessGetMemoryContextInterrupt
 *		Generate information about memory contexts used by the process.
 *
 * Performs a breadth first search on the memory context tree, thus parents
 * statistics are reported before their children in the monitoring function
 * output.
 *
 * Statistics for all the processes are shared via the same dynamic shared
 * area. Individual statistics are tracked independently in
 * per-process DSA pointers. These pointers are stored in a dshash table with
 * key as requesting clients ProcNumber.
 *
 * We calculate maximum number of context's statistics that can be displayed
 * using a pre-determined limit for memory available per process for this
 * utility and maximum size of statistics for each context.  The remaining
 * context statistics if any are captured as a cumulative total at the end of
 * individual context's statistics.
 *
 * If summary is true, we capture the level 1 and level 2 contexts
 * statistics.  For that we traverse the memory context tree recursively in
 * depth first search manner to cover all the children of a parent context, to
 * be able to display a cumulative total of memory consumption by a parent at
 * level 2 and all its children.
 */
void
ProcessGetMemoryContextInterrupt(void)
{
	List	   *contexts;
	HASHCTL		ctl;
	HTAB	   *context_id_lookup;
	int			context_id = 0;
	MemoryStatsEntry *meminfo;
	bool		summary = false;
	MemoryContextCounters stat;
	int			num_individual_stats = 0;
	bool		found;
	MemoryStatsDSHashEntry *entry;
	char		key[CLIENT_KEY_SIZE];
	int			clientProcNumber;
	MemoryContext memstats_ctx = NULL;
	MemoryContext oldcontext = NULL;

	PublishMemoryContextPending = false;

	/*
	 * Create a new memory context which is not a part of TopMemoryContext
	 * tree. This context is used to allocate all memory in this function.
	 * This helps in keeping the memory allocation in this function to report
	 * memory consumption statistics separate. So that it does not affect the
	 * output of this function.
	 */
	memstats_ctx = AllocSetContextCreate((MemoryContext) NULL, "publish_memory_context_statistics",
										 ALLOCSET_SMALL_SIZES);
	oldcontext = MemoryContextSwitchTo(memstats_ctx);

	/*
	 * The hash table is used for constructing "path" column of the view,
	 * similar to its local backend counterpart.
	 */
	ctl.keysize = sizeof(MemoryContext);
	ctl.entrysize = sizeof(MemoryStatsContextId);
	ctl.hcxt = CurrentMemoryContext;

	context_id_lookup = hash_create("pg_get_remote_backend_memory_contexts",
									256,
									&ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* List of contexts to process in the next round - start at the top. */
	contexts = list_make1(TopMemoryContext);

	/*
	 * If DSA exists, created by another process requesting statistics, attach
	 * to it. We expect the client process to create required DSA and Dshash
	 * table.
	 */
	if (MemoryStatsDsaArea == NULL)
		MemoryStatsDsaArea = GetNamedDSA("memory_context_statistics_dsa", &found);

	if (MemoryStatsDsHash == NULL)
		MemoryStatsDsHash = GetNamedDSHash("memory_context_statistics_dshash", &memctx_dsh_params, &found);

	/* Retreive the client key for publishing statistics */
	LWLockAcquire(client_keys_lock, LW_SHARED);
	Assert(client_keys[MyProcNumber] != -1);
	clientProcNumber = client_keys[MyProcNumber];
	LWLockRelease(client_keys_lock);

	snprintf(key, CLIENT_KEY_SIZE, "%d", clientProcNumber);
	elog(LOG, "Server pid %d, Server proc no %d", MyProcPid, MyProcNumber);
	
	/*
	 * The entry lock is held by dshash_find_or_insert to protect writes to
	 * process specific memory. Two different processes publishing statistics
	 * do not block each other.
	 */
	entry = dshash_find_or_insert(MemoryStatsDsHash, key, &found);

	/* Entry has been deleted due to client process exit */
	if (!found)
	{
		end_memorycontext_reporting(entry, oldcontext, context_id_lookup);
		return;
	}

	/* The client has timed out waiting for us to write statistics */
	if (entry->server_id != MyProcPid)
	{
		end_memorycontext_reporting(entry, oldcontext, context_id_lookup);
		return;
	}

	summary = entry->summary;

	/* Should be allocated by a client backend that is requesting statistics */
	Assert(entry->memstats_dsa_pointer != InvalidDsaPointer);
	meminfo = (MemoryStatsEntry *)
		dsa_get_address(MemoryStatsDsaArea, entry->memstats_dsa_pointer);

	if (summary)
	{
		int			cxt_id = 0;
		List	   *path = NIL;
		MemoryStatsContextId *contextid_entry;

		/* Copy TopMemoryContext statistics to DSA */
		memset(&stat, 0, sizeof(stat));
		(*TopMemoryContext->methods->stats) (TopMemoryContext, NULL, NULL,
											 &stat, true);
		path = lcons_int(1, path);
		PublishMemoryContext(meminfo, cxt_id, TopMemoryContext, path, stat,
							 1, 100);

		contextid_entry = (MemoryStatsContextId *) hash_search(context_id_lookup, &TopMemoryContext,
															   HASH_ENTER, &found);
		Assert(!found);

		/*
		 * context id starts with 1
		 */
		contextid_entry->context_id = cxt_id + 1;

		/*
		 * Copy statistics for each of TopMemoryContexts children.  This
		 * includes statistics of at most 100 children per node, with each
		 * child node limited to a depth of 100 in its subtree.
		 */
		for (MemoryContext c = TopMemoryContext->firstchild; c != NULL;
			 c = c->nextchild)
		{
			MemoryContextCounters grand_totals;
			int			num_contexts = 0;

			path = NIL;
			memset(&grand_totals, 0, sizeof(grand_totals));

			cxt_id++;
			contextid_entry = (MemoryStatsContextId *) hash_search(context_id_lookup, &c,
																   HASH_ENTER, &found);
			Assert(!found);
			contextid_entry->context_id = cxt_id + 1;

			MemoryContextStatsInternal(c, 1, 100, 100, &grand_totals,
									   PRINT_STATS_NONE, &num_contexts);

			path = compute_context_path(c, context_id_lookup);

			PublishMemoryContext(meminfo, cxt_id, c, path,
								 grand_totals, num_contexts, 100);
		}
		entry->total_stats = cxt_id + 1;

		/* Notify waiting backends and return */
		end_memorycontext_reporting(entry, oldcontext, context_id_lookup);
		return;
	}
	foreach_ptr(MemoryContextData, cur, contexts)
	{
		List	   *path = NIL;
		MemoryStatsContextId *contextid_entry;

		contextid_entry = (MemoryStatsContextId *) hash_search(context_id_lookup, &cur,
															   HASH_ENTER, &found);
		Assert(!found);

		/*
		 * context id starts with 1
		 */
		contextid_entry->context_id = context_id + 1;

		/*
		 * Figure out the transient context_id of this context and each of its
		 * ancestors, to compute a path for this context.
		 */
		path = compute_context_path(cur, context_id_lookup);

		/* Examine the context stats */
		memset(&stat, 0, sizeof(stat));
		(*cur->methods->stats) (cur, NULL, NULL, &stat, true);

		/* Account for saving one statistics slot for cumulative reporting */
		if (context_id < (MAX_MEMORY_CONTEXT_STATS_NUM - 1))
		{
			/* Copy statistics to DSA memory */
			PublishMemoryContext(meminfo, context_id, cur, path, stat, 1, 100);
		}
		else
		{
			meminfo[MAX_MEMORY_CONTEXT_STATS_NUM - 1].totalspace += stat.totalspace;
			meminfo[MAX_MEMORY_CONTEXT_STATS_NUM - 1].nblocks += stat.nblocks;
			meminfo[MAX_MEMORY_CONTEXT_STATS_NUM - 1].freespace += stat.freespace;
			meminfo[MAX_MEMORY_CONTEXT_STATS_NUM - 1].freechunks += stat.freechunks;
		}

		/*
		 * DSA max limit per process is reached, write aggregate of the
		 * remaining statistics.
		 *
		 * We can store contexts from 0 to max_stats - 1. When context_id is
		 * greater than max_stats, we stop reporting individual statistics
		 * when context_id equals max_stats - 2. As we use max_stats - 1 array
		 * slot for reporting cumulative statistics or "Remaining Totals".
		 */
		if (context_id == (MAX_MEMORY_CONTEXT_STATS_NUM - 2))
		{
			int			namelen = strlen("Remaining Totals");

			num_individual_stats = context_id + 1;
			strlcpy(meminfo[MAX_MEMORY_CONTEXT_STATS_NUM - 1].name, "Remaining Totals", namelen + 1);
			meminfo[MAX_MEMORY_CONTEXT_STATS_NUM - 1].ident[0] = '\0';
			meminfo[MAX_MEMORY_CONTEXT_STATS_NUM - 1].path[0] = 0;
			meminfo[MAX_MEMORY_CONTEXT_STATS_NUM - 1].type = 0;
		}
		context_id++;

		for (MemoryContext c = cur->firstchild; c != NULL; c = c->nextchild)
			contexts = lappend(contexts, c);
	}

	/*
	 * Statistics are not aggregated, i.e individual statistics reported when
	 * context_id <= max_stats.
	 */
	if (context_id <= MAX_MEMORY_CONTEXT_STATS_NUM)
	{
		entry->total_stats = context_id;
		meminfo[MAX_MEMORY_CONTEXT_STATS_NUM - 1].num_agg_stats = 1;
	}
	/* Report number of aggregated memory contexts */
	else
	{
		meminfo[MAX_MEMORY_CONTEXT_STATS_NUM - 1].num_agg_stats = context_id -
			num_individual_stats;

		/*
		 * Total stats equals num_individual_stats + 1 record for cumulative
		 * statistics.
		 */
		entry->total_stats = num_individual_stats + 1;
	}
	entry->stats_written = true;
	/* Notify waiting backends and return */
	end_memorycontext_reporting(entry, oldcontext, context_id_lookup);
}

/*
 * Update timestamp and signal all the waiting client backends after copying
 * all the statistics.
 */
static void
end_memorycontext_reporting(MemoryStatsDSHashEntry *entry, MemoryContext oldcontext, HTAB *context_id_lookup)
{
	MemoryContext curr_ctx = CurrentMemoryContext;

	dshash_release_lock(MemoryStatsDsHash, entry);
	ConditionVariableBroadcast(&entry->memcxt_cv);

	/*
	 * Empty this processes slot, so other clients can request memory
	 * statistics
	 */
	LWLockAcquire(client_keys_lock, LW_EXCLUSIVE);
	client_keys[MyProcNumber] = -1;
	LWLockRelease(client_keys_lock);

	hash_destroy(context_id_lookup);
	MemoryContextSwitchTo(oldcontext);
	MemoryContextReset(curr_ctx);
}

/*
 * compute_context_path
 *
 * Append the transient context_id of this context and each of its ancestors
 * to a list, in order to compute a path.
 */
static List *
compute_context_path(MemoryContext c, HTAB *context_id_lookup)
{
	bool		found;
	List	   *path = NIL;
	MemoryContext cur_context;

	for (cur_context = c; cur_context != NULL; cur_context = cur_context->parent)
	{
		MemoryStatsContextId *cur_entry;

		cur_entry = hash_search(context_id_lookup, &cur_context, HASH_FIND, &found);

		if (!found)
			elog(ERROR, "hash table corrupted, can't construct path value");

		path = lcons_int(cur_entry->context_id, path);
	}

	return path;
}

/*
 * PublishMemoryContext
 *
 * Copy the memory context statistics of a single context to a DSA memory
 */
static void
PublishMemoryContext(MemoryStatsEntry *memcxt_info, int curr_id,
					 MemoryContext context, List *path,
					 MemoryContextCounters stat, int num_contexts, int max_levels)
{
	const char *ident = context->ident;
	const char *name = context->name;

	/*
	 * To be consistent with logging output, we label dynahash contexts with
	 * just the hash table name as with MemoryContextStatsPrint().
	 */
	if (context->ident && strncmp(context->name, "dynahash", 8) == 0)
	{
		name = context->ident;
		ident = NULL;
	}

	if (name != NULL)
	{
		int			namelen = strlen(name);

		if (strlen(name) >= MEMORY_CONTEXT_IDENT_SHMEM_SIZE)
			namelen = pg_mbcliplen(name, namelen,
								   MEMORY_CONTEXT_IDENT_SHMEM_SIZE - 1);

		strlcpy(memcxt_info[curr_id].name, name, namelen + 1);
	}
	else
		/* Clearing the array */
		memcxt_info[curr_id].name[0] = '\0';

	/* Trim and copy the identifier if it is not set to NULL */
	if (ident != NULL)
	{
		int			idlen = strlen(context->ident);

		/*
		 * Some identifiers such as SQL query string can be very long,
		 * truncate oversize identifiers.
		 */
		if (idlen >= MEMORY_CONTEXT_IDENT_SHMEM_SIZE)
			idlen = pg_mbcliplen(ident, idlen,
								 MEMORY_CONTEXT_IDENT_SHMEM_SIZE - 1);

		strlcpy(memcxt_info[curr_id].ident, ident, idlen + 1);
	}
	else
		memcxt_info[curr_id].ident[0] = '\0';

	/* Allocate DSA memory for storing path information */
	if (path == NIL)
		memcxt_info[curr_id].path[0] = 0;
	else
	{
		int			levels = Min(list_length(path), max_levels);

		memcxt_info[curr_id].path_length = levels;
		memcxt_info[curr_id].levels = list_length(path);

		foreach_int(i, path)
		{
			memcxt_info[curr_id].path[foreach_current_index(i)] = i;
			if (--levels == 0)
				break;
		}
	}
	memcxt_info[curr_id].type = context->type;
	memcxt_info[curr_id].totalspace = stat.totalspace;
	memcxt_info[curr_id].nblocks = stat.nblocks;
	memcxt_info[curr_id].freespace = stat.freespace;
	memcxt_info[curr_id].freechunks = stat.freechunks;
	memcxt_info[curr_id].num_agg_stats = num_contexts;
}

void
AtProcExit_memstats_cleanup(int code, Datum arg)
{
	int			idx = MyProcNumber;
	MemoryStatsDSHashEntry *entry;
	char		key[CLIENT_KEY_SIZE];
	bool		found;

	if (MemoryStatsDsHash != NULL)
	{
		snprintf(key, CLIENT_KEY_SIZE, "%d", idx);
		entry = dshash_find_or_insert(MemoryStatsDsHash, key, &found);

		if (found)
		{
			if (MemoryStatsDsaArea != NULL &&
				DsaPointerIsValid(entry->memstats_dsa_pointer))
				dsa_free(MemoryStatsDsaArea, entry->memstats_dsa_pointer);
		}
		dshash_delete_entry(MemoryStatsDsHash, entry);
	}
	LWLockAcquire(client_keys_lock, LW_EXCLUSIVE);
	client_keys[idx] = -1;
	LWLockRelease(client_keys_lock);
}
