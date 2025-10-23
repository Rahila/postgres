/*
 * -------------------------------------------------------------------------
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *             src/test/modules/test_memcontext_reporting/test_memcontext_reporting.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/injection_point.h"
#include "funcapi.h"
#include "utils/injection_point.h"
#include "storage/dsm_registry.h"

PG_MODULE_MAGIC;

extern PGDLLEXPORT void crash(const char *name, const void *private_data, void *arg);

void
crash(const char *name, const void *private_data, void *arg)
{
	abort();
}

/*
 * memcontext_crash_client
 *
 * Ensure that the client process aborts in between memory context
 * reporting.
 */
PG_FUNCTION_INFO_V1(memcontext_crash_client);
Datum
memcontext_crash_client(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	InjectionPointAttach("memcontext-client-crash",
						 "test_memcontext_reporting", "crash", NULL, 0);

#else
	elog(ERROR,
		 "test is not working as intended when injection points are disabled");
#endif
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(memcontext_detach_client);
Datum
memcontext_detach_client(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	InjectionPointDetach("memcontext-client-crash");

#else
	elog(ERROR,
		 "test is not working as intended when injection points are disabled");
#endif
	PG_RETURN_VOID();
}

/*
 * memcontext_crash_server
 *
 * Ensure that the server process crashes in between memory context
 * reporting.
 */
PG_FUNCTION_INFO_V1(memcontext_crash_server);
Datum
memcontext_crash_server(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	InjectionPointAttach("memcontext-server-crash",
						 "test_memcontext_reporting", "crash", NULL, 0);

#else
	elog(ERROR,
		 "test is not working as intended when injection points are disabled");
#endif
	PG_RETURN_VOID();
}

/*
 * memcontext_detach_server
 *
 * Detach the injection point which crashes the server
 * reporting.
 */
PG_FUNCTION_INFO_V1(memcontext_detach_server);
Datum
memcontext_detach_server(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	InjectionPointDetach("memcontext-server-crash");

#else
	elog(ERROR,
		 "test is not working as intended when injection points are disabled");
#endif
	PG_RETURN_VOID();
}

/*
 * dsa_dump_sql
 */
PG_FUNCTION_INFO_V1(dsa_dump_sql);
Datum
dsa_dump_sql(PG_FUNCTION_ARGS)
{
	bool		found;
	size_t		tot_size;
	dsa_area   *memstats_dsa_area;

	memstats_dsa_area = pg_get_memstats_dsa_area();

	if (memstats_dsa_area == NULL)
		memstats_dsa_area = GetNamedDSA("memory_context_statistics_dsa", &found);

	tot_size = dsa_get_total_size(memstats_dsa_area);
	dsa_detach(memstats_dsa_area);
	PG_RETURN_INT64(tot_size);
}
