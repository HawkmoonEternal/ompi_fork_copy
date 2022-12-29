/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2018      Triad National Security, LLC.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OMPI_INSTANCE_COLLECTIVE_H)
#define OMPI_INSTANCE_COLLECTIVE_H

#include "opal/class/opal_object.h"
#include "opal/class/opal_hash_table.h"
#include "opal/util/info_subscriber.h"
#include "ompi/errhandler/errhandler.h"
#include "opal/mca/threads/mutex.h"
#include "ompi/communicator/comm_request.h"

#include "mpi.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/info/info.h"
#include "ompi/proc/proc.h"


#define OMPI_NOTIFY_COLLECTIVE -404



typedef enum{
    OMPI_FUNC_NONE,
    OMPI_FUNC_PMIX_QUERY,
    OMPI_FUNC_PMIX_LOOKUP
} ompi_function_type_t;

typedef enum{
    OMPI_PARAMS_NONE,
    OMPI_PARAMS_INFO,
    OMPI_PARAMS_QUERY,
    OMPI_PARAMS_PDATA
} ompi_parameters_type_t;

typedef enum{
    OMPI_RESULTS_NONE,
    OMPI_RESULTS_INFO
} ompi_results_type_t;

typedef enum{
    OMPI_CBFUNC_INFO
} ompi_cbfunc_type_t;

typedef struct{
    pmix_info_t *info;
    size_t ninfo;
}ompi_info_parameters_t;

typedef struct{
    pmix_query_t *query;
    size_t nqueries;
}ompi_query_parameters_t;

typedef struct{
    pmix_pdata_t *pdata;
    pmix_info_t *info;
    size_t npdata;
    size_t ninfo;
}ompi_pdata_parameters_t;

typedef struct{
    pmix_status_t status;
    pmix_info_t *info;
    size_t ninfo;
}ompi_info_results_t;

typedef ompi_info_results_t ompi_non_results_t;

typedef union{
    ompi_info_parameters_t info_params;
    ompi_query_parameters_t query_params;
    ompi_pdata_parameters_t pdata_params;
} ompi_parameters_t;

typedef union{
    ompi_info_results_t info_results;
} ompi_results_t;

typedef union{
    pmix_info_cbfunc_t info_cbfunc;
}ompi_cbfunc_t;

typedef struct{
    ompi_parameters_type_t type;
    ompi_parameters_t params;
} ompi_collective_parameters_t;


typedef struct{
    ompi_results_type_t type;
    ompi_results_t results;
} ompi_collective_results_t;

typedef struct{
    ompi_cbfunc_type_t type;
    ompi_cbfunc_t cbfunc;
} ompi_collective_cbfunc_t;

typedef struct{
    pmix_proc_t *procs;
    size_t nprocs;
}ompi_collective_procs_t;

/* Collectives */
struct instance_collective_t{
    opal_list_item_t super;
    pmix_status_t status;
    ompi_function_type_t coll_func;
    ompi_collective_procs_t *coll_procs;
    ompi_collective_parameters_t *coll_params;
    ompi_collective_results_t *coll_results;
    ompi_collective_cbfunc_t *coll_cbfunc;
    void * coll_cbdata;
    opal_pmix_lock_t lock;
    bool is_waiting;
};

typedef struct instance_collective_t ompi_instance_collective_t;

OBJ_CLASS_DECLARATION(ompi_instance_collective_t);

int ompi_instance_collectives_init(void);

int ompi_instance_collectives_finalize(void);

void create_collective_query(ompi_instance_collective_t **coll, pmix_status_t status, pmix_proc_t *procs, size_t nprocs, pmix_query_t *query, size_t nqueries, pmix_info_t *results, size_t nresults, pmix_info_cbfunc_t info_cbfunc, void *cbdata);
int send_collective_data_query(pmix_proc_t *procs, pmix_status_t status, size_t nprocs, pmix_query_t *query, size_t nqueries, pmix_info_t *results, size_t nresults);
int recv_collective_data_query(pmix_proc_t *procs, size_t nprocs, pmix_query_t *query, size_t nqueries, pmix_info_cbfunc_t cbfunc, void *cbdata);

void create_collective_lookup(ompi_instance_collective_t **coll, pmix_status_t status, pmix_proc_t *procs, size_t nprocs, pmix_pdata_t *pdata, size_t npdata, pmix_info_t *info, size_t ninfo, pmix_info_t *results, size_t nresults, pmix_info_cbfunc_t info_cbfunc, void *cbdata);
int send_collective_data_lookup(pmix_proc_t *procs, pmix_status_t status, size_t nprocs, pmix_pdata_t *pdata, size_t npdata, pmix_info_t *info, size_t ninfo, pmix_info_t *results, size_t nresults);
int recv_collective_data_lookup(pmix_proc_t *procs, size_t nprocs, pmix_pdata_t *pdata, size_t npdata, pmix_info_t *info, size_t ninfo, pmix_info_cbfunc_t cbfunc, void *cbdata);
int recv_collective_data_lookup_nb(pmix_proc_t *procs, size_t nprocs, pmix_pdata_t *pdata, size_t npdata, pmix_info_t *info, size_t ninfo, pmix_info_cbfunc_t cbfunc, void *cbdata);

void ompi_instance_collective_infocb_send(pmix_status_t status, pmix_info_t *results, size_t nresults, void *cbdata, pmix_release_cbfunc_t release_fn, void *release_cbdata);


#endif /* !defined(OMPI_INSTANCE_COLLECTIVE_H) */
