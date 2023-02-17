/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2018      Triad National Security, LLC.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OMPI_INSTANCE_RC_H)
#define OMPI_INSTANCE_RC_H

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


typedef enum{
    RC_INVALID,
    RC_ANNOUNCED,
    RC_CONFIRMATION_PENDING,
    RC_FINALIZED 
} ompi_rc_status_t;

struct ompi_resource_change_t{
    opal_list_item_t super;
    ompi_mpi_instance_pset_t **delta_psets;
    ompi_mpi_instance_pset_t **bound_psets;
    size_t ndelta_psets;
    size_t nbound_psets;
    ompi_psetop_type_t type;
    ompi_rc_status_t status;
};

typedef struct ompi_resource_change_t ompi_mpi_instance_resource_change_t;

typedef struct res_change_query_cbdata_t{
    opal_list_item_t super;
    opal_pmix_lock_t lock;
    ompi_mpi_instance_resource_change_t *res_change;
}res_change_query_cbdata_t;



int ompi_instance_res_changes_init(void);

int ompi_instance_res_changes_finalize(void);

bool ompi_instance_res_changes_initalized(void);

void rc_finalize_handler(size_t evhdlr_registration_id, pmix_status_t status,
                       const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
                       pmix_info_t results[], size_t nresults,
                       pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata);

/* get res change local */
ompi_mpi_instance_resource_change_t * get_res_change_for_name(char *name);
ompi_mpi_instance_resource_change_t * get_res_change_active_for_name(char *name);

ompi_mpi_instance_resource_change_t * get_res_change_active_for_name(char *name);

ompi_mpi_instance_resource_change_t * get_res_change_for_input_name(char *name);
ompi_mpi_instance_resource_change_t * get_res_change_active_for_input_name(char *name);

ompi_mpi_instance_resource_change_t * get_res_change_for_output_name(char *name);
ompi_mpi_instance_resource_change_t * get_res_change_active_for_output_name(char *name);


int get_res_change_type(char *delta_pset, ompi_psetop_type_t *rc_type);

void ompi_instance_get_res_change_complete (pmix_status_t status, pmix_info_t *results, size_t nresults, void *cbdata, pmix_release_cbfunc_t release_fn, void *release_cbdata);

int get_res_change_info(char *input_name, ompi_psetop_type_t *type, char ***output_names, size_t *noutput_names, int *incl, ompi_rc_status_t *status, opal_info_t **info_used, bool get_by_delta_name);
int get_res_change_info_collective(pmix_proc_t *coll_procs, size_t n_coll_procs, char *input_name, ompi_psetop_type_t *type, char ***output_names, size_t *noutputs,int *incl, ompi_rc_status_t *status, opal_info_t **info_used, bool get_by_delta_name);
int get_res_change_info_collective_nb(pmix_proc_t *coll_procs, size_t n_coll_procs, char *input_name, pmix_info_cbfunc_t cbfunc, void *cbdata);

void res_change_clear_cache(char *delta_pset);

#endif /* !defined(OMPI_INSTANCE_RC_H) */