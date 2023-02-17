/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2018      Triad National Security, LLC.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OMPI_INSTANCE_NB_H)
#define OMPI_INSTANCE_NB_H

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
#include "ompi/instance/instance_op_handle.h"


typedef enum _nb_chain_stage{
    QUERY_RC_STAGE,
    PUBSUB_STAGE,
    QUERY_MEM_STAGE,
    FENCE_STAGE,
    QUERY_PSETOP_STAGE,
    PSETOP_STAGE,
    REQUEST_RC_STAGE,
    LOOKUP_STAGE,
    LAST_STAGE
}nb_chain_stage;

typedef enum _nb_func{
    GET_RC,
    RECV_RC,
    V1_RECV_RC,
    V1_REQ_RC,
    INTEGRATE_RC,
    V1_PSETOP,
    V2A_PSETOP,
    V2B_PSETOP,
    V2C_PSETOP,
    V2A_QUERY_PSETOP,
    V2B_QUERY_PSETOP,
    V2C_QUERY_PSETOP,
    PSET_FENCE,
    GET_PSET_DATA
}nb_func;

typedef struct _nb_chain_info{
    nb_func func;
    nb_chain_stage * stages;
    int cur_stage;
    int nstages;
    int status;
    ompi_request_t * req;
}nb_chain_info;

typedef struct _get_rc_results{
    nb_chain_info chain_info;
    char * delta_pset;
    char * assoc_pset;
    ompi_psetop_type_t * rc_type;
    ompi_rc_status_t * rc_status;
    int *incl;
}get_rc_results;

typedef struct _v1_recv_rc_results{
    nb_chain_info chain_info;
    char * input_pset;
    char * output_pset;
    int * rc_type;
    ompi_rc_status_t * rc_status;
    int *incl;
    int get_by_delta_pset;
}v1_recv_rc_results;

typedef struct _integrate_rc_results{
    nb_chain_info chain_info;
    char ** delta_psets;
    char **assoc_psets;
    size_t ndelta_psets;
    size_t nassoc_psets;
    ompi_psetop_type_t rc_type;
    ompi_rc_status_t rc_status;
    int incl;
    int provider;
    char * pset_buf;
    int * terminate;
}integrate_rc_results;

typedef struct v2a_query_psetop_results{
    nb_chain_info chain_info;
    char *input_name;
    char ***output;
    int *noutput; 
    int *type; 
    int get_by_delta_name;
}v2a_query_psetop_results;

typedef struct v2b_query_psetop_results{
    nb_chain_info chain_info;
    char *input_name;
    ompi_instance_rc_op_handle_t **rc_op_handle;
}v2b_query_psetop_results;


typedef struct _v1_psetop_results{
    nb_chain_info chain_info;
    char *pset_result;
}v1_psetop_results;

typedef struct _v2a_psetop_results{
    nb_chain_info chain_info;
    char ***output;
    int *noutput;
    int *psetop;
}v2a_psetop_results;

/* TODO: We might want the request to return info in the future */
typedef struct _v2b_psetop_results{
    nb_chain_info chain_info;
    ompi_instance_rc_op_handle_t *rc_op_handle;
}v2b_psetop_results;

typedef struct fence_results{
    nb_chain_info chain_info;
}fence_results;

typedef struct pset_data_results{
    nb_chain_info chain_info;
    char *coll_pset;
    ompi_info_t *info;
    ompi_info_t **info_used;
    pmix_pdata_t *pdata;
    pmix_info_t *pmix_info;
    pmix_proc_t *coll_procs;
    size_t n_coll_procs;
    size_t nkeys;
    size_t ninfo;
}pset_data_results;

typedef struct res_change_query_nb_cbdata_t{
    opal_list_item_t super;
    opal_pmix_lock_t lock;
    ompi_mpi_instance_resource_change_t *res_change;
}res_change_query_nb_cbdata_t;


void ompi_instance_nb_req_create(ompi_request_t **req);

/* Callbacks for non-blocking functions */
void pmix_lookup_cb_nb(pmix_status_t status, pmix_pdata_t pdata[], size_t ndata, void *cbdata);

void pmix_op_cb_nb(pmix_status_t status, void *cbdata);

void pmix_info_cb_nb( pmix_status_t status, pmix_info_t *info, size_t ninfo, 
                void *cbdata, pmix_release_cbfunc_t release_fn, void *release_cbdata);
int integrate_res_change_pubsub_nb(int provider, char *delta_pset, char *pset_buf, void *cbdata);
int integrate_res_change_fence_nb(char **delta_pset, size_t ndelta_psets, char **assoc_psets, size_t nassoc_psets, void *cbdata);
int pset_fence_multiple_nb(char **pset_names, int num_psets, ompi_info_t *info, pmix_op_cbfunc_t cbfunc, void *cbdata);
int get_pset_membership_nb(char **pset_names, int npsets, pmix_info_cbfunc_t cbfunc, void *cbdata);
int opal_pmix_lookup_nb(pmix_key_t key, pmix_info_t *lookup_info, size_t ninfo, pmix_lookup_cbfunc_t cbfunc, void *cbdata);
int opal_pmix_lookup_string_wait_nb(char * key, pmix_lookup_cbfunc_t cbfunc, void *cbdata);

int opal_pmix_publish_nb(pmix_key_t key, pmix_value_t value, pmix_op_cbfunc_t cbfunc, void *cbdata);
int opal_pmix_publish_string_nb(char * key, char *val, int val_length, pmix_op_cbfunc_t cbfunc, void *cbdata);


#endif /* !defined(OMPI_INSTANCE_NB_H) */