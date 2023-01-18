/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2018-2022 Triad National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "instance.h"

#include "opal/util/arch.h"

#include "opal/util/show_help.h"
#include "opal/util/argv.h"
#include "opal/runtime/opal_params.h"

#include "ompi/mca/pml/pml.h"
#include "ompi/runtime/params.h"

#include "ompi/interlib/interlib.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/errhandler/errcode.h"
#include "ompi/message/message.h"
#include "ompi/info/info.h"
#include "ompi/attribute/attribute.h"
#include "ompi/op/op.h"
#include "ompi/dpm/dpm.h"
#include "ompi/file/file.h"
#include "ompi/mpiext/mpiext.h"

#include "ompi/mca/hook/base/base.h"
#include "ompi/mca/op/base/base.h"
#include "opal/mca/allocator/base/base.h"
#include "opal/mca/rcache/base/base.h"
#include "opal/mca/mpool/base/base.h"
#include "opal/mca/smsc/base/base.h"
#include "ompi/mca/bml/base/base.h"
#include "ompi/mca/pml/base/base.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/osc/base/base.h"
#include "ompi/mca/part/base/base.h"
#include "ompi/mca/io/base/base.h"
#include "ompi/mca/topo/base/base.h"
#include "opal/mca/pmix/base/base.h"

#include "opal/mca/mpool/base/mpool_base_tree.h"
#include "ompi/mca/pml/base/pml_base_bsend.h"
#include "ompi/util/timings.h"
#include "opal/mca/pmix/pmix-internal.h"

#include "ompi/instance/instance_nb.h"
#include "ompi/instance/instance_psets.h"

static void ompi_instance_nb_switchyard( pmix_status_t status, pmix_info_t *info, size_t ninfo, 
                void *cbdata, 
                pmix_release_cbfunc_t release_fn, void *release_cbdata);


int ompi_instance_nb_req_free(ompi_request_t **req){
    if(*req != MPI_REQUEST_NULL){
        (*req)->req_state = OMPI_REQUEST_INVALID;
        OBJ_RELEASE(*req);
        *req = MPI_REQUEST_NULL;
    }
    return OMPI_SUCCESS;
}

void ompi_instance_nb_req_create(ompi_request_t **req){
    *req = OBJ_NEW(ompi_request_t);
    OMPI_REQUEST_INIT(*req, false);
    (*req)->req_type = OMPI_REQUEST_DYN;
    (*req)->req_status.MPI_ERROR = MPI_SUCCESS;
    (*req)->req_status.MPI_SOURCE = 0;
    (*req)->req_status.MPI_TAG = 0;
    (*req)->req_state = OMPI_REQUEST_ACTIVE;
    (*req)->req_free = ompi_instance_nb_req_free;
}

int v1_recv_rc_results_complete(char *input_name, char *output_name, int *type, int get_by_delta_name, void *cbdata){

    ompi_mpi_instance_resource_change_t *res_change;

    ompi_instance_lock_rc_and_psets();
    /* if we did not find an active res change with a delta pset then at least search for invalid ones.
     * If there still aren't any resource changes found return an error.
     */
    if(NULL == (res_change = get_res_change_active_for_name(input_name)) || NULL == res_change->delta_psets || NULL == res_change->bound_psets){
        if(NULL == (res_change = get_res_change_for_name(input_name)) || NULL == res_change->delta_psets || NULL == res_change->bound_psets || RC_FINALIZED == res_change->status){
            ompi_instance_unlock_rc_and_psets();
            return OPAL_ERR_NOT_FOUND;
        }
    }


    /* lookup requested properties of the resource change */
    *type = MPI_OMPI_CONVT_PSET_OP(res_change->type);

    if(get_by_delta_name){
        strcpy(output_name, res_change->bound_psets[0]->name);
    }else{
        strcpy(output_name, res_change->delta_psets[0]->name);
    }

    get_pset_membership_nb(&output_name, 1, pmix_info_cb_nb, cbdata);

    ompi_instance_unlock_rc_and_psets();

    return OMPI_SUCCESS;
}

int v2a_query_psetop_complete(char *input_name, char ***output, int *noutput, int *type, int get_by_delta_name){
    size_t n;
    ompi_mpi_instance_resource_change_t *res_change;
    
    /* If there still aren't any resource changes found return an error */
    if(NULL == (res_change = get_res_change_active_for_name(input_name))){
        *type = MPI_PSETOP_NULL;
        *noutput = 0;
        return OMPI_SUCCESS;
        
    }

    ompi_instance_lock_rc_and_psets();
    /* lookup requested properties of the resource change */
    *type = (int) res_change->type;

    if(get_by_delta_name){
        *noutput = res_change->nbound_psets;
        *output = malloc(res_change->nbound_psets * sizeof(char *));
    }else{
        *noutput = res_change->ndelta_psets;
        *output = malloc(res_change->ndelta_psets * sizeof(char *));
    }
    ompi_mpi_instance_pset_t *delta_pset_ptr;

    /* If they asked for delta psets, copy it to the output array */
    if(!get_by_delta_name){
        for(n = 0; n < res_change->ndelta_psets; n++){
            (*output)[n] = strdup(res_change->delta_psets[n]->name);    
        }
    }
    /* If they asked for assoc psets, copy them to the output array */
    else{
        for(n = 0; n < res_change->nbound_psets; n++){
            (*output)[n] = strdup(res_change->bound_psets[n]->name);
        }        
    }
    
    /* TODO: provide additional information in info object if requested */

    ompi_instance_unlock_rc_and_psets();
    return OMPI_SUCCESS;
}

int v2b_query_psetop_complete(char *input_name, ompi_instance_rc_op_handle_t **rc_op_handle){
    size_t n;
    int rc;
    char **input, **output;
    ompi_mpi_instance_resource_change_t *res_change;

    ompi_instance_lock_rc_and_psets();

    /* If there still aren't any resource changes found return an error */
    if(NULL == (res_change = get_res_change_active_for_name(input_name))){
        ompi_instance_unlock_rc_and_psets();
        *rc_op_handle = MPI_RC_HANDLE_NULL;
        return OMPI_SUCCESS;
        
    }

    if(OMPI_SUCCESS != (rc = rc_op_handle_create(rc_op_handle))){
        ompi_instance_unlock_rc_and_psets();
        return rc;
    }

    input = (char **) malloc(res_change->nbound_psets * sizeof(char *));
    for(n = 0; n < res_change->nbound_psets; n++){
        input[n] = strdup(res_change->bound_psets[n]->name);
    }

    output = (char **) malloc(res_change->ndelta_psets * sizeof(char *));
    for(n = 0; n < res_change->ndelta_psets; n++){
        output[n] = strdup(res_change->delta_psets[n]->name);
    }

    if(OMPI_SUCCESS != (rc = rc_op_handle_add_op(res_change->type, input, res_change->nbound_psets, output, res_change->ndelta_psets, NULL, *rc_op_handle))){
        ompi_instance_unlock_rc_and_psets();
        free(input);
        free(output);
        return rc;        
    }

    /* TODO: provide additional information in info object if requested */
    ompi_instance_unlock_rc_and_psets();

    free(input);
    free(output);

    return OMPI_SUCCESS;
}

/* Non-blocking functions */
int integrate_res_change_pubsub_nb(int provider, char *delta_pset, char *pset_buf, void *cbdata){
    
    int rc;
    char key[PMIX_MAX_KEYLEN + 1];
    char *prefix = "mpi_integrate:";

    assert(strlen(delta_pset) + strlen(prefix) < PMIX_MAX_KEYLEN);

    strcpy(key, prefix);
    strcat(key, delta_pset);


    /* The provider needs to publish the Pset name */
    if(provider){
        /* Just return the error. The other procs will experience an error in Lookup/Fence */
        if(NULL == pset_buf){
            return OMPI_ERR_BAD_PARAM;
        }
        /* Publish the PSet name*/
        rc = opal_pmix_publish_string_nb(key, pset_buf, strlen(pset_buf), pmix_op_cb_nb, cbdata);

        /* Just return the error. The other procs will experience an error in Lookup/Fence */
        if(OMPI_SUCCESS != rc){
            return rc;
        }
    /* The other processes lookup the Pset name */
    }else{
        /* if they provided a NULL pointer as buffer we skip the lookup */
        if(NULL != pset_buf){
            /* Lookup the PSet name*/
            rc = opal_pmix_lookup_string_wait_nb(key, pmix_lookup_cb_nb, cbdata);
            /* Just return the error. The other procs will experience an error in Lookup/Fence */
            if(OMPI_SUCCESS != rc){
                return rc;
            }
        }
    }
    return OMPI_SUCCESS;
}

int get_pset_membership_nb(char **pset_names, int npsets, pmix_info_cbfunc_t cbfunc, void *cbdata){
    
    int rc, ret;
    bool refresh = true;
    pmix_info_t *info;
    size_t i, n, ninfo;
    pmix_query_t *queries;
    char *key = PMIX_QUERY_PSET_MEMBERSHIP;

    if(0 == npsets || pset_names == NULL){
        return OMPI_ERR_BAD_PARAM;
    }

    /* set query keys */
    PMIX_QUERY_CREATE(queries, npsets);

    for(i = 0; i < npsets; i++){
        PMIX_ARGV_APPEND(rc, queries[i].keys, key);

        queries[i].nqual = 2;
        PMIX_INFO_CREATE(queries[i].qualifiers, 2);
        PMIX_INFO_LOAD(&queries[i].qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
        PMIX_INFO_LOAD(&queries[i].qualifiers[1], PMIX_PSET_NAME, pset_names[i], PMIX_STRING);

    }

    /* Send the query */
    if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(queries, npsets, cbfunc, cbdata))) {
        ret = opal_pmix_convert_status(rc);
        return ret;                                            
    }

    return OMPI_SUCCESS;
}

int pset_fence_multiple_nb(char **pset_names, int num_psets, ompi_info_t *info, pmix_op_cbfunc_t cbfunc, void *cbdata){

    pmix_status_t rc;
    int ret;
    volatile bool active = true;
    bool flag = false;
    bool found = false;
    pmix_proc_t **procs;
    pmix_proc_t *fence_procs;
    pmix_info_t fence_info;
    size_t *nprocs;
    size_t max_procs = 0;
    size_t num_fence_procs = 0;

    ompi_mpi_instance_pset_t * pset_ptr;
    opal_process_name_t * opal_proc_names;

    /* allocate array of pset sizes */
    nprocs = malloc(num_psets * sizeof(size_t));

    /* allocate array of proc arrays */
    procs = malloc(num_psets * sizeof(pmix_proc_t *));
    
    for(int i = 0; i < num_psets; i++){
        /* retrieve pset members */
        if(NULL == (pset_ptr = get_pset_by_name(pset_names[i]))){
            free(nprocs);
            free(procs);
            return OMPI_ERR_NOT_FOUND;
        }
        rc = get_pset_membership(pset_ptr->name, &opal_proc_names, &nprocs[i]);
 
        procs[i] = malloc(nprocs[i] * sizeof(pmix_proc_t));
        for(int j = 0; j < nprocs[i]; j++){
            OPAL_PMIX_CONVERT_NAME(&procs[i][j], &opal_proc_names[j]);
        }
        max_procs += nprocs[i];
    }
    
    /* allocate an array of pmix_proc_t assuming non-overlapping PSets. We shrink it afterwards */
    fence_procs = malloc(max_procs * sizeof(pmix_proc_t));

    /* Iterate over all PSets and insert their members in the fence_procs array if they are not yet inserted */
    for(int i = 0; i < num_psets; i++){
        for(int j = 0; j < nprocs[i]; j++){
            found = false;
            pmix_proc_t proc_to_insert = procs[i][j];
            for(int k = 0; k < num_fence_procs; k++){
                if(PMIX_CHECK_PROCID(&proc_to_insert, &fence_procs[k])){
                    found = true;
                    break;
                }
            }
            if(!found){
                fence_procs[num_fence_procs++] = proc_to_insert;
            }
        }
    }

    /* now resize the array of procs accordingly */
    fence_procs = realloc(fence_procs, num_fence_procs * sizeof(pmix_proc_t));
    
    
    /* Perform the fence operation across the UNION of the pset members */
    PMIX_INFO_CONSTRUCT(&fence_info);
    PMIX_INFO_LOAD(&fence_info, PMIX_COLLECT_DATA, &flag, PMIX_BOOL);

    rc = PMIx_Fence_nb(fence_procs, num_fence_procs, &fence_info, 1,
                                            cbfunc, cbdata);
    

    /* Clean up */
    PMIX_INFO_DESTRUCT(&fence_info);

    for(int i = 0; i < num_psets; i++){
        pset_ptr = get_pset_by_name(pset_names[i]);
        ompi_instance_free_pset_membership(pset_ptr->name);
        free(procs[i]);
    }

    free(fence_procs);
    free(nprocs);
    free(procs);
    
    return OMPI_SUCCESS;
}

/* NOTE! This call needs to be protected by a lock for pset members */
int integrate_res_change_fence_nb(char **delta_psets, size_t ndelta_psets, char **assoc_psets, size_t nassoc_psets, void *cbdata){
    
    int rc;
    size_t n;
    char ** fence_psets;
    fence_psets = malloc((ndelta_psets + nassoc_psets) * sizeof(char *));

    for(n = 0; n < ndelta_psets; n++){
        fence_psets[n] = strdup(delta_psets[n]);
    }
    for(n = ndelta_psets; n < ndelta_psets + nassoc_psets; n++){
        fence_psets[n] = strdup(assoc_psets[n - ndelta_psets]);
    }
    rc = pset_fence_multiple_nb(fence_psets, ndelta_psets + nassoc_psets, NULL, pmix_op_cb_nb, cbdata);
    for(n = 0; n < ndelta_psets + nassoc_psets; n++){
        free(fence_psets[n]);
    }
    free(fence_psets);

    return rc;
}

int opal_pmix_lookup_nb(pmix_key_t key, pmix_info_t *lookup_info, size_t ninfo, pmix_lookup_cbfunc_t cbfunc, void *cbdata){

    int rc;
    char **keys = NULL;

    pmix_argv_append_nosize(&keys, key);

    rc = PMIx_Lookup_nb(keys, lookup_info, ninfo, cbfunc, cbdata);

    pmix_argv_free(keys);

    return rc;
}

int opal_pmix_lookup_string_wait_nb(char * key, pmix_lookup_cbfunc_t cbfunc, void *cbdata){
    int rc;
    bool wait = true;
    pmix_key_t pmix_key;
    pmix_info_t info;



    if(strlen(key) > PMIX_MAX_KEYLEN){
        return OMPI_ERR_BAD_PARAM;
    }
    strcpy(pmix_key, key);

    PMIX_INFO_CONSTRUCT(&info);
    PMIX_INFO_LOAD(&info, PMIX_WAIT, &wait, PMIX_BOOL);

    rc = opal_pmix_lookup_nb(pmix_key, &info, 1, cbfunc, cbdata);

    PMIX_INFO_DESTRUCT(&info);
    return rc;
}

int opal_pmix_publish_nb(pmix_key_t key, pmix_value_t value, pmix_op_cbfunc_t cbfunc, void *cbdata){
    int rc;
    pmix_info_t publish_data;

    PMIX_INFO_CONSTRUCT(&publish_data);
    PMIX_LOAD_KEY(publish_data.key, key);
    PMIX_VALUE_XFER_DIRECT(rc, &publish_data.value, &value);

    rc = PMIx_Publish_nb(&publish_data, 1, cbfunc, cbdata);

    PMIX_INFO_DESTRUCT(&publish_data);

    return rc;
}

int opal_pmix_publish_string_nb(char * key, char *val, int val_length, pmix_op_cbfunc_t cbfunc, void *cbdata){

    int rc;
    pmix_key_t pmix_key;
    pmix_value_t pmix_value;
    PMIX_VALUE_CONSTRUCT(&pmix_value);

    strncpy(pmix_key, key, strlen(key) < PMIX_MAX_KEYLEN ? strlen(key) + 1 : PMIX_MAX_KEYLEN);
    PMIX_VALUE_LOAD(&pmix_value, (void *) val, PMIX_STRING);

    rc = opal_pmix_publish_nb(pmix_key, pmix_value, cbfunc, cbdata);

    PMIX_VALUE_DESTRUCT(&pmix_value);

    return rc;

}

int integrate_res_change_finalize(integrate_rc_results *int_rc_results){
    int rc = PMIX_SUCCESS;
     /* Finalize the resource change. TODO: Find a better way. There is not always a provider. */
    if(int_rc_results->provider && MPI_PSETOP_ADD == int_rc_results->rc_type){
        
        bool non_default = true;
        pmix_info_t *event_info;
        PMIX_INFO_CREATE(event_info, 2);
        (void)snprintf(event_info[0].key, PMIX_MAX_KEYLEN, "%s", PMIX_EVENT_NON_DEFAULT);
        PMIX_VALUE_LOAD(&event_info[0].value, &non_default, PMIX_BOOL);
        (void)snprintf(event_info[1].key, PMIX_MAX_KEYLEN, "%s", PMIX_PSET_NAME);
        PMIX_VALUE_LOAD(&event_info[1].value, int_rc_results->delta_psets[0], PMIX_STRING);
        rc = PMIx_Notify_event(PMIX_RC_FINALIZED, NULL, PMIX_RANGE_RM, event_info, 2, NULL, NULL);
        
        PMIX_INFO_FREE(event_info, 2);
    }

    *(int_rc_results->terminate) = (int_rc_results->rc_type == OMPI_PSETOP_SUB && int_rc_results->incl) ? 1 : 0;

    //ompi_instance_clear_rc_cache(int_rc_results->delta_psets[0]);

    return rc;
}

int v2a_psetop_complete(pmix_status_t status, pmix_info_t *results, size_t nresults, char ***output, int *noutput){
    int rc = status;
    size_t n, noutput_names = 0;
    pmix_value_t *out_name_vals = NULL;

    if(PMIX_SUCCESS == status){
        /* Get the array of pmix_value_t containing the output names*/
        for(n = 0; n < nresults; n++){
            if(PMIX_CHECK_KEY(&results[n], "mpi.set_info.output")){
                out_name_vals = (pmix_value_t *) results[n].value.data.darray->array;
                noutput_names = results[n].value.data.darray->size;
            }
        }
        if(NULL == out_name_vals){
            rc = OMPI_ERR_BAD_PARAM;
            return rc;
        }

        /* Fill in the output for the "resource operation" */
        if(0 == *noutput){
            *output = (char **) malloc(noutput_names * sizeof(char *));
        }else{
            for(n = 0; n < *noutput; n++){
                free((*output)[n]);
            }            
        }

        for(n = 0; n < noutput_names; n++){
            (*output)[n] = strdup(out_name_vals[n].data.string);
        }
        *noutput = noutput_names;
    }else{
        if(PMIX_ERR_EXISTS == status || PMIX_ERR_OUT_OF_RESOURCE == status){
            *noutput = 0;
            return OMPI_SUCCESS;
        }
        return status;

    }

    return OMPI_SUCCESS;
}

int v2b_psetop_complete(pmix_status_t status, pmix_info_t *results, size_t nresults, ompi_instance_rc_op_handle_t *rc_op_handle){
    size_t n, k, noutput_names = 0;
    pmix_value_t *out_name_vals = NULL;

    if(PMIX_SUCCESS == status){
        /* Get the array of pmix_value_t containing the output names*/
        for(n = 0; n < nresults; n++){
            if(PMIX_CHECK_KEY(&results[n], "mpi.set_info.output")){
                out_name_vals = results[n].value.data.darray->array;
                noutput_names = results[n].value.data.darray->size;
            }
        }

        /* Fill in the output for the "resource operation" */
        if(0 == rc_op_handle->rc_op_info.n_output_names){
            rc_op_handle_init_output(rc_op_handle->rc_type, &rc_op_handle->rc_op_info.output_names, &rc_op_handle->rc_op_info.n_output_names);
        }

        for(n = 0; n < rc_op_handle->rc_op_info.n_output_names; n++){
            free(rc_op_handle->rc_op_info.output_names[n]);
            rc_op_handle->rc_op_info.output_names[n] = strdup(out_name_vals[n].data.string);
        }

        /* Fill in the output names for the "set operations" */
        ompi_instance_set_op_handle_t *setop;
        OPAL_LIST_FOREACH(setop, &rc_op_handle->set_ops, ompi_instance_set_op_handle_t){
            if(0 == setop->set_op_info.n_output_names){
                rc_op_handle_init_output(setop->psetop, &setop->set_op_info.output_names, &setop->set_op_info.n_output_names);
            }
            for(k = 0; k < setop->set_op_info.n_output_names && n < noutput_names; k++){
                free(setop->set_op_info.output_names[k]);
                setop->set_op_info.output_names[k] = strdup(out_name_vals[n++].data.string);
            }
        }
    }else{
        if(PMIX_ERR_EXISTS == status || PMIX_ERR_OUT_OF_RESOURCE == status){
            /* FIXME. Need a better way to indicate failure? */
            rc_op_handle->rc_type = MPI_PSETOP_NULL;
            return OMPI_SUCCESS;
        }
        return status;

    }
    return PMIX_SUCCESS;
}

/* Callbacks for non-blocking functions. Packs pdata into an info object of pmix values */
void pmix_lookup_cb_nb(pmix_status_t status, pmix_pdata_t pdata[], size_t ndata, void *cbdata){


    pmix_info_t *info;
    pmix_data_array_t darray;
    pmix_pdata_t *pdata_ptr;
    size_t n;



    PMIX_INFO_CREATE(info, 1);
    PMIX_DATA_ARRAY_CONSTRUCT(&darray, ndata, PMIX_PDATA);
    pdata_ptr = (pmix_pdata_t *) darray.array;
    for(n = 0; n < ndata; n++){
        PMIX_PDATA_XFER(&pdata_ptr[n], &pdata[n]);
    }
    PMIX_INFO_LOAD(&info[0], "mpi_instance_nb_lookup_data", &darray, PMIX_DATA_ARRAY);

    ompi_instance_nb_switchyard(status, info, 1, cbdata, NULL, NULL);

    PMIX_DATA_ARRAY_DESTRUCT(&darray);
    PMIX_INFO_FREE(info, 1);
    
}

void pmix_op_cb_nb(pmix_status_t status, void *cbdata){
    ompi_instance_nb_switchyard(status, NULL, 0, cbdata, NULL, NULL);
}

void pmix_info_cb_nb( pmix_status_t status, pmix_info_t *info, size_t ninfo, 
                void *cbdata, 
                pmix_release_cbfunc_t release_fn, void *release_cbdata){
    
    ompi_instance_nb_switchyard(status, info, ninfo, cbdata, release_fn, release_cbdata);

}

/* Main switchyard for the non-blocking function chains */
static void ompi_instance_nb_switchyard( pmix_status_t status, pmix_info_t *info, size_t ninfo, 
                void *cbdata, 
                pmix_release_cbfunc_t release_fn, void *release_cbdata){

    int rc = OMPI_SUCCESS;
    size_t n, i, ndata;
    pmix_value_t *val_ptr;
    ompi_mpi_instance_pset_t *pset_ptr;

    nb_chain_info * chain_info;
    nb_func func;
    nb_chain_stage prev_stage;
    nb_chain_stage next_stage;
    
    integrate_rc_results * int_rc_results;
    v1_recv_rc_results * v1_recv_rc_res;
    v2a_query_psetop_results * v2a_query_psetop_res;
    v2b_query_psetop_results * v2b_query_psetop_res;
    v1_psetop_results * v1_psetop_res;
    v2a_psetop_results * v2a_psetop_res;
    v2b_psetop_results * v2b_psetop_res;

    pset_data_results * pdata_results;

    chain_info = (nb_chain_info *)cbdata;
    
    func = chain_info->func;
    chain_info->status = status;
    prev_stage = chain_info->stages[chain_info->cur_stage];
    next_stage = chain_info->stages[++chain_info->cur_stage];

    //printf("nb_switchyard: Proc %d, func %d, next_stage %d, status %d\n", opal_process_info.myprocid.rank, func, next_stage, status);

    switch(func){
        /* MPI_Session_dyn_integrate_res_change */
        case INTEGRATE_RC: 

            if(PMIX_SUCCESS == status){
    
                int_rc_results = (integrate_rc_results *)cbdata;
                
                if(next_stage == PUBSUB_STAGE){

                    if(NULL == int_rc_results->pset_buf){
                        ++chain_info->cur_stage;
                        ompi_instance_nb_switchyard(PMIX_SUCCESS, NULL, 0, cbdata, NULL, NULL);
                    }

                    rc = integrate_res_change_pubsub_nb(int_rc_results->provider, int_rc_results->delta_psets[0], int_rc_results->pset_buf, cbdata);

                }else if(next_stage == QUERY_MEM_STAGE){

                    /* store the looked up pset name in the results */
                    if(prev_stage == PUBSUB_STAGE && !int_rc_results->provider){
                        for(n = 0; n < ninfo; n++){
                            if(PMIX_CHECK_KEY(&info[n], "mpi_instance_nb_lookup_data")){
                                pmix_pdata_t *pdata = (pmix_pdata_t *) info[n].value.data.darray->array;
                                ndata = info[n].value.data.darray->size;

                                char key[PMIX_MAX_KEYLEN + 1];
                                char *prefix = "mpi_integrate:";
                                strcpy(key, prefix);
                                strcat(key, int_rc_results->delta_psets[0]);

                                for(i = 0; i < ndata; i++){
                                    if(0 == strcmp(pdata[i].key, key)){
                                        strcpy(int_rc_results->pset_buf, pdata[i].value.data.string);
                                    }
                                }
                            }
                        }                        
                    }

                    char **pset_names = malloc((int_rc_results->ndelta_psets + int_rc_results->nassoc_psets) * sizeof(char*));
                    for(n = 0; n < int_rc_results->ndelta_psets; n++){
                        pset_names[n] = strdup(int_rc_results->delta_psets[n]);
                    }
                    for(n = int_rc_results->ndelta_psets; n < int_rc_results->ndelta_psets + int_rc_results->nassoc_psets; n++){
                        pset_names[n] = strdup(int_rc_results->assoc_psets[n - int_rc_results->ndelta_psets]);
                    }


                    rc = get_pset_membership_nb(pset_names, int_rc_results->ndelta_psets + int_rc_results->nassoc_psets, pmix_info_cb_nb, cbdata);
                    for(n = int_rc_results->ndelta_psets; n < int_rc_results->ndelta_psets + int_rc_results->nassoc_psets; n++){
                        free(pset_names[n]);
                    }
                    free(pset_names);
                
                }else if(next_stage == FENCE_STAGE){
                    ompi_instance_lock_rc_and_psets();
                    if(prev_stage == QUERY_MEM_STAGE){
                        get_pset_membership_complete (status, info, ninfo, NULL, NULL, NULL);
                    }
                    rc = integrate_res_change_fence_nb(int_rc_results->delta_psets, int_rc_results->ndelta_psets, int_rc_results->assoc_psets, int_rc_results->nassoc_psets, cbdata);
                    for(n = 0; n < int_rc_results->ndelta_psets; n++){
                        ompi_instance_free_pset_membership(int_rc_results->delta_psets[n]);
                    }
                    for(n = 0; n < int_rc_results->nassoc_psets; n++){
                        ompi_instance_free_pset_membership(int_rc_results->assoc_psets[n]);
                    }
                    ompi_instance_unlock_rc_and_psets();
                    
                }else if(next_stage == LAST_STAGE){
                    
                    rc = integrate_res_change_finalize(int_rc_results);
                    
                    opal_atomic_wmb();
                    
                    chain_info->req->req_complete = REQUEST_COMPLETED;
                    chain_info->req->req_status.MPI_ERROR = rc;
                    chain_info->req->req_state = OMPI_REQUEST_INVALID;
                    free(chain_info->stages);
                    free(int_rc_results);
                }
            
            }else{
                chain_info->req->req_status.MPI_ERROR = status;
                chain_info->req->req_state = OMPI_REQUEST_INVALID;
                free(chain_info->stages);
            }
            break;
        case V2A_QUERY_PSETOP:
            v2a_query_psetop_res = (v2a_query_psetop_results *)cbdata;
            if(PMIX_SUCCESS == status || PMIX_ERR_NOT_FOUND == status){
                if(next_stage == LAST_STAGE){

                    ompi_instance_get_res_change_complete(status, info, ninfo, NULL, NULL, NULL);

                    rc = v2a_query_psetop_complete(v2a_query_psetop_res->input_name, v2a_query_psetop_res->output, v2a_query_psetop_res->noutput, v2a_query_psetop_res->type, v2a_query_psetop_res->get_by_delta_name);

                    opal_atomic_wmb();
                    
                    chain_info->req->req_complete = REQUEST_COMPLETED;
                    chain_info->req->req_status.MPI_ERROR = rc;
                    chain_info->req->req_state = OMPI_REQUEST_INVALID;
                    free(chain_info->stages);
                    free(v2a_query_psetop_res);

                }
            }else{
                chain_info->req->req_status.MPI_ERROR = status;
                chain_info->req->req_state = OMPI_REQUEST_INVALID;
                free(chain_info->stages);
                free(v2a_query_psetop_res);                  
            }
            break;
        case V2B_QUERY_PSETOP:
            v2b_query_psetop_res = (v2b_query_psetop_results *)cbdata;
            if(PMIX_SUCCESS == status || PMIX_ERR_NOT_FOUND == status){
                if(next_stage == LAST_STAGE){

                    ompi_instance_get_res_change_complete(status, info, ninfo, NULL, NULL, NULL);

                    rc = v2b_query_psetop_complete(v2b_query_psetop_res->input_name, v2b_query_psetop_res->rc_op_handle);

                    opal_atomic_wmb();
                                   
                    chain_info->req->req_status.MPI_ERROR = rc;
                    chain_info->req->req_complete = REQUEST_COMPLETED;
                    chain_info->req->req_state = OMPI_REQUEST_INVALID;
                    free(chain_info->stages);
                    free(v2a_query_psetop_res);

                }
            }else{
                chain_info->req->req_status.MPI_ERROR = status;
                chain_info->req->req_complete = REQUEST_COMPLETED;
                chain_info->req->req_state = OMPI_REQUEST_INVALID;
                free(chain_info->stages);
                free(v2a_query_psetop_res);                  
            }
            break;
            
        case V2A_PSETOP:

            if(PMIX_SUCCESS == status || PMIX_ERR_EXISTS == status || PMIX_ERR_OUT_OF_RESOURCE == status){
            
                v2a_psetop_res = (v2a_psetop_results *)cbdata;

                if(next_stage == LAST_STAGE){
                    
                    rc = v2a_psetop_complete(status, info, ninfo, v2a_psetop_res->output, v2a_psetop_res->noutput);

                    opal_atomic_wmb();
                    
                    chain_info->req->req_complete = REQUEST_COMPLETED;
                    chain_info->req->req_status.MPI_ERROR = rc;
                    chain_info->req->req_state = OMPI_REQUEST_INVALID;
                    free(chain_info->stages);
                    free(v2a_psetop_res);
                }
            }else{

                chain_info->req->req_status.MPI_ERROR = status;
                chain_info->req->req_complete = REQUEST_COMPLETED;
                chain_info->req->req_state = OMPI_REQUEST_INVALID;
                free(chain_info->stages);                
            }
            break;
        /* MPI_Session_dyn_v2b_psetop */
        case V2B_PSETOP:
            if(PMIX_SUCCESS == status || PMIX_ERR_EXISTS == status || PMIX_ERR_OUT_OF_RESOURCE == status){
            
                v2b_psetop_res = (v2b_psetop_results *)cbdata;

                if(next_stage == LAST_STAGE){
                    if(NULL != v2b_psetop_res->rc_op_handle){
                        v2b_psetop_complete(status, info, ninfo, v2b_psetop_res->rc_op_handle);
                    }

                    opal_atomic_wmb();
                    
                    chain_info->req->req_complete = REQUEST_COMPLETED;
                    chain_info->req->req_status.MPI_ERROR = rc;
                    chain_info->req->req_state = OMPI_REQUEST_INVALID;
                    free(chain_info->stages);
                    free(v2b_psetop_res);
                }
            }else{
                chain_info->req->req_status.MPI_ERROR = status;
                chain_info->req->req_complete = REQUEST_COMPLETED;
                chain_info->req->req_state = OMPI_REQUEST_INVALID;
                free(chain_info->stages);                
            }
            break;
        case V1_RECV_RC:
            if(PMIX_SUCCESS == status || PMIX_ERR_NOT_FOUND == status){
                v1_recv_rc_res = (v1_recv_rc_results *)cbdata;

                if(prev_stage == QUERY_RC_STAGE){
                    ompi_instance_get_res_change_complete(status, info, ninfo, NULL, NULL, NULL);

                    rc = v1_recv_rc_results_complete(v1_recv_rc_res->input_pset, v1_recv_rc_res->output_pset, v1_recv_rc_res->rc_type, v1_recv_rc_res->get_by_delta_pset, (void *) v1_recv_rc_res);

                    if(rc == OMPI_ERR_NOT_FOUND){
                        *v1_recv_rc_res->rc_type = OMPI_PSETOP_NULL;
                        *v1_recv_rc_res->incl = 0;
                        rc = OMPI_SUCCESS;
                        next_stage = chain_info->stages[++chain_info->cur_stage];
                    }
                   
                }else if(prev_stage == QUERY_MEM_STAGE){
                    get_pset_membership_complete (status, info, ninfo, NULL, NULL, NULL);
                    rc = is_pset_element(v1_recv_rc_res->output_pset, v1_recv_rc_res->incl);
                    ompi_instance_free_pset_membership(v1_recv_rc_res->output_pset);
                }
                
                if(next_stage == LAST_STAGE){
                    opal_atomic_wmb();
                    
                    chain_info->req->req_complete = REQUEST_COMPLETED;
                    chain_info->req->req_status.MPI_ERROR = rc;
                    chain_info->req->req_state = OMPI_REQUEST_INVALID;
                    free(chain_info->stages);
                    free(v1_recv_rc_res);

                }
            }else{
                chain_info->req->req_status.MPI_ERROR = status;
                chain_info->req->req_state = OMPI_REQUEST_INVALID;
                free(chain_info->stages);                  
            }
            break;
        case V1_PSETOP:
            if(PMIX_SUCCESS == status){
                v1_psetop_res = (v1_psetop_results *)cbdata;

                if(next_stage == LAST_STAGE){
                    for(n = 0; n < ninfo; n++){
                        if(PMIX_CHECK_KEY(&info[n], "mpi.set_info.output") ){
                            val_ptr = (pmix_value_t *) info[n].value.data.darray->array;
                            strcpy(v1_psetop_res->pset_result, val_ptr[0].data.string);
                        }
                    }

                    opal_atomic_wmb();
                    
                    chain_info->req->req_complete = REQUEST_COMPLETED;
                    chain_info->req->req_status.MPI_ERROR = rc;
                    chain_info->req->req_state = OMPI_REQUEST_INVALID;
                    free(chain_info->stages);
                    free(v1_psetop_res);

                }
            }else{
                chain_info->req->req_status.MPI_ERROR = status;
                chain_info->req->req_state = OMPI_REQUEST_INVALID;
                free(chain_info->stages);                  
            }
            break;
        /* nb funcs without output */
        case PSET_FENCE:
        case V1_REQ_RC:
            if(PMIX_SUCCESS == status || PMIX_ERR_EXISTS == status || PMIX_ERR_OUT_OF_RESOURCE == status){

                if(next_stage == LAST_STAGE){
                    opal_atomic_wmb();
                    chain_info->req->req_complete = REQUEST_COMPLETED;
                    chain_info->req->req_status.MPI_ERROR = OMPI_SUCCESS;
                    chain_info->req->req_state = OMPI_REQUEST_INVALID;
                    free(chain_info->stages);
                }
            }else{
                chain_info->req->req_status.MPI_ERROR = status;
                chain_info->req->req_complete = REQUEST_COMPLETED;
                chain_info->req->req_state = OMPI_REQUEST_INVALID;
                free(chain_info->stages);                
            }
            break;
        case GET_PSET_DATA:
            pdata_results = (pset_data_results *)cbdata;

            if(PMIX_SUCCESS == status && NULL != (pset_ptr = get_pset_by_name(pdata_results->coll_pset))){
                
                if(next_stage == LAST_STAGE){

                    if(OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_PRIMARY)){

                        send_collective_data_lookup(pdata_results->coll_procs, status, pdata_results->n_coll_procs, pdata_results->pdata, pdata_results->nkeys, pdata_results->pmix_info, pdata_results->ninfo, info, ninfo);
                    }

                    ndata = 0;
                    for(n = 0; n < ninfo; n++){
                        if(PMIX_CHECK_KEY(&info[n], "mpi_instance_nb_lookup_data")){

                            pmix_pdata_t *pdata = (pmix_pdata_t *) info[n].value.data.darray->array;
                            ndata = info[n].value.data.darray->size;

                            for(i = 0; i < ndata; i++){
                                if(PMIX_STRING == pdata[i].value.type){
                                    ompi_info_set(pdata_results->info, pdata[i].key, pdata[i].value.data.string);
                                }
                            }
                        }
                    }

                    if(ndata == pdata_results->nkeys){
                        *(pdata_results->info_used) = pdata_results->info;
                    }else{
                        ompi_info_free(&pdata_results->info);
                    }

                    PMIX_PROC_FREE(pdata_results->coll_procs, pdata_results->n_coll_procs);
                    PMIX_INFO_FREE(pdata_results->pmix_info, pdata_results->ninfo);
                    PMIX_PDATA_FREE(pdata_results->pdata, pdata_results->nkeys);
                    
                    opal_atomic_wmb();
                    chain_info->req->req_complete = REQUEST_COMPLETED;
                    chain_info->req->req_status.MPI_ERROR = OMPI_SUCCESS;
                    chain_info->req->req_state = OMPI_REQUEST_INVALID;
                    free(chain_info->stages);
                }
            }else{
                if(status == PMIX_ERR_NOT_FOUND){
                    status = OMPI_SUCCESS;
                }else{
                    ompi_info_free(&pdata_results->info);
                }
                PMIX_PROC_FREE(pdata_results->coll_procs, pdata_results->n_coll_procs);
                PMIX_INFO_FREE(pdata_results->pmix_info, pdata_results->ninfo);
                PMIX_PDATA_FREE(pdata_results->pdata, pdata_results->nkeys);

                chain_info->req->req_status.MPI_ERROR = status;
                chain_info->req->req_complete = REQUEST_COMPLETED;
                chain_info->req->req_state = OMPI_REQUEST_INVALID;
                free(chain_info->stages);                
            }
            break;
        default: 
            break;
    }

    if(OMPI_SUCCESS != rc){
        printf("Error in cb_nb progress: %d\n", rc);
    }

    if(NULL != release_fn){
        release_fn(release_cbdata);
    }                               
                            
}

