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


void ompi_instance_nb_req_free(ompi_request_t **req){
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
        printf("provider publishing: %s->%s\n", key, pset_buf);
        /* Publish the PSet name*/
        rc = opal_pmix_publish_string_nb(key, pset_buf, strlen(pset_buf), pmix_op_cb_nb, cbdata);

        /* Just return the error. The other procs will experience an error in Lookup/Fence */
        if(OMPI_SUCCESS != rc){
            return rc;
        }
    /* The other processes lookup the Pset name */
    }else{
        printf("receiver lookup: %s\n", key);
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

    opal_process_name_t * opal_proc_names;
    
    /* allocate array of pset sizes */
    nprocs = malloc(num_psets * sizeof(size_t));

    /* allocate array of proc arrays */
    procs = malloc(num_psets * sizeof(pmix_proc_t *));

    for(int i = 0; i < num_psets; i++){
        /* retrieve pset members */
        get_pset_membership(pset_names[i], &opal_proc_names, &nprocs[i]);

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
        ompi_instance_free_pset_membership(pset_names[i]);
        free(procs[i]);
    }

    free(fence_procs);
    free(nprocs);
    free(procs);
    
    return OMPI_SUCCESS;
}

int integrate_res_change_fence_nb(char *delta_pset, char *assoc_pset, void *cbdata){
    
    int rc;
    char ** fence_psets;
    fence_psets = malloc(2 * sizeof(char *));
    fence_psets[0] = malloc(OPAL_MAX_PSET_NAME_LEN);
    fence_psets[1] = malloc(OPAL_MAX_PSET_NAME_LEN);

    strcpy(fence_psets[0], delta_pset);
    strcpy(fence_psets[1], assoc_pset);
    rc = pset_fence_multiple_nb(fence_psets, 2, NULL, pmix_op_cb_nb, cbdata);
    free(fence_psets[0]);
    free(fence_psets[1]);
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
    if(int_rc_results->provider && MPI_RC_ADD == int_rc_results->rc_type){
        
        bool non_default = true;
        pmix_info_t *event_info;
        PMIX_INFO_CREATE(event_info, 2);
        (void)snprintf(event_info[0].key, PMIX_MAX_KEYLEN, "%s", PMIX_EVENT_NON_DEFAULT);
        PMIX_VALUE_LOAD(&event_info[0].value, &non_default, PMIX_BOOL);
        (void)snprintf(event_info[1].key, PMIX_MAX_KEYLEN, "%s", PMIX_PSET_NAME);
        PMIX_VALUE_LOAD(&event_info[1].value, int_rc_results->delta_pset, PMIX_STRING);
        rc = PMIx_Notify_event(PMIX_RC_FINALIZED, NULL, PMIX_RANGE_NAMESPACE, event_info, 2, NULL, NULL);
        
        PMIX_INFO_FREE(event_info, 2);
    }

    *(int_rc_results->terminate) = (int_rc_results->rc_type == OMPI_RC_SUB && int_rc_results->incl) ? 1 : 0;

    ompi_instance_clear_rc_cache(int_rc_results->delta_pset);

    return rc;
}

/* Callbacks for non-blocking functions. Packs pdata into an info object of pmix values */
void pmix_lookup_cb_nb(pmix_status_t status, pmix_pdata_t pdata[], size_t ndata, void *cbdata){


    pmix_info_t *info;
    pmix_data_array_t darray;
    pmix_pdata_t *pdata_ptr;
    size_t n;

    printf("Proc %d: lookup_cb_nb\n", opal_process_info.myprocid.rank);

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
    size_t n, i;
    integrate_rc_results * int_rc_results;
    request_rc_results * req_rc_results;
    nb_chain_info * chain_info = (nb_chain_info *)cbdata;
    
    nb_func func = chain_info->func;
    chain_info->status = status;
    nb_chain_stage prev_stage = chain_info->stages[chain_info->cur_stage];
    nb_chain_stage next_stage = chain_info->stages[++chain_info->cur_stage];

    printf("Proc %d: next_stage: %d, status %d\n", opal_process_info.myprocid.rank, next_stage, status);

    switch(func){
        /* MPI_Session_dyn_integrate_res_change */
        case INTEGRATE_RC: 

            if(PMIX_SUCCESS == status){
    
                int_rc_results = (integrate_rc_results *)cbdata;
                
                if(next_stage == PUBSUB_STAGE){

                    rc = integrate_res_change_pubsub_nb(int_rc_results->provider, int_rc_results->delta_pset, int_rc_results->pset_buf, cbdata);
                    printf("Proc %d: finished pubsub nb\n", opal_process_info.myprocid.rank);

                }else if(next_stage == QUERY_MEM_STAGE){

                    /* store the looked up pset name in the results */
                    if(prev_stage == PUBSUB_STAGE && !int_rc_results->provider){
                        for(n = 0; n < ninfo; n++){
                            if(PMIX_CHECK_KEY(&info[n], "mpi_instance_nb_lookup_data")){
                                pmix_pdata_t *pdata = (pmix_pdata_t *) info[n].value.data.darray->array;
                                size_t ndata = info[n].value.data.darray->size;

                                char key[PMIX_MAX_KEYLEN + 1];
                                char *prefix = "mpi_integrate:";
                                strcpy(key, prefix);
                                strcat(key, int_rc_results->delta_pset);

                                for(i = 0; i < ndata; i++){
                                    if(0 == strcmp(pdata[i].key, key)){
                                        strcpy(int_rc_results->pset_buf, pdata[i].value.data.string);
                                    }
                                }
                            }
                        }                        
                    }


                    char **pset_names = malloc(2 * sizeof(char*));
                    pset_names[0] = int_rc_results->delta_pset;
                    pset_names[1] = int_rc_results->assoc_pset;

                    printf("SWITCHYARD Proc %d: received delta PSet: %s and assoc: %s\n", opal_process_info.myprocid.rank, int_rc_results->delta_pset, int_rc_results->assoc_pset);

                    rc = get_pset_membership_nb(pset_names, 2, pmix_info_cb_nb, cbdata);
                    printf("get_pset_mmembership returned: %d\n", rc);
                    free(pset_names);
                
                }else if(next_stage == FENCE_STAGE){

                    ompi_instance_lock_rc_and_psets();
                    if(prev_stage == QUERY_MEM_STAGE){
                        get_pset_membership_complete (status, info, ninfo, NULL, NULL, NULL);
                    }
                    rc = integrate_res_change_fence_nb(int_rc_results->delta_pset, int_rc_results->assoc_pset, cbdata);
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
        /* MPI_Session_dyn_request_res_change */
        case REQUEST_RC:
            if(PMIX_SUCCESS == status){
            
                req_rc_results = (request_rc_results *)cbdata;

                if(next_stage == LAST_STAGE){
                    opal_atomic_wmb();
                    
                    chain_info->req->req_complete = REQUEST_COMPLETED;
                    chain_info->req->req_status.MPI_ERROR = rc;
                    chain_info->req->req_state = OMPI_REQUEST_INVALID;
                    free(chain_info->stages);
                    free(req_rc_results);
                }
            }else{
                chain_info->req->req_status.MPI_ERROR = status;
                chain_info->req->req_state = OMPI_REQUEST_INVALID;
                free(chain_info->stages);                
            }

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

