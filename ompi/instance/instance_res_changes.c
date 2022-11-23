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

#include "ompi/instance/instance_psets.h"
#include "ompi/instance/instance_collective.h"
#include "ompi/instance/instance_res_changes.h"

typedef ompi_mpi_instance_resource_change_t * (*ompi_instance_get_res_change_fn_t)(char * name);

bool initialized = false;

/* List of local resource changes */
static opal_list_t ompi_mpi_instance_resource_changes;

ompi_mpi_instance_resource_change_t *res_change_bound_to_self = NULL;

int ompi_instance_res_changes_init(){
    OBJ_CONSTRUCT(&ompi_mpi_instance_resource_changes, opal_list_t);

    return ompi_instance_psets_init();
}

int ompi_instance_res_changes_finalize(){
    OBJ_DESTRUCT(&ompi_mpi_instance_resource_changes);

    return ompi_instance_psets_finalize();
}

bool ompi_instance_res_changes_initalized(){
    return initialized;
}

static void ompi_resource_change_constructor(ompi_mpi_instance_resource_change_t *rc){
    OBJ_CONSTRUCT(&rc->super, opal_list_item_t);
    rc->delta_pset = rc->bound_pset = NULL;
    rc->type = OMPI_RC_NULL;
    rc->status = RC_INVALID;
}
static void ompi_resource_change_destructor(ompi_mpi_instance_resource_change_t *rc){
    OBJ_DESTRUCT(&rc->super);
}

OBJ_CLASS_INSTANCE(ompi_mpi_instance_resource_change_t, opal_object_t, ompi_resource_change_constructor, ompi_resource_change_constructor);

/* delete resource change from local cache */
void rc_finalize_handler(size_t evhdlr_registration_id, pmix_status_t status,
                       const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
                       pmix_info_t results[], size_t nresults,
                       pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata){

    size_t n;
    pmix_status_t rc=PMIX_SUCCESS;
    size_t sz;
    char *pset_name = NULL;
    
    
    ompi_instance_lock_rc_and_psets();
    for(n = 0; n < ninfo; n++){
        if(0 == strcmp(info[n].key, PMIX_PSET_NAME)){
            PMIX_VALUE_UNLOAD(rc, &info[n].value, (void**)&pset_name, &sz);
        }
    }
    if(NULL != pset_name){
        ompi_mpi_instance_resource_change_t *res_change;
        
        if(NULL != (res_change = get_res_change_for_name(pset_name))){
            //res_change = opal_list_remove_item(&ompi_mpi_instance_resource_changes, &res_change->super);
            res_change->status = RC_FINALIZED;
            //OBJ_RELEASE(res_change);
        }
        free(pset_name);
    }
    
    ompi_instance_unlock_rc_and_psets();
    
    cbfunc(PMIX_SUCCESS, NULL, 0, NULL, NULL, cbdata);
}

ompi_mpi_instance_resource_change_t * get_res_change_for_name(char *name){
    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->delta_pset && 0 == strncmp(name, rc_out->delta_pset->name, MPI_MAX_PSET_NAME_LEN)){
            ompi_instance_unlock_rc_and_psets();
            return rc_out;
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return NULL;
}

ompi_mpi_instance_resource_change_t * get_res_change_for_bound_name(char *name){

    if(0 == strcmp(name, "mpi://SELF")){
        return res_change_bound_to_self;
    }
    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->bound_pset && 0 == strcmp(name,rc_out->bound_pset->name)){
            ompi_instance_unlock_rc_and_psets();
            return rc_out;
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return NULL;
    
}

ompi_mpi_instance_resource_change_t * get_res_change_active_for_bound_name(char *name){
    
    if(0 == strcmp(name, "mpi://SELF")){
        return res_change_bound_to_self;
    }
    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->bound_pset && 0 == strcmp(name,rc_out->bound_pset->name) && rc_out->status != RC_INVALID && rc_out->status != RC_FINALIZED){
            ompi_instance_unlock_rc_and_psets();
            return rc_out;
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return NULL; 
}

ompi_mpi_instance_resource_change_t * get_res_change_active_for_name(char *name){

    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->delta_pset && 0 == strcmp(name,rc_out->delta_pset->name) && rc_out->status != RC_INVALID && rc_out->status != RC_FINALIZED){
            ompi_instance_unlock_rc_and_psets();
            return rc_out;
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return NULL; 
}


int ompi_instance_get_rc_type( char *delta_pset, ompi_rc_op_type_t *rc_type){
    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *res_change;
    res_change = get_res_change_for_name(delta_pset);
    *rc_type = res_change->type;
    ompi_instance_unlock_rc_and_psets();
    return OPAL_SUCCESS;
}

int print_res_change(char *name){

    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_out = get_res_change_for_name(name);

    if(NULL == rc_out){
        printf("print_res_change: Resource change %s: NULL\n", name);
        return -1;
    }
    printf("print_res_change: Resource change %s: ", name);
    printf("[type: %d", rc_out->type);
    printf(", status: %d", rc_out->status);
    if(rc_out->bound_pset==NULL){
        printf(", bound pset: NULL]\n");
    }
    printf(", bound pset: %s]\n", rc_out->bound_pset->name);
    ompi_instance_unlock_rc_and_psets();
	return 0;
}

void res_change_clear_cache(char *delta_pset){

    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_remove = get_res_change_for_name(delta_pset);
    if(NULL != rc_remove){
        //opal_list_remove_item(&ompi_mpi_instance_resource_changes, &rc_remove->super);
        rc_remove->status = RC_FINALIZED;
        //OBJ_RELEASE(rc_remove);
    }

    ompi_instance_unlock_rc_and_psets();
}

/* callback of get_res_change. Creates a correspondiung res_change structure in the local list of resource changes */
void ompi_instance_get_res_change_complete (pmix_status_t status, 
		                                            pmix_info_t *results,
		                                            size_t nresults,
                                                    void *cbdata, 
                                                    pmix_release_cbfunc_t release_fn,
                                                    void *release_cbdata)
{
    size_t n, i, k, ninfo;
    pmix_status_t rc;
    size_t sz;
    opal_pmix_lock_t *lock = (opal_pmix_lock_t *) cbdata;
    bool assoc_self = false;

    printf("get_res_change_complete with %d results and status %d\n", nresults, status);

    pmix_info_t * info;
    ompi_mpi_instance_resource_change_t* res_change = OBJ_NEW(ompi_mpi_instance_resource_change_t);
    if(status == PMIX_SUCCESS){
        for(k = 0; k < nresults; k++){

            if(0 == strcmp(results[k].key, PMIX_QUERY_RESULTS)){
                printf("found query results\n");
                info = results[k].value.data.darray->array;
                ninfo = results[k].value.data.darray->size;
        
                if(ninfo >= 4){
                    ompi_instance_lock_rc_and_psets();

                    for (n = 0; n < ninfo; n++) {
                        printf("Result key = %s\n", info[n].key);

                        if (0 == strcmp (info[n].key, PMIX_RC_TYPE)) {
                            res_change->type = info[n].value.data.uint8;
                        } else if (0 == strcmp(info[n].key, PMIX_RC_DELTA)) {

                            ompi_mpi_instance_pset_t *pset = get_pset_by_name(info[n].value.data.string);

                            /* if we don't have this pset already we create a new one */
                            if( NULL == pset){
                                pset = OBJ_NEW(ompi_mpi_instance_pset_t);
                                strcpy(pset->name, info[n].value.data.string);
                                pset->malleable = true;
                                pset->active = true;
                                pset->size = 0;
                                pset->members = NULL;
                                add_pset(pset);
                                
                            }
                            res_change->delta_pset = get_pset_by_name(info[n].value.data.string);
                        } else if (0 == strcmp(info[n].key, PMIX_RC_ASSOC)) {

                            ompi_mpi_instance_pset_t *pset = get_pset_by_name(info[n].value.data.string);

                            /* if we don't have this pset already we create a new one */
                            if( NULL == pset){
                                pset = OBJ_NEW(ompi_mpi_instance_pset_t);
                                strcpy(pset->name, info[n].value.data.string);
                                pset->malleable = true;
                                pset->active = true;
                                pset->size = 0;
                                pset->members = NULL;
                                add_pset(pset);
                            }
                            res_change->bound_pset = get_pset_by_name(info[n].value.data.string);
                        }
                        else if (0 == strcmp(info[n].key, PMIX_QUERY_QUALIFIERS)){

                            pmix_data_array_t *darray = info[n].value.data.darray;
                            pmix_info_t *iptr = (pmix_info_t *) darray->array;
                            for(i = 0; i < darray->size; i++){
                                if(PMIX_CHECK_KEY(&iptr[i], PMIX_RC_ASSOC)){

                                    if(0 == strcmp(iptr[i].value.data.string, "mpi://SELF")){
                                        assoc_self = true;
                                    }
                                }
                            }
                        }
                    }
                
                    if(res_change->type == OMPI_RC_NULL || res_change->delta_pset == NULL){
                        OBJ_RELEASE(res_change);
                    }else{
                        res_change->status = RC_ANNOUNCED;
                        opal_list_append(&ompi_mpi_instance_resource_changes, &res_change->super);
                        if(assoc_self){
                            res_change_bound_to_self = res_change;
                        }
                    }
                    ompi_instance_unlock_rc_and_psets();
                }
            }
        }
    }
    printf("finsihed processing results\n");
    if (NULL != release_fn) {
        release_fn(release_cbdata);
    }
    printf("Wakeup\n");
    if(NULL != lock){
        OPAL_PMIX_WAKEUP_THREAD(lock);
    }
    printf("Proc %d: FINISH get res change complete\n", opal_process_info.myprocid.rank);
}


int get_res_change_info(char *input_name, ompi_rc_op_type_t *type, char *output_name, int *incl, ompi_rc_status_t *status, opal_info_t **info_used, bool get_by_delta_name){
    int ret = OPAL_SUCCESS;
    char pset_search_name[OPAL_MAX_PSET_NAME_LEN];
    pmix_status_t rc;
    pmix_query_t query;
    opal_pmix_lock_t lock;
    bool refresh = true;
    ompi_instance_get_res_change_fn_t get_res_change_active_local = get_by_delta_name ? get_res_change_active_for_name : get_res_change_active_for_bound_name;
    ompi_instance_get_res_change_fn_t get_res_change_local = get_by_delta_name ? get_res_change_for_name : get_res_change_for_bound_name;

    ompi_instance_lock_rc_and_psets();

    if(NULL == input_name){ 
        ompi_instance_unlock_rc_and_psets();
        return OMPI_ERR_BAD_PARAM;
    }

    ompi_mpi_instance_resource_change_t *res_change;
    /* if we don't find a valid & active res change locally, query the runtime. TODO: MPI Info directive QUERY RUNTIME */
    if(NULL == (res_change = get_res_change_active_local(input_name))){
        PMIX_QUERY_CONSTRUCT(&query);
        //PMIX_ARGV_APPEND(rc, query.keys, "PMIX_RC_TYPE");
        //PMIX_ARGV_APPEND(rc, query.keys, "PMIX_RC_PSET");
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_RC_TYPE);
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_RC_ASSOC);
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_RC_DELTA);
        

        query.nqual = 3;
        PMIX_INFO_CREATE(query.qualifiers, 3);
        PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
        PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PROCID, &opal_process_info.myprocid, PMIX_PROC);
        if(get_by_delta_name){
            PMIX_INFO_LOAD(&query.qualifiers[2], PMIX_RC_DELTA, input_name, PMIX_STRING);
        }else{
            PMIX_INFO_LOAD(&query.qualifiers[2], PMIX_RC_ASSOC, input_name, PMIX_STRING);
        }
        
        ompi_instance_unlock_rc_and_psets();
        OPAL_PMIX_CONSTRUCT_LOCK(&lock);
        /*
         * TODO: need to handle this better
         */

        if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(&query, 1, 
                                                     ompi_instance_get_res_change_complete,
                                                     (void*)&lock))) {
           printf("PMIx_Query_info_nb failed with error %d\n", rc);                                              
           
        }
        OPAL_PMIX_WAIT_THREAD(&lock);
        OPAL_PMIX_DESTRUCT_LOCK(&lock);
        ompi_instance_lock_rc_and_psets();
    }
    /* if we did not find an active res change with a delta pset then at least search for invalid ones.
     * If there still aren't any resource changes found return an error.
     */
    if(NULL == (res_change = get_res_change_active_local(input_name)) || NULL == res_change->delta_pset || NULL == res_change->bound_pset){

        if(NULL == (res_change = get_res_change_local(input_name)) || NULL == res_change->delta_pset || NULL == res_change->bound_pset || RC_FINALIZED == res_change->status){
            
            ompi_instance_unlock_rc_and_psets();
            *type = OMPI_RC_NULL;
            *incl = 0;
            return OPAL_ERR_NOT_FOUND;
        }
    }

    /* lookup requested properties of the resource change */
    *type = res_change->type;
    *status = res_change->status;

    ompi_mpi_instance_pset_t *delta_pset_ptr;
    if(NULL != (delta_pset_ptr = res_change->delta_pset)){
        opal_process_name_t *procs = NULL;
        size_t nprocs;
        ompi_instance_unlock_rc_and_psets();
        get_pset_membership(delta_pset_ptr->name, &procs, &nprocs);
        ompi_instance_lock_rc_and_psets();

        /* set the output_name */
        if(get_by_delta_name){
            ompi_mpi_instance_pset_t *assoc_pset_ptr;
            if(NULL != (assoc_pset_ptr = res_change->bound_pset)){
                strcpy(output_name, assoc_pset_ptr->name);
            }           
        }else{
            strcpy(output_name, delta_pset_ptr->name);
        }
        
        *incl = opal_is_pset_member(procs, nprocs, opal_process_info.my_name) ? 1 : 0;
        ompi_instance_free_pset_membership(delta_pset_ptr->name);
    }

    /* reset the res change bound to self. We do this to trigger a lookup next time */
    if(0 == strcmp(input_name, "mpi://SELF")){
        res_change_bound_to_self = NULL;
    }
    /* TODO: provide additional information in info object if requested */

    ompi_instance_unlock_rc_and_psets();
    return OMPI_SUCCESS;
}

int get_res_change_info_collective(pmix_proc_t *coll_procs, size_t n_coll_procs, char *input_name, ompi_rc_op_type_t *type, char *output_name, int *incl, ompi_rc_status_t *status, opal_info_t **info_used, bool get_by_delta_name){
    int ret = OPAL_SUCCESS;
    char pset_search_name[OPAL_MAX_PSET_NAME_LEN];
    pmix_status_t rc;
    pmix_query_t query;
    opal_pmix_lock_t lock;
    bool refresh = true, is_leader;

    ompi_instance_collective_t *coll;

    ompi_instance_get_res_change_fn_t get_res_change_active_local = get_by_delta_name ? get_res_change_active_for_name : get_res_change_active_for_bound_name;
    ompi_instance_get_res_change_fn_t get_res_change_local = get_by_delta_name ? get_res_change_for_name : get_res_change_for_bound_name;

    ompi_instance_lock_rc_and_psets();

    if(NULL == input_name){
        ompi_instance_unlock_rc_and_psets();
        return OMPI_ERR_BAD_PARAM;
    }

    is_leader = is_pset_leader(coll_procs, n_coll_procs, opal_process_info.myprocid);
    

    ompi_mpi_instance_resource_change_t *res_change;
    /* if we don't find a valid & active res change locally, query the runtime. TODO: MPI Info directive QUERY RUNTIME */
    if(NULL == (res_change = get_res_change_active_local(input_name))){
        PMIX_QUERY_CONSTRUCT(&query);
        //PMIX_ARGV_APPEND(rc, query.keys, "PMIX_RC_TYPE");
        //PMIX_ARGV_APPEND(rc, query.keys, "PMIX_RC_PSET");
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_RC_TYPE);
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_RC_ASSOC);
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_RC_DELTA);
        query.nqual = 3;
        PMIX_INFO_CREATE(query.qualifiers, 3);
        PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
        PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PROCID, &opal_process_info.myprocid, PMIX_PROC);

        if(get_by_delta_name){
            PMIX_INFO_LOAD(&query.qualifiers[2], PMIX_RC_DELTA, input_name, PMIX_STRING);
        }else{
            PMIX_INFO_LOAD(&query.qualifiers[2], PMIX_RC_ASSOC, input_name, PMIX_STRING);
        }
        ompi_instance_unlock_rc_and_psets();
        
        if(is_leader){
            OPAL_PMIX_CONSTRUCT_LOCK(&lock);

            create_collective_query(&coll, PMIX_ERR_EMPTY, coll_procs, n_coll_procs, &query, 1, NULL, 0, ompi_instance_get_res_change_complete, &lock);
            /*
             * TODO: need to handle this better
             */
            printf("provider query_info_nb\n");
            printf("created collective with %d coll_procs\n", coll->coll_procs->nprocs);
            if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(&query, 1, 
                                                         ompi_instance_collective_infocb_send,
                                                         (void*)coll))) {
               printf("PMIx_Query_info_nb failed with error %d\n", rc);                                              
            }
            OPAL_PMIX_WAIT_THREAD(&lock);
            OPAL_PMIX_DESTRUCT_LOCK(&lock);

            OBJ_RELEASE(coll);

            
        }else{
            /* No need to provide a lock as cbdata. recv_collective_query is blocking anyways */
            recv_collective_data_query(coll_procs, n_coll_procs, &query, 1, ompi_instance_get_res_change_complete, NULL);
        }
        ompi_instance_lock_rc_and_psets();
    }

    /* if we did not find an active res change with a delta pset then at least search for invalid ones.
     * If there still aren't any resource changes found return an error.
     */
    if(NULL == (res_change = get_res_change_active_local(input_name)) || NULL == res_change->delta_pset || NULL == res_change->bound_pset){
        printf("no active res change found \n");

        if(NULL == (res_change = get_res_change_local(input_name)) || NULL == res_change->delta_pset || NULL == res_change->bound_pset || RC_FINALIZED == res_change->status){
            printf("also no inactive res change found \n");
            ompi_instance_unlock_rc_and_psets();
            *type = OMPI_RC_NULL;
            *incl = 0;
            *status = RC_INVALID;
            return OPAL_ERR_NOT_FOUND;
        }
    }

    /* lookup requested properties of the resource change */
    *type = res_change->type;
    *status = res_change->status;

    ompi_mpi_instance_pset_t *delta_pset_ptr;
    if(NULL != (delta_pset_ptr = res_change->delta_pset)){
        opal_process_name_t *procs = NULL;
        size_t nprocs;
        ompi_instance_unlock_rc_and_psets();
        int res = get_pset_membership(delta_pset_ptr->name, &procs, &nprocs);
        printf("get membership returned %d\n", res);
        ompi_instance_lock_rc_and_psets();

        /* set the output_name */
        if(get_by_delta_name){
            ompi_mpi_instance_pset_t *assoc_pset_ptr;
            if(NULL != (assoc_pset_ptr = res_change->bound_pset)){
                strcpy(output_name, assoc_pset_ptr->name);
            }           
        }else{
            printf("cpy name: %s\n", delta_pset_ptr->name);
            strcpy(output_name, delta_pset_ptr->name);
        }
        printf("Set incl\n");
        *incl = opal_is_pset_member(procs, nprocs, opal_process_info.my_name) ? 1 : 0;
        ompi_instance_free_pset_membership(delta_pset_ptr->name);
    }

    /* reset the res change bound to self. We do this to trigger a lookup next time */
    if(0 == strcmp(input_name, "mpi://SELF")){
        res_change_bound_to_self = NULL;
    }
    /* TODO: provide additional information in info object if requested */
    ompi_instance_unlock_rc_and_psets();

    return OMPI_SUCCESS;
}
