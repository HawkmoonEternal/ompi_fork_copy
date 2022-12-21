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
    rc->delta_psets = rc->bound_psets = NULL;
    rc->ndelta_psets = rc->nbound_psets = 0;
    rc->type = OMPI_RC_NULL;
    rc->status = RC_INVALID;
}
static void ompi_resource_change_destructor(ompi_mpi_instance_resource_change_t *rc){
    free(rc->delta_psets);
    free(rc->bound_psets);
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

ompi_mpi_instance_resource_change_t * get_res_change_for_output_name(char *name){
    size_t n;
    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->delta_psets)
            for(n = 0; n < rc_out->ndelta_psets; n++){
                if(0 == strncmp(name, rc_out->delta_psets[n]->name, MPI_MAX_PSET_NAME_LEN)){
                ompi_instance_unlock_rc_and_psets();
                return rc_out;
            }
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return NULL;
}

ompi_mpi_instance_resource_change_t * get_res_change_for_input_name(char *name){
    size_t n;

    if(0 == strcmp(name, "mpi://SELF")){
        return res_change_bound_to_self;
    }
    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->bound_psets){ 
            for(n = 0; n < rc_out->nbound_psets; n++){
                if(0 == strcmp(name, rc_out->bound_psets[n]->name)){
                    ompi_instance_unlock_rc_and_psets();
                    return rc_out;
                }
            }
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return NULL; 
}

ompi_mpi_instance_resource_change_t * get_res_change_for_name(char *name){
    size_t n;

    if(0 == strcmp(name, "mpi://SELF")){
        return res_change_bound_to_self;
    }
    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->bound_psets){ 
            for(n = 0; n < rc_out->nbound_psets; n++){
                if(0 == strcmp(name, rc_out->bound_psets[n]->name)){
                    ompi_instance_unlock_rc_and_psets();
                    return rc_out;
                }
            }
        }
        if(NULL != rc_out->delta_psets){
            for(n = 0; n < rc_out->ndelta_psets; n++){
                if(0 == strcmp(name, rc_out->delta_psets[n]->name)){
                    ompi_instance_unlock_rc_and_psets();
                    return rc_out;
                }
            }
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return NULL; 
}

ompi_mpi_instance_resource_change_t * get_res_change_active_for_name(char *name){
    size_t n;
    if(0 == strcmp(name, "mpi://SELF")){
        return res_change_bound_to_self;
    }
    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->bound_psets){
            for(n = 0; n < rc_out->nbound_psets; n++){
                if(0 == strcmp(name, rc_out->bound_psets[n]->name) && rc_out->status != RC_INVALID && rc_out->status != RC_FINALIZED){
                    ompi_instance_unlock_rc_and_psets();
                    return rc_out;
                }
            }
        }
        if(NULL != rc_out->delta_psets){
            for(n = 0; n < rc_out->ndelta_psets; n++){
                if(0 == strcmp(name, rc_out->delta_psets[n]->name) && rc_out->status != RC_INVALID && rc_out->status != RC_FINALIZED){
                    ompi_instance_unlock_rc_and_psets();
                    return rc_out;
                }
            }
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return NULL; 
}

ompi_mpi_instance_resource_change_t * get_res_change_active_for_input_name(char *name){
    size_t n;
    if(0 == strcmp(name, "mpi://SELF")){
        return res_change_bound_to_self;
    }
    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->bound_psets){
            for(n = 0; n < rc_out->nbound_psets; n++){
                if(0 == strcmp(name,rc_out->bound_psets[n]->name) && rc_out->status != RC_INVALID && rc_out->status != RC_FINALIZED){
                    ompi_instance_unlock_rc_and_psets();
                    return rc_out;
                }
            }
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return NULL; 
}

ompi_mpi_instance_resource_change_t * get_res_change_active_for_output_name(char *name){
    size_t n;
    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->delta_psets){
            for(n = 0; n < rc_out->ndelta_psets; n++){
                if(0 == strcmp(name, rc_out->delta_psets[n]->name) && rc_out->status != RC_INVALID && rc_out->status != RC_FINALIZED){
                    ompi_instance_unlock_rc_and_psets();
                    return rc_out;
                }
            }
        }
    }
    ompi_instance_unlock_rc_and_psets();
    return NULL; 
}

static void get_res_change_as_query_result(ompi_mpi_instance_resource_change_t *res_change, pmix_info_t **results, size_t *nresults){
    size_t n;
    pmix_data_array_t darray, darray_results;
    pmix_value_t *val_ptr;
    pmix_info_t *info_ptr;

    /* For now we always deafult to 3 attributes */
    *nresults = 1;
    PMIX_INFO_CREATE(*results, 1);
    PMIX_DATA_ARRAY_CONSTRUCT(&darray_results, 4, PMIX_INFO);
    info_ptr = (pmix_info_t *) darray_results.array;

    /* Load the type*/
    PMIX_INFO_LOAD(&info_ptr[0], PMIX_RC_TYPE, &res_change->type, PMIX_UINT8);

    /* Load the input psets */
    PMIX_DATA_ARRAY_CONSTRUCT(&darray, res_change->nbound_psets, PMIX_VALUE);
    val_ptr = (pmix_value_t *) darray.array;
    for(n = 0; n < res_change->nbound_psets; n++){
        PMIX_VALUE_LOAD(&val_ptr[n], res_change->bound_psets[n]->name, PMIX_STRING);
    }
    PMIX_INFO_LOAD(&info_ptr[1], PMIX_RC_ASSOC, &darray, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_DESTRUCT(&darray);

    /* Load the output psets */
    PMIX_DATA_ARRAY_CONSTRUCT(&darray, res_change->ndelta_psets, PMIX_VALUE);
    val_ptr = (pmix_value_t *) darray.array;
    for(n = 0; n < res_change->ndelta_psets; n++){
        PMIX_VALUE_LOAD(&val_ptr[n], res_change->delta_psets[n]->name, PMIX_STRING);
    }
    PMIX_INFO_LOAD(&info_ptr[2], PMIX_RC_DELTA, &darray, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_DESTRUCT(&darray);

    /* Load 0 qualifiers */
    PMIX_DATA_ARRAY_CONSTRUCT(&darray, 0, PMIX_INFO);
    PMIX_INFO_LOAD(&info_ptr[3], PMIX_QUERY_QUALIFIERS, &darray, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_DESTRUCT(&darray);


    PMIX_INFO_LOAD(&(*results)[0], PMIX_QUERY_RESULTS, &darray_results, PMIX_DATA_ARRAY);


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
    if(rc_out->bound_psets == NULL){
        printf(", bound pset: NULL]\n");
    }
    printf(", bound pset: %s]\n", rc_out->bound_psets[0]->name);
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
    size_t n, i, k, ninfo, size;
    pmix_status_t rc;
    size_t sz;
    opal_pmix_lock_t *lock = (opal_pmix_lock_t *) cbdata;
    bool assoc_self = false;
    pmix_value_t *val_ptr;

    pmix_info_t * info;
    ompi_mpi_instance_resource_change_t* res_change = OBJ_NEW(ompi_mpi_instance_resource_change_t);
    if(status == PMIX_SUCCESS){
        for(k = 0; k < nresults; k++){

            if(0 == strcmp(results[k].key, PMIX_QUERY_RESULTS)){
                
                info = results[k].value.data.darray->array;
                ninfo = results[k].value.data.darray->size;
                if(ninfo >= 4){
                    
                    ompi_instance_lock_rc_and_psets();

                    for (n = 0; n < ninfo; n++) {

                        if (0 == strcmp (info[n].key, PMIX_RC_TYPE)) {
                            res_change->type = info[n].value.data.uint8;

                        } else if (0 == strcmp(info[n].key, PMIX_RC_DELTA)) {
                            
                            res_change->ndelta_psets = info[n].value.data.darray->size;
                            res_change->delta_psets = malloc(res_change->ndelta_psets * sizeof(ompi_mpi_instance_pset_t *));
                            val_ptr = (pmix_value_t *) info[n].value.data.darray->array;
                            for(i = 0; i < res_change->ndelta_psets; i++){
                                ompi_mpi_instance_pset_t *pset = get_pset_by_name(val_ptr[i].data.string);
                                /* if we don't have this pset already we create a new one */
                                if( NULL == pset){
                                    pset = OBJ_NEW(ompi_mpi_instance_pset_t);
                                    strcpy(pset->name, val_ptr[i].data.string);
                                    pset->malleable = true;
                                    pset->active = true;
                                    pset->size = 0;
                                    pset->members = NULL;
                                    add_pset(pset);

                                }
                                res_change->delta_psets[i] = get_pset_by_name(val_ptr[i].data.string);
                            }
                        } else if (0 == strcmp(info[n].key, PMIX_RC_ASSOC)) {
                            res_change->nbound_psets = info[n].value.data.darray->size;
                            res_change->bound_psets = malloc(res_change->nbound_psets * sizeof(ompi_mpi_instance_pset_t *));
                            val_ptr = (pmix_value_t *) info[n].value.data.darray->array;
                            for(i = 0; i < res_change->nbound_psets; i++){

                                ompi_mpi_instance_pset_t *pset = get_pset_by_name(val_ptr[i].data.string);

                                /* if we don't have this pset already we create a new one */
                                if( NULL == pset){
                                    pset = OBJ_NEW(ompi_mpi_instance_pset_t);
                                    strcpy(pset->name, val_ptr[i].data.string);
                                    pset->malleable = true;
                                    pset->active = true;
                                    pset->size = 0;
                                    pset->members = NULL;
                                    add_pset(pset);
                                }
                                res_change->bound_psets[i] = get_pset_by_name(val_ptr[i].data.string);
                            }
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
                
                    if(res_change->type == OMPI_RC_NULL || res_change->delta_psets == NULL){
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
    
    if (NULL != release_fn) {
        release_fn(release_cbdata);
    }
    if(NULL != lock){
        OPAL_PMIX_WAKEUP_THREAD(lock);
    }
}


int get_res_change_info(char *input_name, ompi_rc_op_type_t *type, char ***output_names, size_t *noutput_names, int *incl, ompi_rc_status_t *status, opal_info_t **info_used, bool get_by_delta_name){
    int ret = OPAL_SUCCESS;
    size_t n;
    char pset_search_name[OPAL_MAX_PSET_NAME_LEN];
    pmix_status_t rc;
    pmix_query_t query;
    opal_pmix_lock_t lock;
    bool refresh = true;
    ompi_instance_lock_rc_and_psets();

    if(NULL == input_name){ 
        ompi_instance_unlock_rc_and_psets();
        return OMPI_ERR_BAD_PARAM;
    }
    

    ompi_mpi_instance_resource_change_t *res_change;
    /* if we don't find a valid & active res change locally, query the runtime. TODO: MPI Info directive QUERY RUNTIME */

    if(NULL == (res_change = get_res_change_active_for_name(input_name))){
        PMIX_QUERY_CONSTRUCT(&query);
        //PMIX_ARGV_APPEND(rc, query.keys, "PMIX_RC_TYPE");
        //PMIX_ARGV_APPEND(rc, query.keys, "PMIX_RC_PSET");
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_RC_TYPE);
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_RC_ASSOC);
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_RC_DELTA);
        

        query.nqual = 4;
        PMIX_INFO_CREATE(query.qualifiers, 4);
        PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
        PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PROCID, &opal_process_info.myprocid, PMIX_PROC);
        PMIX_INFO_LOAD(&query.qualifiers[2], PMIX_RC_DELTA, input_name, PMIX_STRING);
        PMIX_INFO_LOAD(&query.qualifiers[3], PMIX_RC_ASSOC, input_name, PMIX_STRING);
        
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
    if(NULL == (res_change = get_res_change_active_for_name(input_name)) || NULL == res_change->delta_psets || NULL == res_change->bound_psets){
        if(NULL == (res_change = get_res_change_for_name(input_name)) || NULL == res_change->delta_psets || NULL == res_change->bound_psets || RC_FINALIZED == res_change->status){

            ompi_instance_unlock_rc_and_psets();
            *type = OMPI_RC_NULL;
            *incl = 0;
            return OPAL_ERR_NOT_FOUND;
        }
    }


    /* lookup requested properties of the resource change */
    *type = res_change->type;
    *status = res_change->status;
    *incl = 0;

    if(get_by_delta_name){
        *noutput_names = res_change->nbound_psets;
        *output_names = malloc(res_change->nbound_psets * sizeof(char *));
    }else{
        *noutput_names = res_change->ndelta_psets;
        *output_names = malloc(res_change->ndelta_psets * sizeof(char *));
    }

    ompi_mpi_instance_pset_t *delta_pset_ptr;
    for(n = 0; n < res_change->ndelta_psets; n++){
        if(NULL != (delta_pset_ptr = res_change->delta_psets[n])){
            opal_process_name_t *procs = NULL;
            size_t nprocs;
            ompi_instance_unlock_rc_and_psets();
            get_pset_membership(delta_pset_ptr->name, &procs, &nprocs);
            ompi_instance_lock_rc_and_psets();
            if(*incl != 1){
                *incl = (opal_is_pset_member(procs, nprocs, opal_process_info.my_name) ? 1 : 0);
            }
            ompi_instance_free_pset_membership(delta_pset_ptr->name);
        }
        /* If they asked for delta psets, copy it to the output array */
        if(!get_by_delta_name){
            
            (*output_names)[n] = strdup(delta_pset_ptr->name);    
        }

    }

    
    /* If they asked for assoc psets, copy them to the output array */
    if(get_by_delta_name){
        for(n = 0; n < res_change->nbound_psets; n++){
            (*output_names)[n] = strdup(res_change->bound_psets[n]->name);
        }        
    }
    
    /* reset the res change bound to self. We do this to trigger a lookup next time */
    if(0 == strcmp(input_name, "mpi://SELF")){
        res_change_bound_to_self = NULL;
    }
    /* TODO: provide additional information in info object if requested */

    ompi_instance_unlock_rc_and_psets();
    return OMPI_SUCCESS;
}

int get_res_change_info_collective(pmix_proc_t *coll_procs, size_t n_coll_procs, char *input_name, ompi_rc_op_type_t *type, char ***output_names, size_t *noutput_names, int *incl, ompi_rc_status_t *status, opal_info_t **info_used, bool get_by_delta_name){
    int ret = OPAL_SUCCESS;
    size_t n, nresults;
    char pset_search_name[OPAL_MAX_PSET_NAME_LEN];
    pmix_status_t rc;
    pmix_query_t query;
    pmix_info_t *results;
    opal_pmix_lock_t lock;
    bool refresh = true, is_leader, sent = false;

    ompi_instance_collective_t *coll;

    if(NULL == input_name){
        return OMPI_ERR_BAD_PARAM;
    }

    is_leader = is_pset_leader(coll_procs, n_coll_procs, opal_process_info.myprocid);

    /* Construct the query */
    PMIX_QUERY_CONSTRUCT(&query);
    PMIX_ARGV_APPEND(rc, query.keys, PMIX_RC_TYPE);
    PMIX_ARGV_APPEND(rc, query.keys, PMIX_RC_ASSOC);
    PMIX_ARGV_APPEND(rc, query.keys, PMIX_RC_DELTA);
    query.nqual = 4;
    PMIX_INFO_CREATE(query.qualifiers, 4);
    PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
    PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PROCID, &opal_process_info.myprocid, PMIX_PROC);
    PMIX_INFO_LOAD(&query.qualifiers[2], PMIX_RC_DELTA, input_name, PMIX_STRING);
    PMIX_INFO_LOAD(&query.qualifiers[3], PMIX_RC_ASSOC, input_name, PMIX_STRING);

    create_collective_query(&coll, PMIX_ERR_EMPTY, coll_procs, n_coll_procs, &query, 1, NULL, 0, ompi_instance_get_res_change_complete, &lock);    


    ompi_mpi_instance_resource_change_t *res_change;

    if(is_leader){
        /* if we don't find a valid & active res change locally, query the runtime. */
        if(NULL == (res_change = get_res_change_active_for_name(input_name))){
            OPAL_PMIX_CONSTRUCT_LOCK(&lock);
            if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(&query, 1, 
                                                         ompi_instance_collective_infocb_send,
                                                         (void*)coll))) {
               printf("PMIx_Query_info_nb failed with error %d\n", rc);                                              
            }
            OPAL_PMIX_WAIT_THREAD(&lock);
            OPAL_PMIX_DESTRUCT_LOCK(&lock);
            OBJ_RELEASE(coll);
        }else{
            /* We have found a resource change locally, so send the result to the collective procs */
            get_res_change_as_query_result(res_change, &results, &nresults);
            send_collective_data_query(coll_procs, PMIX_SUCCESS, n_coll_procs, &query, 1, results, nresults);
            PMIX_INFO_FREE(results, nresults);
        }
    }else{
        /* No need to provide a lock as cbdata. recv_collective_query is blocking anyways */
        recv_collective_data_query(coll_procs, n_coll_procs, &query, 1, ompi_instance_get_res_change_complete, NULL);
    }

    /* If there still aren't any resource changes found return an error */
    if(NULL == (res_change = get_res_change_active_for_name(input_name))){
        *type = OMPI_RC_NULL;
        *incl = 0;
        return OPAL_ERR_NOT_FOUND;
        
    }

    ompi_instance_lock_rc_and_psets();
    /* lookup requested properties of the resource change */
    *type = res_change->type;
    *status = res_change->status;
    *incl = 0;
    if(get_by_delta_name){
        *noutput_names = res_change->nbound_psets;
        *output_names = malloc(res_change->nbound_psets * sizeof(char *));
    }else{
        *noutput_names = res_change->ndelta_psets;
        *output_names = malloc(res_change->ndelta_psets * sizeof(char *));
    }
    ompi_mpi_instance_pset_t *delta_pset_ptr;
    for(n = 0; n < res_change->ndelta_psets; n++){
        if(NULL != (delta_pset_ptr = res_change->delta_psets[n])){
            opal_process_name_t *procs = NULL;
            size_t nprocs;
            ompi_instance_unlock_rc_and_psets();
            get_pset_membership(delta_pset_ptr->name, &procs, &nprocs);
            ompi_instance_lock_rc_and_psets();
            if(*incl != 1){
                *incl = (opal_is_pset_member(procs, nprocs, opal_process_info.my_name) ? 1 : 0);
            }
            ompi_instance_free_pset_membership(delta_pset_ptr->name);
        }
        /* If they asked for delta psets, copy it to the output array */
        if(!get_by_delta_name){
            (*output_names)[n] = strdup(delta_pset_ptr->name);    
        }

    }
    
    /* If they asked for assoc psets, copy them to the output array */
    if(get_by_delta_name){
        for(n = 0; n < res_change->nbound_psets; n++){
            (*output_names)[n] = strdup(res_change->bound_psets[n]->name);
        }        
    }
    
    /* reset the res change bound to self. We do this to trigger a lookup next time */
    if(0 == strcmp(input_name, "mpi://SELF")){
        res_change_bound_to_self = NULL;
    }
    /* TODO: provide additional information in info object if requested */

    ompi_instance_unlock_rc_and_psets();
    return OMPI_SUCCESS;
}
