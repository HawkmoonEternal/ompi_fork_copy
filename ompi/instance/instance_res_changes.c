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

static opal_list_t queried_res_changes;

int ompi_instance_res_changes_init(){
    OBJ_CONSTRUCT(&ompi_mpi_instance_resource_changes, opal_list_t);
    OBJ_CONSTRUCT(&queried_res_changes, opal_list_t);

    return ompi_instance_psets_init();
}

int ompi_instance_res_changes_finalize(){
    OBJ_DESTRUCT(&ompi_mpi_instance_resource_changes);
    OBJ_DESTRUCT(&queried_res_changes);

    return ompi_instance_psets_finalize();
}

bool ompi_instance_res_changes_initalized(){
    return initialized;
}

static void ompi_resource_change_constructor(ompi_mpi_instance_resource_change_t *rc){
    rc->delta_psets = rc->bound_psets = NULL;
    rc->ndelta_psets = rc->nbound_psets = 0;
    rc->type = OMPI_PSETOP_NULL;
    rc->status = RC_INVALID;
}
static void ompi_resource_change_destructor(ompi_mpi_instance_resource_change_t *rc){
    free(rc->delta_psets);
    free(rc->bound_psets);
}

OBJ_CLASS_INSTANCE(ompi_mpi_instance_resource_change_t, opal_object_t, ompi_resource_change_constructor, ompi_resource_change_destructor);

static void ompi_res_change_query_cbdata_constructor(res_change_query_cbdata_t *cbdata){
    cbdata->res_change = NULL;
    OPAL_PMIX_CONSTRUCT_LOCK(&cbdata->lock);
}

static void ompi_res_change_query_cbdata_destructor(res_change_query_cbdata_t *cbdata){
    if(NULL != cbdata->res_change){
        OBJ_RELEASE(cbdata->res_change);
    }
    OPAL_PMIX_DESTRUCT_LOCK(&cbdata->lock);
}

OBJ_CLASS_INSTANCE(res_change_query_cbdata_t, opal_object_t, ompi_res_change_query_cbdata_constructor, ompi_res_change_query_cbdata_destructor);



/* delete resource change from local cache */
void rc_finalize_handler(size_t evhdlr_registration_id, pmix_status_t status,
                       const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
                       pmix_info_t results[], size_t nresults,
                       pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata){

    size_t n;
    size_t sz;
    char *pset_name = NULL;
    int rc;
    
    
    ompi_instance_lock_rc_and_psets();
    for(n = 0; n < ninfo; n++){
        if(0 == strcmp(info[n].key, PMIX_PSET_NAME)){
            PMIX_VALUE_UNLOAD(rc, &info[n].value, (void**)&pset_name, &sz);
            if(PMIX_SUCCESS != rc){
                ompi_instance_unlock_rc_and_psets();
                return;
            }
        }
    }

    if(NULL != pset_name){
        ompi_mpi_instance_resource_change_t *res_change;
        
        if(NULL != (res_change = get_res_change_active_for_output_name(pset_name))){
            res_change = (ompi_mpi_instance_resource_change_t *) opal_list_remove_item(&ompi_mpi_instance_resource_changes, &res_change->super);
            res_change->status = RC_FINALIZED;
            OBJ_RELEASE(res_change);
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

static ompi_mpi_instance_resource_change_t * copy_res_change(ompi_mpi_instance_resource_change_t *original){
    
    ompi_mpi_instance_resource_change_t* copy = OBJ_NEW(ompi_mpi_instance_resource_change_t);

    copy->type = original->type;
    copy->status = original->status;
    copy->ndelta_psets = original->ndelta_psets;
    copy->nbound_psets = original->nbound_psets;

    copy->delta_psets = malloc(copy->ndelta_psets * sizeof (ompi_mpi_instance_pset_t *));
    copy->bound_psets = malloc(copy->nbound_psets * sizeof (ompi_mpi_instance_pset_t *));

    size_t n;
    for(n = 0; n < copy->ndelta_psets; n++){
        copy->delta_psets[n] = original->delta_psets[n];
    }
    for(n = 0; n < copy->nbound_psets; n++){
        copy->bound_psets[n] = original->bound_psets[n];
    }

    return copy;

}

static bool cmp_res_change(ompi_mpi_instance_resource_change_t *rc1, ompi_mpi_instance_resource_change_t *rc2){

    if(NULL != rc1->delta_psets && NULL == rc2->delta_psets)return false;
    if(NULL == rc1->delta_psets && NULL != rc2->delta_psets)return false;
    if(NULL != rc1->bound_psets && NULL == rc2->delta_psets)return false;
    if(NULL == rc1->bound_psets && NULL != rc2->bound_psets)return false;
    if(rc1->ndelta_psets != rc2->ndelta_psets)return false;
    if(rc1->nbound_psets != rc2->nbound_psets)return false;
    if(rc1->type != rc2->type)return false;


    for(size_t n = 0; n < rc1->ndelta_psets; n++){
        if(0 != strcmp(rc1->delta_psets[n]->name, rc2->delta_psets[n]->name)){
            return false;
        }
    }

    for(size_t n = 0; n < rc1->nbound_psets; n++){
        if(0 != strcmp(rc1->bound_psets[n]->name, rc2->bound_psets[n]->name)){
            return false;
        }
    }

    return true;
}

static bool res_change_already_defined(ompi_mpi_instance_resource_change_t *res_change){
    ompi_mpi_instance_resource_change_t *rc_ptr = NULL;
    OPAL_LIST_FOREACH(rc_ptr, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
       if(cmp_res_change(res_change, rc_ptr)){
            return true;
       }
    }

    return false;
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
    PMIX_INFO_LOAD(&info_ptr[0], PMIX_QUERY_PSETOP_TYPE, &res_change->type, PMIX_UINT8);

    /* Load the input psets */
    PMIX_DATA_ARRAY_CONSTRUCT(&darray, res_change->nbound_psets, PMIX_VALUE);
    val_ptr = (pmix_value_t *) darray.array;
    for(n = 0; n < res_change->nbound_psets; n++){
        PMIX_VALUE_LOAD(&val_ptr[n], res_change->bound_psets[n]->name, PMIX_STRING);
    }
    PMIX_INFO_LOAD(&info_ptr[1], PMIX_QUERY_PSETOP_INPUT, &darray, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_DESTRUCT(&darray);

    /* Load the output psets */
    PMIX_DATA_ARRAY_CONSTRUCT(&darray, res_change->ndelta_psets, PMIX_VALUE);
    val_ptr = (pmix_value_t *) darray.array;
    for(n = 0; n < res_change->ndelta_psets; n++){
        PMIX_VALUE_LOAD(&val_ptr[n], res_change->delta_psets[n]->name, PMIX_STRING);
    }
    PMIX_INFO_LOAD(&info_ptr[2], PMIX_QUERY_PSETOP_OUTPUT, &darray, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_DESTRUCT(&darray);

    /* Load 0 qualifiers */
    PMIX_DATA_ARRAY_CONSTRUCT(&darray, 0, PMIX_INFO);
    PMIX_INFO_LOAD(&info_ptr[3], PMIX_QUERY_QUALIFIERS, &darray, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_DESTRUCT(&darray);


    PMIX_INFO_LOAD(&(*results)[0], PMIX_QUERY_RESULTS, &darray_results, PMIX_DATA_ARRAY);


}

int ompi_instance_get_rc_type( char *delta_pset, ompi_psetop_type_t *rc_type);

int ompi_instance_get_rc_type( char *delta_pset, ompi_psetop_type_t *rc_type){
    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *res_change;
    res_change = get_res_change_for_name(delta_pset);
    *rc_type = res_change->type;
    ompi_instance_unlock_rc_and_psets();
    return OPAL_SUCCESS;
}


void res_change_clear_cache(char *delta_pset){

    ompi_instance_lock_rc_and_psets();
    ompi_mpi_instance_resource_change_t *rc_remove;
    
    if(NULL == (rc_remove = get_res_change_active_for_name(delta_pset))){
        rc_remove = get_res_change_active_for_output_name(delta_pset);
    }
    

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
    res_change_query_cbdata_t  *query_cbdata = (res_change_query_cbdata_t *) cbdata;
    pmix_value_t *val_ptr;

    pmix_info_t * info;
    ompi_mpi_instance_resource_change_t* queried_res_change, *res_change = OBJ_NEW(ompi_mpi_instance_resource_change_t);
    if(status == PMIX_SUCCESS){
        for(k = 0; k < nresults; k++){

            if(0 == strcmp(results[k].key, PMIX_QUERY_RESULTS)){
                
                info = results[k].value.data.darray->array;
                ninfo = results[k].value.data.darray->size;
                if(ninfo >= 4){
                    
                    ompi_instance_lock_rc_and_psets();

                    for (n = 0; n < ninfo; n++) {
                        if (0 == strcmp (info[n].key, PMIX_QUERY_PSETOP_TYPE)) {
                            res_change->type = info[n].value.data.uint8;

                        } else if (0 == strcmp(info[n].key, PMIX_QUERY_PSETOP_OUTPUT)) {
                            
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
                        } else if (0 == strcmp(info[n].key, PMIX_QUERY_PSETOP_INPUT)) {
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
                    }
                
                    if(res_change->type == OMPI_PSETOP_NULL || res_change->delta_psets == NULL){
                        OBJ_RELEASE(res_change);
                    }else{
                        res_change->status = RC_ANNOUNCED;
                        queried_res_change = copy_res_change(res_change);
                        if(NULL != query_cbdata){
                            query_cbdata->res_change = queried_res_change;
                        }
                        if(!res_change_already_defined(res_change)){
                            opal_list_append(&ompi_mpi_instance_resource_changes, &res_change->super);
                        }else{
                            OBJ_RELEASE(res_change);
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
    if(NULL != query_cbdata){
        OPAL_PMIX_WAKEUP_THREAD(&query_cbdata->lock);
    }
}


int get_res_change_info(char *input_name, ompi_psetop_type_t *type, char ***output_names, size_t *noutput_names, int *incl, ompi_rc_status_t *status, opal_info_t **info_used, bool get_by_delta_name){
    size_t n;
    pmix_status_t rc;
    pmix_query_t query;
    opal_pmix_lock_t lock;
    bool refresh = true;
    res_change_query_cbdata_t *query_cbdata = NULL;

    ompi_instance_lock_rc_and_psets();

    if(NULL == input_name){ 
        ompi_instance_unlock_rc_and_psets();
        return OMPI_ERR_BAD_PARAM;
    }

    ompi_mpi_instance_resource_change_t *res_change;
    /* if we don't find a valid & active res change locally, query the runtime. TODO: MPI Info directive QUERY RUNTIME */

    if(NULL == (res_change = get_res_change_active_for_name(input_name))){
        
        

        PMIX_QUERY_CONSTRUCT(&query);
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSETOP_TYPE);
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSETOP_INPUT);
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSETOP_OUTPUT);
        

        query.nqual = 3;
        PMIX_INFO_CREATE(query.qualifiers, 3);
        PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
        PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PROCID, &opal_process_info.myprocid, PMIX_PROC);
        PMIX_INFO_LOAD(&query.qualifiers[2], PMIX_PSET_NAME, input_name, PMIX_STRING);
        
        ompi_instance_unlock_rc_and_psets();
        
        /*
         * TODO: need to handle this better
         */
        query_cbdata = OBJ_NEW(res_change_query_cbdata_t);

        if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(&query, 1, 
                                                     ompi_instance_get_res_change_complete,
                                                     (void*)query_cbdata))) {
           printf("PMIx_Query_info_nb failed with error %d\n", rc);                                              
           
        }
        OPAL_PMIX_WAIT_THREAD(&query_cbdata->lock);

        res_change = query_cbdata->res_change;

        ompi_instance_lock_rc_and_psets();
    }

    /* 
     * If there still aren't any resource changes found return OMPI_PSETOP_NULL.
     */
    if(NULL == res_change){
        ompi_instance_unlock_rc_and_psets();
        *type = OMPI_PSETOP_NULL;
        *incl = 0;

        if(NULL != query_cbdata){
            OBJ_RELEASE(query_cbdata);
        }

        return OPAL_ERR_NOT_FOUND;
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

    if(NULL != query_cbdata){
        OBJ_RELEASE(query_cbdata);
    }

    ompi_instance_unlock_rc_and_psets();
    return OMPI_SUCCESS;
}

int get_res_change_info_collective(pmix_proc_t *coll_procs, size_t n_coll_procs, char *input_name, ompi_psetop_type_t *type, char ***output_names, size_t *noutput_names, int *incl, ompi_rc_status_t *status, opal_info_t **info_used, bool get_by_delta_name){
    size_t n, nresults;
    pmix_status_t rc;
    pmix_query_t query;
    pmix_info_t *results;
    opal_pmix_lock_t lock;
    bool refresh = true, is_leader;
    res_change_query_cbdata_t *query_cbdata = NULL;

    ompi_instance_collective_t *coll;

    if(NULL == input_name){
        return OMPI_ERR_BAD_PARAM;
    }


    is_leader = is_pset_leader(coll_procs, n_coll_procs, opal_process_info.myprocid);

    /* Construct the query */
    PMIX_QUERY_CONSTRUCT(&query);
    PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSETOP_TYPE);
    PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSETOP_INPUT);
    PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSETOP_OUTPUT);
    query.nqual = 3;
    PMIX_INFO_CREATE(query.qualifiers, 3);
    PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
    PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PROCID, &opal_process_info.myprocid, PMIX_PROC);
    PMIX_INFO_LOAD(&query.qualifiers[2], PMIX_PSET_NAME, input_name, PMIX_STRING);

    query_cbdata = OBJ_NEW(res_change_query_cbdata_t);

    ompi_mpi_instance_resource_change_t *res_change, *queried_res_change;

    ompi_instance_lock_rc_and_psets();
    if(is_leader){
        
        if(NULL == (res_change = get_res_change_active_for_name(input_name))){
            /* if we don't find a valid & active res change locally, query the runtime. */
            ompi_instance_unlock_rc_and_psets();

            create_collective_query(&coll, PMIX_ERR_EMPTY, coll_procs, n_coll_procs, &query, 1, NULL, 0, ompi_instance_get_res_change_complete, query_cbdata);  
            
            if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(&query, 1, 
                                                         ompi_instance_collective_infocb_send,
                                                         (void*)coll))) {
               printf("PMIx_Query_info_nb failed with error %d\n", rc);                                              
            }
            OPAL_PMIX_WAIT_THREAD(&query_cbdata->lock);
            res_change = query_cbdata->res_change;

            ompi_instance_lock_rc_and_psets();
        }else{      
            /* We have found a resource change locally, so send the result to the collective procs */
            get_res_change_as_query_result(res_change, &results, &nresults);
            send_collective_data_query(coll_procs, PMIX_SUCCESS, n_coll_procs, &query, 1, results, nresults);
            PMIX_INFO_FREE(results, nresults);
        }
    }else{
        ompi_instance_unlock_rc_and_psets();
        /* No need to provide a to wait on lock in cbdata. recv_collective_query is blocking anyways */
        recv_collective_data_query(coll_procs, n_coll_procs, &query, 1, ompi_instance_get_res_change_complete, query_cbdata);
        res_change = query_cbdata->res_change;
        ompi_instance_lock_rc_and_psets();
    }

    /* If there still aren't any resource changes found return an error */
    //if(NULL == (res_change = get_res_change_active_for_name(input_name))){
    
    if( NULL == res_change){
        ompi_instance_unlock_rc_and_psets();
        *type = OMPI_PSETOP_NULL;
        *incl = 0;

        if(NULL != query_cbdata){
            OBJ_RELEASE(query_cbdata);
        }

        return OPAL_ERR_NOT_FOUND;
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

    if(NULL != query_cbdata){
        OBJ_RELEASE(query_cbdata);
    }

    ompi_instance_unlock_rc_and_psets();
    return OMPI_SUCCESS;
}

int get_res_change_info_collective_nb(pmix_proc_t *coll_procs, size_t n_coll_procs, char *input_name, pmix_info_cbfunc_t cbfunc, void *cbdata){
    size_t nresults;
    pmix_status_t rc;
    pmix_query_t query;
    pmix_info_t *results;
    bool refresh = true, is_leader;

    ompi_instance_collective_t *coll;

    if(NULL == input_name){
        return OMPI_ERR_BAD_PARAM;
    }

    is_leader = is_pset_leader(coll_procs, n_coll_procs, opal_process_info.myprocid);

    /* Construct the query */
    PMIX_QUERY_CONSTRUCT(&query);
    PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSETOP_TYPE);
    PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSETOP_INPUT);
    PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSETOP_OUTPUT);
    query.nqual = 3;
    PMIX_INFO_CREATE(query.qualifiers, 3);
    PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
    PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PROCID, &opal_process_info.myprocid, PMIX_PROC);
    PMIX_INFO_LOAD(&query.qualifiers[2], PMIX_PSET_NAME, input_name, PMIX_STRING);


    create_collective_query(&coll, PMIX_ERR_EMPTY, coll_procs, n_coll_procs, &query, 1, NULL, 0, cbfunc, cbdata);    


    ompi_mpi_instance_resource_change_t *res_change;

    if(is_leader){
        /* if we do not find a valid & active res change locally, query the runtime. */
        if(NULL == (res_change = get_res_change_active_for_name(input_name))){

            if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(&query, 1, 
                                                         ompi_instance_collective_infocb_send,
                                                         (void*)coll))) {
               printf("PMIx_Query_info_nb failed with error %d\n", rc);                                              
            }
            
        }else{
            /* We have found a resource change locally, so send the result to the collective procs */
            get_res_change_as_query_result(res_change, &results, &nresults);
            ompi_instance_collective_infocb_send(PMIX_SUCCESS, results, nresults, (void *) coll, NULL, NULL);
            PMIX_INFO_FREE(results, nresults);
            rc = PMIX_SUCCESS;
        }
        
    }else{
        
        rc = recv_collective_data_query(coll_procs, n_coll_procs, &query, 1, cbfunc, cbdata);
    }
    
    PMIX_QUERY_DESTRUCT(&query);

    return rc;
}
