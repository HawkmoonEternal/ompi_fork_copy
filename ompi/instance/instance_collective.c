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

#include "ompi/instance/instance_collective.h"

/* List of pending collectives */
static opal_list_t ompi_instance_pending_collectives;

/* Lock the list of pending collectives */
static opal_recursive_mutex_t collectives_lock = OPAL_RECURSIVE_MUTEX_STATIC_INIT;


/* CLASS INSTANCE */
static void collective_constructor(ompi_instance_collective_t *coll);
static void collective_destructor(ompi_instance_collective_t *coll);

void coll_results_release(ompi_collective_results_t *results);
void coll_params_release(ompi_collective_parameters_t *params);
void coll_procs_release(ompi_collective_procs_t *procs);
void coll_cbfunc_release(ompi_collective_cbfunc_t *cbfunc);

void coll_params_create(ompi_collective_parameters_t **coll_params, ompi_parameters_type_t type);
void coll_params_create(ompi_collective_parameters_t **coll_params, ompi_parameters_type_t type);
void coll_params_load_info(ompi_collective_parameters_t *coll_params, pmix_info_t *info, size_t ninfo);
void coll_params_load_query(ompi_collective_parameters_t *coll_params, pmix_query_t *query, size_t nqueries);
void coll_params_load_pdata(ompi_collective_parameters_t *coll_params, pmix_pdata_t *pdata, size_t npdata, pmix_info_t *info, size_t ninfo);

void info_params_release(ompi_info_parameters_t *params);
void query_params_release(ompi_query_parameters_t *params);
void pdata_params_release(ompi_pdata_parameters_t *params);
void info_results_release(ompi_info_results_t *results);
void ompi_collective_release_func(void *cbdata);

void coll_results_create(ompi_collective_results_t **coll_results, ompi_results_type_t type);
void coll_results_load_info(ompi_collective_results_t *coll_results, pmix_info_t *info, size_t ninfo);

void coll_cbfunc_create(ompi_collective_cbfunc_t **coll_cbfunc, ompi_cbfunc_type_t type);
void coll_cbfunc_load_info(ompi_collective_cbfunc_t *coll_cbfunc, pmix_info_cbfunc_t func);

void coll_procs_create(ompi_collective_procs_t **coll_procs, pmix_proc_t *procs, size_t nprocs);

void ompi_procs_serialize(ompi_collective_procs_t *coll_procs, pmix_info_t *info);
void ompi_results_serialize(ompi_collective_results_t *coll_results, pmix_info_t *info);
void ompi_params_serialize(ompi_collective_parameters_t *coll_params, pmix_info_t *info);
void ompi_collective_serialize(ompi_instance_collective_t *coll, pmix_info_t *info);

int ompi_results_deserialize(ompi_collective_results_t **coll_results, pmix_info_t *coll_results_info);
int ompi_params_deserialize(ompi_collective_parameters_t **coll_params, pmix_info_t *coll_params_info);
int ompi_procs_deserialize(ompi_collective_procs_t **coll_procs, pmix_info_t *coll_procs_info);
int ompi_collective_deserialize(ompi_instance_collective_t ** coll, pmix_info_t *_info);

bool cmp_procs(pmix_proc_t *coll_procs1, size_t n_coll_procs1, pmix_proc_t *coll_procs2, size_t n_coll_procs2);
bool cmp_infos(pmix_info_t *info1, pmix_info_t *info2, size_t ninfo);
bool compare_keys(char **keys1, char **keys2);
bool cmp_info_params(ompi_info_parameters_t *params1, ompi_info_parameters_t *params2);
bool cmp_query_params(ompi_query_parameters_t *params1, ompi_query_parameters_t *params2);
bool cmp_lookup_params(ompi_pdata_parameters_t *params1, ompi_pdata_parameters_t *params2);
bool cmp_params(ompi_collective_parameters_t *params1, ompi_collective_parameters_t *params2);
bool cmp_collective(ompi_function_type_t coll_func, pmix_proc_t *coll_procs, size_t n_coll_procs, ompi_collective_parameters_t *coll_params, ompi_instance_collective_t *collective);

ompi_instance_collective_t * search_pending_collectives( ompi_function_type_t coll_func, ompi_collective_procs_t *coll_procs, ompi_collective_parameters_t *coll_params);

void enter_collective_provider( size_t evhdlr_registration_id, pmix_status_t status, const pmix_proc_t *source, pmix_info_t info[], size_t ninfo, pmix_info_t results[], size_t nresults, pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata);    
int enter_collective_receiver(ompi_instance_collective_t *coll_in, bool wait);

int execute_collective_callback(ompi_instance_collective_t *coll);
int ompi_collective_send(ompi_instance_collective_t *coll, pmix_info_t *send_info, size_t n_send_info);


OBJ_CLASS_INSTANCE(ompi_instance_collective_t, opal_list_item_t, collective_constructor, collective_destructor);

int ompi_instance_collectives_init(){
    pmix_status_t status = OMPI_NOTIFY_COLLECTIVE;

    OBJ_CONSTRUCT(&ompi_instance_pending_collectives, opal_list_t);
    PMIx_Register_event_handler(&status, 1, NULL, 0, enter_collective_provider, NULL, NULL);

    return OMPI_SUCCESS;
}

int ompi_instance_collectives_finalize(){

    OBJ_DESTRUCT(&ompi_instance_pending_collectives);

    return OMPI_SUCCESS;
}


void ompi_instance_collective_assign(ompi_instance_collective_t *coll, pmix_status_t status, ompi_collective_procs_t *coll_procs, ompi_function_type_t func_type, ompi_collective_parameters_t *params, ompi_collective_results_t *results, ompi_collective_cbfunc_t *cbfunc, void* cbdata);

void ompi_instance_collective_assign(ompi_instance_collective_t *coll, pmix_status_t status, ompi_collective_procs_t *coll_procs, ompi_function_type_t func_type, ompi_collective_parameters_t *params, ompi_collective_results_t *results, ompi_collective_cbfunc_t *cbfunc, void* cbdata){
    coll->coll_procs = coll_procs;
    coll->coll_cbdata = cbdata;

    coll->status = status;

    coll->coll_func = func_type;
    coll->coll_params = params;
    coll->coll_results = results;
    coll->coll_cbfunc = cbfunc;

}

static void collective_constructor(ompi_instance_collective_t *coll){
    coll->status = PMIX_ERR_EMPTY;
    coll->coll_procs = NULL;
    coll->coll_cbdata = NULL;

    coll->coll_func = OMPI_FUNC_NONE;
    coll->coll_params = NULL;
    coll->coll_results = NULL;
    coll->coll_cbfunc = NULL;

    coll->is_waiting = false;

    OPAL_PMIX_CONSTRUCT_LOCK(&coll->lock);
}
static void collective_destructor(ompi_instance_collective_t *coll){
    coll_results_release(coll->coll_results);
    coll_params_release(coll->coll_params);
    coll_cbfunc_release(coll->coll_cbfunc);
    PMIX_PROC_FREE(coll->coll_procs, coll->n_coll_procs);

    OPAL_PMIX_DESTRUCT_LOCK(&coll->lock);
}


void ompi_collective_release_func(void *cbdata){
    ompi_instance_collective_t *coll = (ompi_instance_collective_t *) cbdata;
    OBJ_RELEASE(coll);
}

/* PARAMS */
static void pmix_query_xfer(pmix_query_t *dest, pmix_query_t *src){
    size_t m;
    PMIX_ARGV_COPY(dest->keys, src->keys);
    dest->nqual = src->nqual;
    PMIX_INFO_CREATE(dest->qualifiers, dest->nqual);
    for(m = 0; m < dest->nqual; m++){
        PMIX_INFO_XFER(&dest->qualifiers[m], &src->qualifiers[m]);
    }
}

static void pmix_pdata_xfer(pmix_pdata_t *dest, pmix_pdata_t *src){
    int rc;
    PMIX_LOAD_KEY(dest, src->key);
    PMIX_VALUE_XFER_DIRECT(rc, &(dest->value), (pmix_value_t *) &(src->value));
    if(PMIX_SUCCESS != rc){
        OMPI_ERROR_LOG(rc);
    }
    PMIX_PROC_LOAD(&dest->proc, src->proc.nspace, src->proc.rank);
}

void coll_params_create(ompi_collective_parameters_t **coll_params, ompi_parameters_type_t type){
    *coll_params = (ompi_collective_parameters_t *) malloc(sizeof(ompi_collective_parameters_t));
    (*coll_params)->type = type;
}

void coll_params_load_info(ompi_collective_parameters_t *coll_params, pmix_info_t *info, size_t ninfo){
    size_t n;

    coll_params->params.info_params.ninfo = ninfo;
    PMIX_INFO_CREATE(coll_params->params.info_params.info, ninfo);   

    for(n = 0; n < ninfo; n++){
        PMIX_INFO_XFER(&coll_params->params.info_params.info[n], &info[n]);
    } 
}


void coll_params_load_query(ompi_collective_parameters_t *coll_params, pmix_query_t *query, size_t nqueries){
    
    size_t n;
    coll_params->params.query_params.nqueries = nqueries;
    PMIX_QUERY_CREATE(coll_params->params.query_params.query, nqueries);
    for(n = 0; n < nqueries; n++){
        pmix_query_xfer(&coll_params->params.query_params.query[n], &query[n]);
    }

}

void coll_params_load_pdata(ompi_collective_parameters_t *coll_params, pmix_pdata_t *pdata, size_t npdata, pmix_info_t *info, size_t ninfo){
    
    size_t n;

    coll_params->params.pdata_params.npdata = npdata;
    PMIX_PDATA_CREATE(coll_params->params.pdata_params.pdata, npdata);
    for(n = 0; n < npdata; n++){
        pmix_pdata_xfer(&coll_params->params.pdata_params.pdata[n], &pdata[n]);
    }

    coll_params->params.pdata_params.ninfo = ninfo;
    PMIX_INFO_CREATE(coll_params->params.pdata_params.info, ninfo);
    for(n = 0; n < ninfo; n++){
        PMIX_INFO_XFER(&coll_params->params.pdata_params.info[n], &info[n]);
    }

}


void info_params_release(ompi_info_parameters_t *params){
    if(0 < params->ninfo){
        PMIX_INFO_FREE(params->info, params->ninfo);
    }
}

void query_params_release(ompi_query_parameters_t *params){
    if(0 < params->nqueries){
        PMIX_QUERY_FREE(params->query, params->nqueries);
    }
}

void pdata_params_release(ompi_pdata_parameters_t *params){
    if(0 < params->npdata){
        PMIX_PDATA_FREE(params->pdata, params->npdata);
    }
    if(0 < params->ninfo){
        PMIX_INFO_FREE(params->info, params->ninfo);
    }
}

void coll_params_release(ompi_collective_parameters_t *params){

    if(NULL == params){
        return;
    }

    switch(params->type){
        case OMPI_PARAMS_INFO:
            info_params_release((ompi_info_parameters_t *) &params->params.info_params);
            break;
        case OMPI_PARAMS_QUERY:
            query_params_release((ompi_query_parameters_t *) &params->params.info_params);
            break;
        default:
            break;
    }
    free(params);
}

/* RESULTS */

void info_results_release(ompi_info_results_t *results){
    if(0 < results->ninfo){
        PMIX_INFO_FREE(results->info, results->ninfo);
    }
}

void coll_results_release(ompi_collective_results_t *results){

    if(NULL == results){
        return;
    }

    switch(results->type){
        case OMPI_RESULTS_INFO:
            info_results_release((ompi_info_results_t *) &results->results.info_results);
            break;
        default:
            break;
    }
    free(results);
}

void coll_results_create(ompi_collective_results_t **coll_results, ompi_results_type_t type){
    *coll_results = (ompi_collective_results_t *) malloc(sizeof(ompi_collective_results_t));
    (*coll_results)->type = type;
}

void coll_results_load_info(ompi_collective_results_t *coll_results, pmix_info_t *info, size_t ninfo){
    size_t n;

    coll_results->results.info_results.ninfo = ninfo;

    if(0 == ninfo) return;

    PMIX_INFO_CREATE(coll_results->results.info_results.info, ninfo);

    for(n = 0; n < ninfo; n++){
        PMIX_INFO_XFER(&coll_results->results.info_results.info[n], &info[n]);
    } 
}

/* CBFUNC */

void coll_cbfunc_release(ompi_collective_cbfunc_t *cbfunc){
    
    free(cbfunc);
}

void coll_cbfunc_create(ompi_collective_cbfunc_t **coll_cbfunc, ompi_cbfunc_type_t type){
    *coll_cbfunc = (ompi_collective_cbfunc_t *) malloc(sizeof(ompi_collective_cbfunc_t));
    (*coll_cbfunc)->type = type;
}

void coll_cbfunc_load_info(ompi_collective_cbfunc_t *coll_cbfunc, pmix_info_cbfunc_t func){
    coll_cbfunc->cbfunc.info_cbfunc = func;
}

/* PROCS */
void coll_procs_create(ompi_collective_procs_t **coll_procs, pmix_proc_t *procs, size_t nprocs){
    size_t n;
    ompi_collective_procs_t *_coll_procs = (ompi_collective_procs_t *) malloc(sizeof(ompi_collective_procs_t));

    *coll_procs = _coll_procs; 
    _coll_procs->nprocs = nprocs;
    PMIX_PROC_CREATE(_coll_procs->procs, _coll_procs->nprocs);
    for(n = 0; n < nprocs; n++){
        PMIX_PROC_LOAD(&_coll_procs->procs[n], procs[n].nspace, procs[n].rank);
    }
}

void coll_procs_release(ompi_collective_procs_t *coll_procs){
    if(NULL == coll_procs){
        return;
    }
    PMIX_PROC_FREE(coll_procs->procs, coll_procs->nprocs);
    free(coll_procs);
}

void ompi_procs_serialize(ompi_collective_procs_t *coll_procs, pmix_info_t *info){
    char *key = "ompi.collective.procs";
    size_t n;
    pmix_proc_t *proc_ptr;
    pmix_data_array_t *darray_ptr;

    PMIX_DATA_ARRAY_CREATE(darray_ptr, coll_procs->nprocs, PMIX_PROC);
    proc_ptr = (pmix_proc_t *)darray_ptr->array;
    for(n = 0; n < coll_procs->nprocs; n++){
        PMIX_PROC_LOAD(&proc_ptr[n], coll_procs->procs[n].nspace, coll_procs->procs[n].rank);
    }
    PMIX_INFO_LOAD(info, key, darray_ptr, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_FREE(darray_ptr);
}

void ompi_results_serialize(ompi_collective_results_t *coll_results, pmix_info_t *info){
    
    char *key_results_outer = "ompi.collective.results";
    char *key_results_type = "ompi.collective.results.type";
    char *key_results_inner = "ompi.collective.results.results";
    size_t n;
    pmix_data_array_t *darray_ptr, *darray_ptr_outer;
    pmix_info_t *info_ptr, *info_ptr_outer;

    PMIX_DATA_ARRAY_CREATE(darray_ptr_outer, 2, PMIX_INFO);
    info_ptr_outer = (pmix_info_t *) darray_ptr_outer->array;

    PMIX_INFO_LOAD(&info_ptr_outer[0], key_results_type, &coll_results->type, PMIX_INT);

    switch (coll_results->type)
    {
    case OMPI_RESULTS_INFO:
        PMIX_DATA_ARRAY_CREATE(darray_ptr, coll_results->results.info_results.ninfo, PMIX_INFO);
        info_ptr = (pmix_info_t *)darray_ptr->array;
        for(n = 0; n < coll_results->results.info_results.ninfo; n++){
            PMIX_INFO_XFER(&info_ptr[n], &coll_results->results.info_results.info[n]);
        }
        PMIX_INFO_LOAD(&info_ptr_outer[1], key_results_inner, darray_ptr, PMIX_DATA_ARRAY);
        PMIX_DATA_ARRAY_FREE(darray_ptr);
        break;
  
    default:
        break;
    }

    PMIX_INFO_LOAD(info, key_results_outer, darray_ptr_outer, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_FREE(darray_ptr_outer);
}



void ompi_params_serialize(ompi_collective_parameters_t *coll_params, pmix_info_t *info){
    char *key_params_outer = "ompi.collective.params";
    char *key_params_type = "ompi.collective.params.type";
    char *key_params_inner1 = "ompi.collective.params.params1";
    char *key_params_inner2 = "ompi.collective.params.params2";

    size_t n;
    pmix_data_array_t *darray_ptr, *darray_ptr_outer;
    pmix_info_t *info_ptr, *info_ptr_outer;
    pmix_query_t *query_ptr;
    pmix_pdata_t *pdata_ptr;

    PMIX_DATA_ARRAY_CREATE(darray_ptr_outer, 3, PMIX_INFO);
    info_ptr_outer = (pmix_info_t *) darray_ptr_outer->array;

    PMIX_INFO_LOAD(&info_ptr_outer[0], key_params_type, &coll_params->type, PMIX_INT);

    switch (coll_params->type)
    {
    case OMPI_PARAMS_INFO:
        PMIX_DATA_ARRAY_CREATE(darray_ptr, coll_params->params.info_params.ninfo, PMIX_INFO);
        info_ptr = (pmix_info_t *)darray_ptr->array;
        for(n = 0; n < coll_params->params.info_params.ninfo; n++){
            PMIX_INFO_XFER(&info_ptr[n], &coll_params->params.info_params.info[n]);
        }
        PMIX_INFO_LOAD(&info_ptr_outer[1], key_params_inner1, darray_ptr, PMIX_DATA_ARRAY);
        PMIX_DATA_ARRAY_FREE(darray_ptr);
        break;
    case OMPI_PARAMS_QUERY:
        PMIX_DATA_ARRAY_CREATE(darray_ptr, coll_params->params.query_params.nqueries, PMIX_QUERY);
        query_ptr = (pmix_query_t *)darray_ptr->array;
        for(n = 0; n < coll_params->params.query_params.nqueries; n++){
            pmix_query_xfer(&query_ptr[n], &coll_params->params.query_params.query[n]);
        }
        PMIX_INFO_LOAD(&info_ptr_outer[1], key_params_inner1, darray_ptr, PMIX_DATA_ARRAY);
        PMIX_DATA_ARRAY_FREE(darray_ptr);
        break;
    case OMPI_PARAMS_PDATA:
        PMIX_DATA_ARRAY_CREATE(darray_ptr, coll_params->params.pdata_params.npdata, PMIX_PDATA);
        pdata_ptr = (pmix_pdata_t *)darray_ptr->array;
        for(n = 0; n < coll_params->params.pdata_params.npdata; n++){
            pmix_pdata_xfer(&pdata_ptr[n], &coll_params->params.pdata_params.pdata[n]);
        }
        PMIX_INFO_LOAD(&info_ptr_outer[1], key_params_inner1, darray_ptr, PMIX_DATA_ARRAY);
        PMIX_DATA_ARRAY_FREE(darray_ptr);

        PMIX_DATA_ARRAY_CREATE(darray_ptr, coll_params->params.pdata_params.ninfo, PMIX_INFO);
        info_ptr = (pmix_info_t *)darray_ptr->array;
        for(n = 0; n < coll_params->params.pdata_params.ninfo; n++){
            PMIX_INFO_XFER(&info_ptr[n], &coll_params->params.pdata_params.info[n]);
        }
        PMIX_INFO_LOAD(&info_ptr_outer[2], key_params_inner2, darray_ptr, PMIX_DATA_ARRAY);
        PMIX_DATA_ARRAY_FREE(darray_ptr);

        break;    
    default:
        break;
    }

    PMIX_INFO_LOAD(info, key_params_outer, darray_ptr_outer, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_FREE(darray_ptr_outer);
}

void ompi_collective_serialize(ompi_instance_collective_t *coll, pmix_info_t *info){
    pmix_info_t *coll_info;
    pmix_data_array_t *coll_darray;
    size_t n = 0, ninfo;
    ninfo = (NULL != coll->coll_params  ? 1 : 0) +
            (NULL != coll->coll_results ? 1 : 0) +
            (NULL != coll->coll_procs   ? 1 : 0) +
            + 2;

    if(ninfo < 4){
        printf("WARNING (serialize collective): Collective is not fully described!\n");
    }

    PMIX_DATA_ARRAY_CREATE(coll_darray, ninfo, PMIX_INFO);
    
    coll_info = (pmix_info_t *) coll_darray->array; 


    PMIX_INFO_LOAD(&coll_info[n++], "ompi.collective.func", &coll->coll_func, PMIX_INT);
    PMIX_INFO_LOAD(&coll_info[n++], "ompi.collective.status", &coll->status, PMIX_STATUS);

    if(NULL != coll->coll_params){
        ompi_params_serialize(coll->coll_params, &coll_info[n++]);
    }

    if(NULL != coll->coll_params){
        ompi_results_serialize(coll->coll_results, &coll_info[n++]);   
    }

    if(NULL != coll->coll_procs){
        ompi_procs_serialize(coll->coll_procs, &coll_info[n++]);
    }



    PMIX_INFO_LOAD(info, "ompi.collective", coll_darray, PMIX_DATA_ARRAY);
}


int ompi_results_deserialize(ompi_collective_results_t **coll_results, pmix_info_t *coll_results_info){
    size_t size;
    pmix_info_t *info_ptr, *info_ptr_outer;
    ompi_results_type_t results_type;

    info_ptr_outer = (pmix_info_t *) coll_results_info[0].value.data.darray->array;
    
    results_type = info_ptr_outer[0].value.data.integer;
    coll_results_create(coll_results, results_type);

    switch (results_type)
    {
    case OMPI_RESULTS_INFO:

        info_ptr = (pmix_info_t *) info_ptr_outer[1].value.data.darray->array;
        size = info_ptr_outer[1].value.data.darray->size;

        coll_results_load_info(*coll_results, info_ptr, size);

        break;
    
    default:
        return OMPI_ERR_BAD_PARAM;
    }
    return OMPI_SUCCESS;
}

int ompi_params_deserialize(ompi_collective_parameters_t **coll_params, pmix_info_t *coll_params_info){
    size_t size, size2;
    pmix_info_t *info_ptr, *info_ptr_outer;
    pmix_query_t *query_ptr;
    pmix_pdata_t *pdata_ptr;
    ompi_parameters_type_t params_type;
    
    info_ptr_outer = (pmix_info_t *) coll_params_info[0].value.data.darray->array;

    params_type = info_ptr_outer[0].value.data.integer;

    coll_params_create(coll_params, params_type);
    switch (params_type)
    {
    case OMPI_PARAMS_INFO:
        info_ptr = (pmix_info_t *) info_ptr_outer[1].value.data.darray->array;
        size = info_ptr_outer[1].value.data.darray->size;
       
        coll_params_load_info(*coll_params, info_ptr, size);
       

        break;

    case OMPI_PARAMS_QUERY:
        query_ptr = (pmix_query_t *) info_ptr_outer[1].value.data.darray->array;
        size = info_ptr_outer[1].value.data.darray->size;

        coll_params_load_query(*coll_params, query_ptr, size);
        break;
    case OMPI_PARAMS_PDATA:
        pdata_ptr = (pmix_pdata_t *) info_ptr_outer[1].value.data.darray->array;
        size = info_ptr_outer[1].value.data.darray->size;
        info_ptr = (pmix_info_t *) info_ptr_outer[2].value.data.darray->array;
        size2 = info_ptr_outer[2].value.data.darray->size;

        coll_params_load_pdata(*coll_params, pdata_ptr, size, info_ptr, size2);
        break;
    
    default:
        return OMPI_ERR_BAD_PARAM;
    }
    return OMPI_SUCCESS;
}

int ompi_procs_deserialize(ompi_collective_procs_t **coll_procs, pmix_info_t *coll_procs_info){
    size_t nprocs;
    pmix_proc_t *procs;

    procs = (pmix_proc_t *) coll_procs_info->value.data.darray->array;
    nprocs = coll_procs_info->value.data.darray->size;
       
    coll_procs_create(coll_procs, procs, nprocs);
       
    return OMPI_SUCCESS;
}

int ompi_collective_deserialize(ompi_instance_collective_t ** coll, pmix_info_t *_info){

    ompi_collective_results_t *coll_results = NULL;
    ompi_collective_parameters_t *coll_params = NULL;
    ompi_collective_procs_t *coll_procs = NULL;
    ompi_function_type_t function_type = OMPI_FUNC_NONE;
    pmix_info_t *info = NULL;
    pmix_status_t status = PMIX_ERR_EMPTY;

    size_t ninfo, n;
    int ret = OMPI_SUCCESS;

    ninfo = _info->value.data.darray->size;
    info = (pmix_info_t *) _info->value.data.darray->array;

    if(NULL == info || 0 == ninfo){
        return OMPI_ERR_BAD_PARAM;
    }

    *coll = OBJ_NEW(ompi_instance_collective_t);


    for(n = 0; n < ninfo; n++){
        if(PMIX_CHECK_KEY(&info[n], "ompi.collective.func")){
            function_type = info[n].value.data.integer;
        }else if(PMIX_CHECK_KEY(&info[n], "ompi.collective.status")){
            status = info[n].value.data.status;
        }else if(PMIX_CHECK_KEY(&info[n], "ompi.collective.results")){
            ret = ompi_results_deserialize(&coll_results, &info[n]);
        }else if(PMIX_CHECK_KEY(&info[n], "ompi.collective.params")){
            ret = ompi_params_deserialize(&coll_params, &info[n]);
        }else if(PMIX_CHECK_KEY(&info[n], "ompi.collective.procs")){
           ret = ompi_procs_deserialize(&coll_procs, &info[n]);
        }

        if(OMPI_SUCCESS != ret){
            printf("Error %d for deserializing coll info key %s\n", ret, info[n].key);
        }
    }
    ompi_instance_collective_assign(*coll, status, coll_procs, function_type, coll_params, coll_results, NULL, NULL);

    if(OMPI_SUCCESS != ret || NULL == coll_results || NULL == coll_params || NULL == coll_procs){
        printf("Collective deserialization failed\n");
        if(OMPI_SUCCESS == ret){
            ret = OMPI_ERR_BAD_PARAM;
        }
        OBJ_RELEASE(*coll);
        return ret;
    }

    return OMPI_SUCCESS;
}

/* TODO: This is an O(n^2) function as we assume arbitrary order */
bool cmp_procs(pmix_proc_t *coll_procs1, size_t n_coll_procs1, pmix_proc_t *coll_procs2, size_t n_coll_procs2){
    size_t n, m;
    bool found;
    pmix_proc_t proc1, proc2;

    if( n_coll_procs1 != n_coll_procs2){
        return false;
    }

    for(n = 0; n < n_coll_procs1; n++){
        proc1 = coll_procs1[n];

        found = false;
        for(m = 0; m < n_coll_procs2; m++){
            proc2 = coll_procs2[m];
            
            if(PMIX_CHECK_PROCID(&proc1, &proc2)){
                found = true;
                break;
            }
        }
        if(!found){
            return false;
        }
    }

    return true;
}

bool cmp_infos(pmix_info_t *info1, pmix_info_t *info2, size_t ninfo){
    size_t n, m;
    bool found;

    for(n = 0; n < ninfo; n++){

        if(PMIX_CHECK_KEY(&info1[n], PMIX_PROCID)){
            continue;
        }

        found = false;
        for(m = 0; m < ninfo; m++){
            /* FIXME: we need to account for different pmix_procids given, as they are individual info. 
             * need to add a parameter for keys to be excluded from the comparison
             */

            if(PMIX_CHECK_KEY(&info1[n], info2[m].key) && (PMIX_EQUAL == PMIx_Value_compare(&info1[n].value, &info2[m].value))){
                found = true;
                break;
            }
        }
        if(!found){
            return false;
        }
    }
    return true;
}

bool cmp_info_params(ompi_info_parameters_t *params1, ompi_info_parameters_t *params2){

    if(params1->ninfo != params2->ninfo){
        return false;
    }

    return cmp_infos(params1->info, params2->info, params1->ninfo);
}

bool compare_keys(char **keys1, char **keys2){
    char **p1, **p2;
    bool found;

    if(pmix_argv_count(keys1) != pmix_argv_count(keys2)){
        return false;
    }

    for (p1 = keys1; *p1; p1++){
        found = false;
        for (p2 = keys2; *p2; p2++){
            if(0 == strcmp(*p1, *p2)){
                found = true;
                break;
            }
        }
        if(!found){
            return false;
        }
    }

    return true;
}

bool cmp_query_params(ompi_query_parameters_t *params1, ompi_query_parameters_t *params2){
    size_t n, m;
    bool found;
    pmix_query_t *query1, *query2;

    if(params1->nqueries != params2->nqueries){
        return false;
    }

    for(n = 0; n < params1->nqueries; n++){
        query1 = &params1->query[n];

        found = false;
        for(m = 0; m < params2->nqueries; m++){
            query2 = &params2->query[m];

            if(query1->nqual != query2->nqual){
                continue;
            }
            if(!compare_keys(query1->keys, query2->keys)){
                continue;
            }

            if(cmp_infos(query1->qualifiers, query2->qualifiers, query1->nqual)){
                found = true;
                break;
            }
        }
        if(!found){
            return false;
        }
    }

    return true;
}

bool cmp_lookup_params(ompi_pdata_parameters_t *params1, ompi_pdata_parameters_t *params2){
    size_t n, m;
    bool found;
    pmix_pdata_t *pdata1, *pdata2;
    pmix_info_t *info1, *info2;

    if(params1->npdata != params2->npdata || params1->ninfo != params2->ninfo){
        return false;
    }

    for(n = 0; n < params1->npdata; n++){
        pdata1 = &params1->pdata[n];

        found = false;
        for(m = 0; m < params2->npdata; m++){
            pdata2 = &params2->pdata[m];

            if(!PMIX_CHECK_KEY(pdata1, pdata2->key)){
                continue;
            }
        
            found = true;
            break;
        }
        
        if(!found){
            return false;
        }
    }

    for(n = 0; n < params1->ninfo; n++){
        info1 = &params1->info[n];

        found = false;
        for(m = 0; m < params2->ninfo; m++){
            info2 = &params2->info[m];

            if(PMIX_CHECK_KEY(info1, info2->key) && (PMIX_EQUAL == PMIx_Value_compare(&info1->value, &info2->value))){
                found = true;
                break;
            }
        }
        if(!found){
            return false;
        }
    }

    return true;
}

bool cmp_params(ompi_collective_parameters_t *params1, ompi_collective_parameters_t *params2){
    
    if(params1->type != params2->type){
        return false;
    }

    switch(params1->type){
        case OMPI_PARAMS_INFO:
            return cmp_info_params(&params1->params.info_params, &params2->params.info_params);
        case OMPI_PARAMS_QUERY:
            return cmp_query_params(&params1->params.query_params, &params2->params.query_params);
        case OMPI_PARAMS_PDATA:
            return cmp_lookup_params(&params1->params.pdata_params, &params2->params.pdata_params);
        default:
            return false;
    }
}

bool cmp_collective(ompi_function_type_t coll_func, pmix_proc_t *coll_procs, size_t n_coll_procs, ompi_collective_parameters_t *coll_params, ompi_instance_collective_t *collective){
    
    if(coll_func != collective->coll_func){
        return false;
    }

    if(!cmp_procs(coll_procs, n_coll_procs, collective->coll_procs->procs, collective->coll_procs->nprocs)){
        return false;
    }
    if(!cmp_params(coll_params, collective->coll_params)){
        return false;
    }
    return true;

}


ompi_instance_collective_t * search_pending_collectives( ompi_function_type_t coll_func, ompi_collective_procs_t *coll_procs, ompi_collective_parameters_t *coll_params){
    
    ompi_instance_collective_t *coll_out = NULL;
    OPAL_LIST_FOREACH(coll_out, &ompi_instance_pending_collectives, ompi_instance_collective_t){
        if(cmp_collective(coll_func, coll_procs->procs, coll_procs->nprocs, coll_params, coll_out)){
            return coll_out;
        }
    }
    return NULL;
}



/* Note! The coll->cbfunc is reponsible for freeing the cbdata if desired */
int execute_collective_callback(ompi_instance_collective_t *coll){

    ompi_info_results_t *info_results;

    switch(coll->coll_cbfunc->type){
        case OMPI_CBFUNC_INFO:

            info_results = &coll->coll_results->results.info_results;
            coll->coll_cbfunc->cbfunc.info_cbfunc(coll->status, info_results->info, info_results->ninfo, coll->coll_cbdata, NULL, NULL);

            break;
        default:
            break;
    }
    return OMPI_SUCCESS;
}

/* Serialize and send an ompi_instance_collective_t object to all other procs of the collective */
int ompi_collective_send(ompi_instance_collective_t *coll, pmix_info_t *send_info, size_t n_send_info){
    pmix_info_t *info; 
    size_t ninfo, ntarget_procs, n, k;
    pmix_status_t ret;
    pmix_proc_t *target_procs;
    pmix_data_array_t *darray;

    ntarget_procs = coll->coll_procs->nprocs - 1;

    if(0 >= ntarget_procs){
        return OMPI_SUCCESS;
    }

    PMIX_DATA_ARRAY_CREATE(darray, ntarget_procs, PMIX_PROC);
    target_procs = (pmix_proc_t *) darray->array;

    n = 0;
    for(k = 0; n < ntarget_procs; k++){
        
        if(PMIX_CHECK_PROCID(&coll->coll_procs->procs[k], &opal_process_info.myprocid)){
            continue;
        }
        PMIX_PROC_LOAD(&target_procs[n], coll->coll_procs->procs[k].nspace, coll->coll_procs->procs[k].rank);
        
        n++;
    }

    
    ninfo = n_send_info + 3;

    PMIX_INFO_CREATE(info, ninfo);

    n = 0;
    ompi_collective_serialize(coll, &info[n++]);

    PMIX_INFO_LOAD(&info[n++], PMIX_EVENT_NON_DEFAULT, NULL, PMIX_BOOL);
    PMIX_INFO_LOAD(&info[n++], PMIX_EVENT_CUSTOM_RANGE, darray, PMIX_DATA_ARRAY);

    for(k = 0; k < n_send_info; k++){
        PMIX_INFO_XFER(&info[n++], &send_info[k]);
    }

    ret = PMIx_Notify_event(OMPI_NOTIFY_COLLECTIVE, &opal_process_info.myprocid, PMIX_RANGE_CUSTOM, info, ninfo, NULL, NULL);

    PMIX_INFO_FREE(info, ninfo);
    PMIX_DATA_ARRAY_FREE(darray);

    return ret;

}

/* Enter the collective to receive the results of a collective operation. 
 * NOTE: The provided collective will be released upon return of this function
 */
int enter_collective_receiver(ompi_instance_collective_t *coll_in, bool wait){
    
    ompi_instance_collective_t *coll;
    int ret = OMPI_SUCCESS;

    /* Lock the list of pending collectives */
    opal_mutex_lock(&collectives_lock);


    coll = search_pending_collectives(coll_in->coll_func, coll_in->coll_procs, coll_in->coll_params);

    /*  There is no pending collective for this request, so we add it.
     *  The callback will be called by the provider 
     */
    if(NULL == coll){
        coll_in->is_waiting = wait;

        opal_list_append(&ompi_instance_pending_collectives, &coll_in->super);

        opal_mutex_unlock(&collectives_lock);

        /* If we waited for the collective to be executed, we are the one to release it */
        if(wait){
            OPAL_PMIX_WAIT_THREAD(&coll_in->lock);
            OBJ_RELEASE(coll_in);
        }

    /* There was a matching pending collective added by the provider 
     * We call the callback using the results from the provider
     */
    }else{

        opal_list_remove_item(&ompi_instance_pending_collectives, &coll->super);
        opal_mutex_unlock(&collectives_lock);
        
        coll->coll_cbfunc = coll_in->coll_cbfunc;
        coll->coll_cbdata = coll_in->coll_cbdata;

        ret = execute_collective_callback(coll);

        /* Protect shared pointer */
        coll->coll_cbfunc = NULL;
        coll->coll_cbdata = NULL;
        
        OBJ_RELEASE(coll);
        OBJ_RELEASE(coll_in);
    }

    return ret;
    
}

/* This is the event-handler for the PMIx_Notification sent by the provider of the collective */
void enter_collective_provider(size_t evhdlr_registration_id, pmix_status_t status, const pmix_proc_t *source, pmix_info_t info[], size_t ninfo, pmix_info_t results[], size_t nresults, pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata){
    
    int ret = OMPI_ERR_BAD_PARAM;
    size_t n;
    bool receiver_is_waiting;

    ompi_instance_collective_t *coll, *coll_in;

    for(n = 0; n < ninfo; n++){
        if(PMIX_CHECK_KEY(&info[n], "ompi.collective")){
            ret = ompi_collective_deserialize(&coll_in, &info[n]);
        }
    }

    if(ret != OMPI_SUCCESS){
        return;
    }

    /* Lock the list of pending collectives */
    opal_mutex_lock(&collectives_lock);

    coll = search_pending_collectives(coll_in->coll_func, coll_in->coll_procs, coll_in->coll_params);


    /*  The receiver side has not yet added a pending collective for this request, so we add one.
     *  The callback will be called by the receiver 
     */
    if(NULL == coll || NULL == coll->coll_cbfunc){

        opal_list_append(&ompi_instance_pending_collectives, &coll_in->super);
        opal_mutex_unlock(&collectives_lock);


    /* There was a matching pending collective added by the receiver 
     * We call the callback using the results from the provider
     */
    }else if(NULL != coll->coll_cbfunc){
        /* Remove coll from the list of pending collectives */
        opal_list_remove_item(&ompi_instance_pending_collectives, &coll->super);

        receiver_is_waiting = coll->is_waiting;

        opal_mutex_unlock(&collectives_lock);

        coll->coll_results = coll_in->coll_results;
        coll->status = coll_in->status;
        
        execute_collective_callback(coll);
        coll->coll_results = NULL;

        /* If the receiver is waiting on the lock, we only need to wake it up 
         * If the receiver is not waiting we need to release the collective ourselves
         */
        if(receiver_is_waiting){
            OPAL_PMIX_WAKEUP_THREAD(&coll->lock);
        }else{
            OBJ_RELEASE(coll);
        }

        OBJ_RELEASE(coll_in);

    }
    
    if(NULL != cbfunc){
        cbfunc(PMIX_EVENT_ACTION_COMPLETE, NULL, 0, NULL, NULL, cbdata);
    }
    
}


/*  EXPOSED TO USER */

/* Collective query */

/* Create an ompi_collective_t object corresponding to a PMIx_Lookup call */
void create_collective_lookup(ompi_instance_collective_t **coll, pmix_status_t status, pmix_proc_t *procs, size_t nprocs, pmix_pdata_t *pdata, size_t npdata, pmix_info_t *info, size_t ninfo, pmix_info_t *results, size_t nresults, pmix_info_cbfunc_t info_cbfunc, void *cbdata){
    
    ompi_collective_procs_t *coll_procs;
    ompi_collective_results_t *coll_results;
    ompi_collective_parameters_t *coll_params;
    ompi_collective_cbfunc_t *coll_cbfunc = NULL;

    coll_procs_create(&coll_procs, procs, nprocs);

    coll_results_create(&coll_results, OMPI_RESULTS_INFO);
    coll_results_load_info(coll_results, results, nresults);

    coll_params_create(&coll_params, OMPI_PARAMS_PDATA);
    coll_params_load_pdata(coll_params, pdata, npdata, info, ninfo);

    if(NULL != info_cbfunc){
        coll_cbfunc_create(&coll_cbfunc, OMPI_CBFUNC_INFO);
        coll_cbfunc_load_info(coll_cbfunc, info_cbfunc);
    }

    *coll = OBJ_NEW(ompi_instance_collective_t);
    ompi_instance_collective_assign(*coll, status, coll_procs, OMPI_FUNC_PMIX_LOOKUP, coll_params, coll_results, coll_cbfunc, cbdata);
}

/* Create an ompi_collective_t object corresponding to a PMIx_query_info call */
void create_collective_query(ompi_instance_collective_t **coll, pmix_status_t status, pmix_proc_t *procs, size_t nprocs, pmix_query_t *query, size_t nqueries, pmix_info_t *results, size_t nresults, pmix_info_cbfunc_t info_cbfunc, void *cbdata){
    
    ompi_collective_procs_t *coll_procs;
    ompi_collective_results_t *coll_results;
    ompi_collective_parameters_t *coll_params;
    ompi_collective_cbfunc_t *coll_cbfunc = NULL;

    coll_procs_create(&coll_procs, procs, nprocs);

    coll_results_create(&coll_results, OMPI_RESULTS_INFO);
    coll_results_load_info(coll_results, results, nresults);

    coll_params_create(&coll_params, OMPI_PARAMS_QUERY);
    coll_params_load_query(coll_params, query, nqueries);

    if(NULL != info_cbfunc){
        coll_cbfunc_create(&coll_cbfunc, OMPI_CBFUNC_INFO);
        coll_cbfunc_load_info(coll_cbfunc, info_cbfunc);
    }

    *coll = OBJ_NEW(ompi_instance_collective_t);
    ompi_instance_collective_assign(*coll, status, coll_procs, OMPI_FUNC_PMIX_QUERY, coll_params, coll_results, coll_cbfunc, cbdata);
}

/* Send the results of a PMIx_query_info to the other procs of the collective call */
int send_collective_data_query(pmix_proc_t *procs, pmix_status_t status, size_t nprocs, pmix_query_t *query, size_t nqueries, pmix_info_t *results, size_t nresults){

    int ret;
    ompi_instance_collective_t *coll;

    create_collective_query(&coll, status, procs, nprocs, query, nqueries, results, nresults, NULL, NULL);
    
    ret = ompi_collective_send(coll, NULL, 0);

    OBJ_RELEASE(coll);

    return ret;
}

/* Receive the results of a PMIx_query_info collective call */
int recv_collective_data_query(pmix_proc_t *procs, size_t nprocs, pmix_query_t *query, size_t nqueries, pmix_info_cbfunc_t cbfunc, void *cbdata){

    int ret;
    ompi_instance_collective_t *coll;

    create_collective_query(&coll, PMIX_ERR_EMPTY, procs, nprocs, query, nqueries, NULL, 0, cbfunc, cbdata);
    
    ret = enter_collective_receiver(coll, true);
    
    return ret;
    
}

/* Send the results of a PMIx_Lookup to the other procs of the collective call */
int send_collective_data_lookup(pmix_proc_t *procs, pmix_status_t status, size_t nprocs, pmix_pdata_t *pdata, size_t npdata, pmix_info_t *info, size_t ninfo, pmix_info_t *results, size_t nresults){

    int ret;
    ompi_instance_collective_t *coll;

    create_collective_lookup(&coll, status, procs, nprocs, pdata, npdata, info, ninfo, results, nresults, NULL, NULL);
    
    ret = ompi_collective_send(coll, NULL, 0);

    OBJ_RELEASE(coll);

    return ret;
}

/* Receive the results of a PMIx_Lookup collective call */
int recv_collective_data_lookup(pmix_proc_t *procs, size_t nprocs, pmix_pdata_t *pdata, size_t npdata, pmix_info_t *info, size_t ninfo, pmix_info_cbfunc_t cbfunc, void *cbdata){

    int ret;
    ompi_instance_collective_t *coll;

    create_collective_lookup(&coll, PMIX_ERR_EMPTY, procs, nprocs, pdata, npdata, info, ninfo, NULL, 0, cbfunc, cbdata);
    
    ret = enter_collective_receiver(coll, true);
    
    return ret;    
}

/* Receive the results of a PMIx_Lookup_nb collective call */
int recv_collective_data_lookup_nb(pmix_proc_t *procs, size_t nprocs, pmix_pdata_t *pdata, size_t npdata, pmix_info_t *info, size_t ninfo, pmix_info_cbfunc_t cbfunc, void *cbdata){

    int ret;
    ompi_instance_collective_t *coll;

    create_collective_lookup(&coll, PMIX_ERR_EMPTY, procs, nprocs, pdata, npdata, info, ninfo, NULL, 0, cbfunc, cbdata);
    
    ret = enter_collective_receiver(coll, false);
    
    return ret;    
}


/* Cbfunc to be used for collective functions with results type info */
void ompi_instance_collective_infocb_send(pmix_status_t status, pmix_info_t *results, size_t nresults, void *cbdata, pmix_release_cbfunc_t release_fn, void *release_cbdata)
{
    ompi_instance_collective_t *coll = (ompi_instance_collective_t *)cbdata;

    coll->status = status;
    coll_results_create(&coll->coll_results, OMPI_RESULTS_INFO);
    coll_results_load_info(coll->coll_results, results, nresults);

    ompi_collective_send(coll, NULL, 0);

    coll->coll_cbfunc->cbfunc.info_cbfunc(status, results, nresults, coll->coll_cbdata,  release_fn, release_cbdata);

    OBJ_RELEASE(coll);
}

