/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2018-2021 Triad National Security, LLC. All rights
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
#include "ompi/mca/bml/base/base.h"
#include "ompi/mca/pml/base/base.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/osc/base/base.h"
#include "ompi/mca/io/base/base.h"
#include "ompi/mca/topo/base/base.h"
#include "opal/mca/pmix/base/base.h"

#include "opal/mca/mpool/base/mpool_base_tree.h"
#include "ompi/mca/pml/base/pml_base_bsend.h"
#include "ompi/util/timings.h"
#include "opal/mca/pmix/pmix-internal.h"

ompi_predefined_instance_t ompi_mpi_instance_null = {{{{0}}}};

static opal_recursive_mutex_t instance_lock = OPAL_RECURSIVE_MUTEX_STATIC_INIT;
static opal_recursive_mutex_t tracking_structures_lock;
static ompi_mpi_instance_pset_t app_pset;
static ompi_mpi_instance_pset_t self_pset;

/** MPI_Init instance */
ompi_instance_t *ompi_mpi_instance_default = NULL;

enum {
    OMPI_INSTANCE_INITIALIZING = -1,
    OMPI_INSTANCE_FINALIZING   = -2,
};

opal_atomic_int32_t ompi_instance_count = 0;

static const char *ompi_instance_builtin_psets[] = {
    "mpi://WORLD",
    "mpi://SELF",
    "mpix://SHARED",
};

static const int32_t ompi_instance_builtin_count = 3;

/** finalization functions that need to be called on teardown */
static opal_finalize_domain_t ompi_instance_basic_domain;
static opal_finalize_domain_t ompi_instance_common_domain;

static void ompi_instance_construct (ompi_instance_t *instance)
{
    instance->i_f_to_c_index = opal_pointer_array_add (&ompi_instance_f_to_c_table, instance);
    instance->i_name[0] = '\0';
    instance->i_flags = 0;
    instance->i_keyhash = NULL;
    instance->errhandler_type = OMPI_ERRHANDLER_TYPE_INSTANCE;
}

OBJ_CLASS_INSTANCE(ompi_instance_t, opal_infosubscriber_t, ompi_instance_construct, NULL);

/* NTH: frameworks needed by MPI */
static mca_base_framework_t *ompi_framework_dependencies[] = {
    &ompi_hook_base_framework, &ompi_op_base_framework,
    &opal_allocator_base_framework, &opal_rcache_base_framework, &opal_mpool_base_framework,
    &ompi_bml_base_framework, &ompi_pml_base_framework, &ompi_coll_base_framework,
    &ompi_osc_base_framework, NULL,
};

static mca_base_framework_t *ompi_lazy_frameworks[] = {
    &ompi_io_base_framework, &ompi_topo_base_framework, NULL,
};


static int ompi_mpi_instance_finalize_common (void);

/*
 * Hash tables for MPI_Type_create_f90* functions
 */
opal_hash_table_t ompi_mpi_f90_integer_hashtable = {{0}};
opal_hash_table_t ompi_mpi_f90_real_hashtable = {{0}};
opal_hash_table_t ompi_mpi_f90_complex_hashtable = {{0}};



/* PSETS */
OBJ_CLASS_INSTANCE(ompi_mpi_instance_pset_t, opal_object_t, pset_constructor, pset_destructor);
static size_t  ompi_mpi_instance_num_pmix_psets;
static opal_list_t ompi_mpi_instance_pmix_psets;

inline ompi_rc_op_type_t MPI_OMPI_CONV_PSET_OP(int mpi_pset_op){
    switch(mpi_pset_op){
        case MPI_PSETOP_UNION:
            return OMPI_PSETOP_UNION;
        case MPI_PSETOP_DIFFERENCE:
            return OMPI_PSETOP_DIFFERENCE;
        case MPI_PSETOP_INTERSECTION:
            return OMPI_PSETOP_INTERSECTION;
        default:
            return OMPI_PSETOP_NULL;
    }
}

ompi_mpi_instance_pset_t * get_pset_by_name(char *name){
    opal_mutex_lock (&tracking_structures_lock);

    if(0 == strcmp(name, "mpi://app")){
        opal_mutex_unlock (&tracking_structures_lock);
        return &app_pset;
    }else if(0 == strcmp(name, "mpi://SELF")){
        opal_mutex_unlock (&tracking_structures_lock);
        return &self_pset;
    }

    ompi_mpi_instance_pset_t *pset_out=NULL;
    OPAL_LIST_FOREACH(pset_out, &ompi_mpi_instance_pmix_psets, ompi_mpi_instance_pset_t){
        if(0 == strcmp(name,pset_out->name)){
            opal_mutex_unlock (&tracking_structures_lock);
            return pset_out;
        }
    }
    opal_mutex_unlock (&tracking_structures_lock);
    return NULL;
}

ompi_mpi_instance_pset_t * get_nth_pset_in_list(int n){
    opal_mutex_lock (&tracking_structures_lock);
    int count=0;
    ompi_mpi_instance_pset_t *pset_out=NULL;
    OPAL_LIST_FOREACH(pset_out, &ompi_mpi_instance_pmix_psets, ompi_mpi_instance_pset_t){
        if(count++==n){
            opal_mutex_unlock (&tracking_structures_lock);
            return pset_out;
        }
    }
    opal_mutex_unlock (&tracking_structures_lock);
    return NULL;
}

bool is_pset_leader(pmix_proc_t *pset_members, size_t nmembers, pmix_proc_t proc){
    size_t n;
    for(n=0; n<nmembers; n++){
        int nspace_cmp=strcmp(proc.nspace,pset_members[n].nspace);
        if( 0 < nspace_cmp || (0==nspace_cmp && pset_members[n].rank<proc.rank))return false;
    }
    return true;
}

bool is_pset_member(pmix_proc_t *pset_members, size_t nmembers, pmix_proc_t proc){

    size_t n;
    for(n = 0; n < nmembers; n++){
        if(0==strcmp(proc.nspace,pset_members[n].nspace) && pset_members[n].rank==proc.rank)return true;
    }
    return false;
}

bool opal_is_pset_member(opal_process_name_t *procs, size_t nprocs, opal_process_name_t proc){
    opal_mutex_lock(&tracking_structures_lock);
    size_t n;
    for(n = 0; n < nprocs; n++){

        if(proc.jobid == procs[n].jobid &&  proc.vpid == procs[n].vpid){
            opal_mutex_unlock(&tracking_structures_lock);
            return true;
        }
    }
    opal_mutex_unlock(&tracking_structures_lock);
    return false;
}

bool opal_is_pset_member_local(char *pset_name, opal_process_name_t proc){
    opal_mutex_lock(&tracking_structures_lock);
    ompi_mpi_instance_pset_t *pset = get_pset_by_name(pset_name);
    if(NULL == pset)return false;

    size_t n;
    for(n = 0; n < pset->size; n++){
        if(proc.jobid == pset->members[n].jobid && pset->members[n].vpid == proc.vpid){
            opal_mutex_unlock(&tracking_structures_lock);
            return true;
        }
    }
    opal_mutex_unlock(&tracking_structures_lock);
    return false;
}

/* cache/update pset locally */
void pset_define_handler(size_t evhdlr_registration_id, pmix_status_t status,
                       const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
                       pmix_info_t results[], size_t nresults,
                       pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata){

    size_t n, sz, nmembers=0;

    pmix_status_t rc=PMIX_SUCCESS;
    pmix_proc_t *data_array;
    pmix_proc_t *members;

    char pset_name[PMIX_MAX_KEYLEN]={0};

    //opal_mutex_lock(&tracking_structures_lock);
    //for(n=0; n<ninfo; n++){
    //    if(0 == strcmp(info[n].key, PMIX_PSET_NAME)){
    //        strncpy(pset_name, info[n].value.data.string, PMIX_MAX_KEYLEN);
    //    }else if(0 == strcmp(info[n].key, PMIX_PSET_MEMBERS)){
    //            data_array=(pmix_proc_t*)info[n].value.data.darray->array;
    //            nmembers=info[n].value.data.darray->size;
    //    }
    //}
    //bool new_pset=false;
    //if(strlen(pset_name) > 0){
    //    ompi_mpi_instance_pset_t *pset;
    //    if(NULL == (pset = get_pset_by_name(pset_name))){
    //        pset=OBJ_NEW(ompi_mpi_instance_pset_t);
    //        new_pset=true;
    //    }
    //    strncpy(pset->name, pset_name, PMIX_MAX_KEYLEN);
    //    pset->malleable=true;
    //    pset->active=true;
    //    pset->size=nmembers;
//
    //    if(0 < nmembers){
    //        pset->size = nmembers;
    //        pset->members = malloc(nmembers*sizeof(opal_process_name_t));
    //        for(n=0; n < nmembers; n++){
    //            OPAL_PMIX_CONVERT_PROCT(rc, &pset->members[n], &data_array[n]);
    //        }
    //    }
    //    if(new_pset){
    //        opal_list_append(&ompi_mpi_instance_pmix_psets, &pset->super);
    //    }
    //}
//
    //opal_mutex_unlock(&tracking_structures_lock);

    cbfunc(PMIX_EVENT_ACTION_COMPLETE, NULL, 0, NULL, NULL, cbdata);
}

/* set local pset to inactive */
void pset_delete_handler(size_t evhdlr_registration_id, pmix_status_t status,
                       const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
                       pmix_info_t results[], size_t nresults,
                       pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata){

    size_t n, sz;
    pmix_status_t rc = PMIX_SUCCESS;
    char *pset_name = NULL;
    

    opal_mutex_lock(&tracking_structures_lock);
    for(n=0; n<ninfo; n++){
        if(0 == strcmp(info[n].key, PMIX_PSET_NAME)){
            PMIX_VALUE_UNLOAD(rc, &info[n].value, (void**)&pset_name, &sz);
        }
    }
    if(NULL != pset_name){
        ompi_mpi_instance_pset_t *pset;
        if(NULL != (pset=get_pset_by_name(pset_name))){
            pset->active=false;
        }
        free(pset_name);
    }

    opal_mutex_unlock(&tracking_structures_lock);

    cbfunc(PMIX_SUCCESS, NULL, 0, NULL, NULL, cbdata);
}

/* Resource Changes */

OBJ_CLASS_INSTANCE(ompi_mpi_instance_resource_change_t, opal_object_t, ompi_resource_change_constructor, ompi_resource_change_constructor);
static opal_list_t ompi_mpi_instance_resource_changes;

inline ompi_rc_op_type_t MPI_OMPI_CONV_RC_OP(int mpi_rc_op){
    switch(mpi_rc_op){
        case MPI_RC_NULL:
            return OMPI_RC_NULL;
        case MPI_RC_ADD:
            return OMPI_RC_ADD;
        case MPI_RC_SUB:
            return OMPI_RC_SUB;
        default:
            return OMPI_RC_NULL;
    }
}

inline int MPI_OMPI_CONVT_RC_OP(ompi_rc_op_type_t ompi_rc_op_type){
    switch(ompi_rc_op_type){
        case OMPI_RC_NULL:
            return MPI_RC_NULL;
        case OMPI_RC_ADD:
            return MPI_RC_ADD;
        case OMPI_RC_SUB:
            return MPI_RC_SUB;
        default:
            return MPI_RC_NULL;
    }
}

ompi_mpi_instance_resource_change_t * get_res_change_by_name(char *name){
    opal_mutex_lock(&tracking_structures_lock);
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->delta_pset && 0 == strncmp(name, rc_out->delta_pset->name, MPI_MAX_PSET_NAME_LEN)){
            opal_mutex_unlock (&tracking_structures_lock);
            return rc_out;
        }
    }
    opal_mutex_unlock(&tracking_structures_lock);
    return NULL;
}

ompi_mpi_instance_resource_change_t * get_res_change_for_bound_name(char *name){
    opal_mutex_lock(&tracking_structures_lock);
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->bound_pset && 0 == strcmp(name,rc_out->bound_pset->name)){
            opal_mutex_unlock (&tracking_structures_lock);
            return rc_out;
        }
    }
    opal_mutex_unlock(&tracking_structures_lock);
    return NULL;
    
}

ompi_mpi_instance_resource_change_t * get_res_change_active_for_bound_name(char *name){
    opal_mutex_lock(&tracking_structures_lock);
    ompi_mpi_instance_resource_change_t *rc_out = NULL;
    OPAL_LIST_FOREACH(rc_out, &ompi_mpi_instance_resource_changes, ompi_mpi_instance_resource_change_t){
        if(NULL != rc_out->bound_pset && 0 == strcmp(name,rc_out->bound_pset->name) && rc_out->status != RC_INVALID && rc_out->status != RC_FINALIZED){
            opal_mutex_unlock (&tracking_structures_lock);
            return rc_out;
        }
    }
    opal_mutex_unlock(&tracking_structures_lock);
    return NULL; 
}


int ompi_instance_get_rc_type(char *delta_pset, ompi_rc_op_type_t *rc_type){
    opal_mutex_lock(&tracking_structures_lock);
    ompi_mpi_instance_resource_change_t *res_change;
    res_change = get_res_change_by_name(delta_pset);
    *rc_type=res_change->type;
    opal_mutex_unlock(&tracking_structures_lock);
    return OPAL_SUCCESS;
}

int print_res_change(char *name){

    ompi_mpi_instance_resource_change_t *rc_out=get_res_change_by_name(name);

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
	return 0;
}

/* pset utility functions. Might need to be shifted elsewhere */
int opal_pmix_proc_array_conv(opal_process_name_t *opal_procs, pmix_proc_t **pmix_procs, size_t nprocs){
    pmix_proc_t *pmix_proc_array= *pmix_procs=malloc(nprocs*sizeof(pmix_proc_t));
    int n;
    for(n=0; n<nprocs; n++){
        OPAL_PMIX_CONVERT_NAME(&pmix_proc_array[n], &opal_procs[n]);
    }
    return OMPI_SUCCESS;
}

int pmix_opal_proc_array_conv(pmix_proc_t *pmix_procs, opal_process_name_t **opal_procs, size_t nprocs){
    opal_process_name_t *opal_proc_array= *opal_procs=malloc(nprocs*sizeof(pmix_proc_t));
    int n;
    int rc;
    for(n=0; n<nprocs; n++){
        OPAL_PMIX_CONVERT_PROCT(rc, &opal_proc_array[n], &pmix_procs[n]);
    }
    return OMPI_SUCCESS;
}

/* delete resource change from local cache */
static void rc_finalize_handler(size_t evhdlr_registration_id, pmix_status_t status,
                       const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
                       pmix_info_t results[], size_t nresults,
                       pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata){

    size_t n;
    pmix_status_t rc=PMIX_SUCCESS;
    size_t sz;
    char *pset_name = NULL;
    

    opal_mutex_lock(&tracking_structures_lock);
    for(n = 0; n < ninfo; n++){
        if(0 == strcmp(info[n].key, PMIX_PSET_NAME)){
            PMIX_VALUE_UNLOAD(rc, &info[n].value, (void**)&pset_name, &sz);
        }
    }
    if(NULL != pset_name){
        ompi_mpi_instance_resource_change_t *res_change;
        
        if(NULL != (res_change = get_res_change_by_name(pset_name))){
            //res_change = opal_list_remove_item(&ompi_mpi_instance_resource_changes, &res_change->super);
            res_change->status = RC_FINALIZED;
            //OBJ_RELEASE(res_change);
        }
        free(pset_name);
    }
    
    opal_mutex_unlock(&tracking_structures_lock);
    cbfunc(PMIX_SUCCESS, NULL, 0, NULL, NULL, cbdata);
}

pmix_proc_t ompi_intance_get_pmixid(){
    return opal_process_info.myprocid;
}

/* struct to hold current res_change */
typedef struct{
    char *rc_tag;
    char *rc_type;
    char *rc_pset;
    char *rc_port;
} ompi_instance_res_change;
static ompi_instance_res_change ompi_mpi_instance_res_change;


void ompi_instance_clear_rc_cache(char *delta_pset){
    opal_mutex_lock(&tracking_structures_lock);
    ompi_mpi_instance_resource_change_t *rc_remove = get_res_change_by_name(delta_pset);
    if(NULL != rc_remove){
        //opal_list_remove_item(&ompi_mpi_instance_resource_changes, &rc_remove->super);
        rc_remove->status = RC_FINALIZED;
        //OBJ_RELEASE(rc_remove);
    }

    opal_mutex_unlock(&tracking_structures_lock);
}

/*
 * Per MPI-2:9.5.3, MPI_REGISTER_DATAREP is a memory leak.  There is
 * no way to *de*register datareps once they've been registered.  So
 * we have to track all registrations here so that they can be
 * de-registered during MPI_FINALIZE so that memory-tracking debuggers
 * don't show Open MPI as leaking memory.
 */
opal_list_t ompi_registered_datareps = {{0}};

opal_pointer_array_t ompi_instance_f_to_c_table = {{0}};

static int ompi_instance_print_error (const char *error, int ret)
{
    /* Only print a message if one was not already printed */
    if (NULL != error && OMPI_ERR_SILENT != ret) {
        const char *err_msg = opal_strerror(ret);
        opal_show_help("help-mpi-runtime.txt",
                       "mpi_init:startup:internal-failure", true,
                       "MPI_INIT", "MPI_INIT", error, err_msg, ret);
    }

    return ret;
}

static int ompi_mpi_instance_cleanup_pml (void)
{
    /* call del_procs on all allocated procs even though some may not be known
     * to the pml layer. the pml layer is expected to be resilient and ignore
     * any unknown procs. */
    size_t nprocs = 0;
    ompi_proc_t **procs;

    procs = ompi_proc_get_allocated (&nprocs);
    MCA_PML_CALL(del_procs(procs, nprocs));
    free(procs);

    return OMPI_SUCCESS;
}

/**
 * Static functions used to configure the interactions between the OPAL and
 * the runtime.
 */
static char *_process_name_print_for_opal (const opal_process_name_t procname)
{
    ompi_process_name_t *rte_name = (ompi_process_name_t*)&procname;
    return OMPI_NAME_PRINT(rte_name);
}

static int _process_name_compare (const opal_process_name_t p1, const opal_process_name_t p2)
{
    ompi_process_name_t *o1 = (ompi_process_name_t *) &p1;
    ompi_process_name_t *o2 = (ompi_process_name_t *) &p2;
    return ompi_rte_compare_name_fields(OMPI_RTE_CMP_ALL, o1, o2);
}

static int _convert_string_to_process_name (opal_process_name_t *name, const char* name_string)
{
    return ompi_rte_convert_string_to_process_name(name, name_string);
}

static int _convert_process_name_to_string (char **name_string, const opal_process_name_t *name)
{
    return ompi_rte_convert_process_name_to_string(name_string, name);
}

static int32_t ompi_mpi_instance_init_basic_count;
static bool ompi_instance_basic_init;

void ompi_mpi_instance_release (void)
{
    opal_mutex_lock (&instance_lock);

    if (0 != --ompi_mpi_instance_init_basic_count) {
        opal_mutex_unlock (&instance_lock);
        return;
    }

    OPAL_LIST_DESTRUCT(&ompi_mpi_instance_resource_changes);
    OPAL_LIST_DESTRUCT(&ompi_mpi_instance_pmix_psets);
    OBJ_DESTRUCT(&app_pset);
    OBJ_DESTRUCT(&self_pset);

    opal_finalize_cleanup_domain (&ompi_instance_basic_domain);
    OBJ_DESTRUCT(&ompi_instance_basic_domain);

    opal_finalize_util ();

    opal_mutex_unlock (&instance_lock);
}

int ompi_mpi_instance_retain (void)
{
    int ret;
    opal_mutex_lock (&instance_lock);

    if (0 < ompi_mpi_instance_init_basic_count++) {
        opal_mutex_unlock (&instance_lock);
        return OMPI_SUCCESS;
    }

    /* Setup enough to check get/set MCA params */
    if (OPAL_SUCCESS != (ret = opal_init_util (NULL, NULL))) {
        opal_mutex_unlock (&instance_lock);
        return ompi_instance_print_error ("ompi_mpi_instance_init: opal_init_util failed", ret);
    }

    ompi_instance_basic_init = true;

    OBJ_CONSTRUCT(&ompi_instance_basic_domain, opal_finalize_domain_t);
    opal_finalize_domain_init (&ompi_instance_basic_domain, "ompi_mpi_instance_retain");
    opal_finalize_set_domain (&ompi_instance_basic_domain);

    /* Setup f to c table */
    OBJ_CONSTRUCT(&ompi_instance_f_to_c_table, opal_pointer_array_t);
    if (OPAL_SUCCESS != opal_pointer_array_init (&ompi_instance_f_to_c_table, 8,
                                                 OMPI_FORTRAN_HANDLE_MAX, 32)) {
        return OMPI_ERROR;
    }

    /* setup the default error handler on instance_null */
    OBJ_CONSTRUCT(&ompi_mpi_instance_null, ompi_instance_t);
    ompi_mpi_instance_null.instance.error_handler = &ompi_mpi_errors_return.eh;

    /* Convince OPAL to use our naming scheme */
    opal_process_name_print = _process_name_print_for_opal;
    opal_compare_proc = _process_name_compare;
    opal_convert_string_to_process_name = _convert_string_to_process_name;
    opal_convert_process_name_to_string = _convert_process_name_to_string;
    opal_proc_for_name = ompi_proc_for_name;

    /* Register MCA variables */
    if (OPAL_SUCCESS != (ret = ompi_mpi_register_params ())) {
        opal_mutex_unlock (&instance_lock);
        return ompi_instance_print_error ("ompi_mpi_init: ompi_register_mca_variables failed", ret);
    }

    /* initialize error handlers */
    if (OMPI_SUCCESS != (ret = ompi_errhandler_init ())) {
        opal_mutex_unlock (&instance_lock);
        return ompi_instance_print_error ("ompi_errhandler_init() failed", ret);
    }

    /* initialize error codes */
    if (OMPI_SUCCESS != (ret = ompi_mpi_errcode_init ())) {
        opal_mutex_unlock (&instance_lock);
        return ompi_instance_print_error ("ompi_mpi_errcode_init() failed", ret);
    }

    /* initialize internal error codes */
    if (OMPI_SUCCESS != (ret = ompi_errcode_intern_init ())) {
        opal_mutex_unlock (&instance_lock);
        return ompi_instance_print_error ("ompi_errcode_intern_init() failed", ret);
    }

    /* initialize info */
    if (OMPI_SUCCESS != (ret = ompi_mpiinfo_init ())) {
        return ompi_instance_print_error ("ompi_info_init() failed", ret);
    }

    ompi_instance_basic_init = false;

    opal_mutex_unlock (&instance_lock);

    return OMPI_SUCCESS;
}

static void fence_release(pmix_status_t status, void *cbdata)
{
    volatile bool *active = (volatile bool*)cbdata;
    OPAL_ACQUIRE_OBJECT(active);
    *active = false;
    OPAL_POST_OBJECT(active);
}

static void evhandler_reg_callbk(pmix_status_t status,
                                 size_t evhandler_ref,
                                 void *cbdata)
{
    opal_pmix_lock_t *lock = (opal_pmix_lock_t*)cbdata;

    lock->status = status;
    OPAL_PMIX_WAKEUP_THREAD(lock);
}

/**
 * @brief Function that starts up the common components needed by all instances
 */
static int ompi_mpi_instance_init_common (void)
{
    int ret;
    ompi_proc_t **procs;
    size_t nprocs;
    volatile bool active;
    bool background_fence = false;
    pmix_info_t info[2];
    pmix_status_t rc;
    opal_pmix_lock_t mylock;
    OMPI_TIMING_INIT(64);

    ret = ompi_mpi_instance_retain ();
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }

    OBJ_CONSTRUCT(&ompi_instance_common_domain, opal_finalize_domain_t);
    opal_finalize_domain_init (&ompi_instance_common_domain, "ompi_mpi_instance_init_common");
    opal_finalize_set_domain (&ompi_instance_common_domain);

    if (OPAL_SUCCESS != (ret = opal_arch_set_fortran_logical_size(sizeof(ompi_fortran_logical_t)))) {
        return ompi_instance_print_error ("ompi_mpi_init: opal_arch_set_fortran_logical_size failed", ret);
    }

    /* _After_ opal_init_util() but _before_ orte_init(), we need to
       set an MCA param that tells libevent that it's ok to use any
       mechanism in libevent that is available on this platform (e.g.,
       epoll and friends).  Per opal/event/event.s, we default to
       select/poll -- but we know that MPI processes won't be using
       pty's with the event engine, so it's ok to relax this
       constraint and let any fd-monitoring mechanism be used. */

    ret = mca_base_var_find("opal", "event", "*", "event_include");
    if (ret >= 0) {
        char *allvalue = "all";
        /* We have to explicitly "set" the MCA param value here
           because libevent initialization will re-register the MCA
           param and therefore override the default. Setting the value
           here puts the desired value ("all") in different storage
           that is not overwritten if/when the MCA param is
           re-registered. This is unless the user has specified a different
           value for this MCA parameter. Make sure we check to see if the
           default is specified before forcing "all" in case that is not what
           the user desires. Note that we do *NOT* set this value as an
           environment variable, just so that it won't be inherited by
           any spawned processes and potentially cause unintented
           side-effects with launching RTE tools... */
        mca_base_var_set_value(ret, allvalue, 4, MCA_BASE_VAR_SOURCE_DEFAULT, NULL);
    }

    OMPI_TIMING_NEXT("initialization");

    /* Setup RTE */
    if (OMPI_SUCCESS != (ret = ompi_rte_init (NULL, NULL))) {
        return ompi_instance_print_error ("ompi_mpi_init: ompi_rte_init failed", ret);
    }
    /* open the ompi hook framework */
    for (int i = 0 ; ompi_framework_dependencies[i] ; ++i) {
        ret = mca_base_framework_open (ompi_framework_dependencies[i], 0);
        if (OPAL_UNLIKELY(OPAL_SUCCESS != ret)) {
            char error_msg[256];
            snprintf (error_msg, sizeof(error_msg), "mca_base_framework_open on %s_%s failed",
                      ompi_framework_dependencies[i]->framework_project,
                      ompi_framework_dependencies[i]->framework_name);
            return ompi_instance_print_error (error_msg, ret);
        }
    }

    OBJ_CONSTRUCT(&ompi_mpi_instance_pmix_psets, opal_list_t);
    OBJ_CONSTRUCT(&ompi_mpi_instance_resource_changes, opal_list_t);
    OBJ_CONSTRUCT(&tracking_structures_lock, opal_recursive_mutex_t);


    char app_pset_name[] = "mpi://app";
    char self_pset_name[] = "mpi://SELF";
    OBJ_CONSTRUCT(&app_pset, ompi_mpi_instance_pset_t);
    OBJ_CONSTRUCT(&self_pset, ompi_mpi_instance_pset_t);
    strcpy(app_pset.name, app_pset_name);
    strcpy(self_pset.name, self_pset_name);

    OMPI_TIMING_NEXT("rte_init");
    OMPI_TIMING_IMPORT_OPAL("orte_ess_base_app_setup");
    OMPI_TIMING_IMPORT_OPAL("rte_init");

    ompi_rte_initialized = true;

    /* Register the default errhandler callback  */
    /* we want to go first */
    PMIX_INFO_LOAD(&info[0], PMIX_EVENT_HDLR_PREPEND, NULL, PMIX_BOOL);
    /* give it a name so we can distinguish it */
    PMIX_INFO_LOAD(&info[1], PMIX_EVENT_HDLR_NAME, "MPI-Default", PMIX_STRING);
    OPAL_PMIX_CONSTRUCT_LOCK(&mylock);
    PMIx_Register_event_handler(NULL, 0, info, 2, ompi_errhandler_callback, evhandler_reg_callbk, (void*)&mylock);
    OPAL_PMIX_WAIT_THREAD(&mylock);
    rc = mylock.status;
    OPAL_PMIX_DESTRUCT_LOCK(&mylock);
    PMIX_INFO_DESTRUCT(&info[0]);
    PMIX_INFO_DESTRUCT(&info[1]);
    if (PMIX_SUCCESS != rc) {
        ret = opal_pmix_convert_status(rc);
        return ret;
    }

    pmix_status_t code;
    /* register event handler to track the runtimes actions related to psets */
    code = PMIX_PROCESS_SET_DEFINE;
    OPAL_PMIX_CONSTRUCT_LOCK(&mylock);
    PMIx_Register_event_handler(&code, 1, NULL, 0, pset_define_handler, evhandler_reg_callbk, (void*)&mylock);
    OPAL_PMIX_WAIT_THREAD(&mylock);
    rc = mylock.status;
    OPAL_PMIX_DESTRUCT_LOCK(&mylock);
    if (PMIX_SUCCESS != rc) {
        ret = opal_pmix_convert_status(rc);
        return ret;
    }

    code = PMIX_PROCESS_SET_DELETE;
    OPAL_PMIX_CONSTRUCT_LOCK(&mylock);
    PMIx_Register_event_handler(&code, 1, NULL, 0, pset_delete_handler, evhandler_reg_callbk, (void*)&mylock);
    OPAL_PMIX_WAIT_THREAD(&mylock);
    rc = mylock.status;
    OPAL_PMIX_DESTRUCT_LOCK(&mylock);
    if (PMIX_SUCCESS != rc) {
        ret = opal_pmix_convert_status(rc);
        return ret;
    }

    /* register event handler to track the runtimes actions related to resource changes */
    code = PMIX_RC_FINALIZED;
    OPAL_PMIX_CONSTRUCT_LOCK(&mylock);
    rc=PMIx_Register_event_handler(&code, 1, NULL, 0, rc_finalize_handler, evhandler_reg_callbk, (void*)&mylock);
    OPAL_PMIX_WAIT_THREAD(&mylock);
    rc = mylock.status;
    OPAL_PMIX_DESTRUCT_LOCK(&mylock);
    if (PMIX_SUCCESS != rc) {
        ret = opal_pmix_convert_status(rc);
        return ret;
    }


    /* initialize info */
    if (OMPI_SUCCESS != (ret = ompi_mpiinfo_init_mpi3())) {
        return ompi_instance_print_error ("ompi_info_init_mpi3() failed", ret);
    }

    /* declare our presence for interlib coordination, and
     * register for callbacks when other libs declare. XXXXXX -- TODO -- figure out how
     * to specify the thread level when different instances may request different levels. */
    if (OMPI_SUCCESS != (ret = ompi_interlib_declare(MPI_THREAD_MULTIPLE, OMPI_IDENT_STRING))) {
        return ompi_instance_print_error ("ompi_interlib_declare", ret);
    }

    /* initialize datatypes. This step should be done early as it will
     * create the local convertor and local arch used in the proc
     * init.
     */
    if (OMPI_SUCCESS != (ret = ompi_datatype_init())) {
        return ompi_instance_print_error ("ompi_datatype_init() failed", ret);
    }

    /* Initialize OMPI procs */
    if (OMPI_SUCCESS != (ret = ompi_proc_init())) {
        return ompi_instance_print_error ("mca_proc_init() failed", ret);
    }

    /* Initialize the op framework. This has to be done *after*
       ddt_init, but befor mca_coll_base_open, since some collective
       modules (e.g., the hierarchical coll component) may need ops in
       their query function. */
    if (OMPI_SUCCESS != (ret = ompi_op_base_find_available (OPAL_ENABLE_PROGRESS_THREADS, ompi_mpi_thread_multiple))) {
        return ompi_instance_print_error ("ompi_op_base_find_available() failed", ret);
    }

    if (OMPI_SUCCESS != (ret = ompi_op_init())) {
        return ompi_instance_print_error ("ompi_op_init() failed", ret);
    }

    /* In order to reduce the common case for MPI apps (where they
       don't use MPI-2 IO or MPI-1/3 topology functions), the io and
       topo frameworks are initialized lazily, at the first use of
       relevant functions (e.g., MPI_FILE_*, MPI_CART_*, MPI_GRAPH_*),
       so they are not opened here. */

    /* Select which MPI components to use */

    if (OMPI_SUCCESS != (ret = mca_pml_base_select (OPAL_ENABLE_PROGRESS_THREADS, ompi_mpi_thread_multiple))) {
        return ompi_instance_print_error ("mca_pml_base_select() failed", ret);
    }

    OMPI_TIMING_IMPORT_OPAL("orte_init");
    OMPI_TIMING_NEXT("rte_init-commit");

    /* exchange connection info - this function may also act as a barrier
     * if data exchange is required. The modex occurs solely across procs
     * in our job. If a barrier is required, the "modex" function will
     * perform it internally */
    rc = PMIx_Commit();
    if (PMIX_SUCCESS != rc) {
        ret = opal_pmix_convert_status(rc);
        return ret;  /* TODO: need to fix this */
    }

   OMPI_TIMING_NEXT("commit");
#if (OPAL_ENABLE_TIMING)
    if (OMPI_TIMING_ENABLED && !opal_pmix_base_async_modex &&
            opal_pmix_collect_all_data && !ompi_singleton) {
        if (PMIX_SUCCESS != (rc = PMIx_Fence(NULL, 0, NULL, 0))) {
            ret = opal_pmix_convert_status(rc);
            return ompi_instance_print_error ("timing: pmix-barrier-1 failed", ret);
        }
        OMPI_TIMING_NEXT("pmix-barrier-1");
        if (PMIX_SUCCESS != (rc = PMIx_Fence(NULL, 0, NULL, 0))) {
            return ompi_instance_print_error ("timing: pmix-barrier-2 failed", ret);
        }
        OMPI_TIMING_NEXT("pmix-barrier-2");
    }
#endif
#define MPI_MALLEABLE 1
pmix_proc_t *fence_procs=NULL;
size_t fence_nprocs=0;
#if MPI_MALLEABLE
    ompi_rc_op_type_t rc_op;
    ompi_rc_status_t rc_status;
    char delta_pset[PMIX_MAX_KEYLEN];
    char bound_pset[] = "mpi://app";
    int incl = 0;
    //ompi_instance_refresh_pmix_psets(PMIX_QUERY_PSET_NAMES);
    int res_rc = ompi_instance_get_res_change(ompi_mpi_instance_default, bound_pset, &rc_op, delta_pset, &incl, &rc_status,  NULL, false);
    if(rc_op != OMPI_RC_NULL && incl){
        struct timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 10000;
        opal_process_name_t *pset_procs;

        size_t pset_nprocs;
        if(PMIX_SUCCESS!= (rc = ompi_instance_get_pset_membership(ompi_mpi_instance_default, delta_pset, &pset_procs, &pset_nprocs))){
            ret = opal_pmix_convert_status(rc);
            return ret;  /* TODO: need to fix this */
        }
        if(opal_is_pset_member(pset_procs, pset_nprocs, opal_process_info.my_name)){
            fence_nprocs = pset_nprocs;
            opal_pmix_proc_array_conv(pset_procs, &fence_procs, fence_nprocs);
        }
    }
#endif
   if (!ompi_singleton) {
        if (opal_pmix_base_async_modex) {
            /* if we are doing an async modex, but we are collecting all
             * data, then execute the non-blocking modex in the background.
             * All calls to modex_recv will be cached until the background
             * modex completes. If collect_all_data is false, then we skip
             * the fence completely and retrieve data on-demand from the
             * source node.
             */
            if (opal_pmix_collect_all_data) {
                /* execute the fence_nb in the background to collect
                 * the data */
                
                background_fence = true;
                active = true;
                OPAL_POST_OBJECT(&active);
                PMIX_INFO_LOAD(&info[0], PMIX_COLLECT_DATA, &opal_pmix_collect_all_data, PMIX_BOOL);
                if( PMIX_SUCCESS != (rc = PMIx_Fence_nb(fence_procs, fence_nprocs, NULL, 0,
                                                        fence_release,
                                                        (void*)&active))) {
                    ret = opal_pmix_convert_status(rc);
                    return ompi_instance_print_error ("PMIx_Fence_nb() failed", ret);
                }
            }
        } else {
            /* we want to do the modex - we block at this point, but we must
             * do so in a manner that allows us to call opal_progress so our
             * event library can be cycled as we have tied PMIx to that
             * event base */
            active = true;
            OPAL_POST_OBJECT(&active);
            PMIX_INFO_LOAD(&info[0], PMIX_COLLECT_DATA, &opal_pmix_collect_all_data, PMIX_BOOL);
            rc = PMIx_Fence_nb(fence_procs, fence_nprocs, info, 1, fence_release, (void*)&active);
            if( PMIX_SUCCESS != rc) {
                ret = opal_pmix_convert_status(rc);
                return ompi_instance_print_error ("PMIx_Fence() failed", ret);
            }
            /* cannot just wait on thread as we need to call opal_progress */
            OMPI_LAZY_WAIT_FOR_COMPLETION(active);
        }
    }
    if(NULL != fence_procs){
        free(fence_procs);
    }
    if(rc_op != OMPI_RC_NULL){
        ompi_instance_free_pset_membership(delta_pset);
    }
    OMPI_TIMING_NEXT("modex");

    /* select buffered send allocator component to be used */
    if (OMPI_SUCCESS != (ret = mca_pml_base_bsend_init ())) {
        return ompi_instance_print_error ("mca_pml_base_bsend_init() failed", ret);
    }

    if (OMPI_SUCCESS != (ret = mca_coll_base_find_available (OPAL_ENABLE_PROGRESS_THREADS, ompi_mpi_thread_multiple))) {
        return ompi_instance_print_error ("mca_coll_base_find_available() failed", ret);
    }

    if (OMPI_SUCCESS != (ret = ompi_osc_base_find_available (OPAL_ENABLE_PROGRESS_THREADS, ompi_mpi_thread_multiple))) {
        return ompi_instance_print_error ("ompi_osc_base_find_available() failed", ret);
    }

    /* io and topo components are not selected here -- see comment
       above about the io and topo frameworks being loaded lazily */

    /* Initialize each MPI handle subsystem */
    /* initialize requests */
    if (OMPI_SUCCESS != (ret = ompi_request_init ())) {
        return ompi_instance_print_error ("ompi_request_init() failed", ret);
    }

    if (OMPI_SUCCESS != (ret = ompi_message_init ())) {
        return ompi_instance_print_error ("ompi_message_init() failed", ret);
    }

    /* initialize groups  */
    if (OMPI_SUCCESS != (ret = ompi_group_init ())) {
        return ompi_instance_print_error ("ompi_group_init() failed", ret);
    }

    ompi_mpi_instance_append_finalize (ompi_mpi_instance_cleanup_pml);

    /* initialize communicator subsystem */
    if (OMPI_SUCCESS != (ret = ompi_comm_init ())) {
        opal_mutex_unlock (&instance_lock);
        return ompi_instance_print_error ("ompi_comm_init() failed", ret);
    }

    if (mca_pml_base_requires_world ()) {
        /* need to set up comm world for this instance -- XXX -- FIXME -- probably won't always
         * be the case. */
        if (OMPI_SUCCESS != (ret = ompi_comm_init_mpi3 ())) {
            return ompi_instance_print_error ("ompi_comm_init_mpi3 () failed", ret);
        }
    }

    /* initialize file handles */
    if (OMPI_SUCCESS != (ret = ompi_file_init ())) {
        return ompi_instance_print_error ("ompi_file_init() failed", ret);
    }

    /* initialize windows */
    if (OMPI_SUCCESS != (ret = ompi_win_init ())) {
        return ompi_instance_print_error ("ompi_win_init() failed", ret);
    }

    /* Setup the dynamic process management (DPM) subsystem */
    if (OMPI_SUCCESS != (ret = ompi_dpm_init ())) {
        return ompi_instance_print_error ("ompi_dpm_init() failed", ret);
    }


    /* identify the architectures of remote procs and setup
     * their datatype convertors, if required
     */
    if (OMPI_SUCCESS != (ret = ompi_proc_complete_init())) {
        return ompi_instance_print_error ("ompi_proc_complete_init failed", ret);
    }

    /* start PML/BTL's */
    ret = MCA_PML_CALL(enable(true));
    if( OMPI_SUCCESS != ret ) {
        return ompi_instance_print_error ("PML control failed", ret);
    }

    /* some btls/mtls require we call add_procs with all procs in the job.
     * since the btls/mtls have no visibility here it is up to the pml to
     * convey this requirement */
    if (mca_pml_base_requires_world ()) {
        if (NULL == (procs = ompi_proc_world (&nprocs))) {
            return ompi_instance_print_error ("ompi_proc_get_allocated () failed", ret);
        }
    } else {
        /* add all allocated ompi_proc_t's to PML (below the add_procs limit this
         * behaves identically to ompi_proc_world ()) */
        if (NULL == (procs = ompi_proc_get_allocated (&nprocs))) {
            return ompi_instance_print_error ("ompi_proc_get_allocated () failed", ret);
        }
    }

    ret = MCA_PML_CALL(add_procs(procs, nprocs));
    free(procs);
    /* If we got "unreachable", then print a specific error message.
       Otherwise, if we got some other failure, fall through to print
       a generic message. */
    if (OMPI_ERR_UNREACH == ret) {
        opal_show_help("help-mpi-runtime.txt",
                       "mpi_init:startup:pml-add-procs-fail", true);
        return ret;
    } else if (OMPI_SUCCESS != ret) {
        return ompi_instance_print_error ("PML add procs failed", ret);
    }

    /* Determine the overall threadlevel support of all processes
       in MPI_COMM_WORLD. This has to be done before calling
       coll_base_comm_select, since some of the collective components
       e.g. hierarch, might create subcommunicators. The threadlevel
       requested by all processes is required in order to know
       which cid allocation algorithm can be used. */
    if (OMPI_SUCCESS != ( ret = ompi_comm_cid_init ())) {
        return ompi_instance_print_error ("ompi_mpi_init: ompi_comm_cid_init failed", ret);
    }

    /* Do we need to wait for a debugger? */
    ompi_rte_wait_for_debugger();

    /* Next timing measurement */
    OMPI_TIMING_NEXT("modex-barrier");
    if (!ompi_singleton && !MPI_ALL_ASYNC) {
        /* if we executed the above fence in the background, then
         * we have to wait here for it to complete. However, there
         * is no reason to do two barriers! */
        if (background_fence) {
            OMPI_LAZY_WAIT_FOR_COMPLETION(active);
        } else if (!ompi_async_mpi_init) {
            /* wait for everyone to reach this point - this is a hard
             * barrier requirement at this time, though we hope to relax
             * it at a later point */
            bool flag = false;
            active = true;
            OPAL_POST_OBJECT(&active);
            PMIX_INFO_LOAD(&info[0], PMIX_COLLECT_DATA, &flag, PMIX_BOOL);
            if (PMIX_SUCCESS != (rc = PMIx_Fence_nb(NULL, 0, info, 1,
                                                    fence_release, (void*)&active))) {
                ret = opal_pmix_convert_status(rc);
                return ompi_instance_print_error ("PMIx_Fence_nb() failed", ret);
            }
            OMPI_LAZY_WAIT_FOR_COMPLETION(active);
        }
    }
    /* check for timing request - get stop time and report elapsed
       time if so, then start the clock again */
    OMPI_TIMING_NEXT("barrier");

#if OPAL_ENABLE_PROGRESS_THREADS == 0
    /* Start setting up the event engine for MPI operations.  Don't
       block in the event library, so that communications don't take
       forever between procs in the dynamic code.  This will increase
       CPU utilization for the remainder of MPI_INIT when we are
       blocking on RTE-level events, but may greatly reduce non-TCP
       latency. */
    opal_progress_set_event_flag(OPAL_EVLOOP_NONBLOCK);
#endif

    /* Undo OPAL calling opal_progress_event_users_increment() during
       opal_init, to get better latency when not using TCP.  Do
       this *after* dyn_init, as dyn init uses lots of RTE
       communication and we don't want to hinder the performance of
       that code. */
    opal_progress_event_users_decrement();

    /* see if yield_when_idle was specified - if so, use it */
    opal_progress_set_yield_when_idle (ompi_mpi_yield_when_idle);

    /* negative value means use default - just don't do anything */
    if (ompi_mpi_event_tick_rate >= 0) {
        opal_progress_set_event_poll_rate(ompi_mpi_event_tick_rate);
    }

    /* At this point, we are fully configured and in MPI mode.  Any
       communication calls here will work exactly like they would in
       the user's code.  Setup the connections between procs and warm
       them up with simple sends, if requested */

    if (OMPI_SUCCESS != (ret = ompi_mpiext_init())) {
        return ompi_instance_print_error ("ompi_mpiext_init", ret);
    }

    /* Initialize the registered datarep list to be empty */
    OBJ_CONSTRUCT(&ompi_registered_datareps, opal_list_t);

    /* Initialize the arrays used to store the F90 types returned by the
     *  MPI_Type_create_f90_XXX functions.
     */
    OBJ_CONSTRUCT( &ompi_mpi_f90_integer_hashtable, opal_hash_table_t);
    opal_hash_table_init(&ompi_mpi_f90_integer_hashtable, 16 /* why not? */);

    OBJ_CONSTRUCT( &ompi_mpi_f90_real_hashtable, opal_hash_table_t);
    opal_hash_table_init(&ompi_mpi_f90_real_hashtable, FLT_MAX_10_EXP);

    OBJ_CONSTRUCT( &ompi_mpi_f90_complex_hashtable, opal_hash_table_t);
    opal_hash_table_init(&ompi_mpi_f90_complex_hashtable, FLT_MAX_10_EXP);

    return OMPI_SUCCESS;
}

int ompi_mpi_instance_init (int ts_level,  opal_info_t *info, ompi_errhandler_t *errhandler, ompi_instance_t **instance)
{
    ompi_instance_t *new_instance;
    int ret;

    *instance = &ompi_mpi_instance_null.instance;

    /* If thread support was enabled, then setup OPAL to allow for them by deault. This must be done
     * early to prevent a race condition that can occur with orte_init(). */
    if (ts_level == MPI_THREAD_MULTIPLE) {
        opal_set_using_threads(true);
    }

    opal_mutex_lock (&instance_lock);
    if (0 == opal_atomic_fetch_add_32 (&ompi_instance_count, 1)) {
        ret = ompi_mpi_instance_init_common ();
        if (OPAL_UNLIKELY(OPAL_SUCCESS != ret)) {
            opal_mutex_unlock (&instance_lock);
            return ret;
        }
    }

    new_instance = OBJ_NEW(ompi_instance_t);
    if (OPAL_UNLIKELY(NULL == new_instance)) {
        if (0 == opal_atomic_add_fetch_32 (&ompi_instance_count, -1)) {
            ret = ompi_mpi_instance_finalize_common ();
            if (OPAL_UNLIKELY(OPAL_SUCCESS != ret)) {
                opal_mutex_unlock (&instance_lock);
            }
        }
        opal_mutex_unlock (&instance_lock);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    new_instance->error_handler = errhandler;
    OBJ_RETAIN(new_instance->error_handler);

    /* Copy info if there is one. */
    if (OPAL_UNLIKELY(NULL != info)) {
        new_instance->super.s_info = OBJ_NEW(opal_info_t);
        if (info) {
            opal_info_dup(info, &new_instance->super.s_info);
        }
    }

    *instance = new_instance;
    opal_mutex_unlock (&instance_lock);

    return OMPI_SUCCESS;
}

static int ompi_mpi_instance_finalize_common (void)
{
    uint32_t key;
    ompi_datatype_t *datatype;
    int ret;

    /* As finalize is the last legal MPI call, we are allowed to force the release
     * of the user buffer used for bsend, before going anywhere further.
     */
    (void) mca_pml_base_bsend_detach (NULL, NULL);

    /* Shut down any bindings-specific issues: C++, F77, F90 */

    /* Remove all memory associated by MPI_REGISTER_DATAREP (per
       MPI-2:9.5.3, there is no way for an MPI application to
       *un*register datareps, but we don't want the OMPI layer causing
       memory leaks). */
    OPAL_LIST_DESTRUCT(&ompi_registered_datareps);

    /* Remove all F90 types from the hash tables */
    OPAL_HASH_TABLE_FOREACH(key, uint32, datatype, &ompi_mpi_f90_integer_hashtable)
        OBJ_RELEASE(datatype);
    OBJ_DESTRUCT(&ompi_mpi_f90_integer_hashtable);
    OPAL_HASH_TABLE_FOREACH(key, uint32, datatype, &ompi_mpi_f90_real_hashtable)
        OBJ_RELEASE(datatype);
    OBJ_DESTRUCT(&ompi_mpi_f90_real_hashtable);
    OPAL_HASH_TABLE_FOREACH(key, uint32, datatype, &ompi_mpi_f90_complex_hashtable)
        OBJ_RELEASE(datatype);
    OBJ_DESTRUCT(&ompi_mpi_f90_complex_hashtable);

    /* If requested, print out a list of memory allocated by ALLOC_MEM
       but not freed by FREE_MEM */
    if (0 != ompi_debug_show_mpi_alloc_mem_leaks) {
        mca_mpool_base_tree_print (ompi_debug_show_mpi_alloc_mem_leaks);
    }

    opal_finalize_cleanup_domain (&ompi_instance_common_domain);

    if (NULL != ompi_mpi_main_thread) {
        OBJ_RELEASE(ompi_mpi_main_thread);
        ompi_mpi_main_thread = NULL;
    }

    /* Leave the RTE */
    if (OMPI_SUCCESS != (ret = ompi_rte_finalize())) {
        return ret;
    }
    ompi_rte_initialized = false;

    for (int i = 0 ; ompi_lazy_frameworks[i] ; ++i) {
        if (0 < ompi_lazy_frameworks[i]->framework_refcnt) {
            /* May have been "opened" multiple times. We want it closed now! */
            ompi_lazy_frameworks[i]->framework_refcnt = 1;

            ret = mca_base_framework_close (ompi_lazy_frameworks[i]);
            if (OPAL_UNLIKELY(OPAL_SUCCESS != ret)) {
                return ret;
            }
        }
    }

    int last_framework = 0;
    for (int i = 0 ; ompi_framework_dependencies[i] ; ++i) {
        last_framework = i;
    }

    for (int j = last_framework ; j >= 0; --j) {
        ret = mca_base_framework_close (ompi_framework_dependencies[j]);
        if (OPAL_UNLIKELY(OPAL_SUCCESS != ret)) {
            return ret;
        }
    }
    ompi_proc_finalize();
    OBJ_DESTRUCT(&ompi_mpi_instance_null);

    ompi_mpi_instance_release ();
    if (0 == opal_initialized) {
        /* if there is no MPI_T_init_thread that has been MPI_T_finalize'd,
         * then be gentle to the app and release all the memory now (instead
         * of the opal library destructor */
        /* if there is no MPI_T_init_thread that has been MPI_T_finalize'd,
         * then be gentle to the app and release all the memory now (instead
         * of the opal library destructor */
        opal_class_finalize ();
    }
    return OMPI_SUCCESS;
}

int ompi_mpi_instance_finalize (ompi_instance_t **instance)
{
    int ret = OMPI_SUCCESS;

    OBJ_RELEASE(*instance);

    opal_mutex_lock (&instance_lock);
    if (0 == opal_atomic_add_fetch_32 (&ompi_instance_count, -1)) {
        ret = ompi_mpi_instance_finalize_common ();
        if (OPAL_UNLIKELY(OPAL_SUCCESS != ret)) {
            opal_mutex_unlock (&instance_lock);
        }
    }
    opal_mutex_unlock (&instance_lock);

    *instance = &ompi_mpi_instance_null.instance;

    return ret;
}

/* Apply the specified resource change to the ompi internal data */
int ompi_mpi_instance_refresh (ompi_instance_t *instance, opal_info_t *info, char *pset_name, ompi_rc_op_type_t rc_type, char *result_pset, bool root){
    int ret, i;
    pmix_status_t rc;

    size_t nprocs;
    pmix_proc_t *fence_procs;
    opal_process_name_t *pset_procs=NULL;
    opal_process_name_t wildcard_rank;
    ompi_process_name_t ompi_proc_name;
    ompi_proc_t **ompi_procs=NULL;
    ompi_mpi_instance_resource_change_t *res_change;
    

    opal_mutex_lock (&instance_lock);

    ompi_instance_get_pset_membership(instance, pset_name, &pset_procs, &nprocs);
    
    if(NULL == pset_procs)printf("pset procs are null\n");
    //opal_mutex_lock(&tracking_structures_lock);   
	//printf("RANK %d: member: %d, type:%d!\n", opal_process_info.myprocid.rank, opal_is_pset_member(pset_procs, nprocs, opal_process_info.myprocid.rank), rc_type);    
    /* if this process is to be removed don't update the instance as it should finalize anyways */
    if(opal_is_pset_member(pset_procs, nprocs, opal_process_info.my_name) && rc_type == OMPI_RC_SUB){
	//printf("RANK %d: I am a member of the delta_pset!\n", opal_process_info.myprocid.rank);
        ompi_instance_free_pset_membership(pset_name);
        //opal_mutex_unlock(&tracking_structures_lock);

        struct timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 1000000;
        pmix_info_t *lookup_info;
        pmix_pdata_t lookup_data;

         /* Query Runtime until the finalization is published, then Session_finalize is allowed */
        PMIX_PDATA_CONSTRUCT(&lookup_data);
        PMIX_INFO_CREATE(lookup_info, 2);
        int wait = 0;
        int timeout = 120;
        PMIX_INFO_LOAD(&lookup_info[0], PMIX_WAIT, &wait, PMIX_INT);
        PMIX_INFO_LOAD(&lookup_info[1], PMIX_TIMEOUT, &timeout, PMIX_INT);
        (void)snprintf(lookup_data.key, PMIX_MAX_KEYLEN, "%s:finalize", pset_name);
        
        rc = PMIx_Lookup(&lookup_data, 1, lookup_info, 2);

        PMIX_INFO_FREE(lookup_info, 2);
        PMIX_PDATA_DESTRUCT(&lookup_data);

        //ompi_instance_free_pset_membership(pset_name);
        //opal_mutex_unlock(&tracking_structures_lock);
        opal_mutex_unlock (&instance_lock);
        return OPAL_ERR_BAD_PARAM;
    }

    
    /* If the resource change type is MPI_RC_SUB we need to remove all local endppoints a */
    if(rc_type == OMPI_RC_SUB){
        /* first we remove all endpoints as the local ranks could have changed. TDOD: only do it if they really changed */
        ompi_proc_t *proc;
        wildcard_rank.jobid = OMPI_PROC_MY_NAME->jobid;
        wildcard_rank.vpid = OMPI_NAME_WILDCARD->vpid;
        /* retrieve the local peers */
        char *val = NULL;
        OPAL_MODEX_RECV_VALUE(ret, PMIX_LOCAL_PEERS,
                              &wildcard_rank, &val, PMIX_STRING);

        if (OPAL_SUCCESS == ret && NULL != val) {
            char **peers = opal_argv_split(val, ',');
            free(val);
            /* remove endpoint information for all local peers */
            for (i=0; NULL != peers[i]; i++) {
		    
                ompi_vpid_t local_rank = strtoul(peers[i], NULL, 10);

		if(OMPI_PROC_MY_NAME->vpid == local_rank){
                    continue;
                }
                ompi_proc_name.jobid=OMPI_PROC_MY_NAME->jobid;
                ompi_proc_name.vpid=local_rank;
                bool is_new;
                proc = ompi_proc_find_and_add(&ompi_proc_name, &is_new);
                
                MCA_PML_CALL(del_procs(&proc,1)); 
            }
            /* also delete endpoint information for ourself */
            MCA_PML_CALL(del_procs(&ompi_proc_local_proc,1));
            
            opal_argv_free(peers);
        }
        
        
        /* now we destrcut all ompi_proc's that are removed by the resource res change. This will never include ourself */
        for(i=0; i < nprocs; i++){

            /* look for existing ompi_proc_t that matches this name */
            proc = (ompi_proc_t *) ompi_proc_lookup (pset_procs[i]);
            if( NULL != proc){
                ompi_proc_destruct(proc);
            }
        }
    }

    /* now we need to update the job info */
    //opal_mutex_unlock(&tracking_structures_lock);
    opal_mutex_unlock (&instance_lock);
    //printf("get job data\n");
    PMIx_Get_job_data();

    opal_mutex_lock (&instance_lock);
    //printf("lock again\n");
    //opal_mutex_lock(&tracking_structures_lock);

    //printf("refresh job size\n");
    /* update opal job size and peer information */
    ompi_rte_refresh_job_size();
    ompi_rte_refresh_peers(rc_type == OMPI_RC_SUB);
    

    /* 
    *  Now we (re-)add the local procs i.e. local peers 
    *  NOTE:    If non continous namespaces are used ompi_proc_complete_init() will only init local procs. 
    *           The rest will be added on demand at the latest at create_group_from_pset
    */
    ompi_proc_complete_init();

    /* 
    *  If we have a resource addition we need to add all remaining (remote) procs
    *  We do it here so we do not need to add them during communication setup
    */

    //ompi_instance_get_pset_membership(instance, pset_name, &pset_procs, &nprocs);
    if(rc_type == OMPI_RC_ADD){
        // add remote
        for (int i = 0 ; i < nprocs ; ++i ) {

            (void) ompi_proc_for_name (pset_procs[i]);
        }
        ompi_proc_list_sort();
    }
    ompi_instance_free_pset_membership(pset_name);

    if(rc_type == OMPI_RC_SUB){
        ompi_instance_get_pset_membership(instance, result_pset, &pset_procs, &nprocs);
        opal_pmix_proc_array_conv(pset_procs, &fence_procs, nprocs);
        PMIx_Fence(fence_procs, nprocs, NULL, 0);
        ompi_instance_free_pset_membership(result_pset);
        free(fence_procs);
    }
	//printf("after fence\n");

    //opal_mutex_unlock(&tracking_structures_lock);

    /* 
    *  We need to add the updated endpoint information for the local procs. 
    *  TODO: call add procs only for local ones 
    */    
    if (NULL == (ompi_procs = ompi_proc_get_allocated (&nprocs))) {
        printf("ompi_proc_get_allocated () failed\n");
    }

	//printf("Rank %d: have allocated %d procs\n",opal_process_info.myprocid.rank,nprocs);

    ret = MCA_PML_CALL(add_procs(ompi_procs, nprocs));
    free(ompi_procs);

    //printf("Rank %d: MCA_PML_CALL status: %d\n",opal_process_info.myprocid.rank, ret);



    if(root && rc_type == OMPI_RC_SUB){
        /* publish the accept */
        pmix_info_t publish_data;
        PMIX_INFO_CONSTRUCT(&publish_data);
        (void)snprintf(publish_data.key, PMIX_MAX_KEYLEN, "%s:finalize", pset_name);
        PMIX_VALUE_LOAD(&publish_data.value, result_pset, PMIX_STRING);
        //printf("finalize:publish\n");
        rc = PMIx_Publish(&publish_data, 1);
        //printf("finalize:publish status %d\n", rc);
        PMIX_PDATA_DESTRUCT(&publish_data);
    }
    opal_mutex_unlock (&instance_lock);
    return OPAL_SUCCESS;
}

static void ompi_instance_get_res_change_complete (pmix_status_t status, 
		                                            pmix_info_t *info,
		                                            size_t ninfo,
                                                    void *cbdata, 
                                                    pmix_release_cbfunc_t release_fn,
                                                    void *release_cbdata)
{
    size_t n;
    pmix_status_t rc;
    size_t sz;
    opal_pmix_lock_t *lock = (opal_pmix_lock_t *) cbdata;

    ompi_mpi_instance_resource_change_t* res_change = OBJ_NEW(ompi_mpi_instance_resource_change_t);
    
    if(status == PMIX_SUCCESS && ninfo >= 3){
        opal_mutex_lock(&tracking_structures_lock);

        for (n = 0; n < ninfo; n++) {
            if (0 == strcmp (info[n].key, "PMIX_RC_TYPE")) {
                res_change->type = info[n].value.data.uint8;
            } else if (0 == strcmp(info[n].key,"PMIX_RC_PSET")) {
                ompi_mpi_instance_pset_t *pset = get_pset_by_name(info[n].value.data.string);

                /* if we don't have this pset already we create a new one */
                if( NULL == pset){
                    pset = OBJ_NEW(ompi_mpi_instance_pset_t);
                    strcpy(pset->name, info[n].value.data.string);
                    pset->malleable=true;
                    pset->active=true;
                    pset->size=0;
                    pset->members=NULL;
                    opal_list_append(&ompi_mpi_instance_pmix_psets, &pset->super);
                }
                res_change->delta_pset = get_pset_by_name(info[n].value.data.string);
            } else if (0 == strcmp(info[n].key, PMIX_PSET_NAME)) {
                
                res_change->bound_pset = get_pset_by_name(info[n].value.data.string);
            }
        }

        if(res_change->type == OMPI_RC_NULL || res_change->delta_pset == NULL){
            OBJ_RELEASE(res_change);
        }else{
            res_change->status = RC_ANNOUNCED;
            opal_list_append(&ompi_mpi_instance_resource_changes, res_change);
        }
        opal_mutex_unlock(&tracking_structures_lock);
    }
      
    
    if (NULL != release_fn) {
        release_fn(release_cbdata);
    }
    OPAL_PMIX_WAKEUP_THREAD(lock);
}

int ompi_instance_get_res_change(ompi_instance_t *instance, char *pset_name, ompi_rc_op_type_t *type, char *delta_pset, int *incl, ompi_rc_status_t *status, opal_info_t **info_used, bool return_info){
    int ret = OPAL_SUCCESS;
    pmix_status_t rc;
    pmix_query_t query;
    opal_pmix_lock_t lock;
    bool refresh = true;

    //printf("start query info nb before locks for %d\n", opal_process_info.myprocid.rank);

    opal_mutex_lock (&instance_lock);  
    opal_mutex_lock (&tracking_structures_lock);

    if(NULL == pset_name){
        pset_name = "mpi://app";
    }

    //printf("start query info nb  after locks for %d\n", opal_process_info.myprocid.rank);

    ompi_mpi_instance_resource_change_t *res_change;

    /*
    if(NULL == (res_change = get_res_change_active_for_bound_name(pset_name))){
        printf("start query info nb NULL for %d\n", opal_process_info.myprocid.rank);
    }else{
        printf("start query info nb res status %d for %d\n", res_change->status, opal_process_info.myprocid.rank);
    }
    */
    
    /* if we don't find a valid & active res change locally, query the runtime. TODO: MPI Info directive QUERY RUNTIME */
    if(NULL == (res_change = get_res_change_active_for_bound_name(pset_name))){
        PMIX_QUERY_CONSTRUCT(&query);
        PMIX_ARGV_APPEND(rc, query.keys, "PMIX_RC_TYPE");
        PMIX_ARGV_APPEND(rc, query.keys, "PMIX_RC_PSET");

        query.nqual = 2;
        PMIX_INFO_CREATE(query.qualifiers, 2);
        PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
        PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PSET_NAME, pset_name, PMIX_STRING);
        
        opal_mutex_unlock (&tracking_structures_lock);
        OPAL_PMIX_CONSTRUCT_LOCK(&lock);
        //printf("start query info nb for %d\n", opal_process_info.myprocid.rank);
        /*
         * TODO: need to handle this better
         */
        if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(&query, 1, 
                                                     ompi_instance_get_res_change_complete,
                                                     (void*)&lock))) {
           printf("PMIx_Query_info_nb failed with error %d\n", rc);                                              
           opal_mutex_unlock (&instance_lock);
        }
        OPAL_PMIX_WAIT_THREAD(&lock);
        OPAL_PMIX_DESTRUCT_LOCK(&lock);
        opal_mutex_lock (&tracking_structures_lock);
    }

    /* if we don't have found an active res change with a delta pset 
     * search for invalid ones
     * If there arent any resource changes founf return an error 
     */
    if(NULL == (res_change = get_res_change_active_for_bound_name(pset_name)) || NULL == res_change->delta_pset){
        if(NULL == (res_change = get_res_change_for_bound_name(pset_name)) || NULL == res_change->delta_pset || RC_FINALIZED == res_change->status){
            opal_mutex_unlock (&tracking_structures_lock);
            *type = OMPI_RC_NULL;
            *incl = false;
            return OPAL_ERR_NOT_FOUND;
        }
    }

    /* lookup props of the resource change */
    *type=res_change->type;
    *status=res_change->status;

    /* check if we are a member of the pset */
    ompi_mpi_instance_pset_t *delta_pset_ptr;
    if(NULL != (delta_pset_ptr = res_change->delta_pset)){
        opal_process_name_t *procs = NULL;
        size_t nprocs;
        opal_mutex_unlock(&tracking_structures_lock);
        ompi_instance_get_pset_membership(ompi_mpi_instance_default, delta_pset_ptr->name, &procs, &nprocs);
        opal_mutex_lock (&tracking_structures_lock);
        strcpy(delta_pset, delta_pset_ptr->name);
        *incl = opal_is_pset_member(procs, nprocs, opal_process_info.my_name) ? 1 : 0;
        ompi_instance_free_pset_membership(delta_pset_ptr->name);
    }

    /* TODO: provide additional information in info object if requested */
    opal_mutex_unlock (&tracking_structures_lock);
    opal_mutex_unlock (&instance_lock);
    return OMPI_SUCCESS;
}

int ompi_instance_request_res_change(MPI_Session session, int delta, char *delta_pset, ompi_rc_op_type_t rc_type, MPI_Info *info){
        
    pmix_proc_t myproc = {"UNDEF", PMIX_RANK_UNDEF};
    int MAX_CMD_LEN = MPI_MAX_PSET_NAME_LEN;
    char rc_cmd[MAX_CMD_LEN];
    bool non_default=true;

    if(rc_type == MPI_RC_ADD){
        sprintf(rc_cmd, "pmix_session add %d", delta);
    }else{
        sprintf(rc_cmd, "pmix_session sub %d", delta);
    }

    pmix_info_t *pinfo;
    PMIX_INFO_CREATE(pinfo,2);

    strcpy(pinfo[0].key, "PMIX_RC_CMD");
    PMIX_VALUE_LOAD(&pinfo[0].value, (void**)&rc_cmd, PMIX_STRING);

    //strcpy(pinfo[1].key, "PMIX_RC_PSET");
    //PMIX_VALUE_LOAD(&pinfo[1].value, (void**)&delta_pset, PMIX_STRING);

    (void)snprintf(pinfo[1].key, PMIX_MAX_KEYLEN, "%s", PMIX_EVENT_NON_DEFAULT);
    PMIX_VALUE_LOAD(&pinfo[1].value, &non_default, PMIX_BOOL);

    PMIx_Notify_event(PMIX_RC_DEFINE, &myproc, PMIX_RANGE_RM, pinfo, 2, NULL, NULL);

    PMIX_INFO_FREE(pinfo, 2);
    return OMPI_SUCCESS;
}

int ompi_instance_accept_res_change(ompi_instance_t *instance, opal_info_t **info_used, char *delta_pset, char* new_pset, bool blocking){

    pmix_status_t rc;
    int ninfo = 0;

    /* define those in OMPI etc. */
    char *OMPI_CONFIRM_VAL="rc_confirmed";

    /* Not sure yet what data should be poseted at confimation */
    pmix_data_type_t confirm_data_type=PMIX_STRING;

    /* publish/lookup data structs */
    pmix_info_t publish_data;
    pmix_pdata_t lookup_data;
    pmix_info_t *lookup_info = NULL;

    if(blocking){
        //printf("blocking accept\n");
        ninfo = 2;

        PMIX_INFO_CREATE(lookup_info, ninfo);
        int wait = 0;
        int timeout = 120;
        PMIX_INFO_LOAD(&lookup_info[0], PMIX_WAIT, &wait, PMIX_INT);
        PMIX_INFO_LOAD(&lookup_info[1], PMIX_TIMEOUT, &timeout, PMIX_INT);

    }
    
    opal_mutex_lock (&tracking_structures_lock);
    ompi_mpi_instance_resource_change_t *res_change = get_res_change_by_name(delta_pset);
    if(NULL == res_change){
        printf("Invalid resource change name\n");
        return OPAL_ERR_BAD_PARAM;
    }
    ompi_rc_op_type_t rc_type = res_change->type;
    ompi_rc_status_t rc_status = res_change->status;
    opal_mutex_unlock (&tracking_structures_lock);
    
    /*
    *   Might want to look up the RC type (should be cached), or use info object/parameter:
    *       PMIx_query_info() ...
    *   For now we default to the single rc change: ompi_mpi_instance_res_change   
    */
    

    if(rc_type == OMPI_RC_ADD){
        if(rc_status == RC_ANNOUNCED){
            /* publish the accept */
            PMIX_INFO_CONSTRUCT(&publish_data);
            (void)snprintf(publish_data.key, PMIX_MAX_KEYLEN, "%s:accept", delta_pset);
            PMIX_VALUE_LOAD(&publish_data.value, new_pset, PMIX_STRING);
            //printf("accept:publish\n");
            rc = PMIx_Publish(&publish_data, 1);
            //printf("accept:publish status %d\n", rc);
            PMIX_PDATA_DESTRUCT(&publish_data);
        }

        /* check if the resource change is already confirmed */ 
        PMIX_PDATA_CONSTRUCT(&lookup_data);
        (void)snprintf(lookup_data.key, PMIX_MAX_KEYLEN, "%s:confirm", delta_pset);

        rc = PMIx_Lookup(&lookup_data, 1, lookup_info, ninfo);

        if(NULL != lookup_info){
            PMIX_INFO_FREE(lookup_info, ninfo);
        }

        if(rc == PMIX_ERR_NOT_FOUND){
            opal_mutex_lock(&tracking_structures_lock);
            res_change = get_res_change_by_name(delta_pset);
            res_change->status = RC_CONFIRMATION_PENDING;
            opal_mutex_unlock(&tracking_structures_lock);
            PMIX_PDATA_DESTRUCT(&lookup_data);

            return opal_pmix_convert_status(rc);
        }

        /* If the resource change was confirmed, accept it */
        if(0 == strcmp(OMPI_CONFIRM_VAL, lookup_data.value.data.string)){
            PMIX_PDATA_DESTRUCT(&lookup_data);
            
            /* Finalize the resource change */
            bool non_default=true;
            pmix_info_t *event_info;
            PMIX_INFO_CREATE(event_info, 2);
            (void)snprintf(event_info[0].key, PMIX_MAX_KEYLEN, "%s", PMIX_EVENT_NON_DEFAULT);
            PMIX_VALUE_LOAD(&event_info[0].value, &non_default, PMIX_BOOL);
            (void)snprintf(event_info[1].key, PMIX_MAX_KEYLEN, "%s", PMIX_PSET_NAME);
            PMIX_VALUE_LOAD(&event_info[1].value, delta_pset, PMIX_STRING);
            PMIx_Notify_event(PMIX_RC_FINALIZED, NULL, PMIX_RANGE_NAMESPACE, event_info, 2, NULL, NULL);
            ompi_instance_clear_rc_cache(delta_pset);
            PMIX_INFO_FREE(event_info, 2);

            return MPI_SUCCESS;
        }
        /* We just need to inform the RM that we accept the substraction */
    }else if(rc_type == OMPI_RC_SUB){
        
        /*
        bool non_default=true;
        pmix_info_t *event_info;
        PMIX_INFO_CREATE(event_info, 2);
        (void)snprintf(event_info[0].key, PMIX_MAX_KEYLEN, "%s", PMIX_EVENT_NON_DEFAULT);
        PMIX_VALUE_LOAD(&event_info[0].value, &non_default, PMIX_BOOL);
        (void)snprintf(event_info[1].key, PMIX_MAX_KEYLEN, "%s", PMIX_PSET_NAME);
        PMIX_VALUE_LOAD(&event_info[1].value, delta_pset, PMIX_STRING);
        rc = PMIx_Notify_event(PMIX_RC_FINALIZED, NULL, PMIX_RANGE_RM, event_info, 2, NULL, NULL);
        PMIX_INFO_FREE(event_info, 2);
        */
        ompi_instance_clear_rc_cache(delta_pset);
        return MPI_SUCCESS;
    }
    return PMIX_ERR_BAD_PARAM;
}

int ompi_instance_confirm_res_change(ompi_instance_t *instance, opal_info_t **info_used, char *delta_pset, char **new_pset){

    pmix_status_t rc;
    pmix_pdata_t lookup_data;
    pmix_info_t publish_data, *lookup_info;
    char *pmix_wait = PMIX_WAIT;
    size_t delta_pset_nmembers;
    pmix_proc_t *delta_pset_members;
    opal_process_name_t *delta_pset_members_names;
    char key[PMIX_MAX_KEYLEN];
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 10000;
    bool is_leader=false;
    /* define those in OMPI etc. */
    char *OMPI_CONFIRM_VAL="rc_confirmed";
    
    /* get the members of the delta_pset. These will participate in the fence operation */
    ompi_instance_get_pset_membership (instance, delta_pset, &delta_pset_members_names, &delta_pset_nmembers);
    opal_mutex_lock(&tracking_structures_lock);
    opal_pmix_proc_array_conv(delta_pset_members_names, &delta_pset_members, delta_pset_nmembers);
    ompi_instance_free_pset_membership(delta_pset);
    opal_mutex_unlock(&tracking_structures_lock);

    is_leader = is_pset_leader(delta_pset_members, delta_pset_nmembers, opal_process_info.myprocid);
    

    int counter=0;

     /* Query Runtime until the accept is publish, then join the fence */
    PMIX_PDATA_CONSTRUCT(&lookup_data);
    PMIX_INFO_CREATE(lookup_info, 2);
    int wait = 0;
    int timeout = 120;
    PMIX_INFO_LOAD(&lookup_info[0], PMIX_WAIT, &wait, PMIX_INT);
    PMIX_INFO_LOAD(&lookup_info[1], PMIX_TIMEOUT, &timeout, PMIX_INT);
    (void)snprintf(lookup_data.key, PMIX_MAX_KEYLEN, "%s:accept", delta_pset);
    
    rc = PMIx_Lookup(&lookup_data, 1, lookup_info, 2);

    PMIX_INFO_FREE(lookup_info, 2);

    if(rc != PMIX_SUCCESS){
        printf("Error in confirm:lookup: %d\n", rc);
        PMIX_PDATA_DESTRUCT(&lookup_data);
        return OPAL_ERR_NOT_FOUND;
    }
    
    //printf("confirm:fence \n");
    rc = PMIx_Fence(delta_pset_members, delta_pset_nmembers, NULL, 0);
    free(delta_pset_members);
    //printf("is leader: %d\n", is_leader);
    /* All new processes have participated in the fence so Publish Confirm now*/    
    if(is_leader){
        PMIX_INFO_CONSTRUCT(&publish_data);
        (void)snprintf(publish_data.key, PMIX_MAX_KEYLEN, "%s:confirm", delta_pset);
        PMIX_VALUE_LOAD(&publish_data.value, OMPI_CONFIRM_VAL, PMIX_STRING);
        /* publish the confimation */
        rc=PMIx_Publish(&publish_data, 1);

        PMIX_INFO_DESTRUCT(&publish_data);
    }
    ompi_instance_clear_rc_cache(delta_pset);

    if(rc != PMIX_SUCCESS){
        PMIX_PDATA_DESTRUCT(&lookup_data);
        /* TODO: convert this to a MPI_ERROR */
        return rc;
    }
    /* If the resource change was accepted, get the new_pset name */
    strcpy(*new_pset, lookup_data.value.data.string);
    PMIX_PDATA_DESTRUCT(&lookup_data);
    return OPAL_SUCCESS;
}

static void ompi_instance_get_num_psets_complete (pmix_status_t status, 
		                                  pmix_info_t *info,
		                                  size_t ninfo,
                                                  void *cbdata, 
                                                  pmix_release_cbfunc_t release_fn,
                                                  void *release_cbdata)
{
    size_t n;
    pmix_status_t rc;
    size_t sz;
    size_t num_pmix_psets = 0;
    char *pset_names = NULL;
    char **pset_names_ptr=&pset_names;

    opal_pmix_lock_t *lock = (opal_pmix_lock_t *) cbdata;

    for (n = 0; n < ninfo; n++) {
        if (0 == strcmp(info[n].key, PMIX_QUERY_NUM_PSETS)) {
            PMIX_VALUE_UNLOAD(rc,
                              &info[n].value,
                              (void **)&num_pmix_psets,
                              &sz);

            ompi_mpi_instance_num_pmix_psets = num_pmix_psets;
        } else if (0 == strcmp (info[n].key, PMIX_QUERY_PSET_NAMES)) {

            char** names = opal_argv_split (info[n].value.data.string, ',');
            size_t num_names=opal_argv_count(names);

            opal_mutex_lock(&tracking_structures_lock);
            /* add psets we didn't know about before the query*/
            int i;
            for(i=0; i<num_names; i++){
                if(NULL == get_pset_by_name(names[i])){
                    ompi_mpi_instance_pset_t *new_pset;
                    new_pset=OBJ_NEW(ompi_mpi_instance_pset_t);
                    strcpy(new_pset->name, names[i]);
                    new_pset->size=0;
                    new_pset->members=NULL;
                    new_pset->malleable=false;
                    opal_list_append(&ompi_mpi_instance_pmix_psets,&new_pset->super);
                }
            }
            ompi_mpi_instance_num_pmix_psets = opal_list_get_size(&ompi_mpi_instance_pmix_psets);
            free(pset_names);
            opal_mutex_unlock(&tracking_structures_lock);
        }
    }

    if (NULL != release_fn) {
        release_fn(release_cbdata);
    }
    OPAL_PMIX_WAKEUP_THREAD(lock);
}

static void ompi_instance_refresh_pmix_psets (const char *key)
{
    pmix_status_t rc;
    pmix_query_t query;
    opal_pmix_lock_t lock;
    bool refresh = true;

    opal_mutex_lock (&instance_lock);

    PMIX_QUERY_CONSTRUCT(&query);
    PMIX_ARGV_APPEND(rc, query.keys, key);
    PMIX_INFO_CREATE(query.qualifiers, 1);
    query.nqual = 1;
    PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);

    OPAL_PMIX_CONSTRUCT_LOCK(&lock);

    /*
     * TODO: need to handle this better
     */
    if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(&query, 1, 
                                                 ompi_instance_get_num_psets_complete,
                                                 (void*)&lock))) {
       opal_mutex_unlock (&instance_lock);
    }

    OPAL_PMIX_WAIT_THREAD(&lock);
    OPAL_PMIX_DESTRUCT_LOCK(&lock);

    opal_mutex_unlock (&instance_lock);
}


int ompi_instance_get_num_psets (ompi_instance_t *instance, int *npset_names)
{
    ompi_instance_refresh_pmix_psets (PMIX_QUERY_PSET_NAMES);
    *npset_names = ompi_instance_builtin_count + ompi_mpi_instance_num_pmix_psets;

    return OMPI_SUCCESS;
}

int ompi_instance_get_nth_pset (ompi_instance_t *instance, int n, int *len, char *pset_name)
{
    if (n >= ompi_instance_builtin_count + ompi_mpi_instance_num_pmix_psets) {
        ompi_instance_refresh_pmix_psets (PMIX_QUERY_PSET_NAMES);
    }

    if ((size_t) n >= (ompi_instance_builtin_count + ompi_mpi_instance_num_pmix_psets) || n < 0) {
        return OMPI_ERR_BAD_PARAM;
    }

    if (0 == *len) {
        if (n < ompi_instance_builtin_count) {
            *len = strlen(ompi_instance_builtin_psets[n]) + 1;
        } else {
            *len = strlen (get_nth_pset_in_list(n - ompi_instance_builtin_count)->name) + 1;
        }
        return OMPI_SUCCESS;
    }

    if (n < ompi_instance_builtin_count) {
        strncpy (pset_name, ompi_instance_builtin_psets[n], *len);
    } else {
        strncpy (pset_name, get_nth_pset_in_list(n - ompi_instance_builtin_count)->name, *len);
    }

    return OMPI_SUCCESS;
}

int ompi_instance_get_pset_membership (ompi_instance_t *instance, char *pset_name, opal_process_name_t **members, size_t *nmembers){
    
    pmix_status_t rc;
    int ret;
    opal_pmix_lock_t lock;
    bool refresh = true;
    pmix_info_t *info;
    size_t i, n, ninfo;
    pmix_query_t query;
    char *key = PMIX_QUERY_PSET_MEMBERSHIP;

    //printf("Rank %d: get_pset_membership: %s\n", opal_process_info.my_name.vpid, pset_name);

    opal_mutex_lock (&tracking_structures_lock);

    ompi_mpi_instance_pset_t *pset = get_pset_by_name(pset_name);
    bool new_pset = (pset == NULL);

    /* query the runtime */
    if(NULL == pset || NULL == pset->members || 0 == pset->size){ 
        opal_mutex_unlock (&tracking_structures_lock);

        /* set query keys */
        PMIX_QUERY_CONSTRUCT(&query);
        PMIX_ARGV_APPEND(rc, query.keys, key);

        query.nqual = 2;
        PMIX_INFO_CREATE(query.qualifiers, 2);
        PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
        PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PSET_NAME, pset_name, PMIX_STRING);

        if (PMIX_SUCCESS != (rc = PMIx_Query_info(&query, 1, &info, &ninfo)) || 0 == ninfo) {
            ret = opal_pmix_convert_status(rc);
            return ompi_instance_print_error ("PMIx_Query_info() failed", ret);                                            
        }

        /* set pset members in local pset */
        opal_mutex_lock (&tracking_structures_lock);
        for(n = 0; n < ninfo; n++){
            //printf("Rank %d: found key %s\n", opal_process_info.my_name.vpid, info[n].key);
            if(0 == strcmp(info[n].key, key)){
                if(new_pset){
                    pset = OBJ_NEW(ompi_mpi_instance_pset_t);
                    strcpy(pset->name, pset_name);
                }
                
                pmix_data_array_t *data_array = info[n].value.data.darray;
                pmix_proc_t *members_array = (pmix_proc_t*) data_array->array;

                pset->size = data_array->size;
                *nmembers = pset->size;
                pset->members = calloc(*nmembers, sizeof(opal_process_name_t));
                for(i = 0; i < *nmembers; i++){
                    OPAL_PMIX_CONVERT_PROCT(rc, &pset->members[i], &members_array[i]);
                }
                
                if(new_pset){
                    opal_list_append(&ompi_mpi_instance_pmix_psets, &pset->super);
                }

                *members = pset->members;
                
            }
        }
        PMIX_INFO_FREE(info, ninfo);

    /* do a local lookup */    
    }else{
        //printf("local lookup\n");
        if(NULL == pset->members){
            opal_mutex_unlock (&tracking_structures_lock);
            PMIX_ERR_NOT_FOUND;
        }
        *nmembers = pset->size;
        *members = pset->members;
    }
    opal_mutex_unlock (&tracking_structures_lock);
    return OMPI_SUCCESS;
}

int ompi_instance_free_pset_membership (char *pset_name){
    ompi_mpi_instance_pset_t *pset = NULL;
    pset = get_pset_by_name(pset_name);

    if(NULL != pset && NULL != pset->members){
        free(pset->members);
        pset->members = NULL;
    }
    return OMPI_SUCCESS;
}


int ompi_instance_pset_fence(ompi_instance_t *instance, char *pset_name){

    pmix_status_t rc;
    int ret;
    volatile bool active = true;
    bool flag = true;
    pmix_info_t info;
    pmix_proc_t *procs;
    size_t nprocs;

    /* retrieve pset members */  
    ompi_instance_get_pset_membership(instance, pset_name, &procs, &nprocs);
    
    /* Perform the fence operation across all pset members */
    OPAL_POST_OBJECT(&active);
    PMIX_INFO_LOAD(&info, PMIX_COLLECT_DATA, &flag, PMIX_BOOL);
    if (PMIX_SUCCESS != (rc = PMIx_Fence_nb(NULL, 0, &info, 1,
                                            fence_release, (void*)&active))) {
        ret = opal_pmix_convert_status(rc);
        return ompi_instance_print_error ("PMIx_Fence_nb() failed", ret);
    }
    OMPI_LAZY_WAIT_FOR_COMPLETION(active);
    
    ompi_instance_free_pset_membership(pset_name);
    return OMPI_SUCCESS;
}

int ompi_instance_pset_create_op(ompi_instance_t *instance, const char *pset1, const char *pset2, char *pref_name, char *pset_result, ompi_psetop_type_t op){

    pmix_status_t rc;
    pmix_info_t *info, *results;
    opal_pmix_lock_t lock;
    size_t sz, n, ninfo, nresults;
    int ret;

    opal_mutex_lock (&instance_lock);
    
    ninfo = NULL == pref_name ? 2 : 3;
    PMIX_INFO_CREATE(info, ninfo);
    PMIX_INFO_LOAD(&(info[0]), PMIX_PSETOP_P1, pset1, PMIX_STRING);
    PMIX_INFO_LOAD(&(info[1]), PMIX_PSETOP_P2, pset2, PMIX_STRING);
    if(NULL != pref_name){
        PMIX_INFO_LOAD(&(info[2]), PMIX_PSETOP_PREF_NAME, pref_name, PMIX_STRING);
    }

    OPAL_PMIX_CONSTRUCT_LOCK(&lock);

    /*
     * TODO: need to handle this better
     */
    if (PMIX_SUCCESS != (rc = PMIx_Pset_Op_request( op,
                                            info, ninfo, 
                                            &results, &nresults))) {
        PMIX_INFO_FREE(info, ninfo);                                             
        opal_mutex_unlock (&instance_lock);
        ret = opal_pmix_convert_status(rc);
        return ompi_instance_print_error ("PMIx_Fence_nb() failed", ret);
    }

    for(n=0; n < nresults; n++){
        if(PMIX_CHECK_KEY(&results[n], PMIX_PSETOP_PRESULT) ){
            strcpy(pset_result, results[n].value.data.string);
        }
    }

    PMIX_INFO_FREE(info, ninfo);
    PMIX_INFO_FREE(results, nresults);
    opal_mutex_unlock (&instance_lock);

    if(PMIX_SUCCESS != rc){
        ret = opal_pmix_convert_status(rc);
        return ompi_instance_print_error ("PMIX_VALUE_UNLOAD() failed", ret);
    }

    return OMPI_SUCCESS;
}

static int ompi_instance_group_world (ompi_instance_t *instance, ompi_group_t **group_out)
{
    ompi_group_t *group;
    size_t size;

    size = ompi_process_info.num_procs;

    group = ompi_group_allocate (size);
    if (OPAL_UNLIKELY(NULL == group)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (size_t i = 0 ; i < size ; ++i) {
        opal_process_name_t name = {.vpid = i, .jobid = OMPI_PROC_MY_NAME->jobid};
        /* look for existing ompi_proc_t that matches this name */
        group->grp_proc_pointers[i] = (ompi_proc_t *) ompi_proc_lookup (name);
        if (NULL == group->grp_proc_pointers[i]) {
            /* set sentinel value */
            group->grp_proc_pointers[i] = (ompi_proc_t *) ompi_proc_name_to_sentinel (name);
        } else {
            OBJ_RETAIN (group->grp_proc_pointers[i]);
        }
    }

    ompi_set_group_rank (group, ompi_proc_local());

    group->grp_instance = instance;

    *group_out = group;
    return OMPI_SUCCESS;
}

static int ompi_instance_group_shared (ompi_instance_t *instance, ompi_group_t **group_out)
{
    ompi_group_t *group;
    opal_process_name_t wildcard_rank;
    int ret;
    size_t size;
    char **peers;
    char *val;

    /* Find out which processes are local */
    wildcard_rank.jobid = OMPI_PROC_MY_NAME->jobid;
    wildcard_rank.vpid = OMPI_NAME_WILDCARD->vpid;

    OPAL_MODEX_RECV_VALUE(ret, PMIX_LOCAL_PEERS, &wildcard_rank, &val, PMIX_STRING);
    if (OPAL_SUCCESS != ret || NULL == val) {
        return OMPI_ERROR;
    }

    peers = opal_argv_split(val, ',');
    free (val);
    if (OPAL_UNLIKELY(NULL == peers)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    size = opal_argv_count (peers);

    group = ompi_group_allocate (size);
    if (OPAL_UNLIKELY(NULL == group)) {
        opal_argv_free (peers);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (size_t i = 0 ; NULL != peers[i] ; ++i) {
        opal_process_name_t name = {.vpid = strtoul(peers[i], NULL, 10), .jobid = OMPI_PROC_MY_NAME->jobid};
        /* look for existing ompi_proc_t that matches this name */
        group->grp_proc_pointers[i] = (ompi_proc_t *) ompi_proc_lookup (name);
        if (NULL == group->grp_proc_pointers[i]) {
            /* set sentinel value */
            group->grp_proc_pointers[i] = (ompi_proc_t *) ompi_proc_name_to_sentinel (name);
        } else {
            OBJ_RETAIN (group->grp_proc_pointers[i]);
        }
    }

    opal_argv_free (peers);

    /* group is dense */
    ompi_set_group_rank (group, ompi_proc_local());

    group->grp_instance = instance;

    *group_out = group;
    return OMPI_SUCCESS;
}

static int ompi_instance_group_self (ompi_instance_t *instance, ompi_group_t **group_out)
{
    ompi_group_t *group;
    size_t size;

    group = OBJ_NEW(ompi_group_t);
    if (OPAL_UNLIKELY(NULL == group)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    group->grp_proc_pointers = ompi_proc_self(&size);
    group->grp_my_rank       = 0;
    group->grp_proc_count   = size;

    /* group is dense */
    OMPI_GROUP_SET_DENSE (group);

    group->grp_instance = instance;

    *group_out = group;
    return OMPI_SUCCESS;
}

static int ompi_instance_group_pmix_pset (ompi_instance_t *instance, const char *pset_name, ompi_group_t **group_out)
{
    pmix_status_t rc = OPAL_ERR_NOT_FOUND;
    pmix_proc_t p;
    ompi_group_t *group;
    pmix_value_t *pval = NULL;
    char *stmp = NULL;
    size_t size = 0;
    opal_process_name_t *pset_members = NULL;
    size_t pset_nmembers;
    int timeout_counter=0;
    int timeout=100000;
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 100000;

    while (pset_members == NULL){
        if(OPAL_SUCCESS != (rc = ompi_instance_get_pset_membership(instance, pset_name, &pset_members, &pset_nmembers))){
            return OPAL_ERR_BAD_PARAM;
        }
        if(pset_members == NULL){
            //printf("pset_members = NULL\n");
            nanosleep(&ts, NULL);
        }
    }
    opal_mutex_lock (&tracking_structures_lock);
    //if(NULL == pset_members)printf("pset_members = NULL\n");
    if(PMIX_SUCCESS != rc){
        opal_mutex_unlock (&tracking_structures_lock);
        if(PMIX_ERR_NOT_FOUND == rc){
            return MPI_ERR_ARG;
        }
        return MPI_ERR_INTERN;
    }
    group = ompi_group_allocate (pset_nmembers);
    if (OPAL_UNLIKELY(NULL == group)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (size_t i = 0 ; i < pset_nmembers ; ++i) {
        opal_process_name_t name = pset_members[i];
        /* look for existing ompi_proc_t that matches this name */
	//opal_proc_t *tmp = ompi_proc_for_name(pset_members[i]);
        group->grp_proc_pointers[size] = (ompi_proc_t *) ompi_proc_lookup (name);
        if (NULL == group->grp_proc_pointers[size]) {
            /* set sentinel value */
            group->grp_proc_pointers[size] = (ompi_proc_t *) ompi_proc_name_to_sentinel (name);
        } else {
            OBJ_RETAIN (group->grp_proc_pointers[size]);
        }
        
        ++size;
    }
    //opal_proc_t **procs;
    //size_t nprocs;
    //procs = ompi_proc_get_allocated (&nprocs);
    //int ret;
    //ret = MCA_PML_CALL(add_procs(procs, nprocs));
    //ompi_set_group_rank (group, ompi_proc_local());

    group->grp_instance = instance;

    *group_out = group;
    ompi_instance_free_pset_membership(pset_name);
    opal_mutex_unlock (&tracking_structures_lock);
    return OMPI_SUCCESS;
}

/* TODO: need to adjust for new concept */
static int ompi_instance_get_pmix_pset_size (ompi_instance_t *instance, const char *pset_name, size_t *size_out)
{
    pmix_status_t rc;
    pmix_proc_t p;
    pmix_value_t *pval = NULL;
    size_t size = 0;
    char *stmp = NULL;

    for (size_t i = 0 ; i < ompi_process_info.num_procs ; ++i) {
        opal_process_name_t name = {.vpid = i, .jobid = OMPI_PROC_MY_NAME->jobid};

        OPAL_PMIX_CONVERT_NAME(&p, &name);
        rc = PMIx_Get(&p, PMIX_PSET_NAME, NULL, 0, &pval);
        if (OPAL_UNLIKELY(PMIX_SUCCESS != rc)) {
            return rc;
        }

        PMIX_VALUE_UNLOAD(rc,
                          pval,
                          (void **)&stmp,
                          &size);

        size += (0 == strcmp (pset_name, stmp));
        PMIX_VALUE_RELEASE(pval);
        free(stmp);

        ++size;
    }


    *size_out = size;

    return OMPI_SUCCESS;
}

int ompi_group_from_pset (ompi_instance_t *instance, const char *pset_name, ompi_group_t **group_out)
{
    if (group_out == MPI_GROUP_NULL) {
        return OMPI_ERR_BAD_PARAM;
    }
    if (0 == strncmp (pset_name, "mpi://", 6)) {
        pset_name += 6;
        if (0 == strcmp (pset_name, "WORLD")) {
        return ompi_instance_group_world (instance, group_out);
        }
        if (0 == strcmp (pset_name, "SELF")) {
            return ompi_instance_group_self (instance, group_out);
        }
    }

    if (0 == strncmp (pset_name, "mpix://", 7)) {
        pset_name += 7;
        if (0 == strcmp (pset_name, "SHARED")) {
            return ompi_instance_group_shared (instance, group_out);
        }
    }

    return ompi_instance_group_pmix_pset (instance, pset_name, group_out);
}

int ompi_instance_get_pset_info (ompi_instance_t *instance, const char *pset_name, opal_info_t **info_used)
{
    ompi_info_t *info = ompi_info_allocate ();
    char tmp[16];
    size_t size = 0UL;
    int ret;

    *info_used = (opal_info_t *) MPI_INFO_NULL;

    if (OPAL_UNLIKELY(NULL == info)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    if (0 == strncmp (pset_name, "mpi://", 6)) {
        pset_name += 6;
        if (0 == strcmp (pset_name, "world")) {
            size = ompi_process_info.num_procs;
        } else if (0 == strcmp (pset_name, "self")) {
            size = 1;
        } else if (0 == strcmp (pset_name, "shared")) {
            size = ompi_process_info.num_local_peers + 1;
        }
    } else {
        ompi_instance_get_pmix_pset_size (instance, pset_name, &size);
    }

    ret = opal_info_set (&info->super, MPI_INFO_KEY_SESSION_PSET_SIZE, tmp);
    if (OPAL_UNLIKELY(OPAL_SUCCESS != ret)) {
        ompi_info_free (&info);
        return ret;
    }

    *info_used = &info->super;

    return OMPI_SUCCESS;
}
