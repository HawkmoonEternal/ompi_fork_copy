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

#define MPI_ALLOC_SET_REQUEST PMIX_ALLOC_EXTERNAL + 1

ompi_predefined_instance_t ompi_mpi_instance_null = {{{{0}}}};
ompi_predefined_rc_op_handle_t ompi_mpi_rc_op_handle_null = {0};
ompi_instance_rc_op_handle_t * ompi_mpi_rc_op_handle_null_ptr = (ompi_instance_rc_op_handle_t *) &ompi_mpi_rc_op_handle_null;

static opal_recursive_mutex_t instance_lock = OPAL_RECURSIVE_MUTEX_STATIC_INIT;
static opal_recursive_mutex_t tracking_structures_lock;




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
    OBJ_CONSTRUCT(&instance->s_lock, opal_mutex_t);
    instance->errhandler_type = OMPI_ERRHANDLER_TYPE_INSTANCE;
}

static void ompi_instance_destruct(ompi_instance_t *instance)
{
    OBJ_DESTRUCT(&instance->s_lock);
}

OBJ_CLASS_INSTANCE(ompi_instance_t, opal_infosubscriber_t, ompi_instance_construct, ompi_instance_destruct);

/* NTH: frameworks needed by MPI */
static mca_base_framework_t *ompi_framework_dependencies[] = {
    &ompi_hook_base_framework, &ompi_op_base_framework,
    &opal_allocator_base_framework, &opal_rcache_base_framework, &opal_mpool_base_framework, &opal_smsc_base_framework,
    &ompi_bml_base_framework, &ompi_pml_base_framework, &ompi_coll_base_framework,
    &ompi_osc_base_framework, NULL,
};

static mca_base_framework_t *ompi_lazy_frameworks[] = {
    &ompi_io_base_framework, &ompi_topo_base_framework, NULL,
};


static int ompi_mpi_instance_finalize_common (void);

/*
 * Per MPI-2:9.5.3, MPI_REGISTER_DATAREP is a memory leak.  There is
 * no way to *de*register datareps once they've been registered.  So
 * we have to track all registrations here so that they can be
 * de-registered during MPI_FINALIZE so that memory-tracking debuggers
 * don't show Open MPI as leaking memory.
 */
opal_list_t ompi_registered_datareps = {{0}};

opal_pointer_array_t ompi_instance_f_to_c_table = {{0}};

/*
 * PMIx event handlers
 */

static size_t ompi_default_pmix_err_handler = 0;
static size_t ompi_ulfm_pmix_err_handler = 0;

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


/*
 * Hash tables for MPI_Type_create_f90* functions
 */
opal_hash_table_t ompi_mpi_f90_integer_hashtable = {{0}};
opal_hash_table_t ompi_mpi_f90_real_hashtable = {{0}};
opal_hash_table_t ompi_mpi_f90_complex_hashtable = {{0}};



pmix_proc_t ompi_intance_get_pmixid(){
    return opal_process_info.myprocid;
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

void ompi_mpi_instance_release (void)
{
    opal_mutex_lock (&instance_lock);

    if (0 != --ompi_mpi_instance_init_basic_count) {
        opal_mutex_unlock (&instance_lock);
        return;
    }

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
        opal_mutex_unlock (&instance_lock);
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
        opal_mutex_unlock (&instance_lock);
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
    lock->errhandler_ref = evhandler_ref;

    OPAL_PMIX_WAKEUP_THREAD(lock);
}

static void evhandler_dereg_callbk(pmix_status_t status,
                                 void *cbdata)
{
    opal_pmix_lock_t *lock = (opal_pmix_lock_t*)cbdata;
    
    lock->status = status;

    OPAL_PMIX_WAKEUP_THREAD(lock);
}       


/**
 * @brief Function that starts up the common components needed by all instances
 */
static int ompi_mpi_instance_init_common (int argc, char **argv)
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

    OBJ_CONSTRUCT(&tracking_structures_lock, opal_recursive_mutex_t);

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
           any spawned processes and potentially cause unintended
           side-effects with launching RTE tools... */
        mca_base_var_set_value(ret, allvalue, 4, MCA_BASE_VAR_SOURCE_DEFAULT, NULL);
    }

    OMPI_TIMING_NEXT("initialization");

    /* Setup RTE */
    if (OMPI_SUCCESS != (ret = ompi_rte_init (&argc, &argv))) {
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




    OMPI_TIMING_NEXT("rte_init");
    OMPI_TIMING_IMPORT_OPAL("orte_ess_base_app_setup");
    OMPI_TIMING_IMPORT_OPAL("rte_init");

    ompi_rte_initialized = true;
    /* if we are oversubscribed, then set yield_when_idle
     * accordingly */
    if (ompi_mpi_oversubscribed) {
        ompi_mpi_yield_when_idle = true;
    }

    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 10000;

    /* initialize the psets subsystem */
    if (OMPI_SUCCESS != (ret = ompi_instance_psets_init())){
        return ret;
    }

    /* Get our launch PSet. 
     * FIXME:   There is a sligt race-condition, where the INITIAL launch PSet is not yet defined,
     *          as PRRTE is defining it after the Spawn command. The Spawn needs to be adjusted
     *          some day, but for now we just try again until it is defined
     */
    char **ompi_instance_builtin_psets_aliases = (char **) calloc(ompi_instance_builtin_count, sizeof(char *));
    while (OMPI_SUCCESS != (ret = ompi_instance_get_launch_pset(&ompi_instance_builtin_psets_aliases[0], &opal_process_info.myprocid))){
        nanosleep(&ts, NULL);
    }
    /* initialize the builtin PSets */
    if (OMPI_SUCCESS != (ret = ompi_instance_builtin_psets_init(ompi_instance_builtin_count, (char **) ompi_instance_builtin_psets, NULL, NULL, ompi_instance_builtin_psets_aliases))){
        return ret;
    }
    if(OMPI_SUCCESS != (ret = pset_init_flags("mpi://WORLD"))){
        return ret;
    }

    /* Init the PSet flags*/
    ompi_mpi_instance_pset_t *mpi_self_pset = get_pset_by_name("mpi://SELF");
    ompi_mpi_instance_pset_t *mpi_world_pset = get_pset_by_name("mpi://WORLD");
    
    OMPI_PSET_FLAG_SET(mpi_self_pset, OMPI_PSET_FLAG_INIT);
    OMPI_PSET_FLAG_SET(mpi_self_pset, OMPI_PSET_FLAG_PRIMARY);
    OMPI_PSET_FLAG_SET(mpi_self_pset, OMPI_PSET_FLAG_INCLUDED);
    if(OMPI_PSET_FLAG_TEST(mpi_world_pset, OMPI_PSET_FLAG_DYN)){
        OMPI_PSET_FLAG_SET(mpi_self_pset, OMPI_PSET_FLAG_DYN);
    }

    mpi_self_pset->members = (opal_process_name_t *) malloc(sizeof(opal_process_name_t));
    mpi_self_pset->size = 1;
    mpi_self_pset->members[0] = opal_process_info.my_name;

    for(int i = 0; i < ompi_instance_builtin_count; i++){
        free(ompi_instance_builtin_psets_aliases[i]);
    }
    free(ompi_instance_builtin_psets_aliases);

    /* initialize the resource change subsystem */
    if (OMPI_SUCCESS != (ret = ompi_instance_res_changes_init())){
        return ret;
    }

    /* initialize the PSet collectives subsystem */
    if (OMPI_SUCCESS != (ret = ompi_instance_collectives_init())){
        return ret;
    }


    /* Register the default errhandler callback  */
    /* give it a name so we can distinguish it */
    PMIX_INFO_LOAD(&info[0], PMIX_EVENT_HDLR_NAME, "MPI-Default", PMIX_STRING);
    OPAL_PMIX_CONSTRUCT_LOCK(&mylock);
    PMIx_Register_event_handler(NULL, 0, info, 1, ompi_errhandler_callback, evhandler_reg_callbk, (void*)&mylock);
    OPAL_PMIX_WAIT_THREAD(&mylock);
    rc = mylock.status;
    ompi_default_pmix_err_handler = mylock.errhandler_ref;
    OPAL_PMIX_DESTRUCT_LOCK(&mylock);
    PMIX_INFO_DESTRUCT(&info[0]);
    if (PMIX_SUCCESS != rc) {
        ompi_default_pmix_err_handler = 0;
        ret = opal_pmix_convert_status(rc);
        return ret;
    }

    /* register event handler to track the runtimes actions related to psets */
    pmix_status_t code;
    code = PMIX_PROCESS_SET_DEFINE;
    OPAL_PMIX_CONSTRUCT_LOCK(&mylock);
    PMIx_Register_event_handler(&code, 1, NULL, 0, pset_define_handler, evhandler_reg_callbk, (void*)&mylock);
    OPAL_PMIX_WAIT_THREAD(&mylock);
    rc = mylock.status;
    OPAL_PMIX_DESTRUCT_LOCK(&mylock);
    if (PMIX_SUCCESS != rc) {
        ompi_default_pmix_err_handler = 0;
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

    /* Register the ULFM errhandler callback  */
    /* we want to go first */
    PMIX_INFO_LOAD(&info[0], PMIX_EVENT_HDLR_PREPEND, NULL, PMIX_BOOL);
    /* give it a name so we can distinguish it */
    PMIX_INFO_LOAD(&info[1], PMIX_EVENT_HDLR_NAME, "ULFM-Event-handler", PMIX_STRING);
    OPAL_PMIX_CONSTRUCT_LOCK(&mylock);
    pmix_status_t codes[2] = { PMIX_ERR_PROC_ABORTED, PMIX_ERR_LOST_CONNECTION };
    PMIx_Register_event_handler(codes, 1, info, 2, ompi_errhandler_callback, evhandler_reg_callbk, (void*)&mylock);
    OPAL_PMIX_WAIT_THREAD(&mylock);
    rc = mylock.status;
    ompi_ulfm_pmix_err_handler = mylock.errhandler_ref;
    OPAL_PMIX_DESTRUCT_LOCK(&mylock);
    PMIX_INFO_DESTRUCT(&info[0]);
    PMIX_INFO_DESTRUCT(&info[1]);
    if (PMIX_SUCCESS != rc) {
        ompi_ulfm_pmix_err_handler = 0;
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
       ddt_init, but before mca_coll_base_open, since some collective
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



/* If we use malleable MPI dynamic processes cannot fence across all processes in the namespace 
 * Instead they need to fence across their launch PSet
*/
#define MPI_MALLEABLE 1

pmix_proc_t *fence_procs = NULL;
size_t fence_nprocs = 0;

#if MPI_MALLEABLE
    ompi_psetop_type_t rc_op;
    ompi_rc_status_t rc_status;
    char **delta_psets = NULL;
    char bound_pset[] = "mpi://WORLD"; // Check for resource changes associated with our launch PSet
    int incl = 0;
    size_t ndelta_psets = 0;

    if(OMPI_SUCCESS != (rc = ompi_instance_get_res_change(ompi_mpi_instance_default, bound_pset, &rc_op, &delta_psets, &ndelta_psets, &incl, &rc_status,  NULL, false))){
        if(OMPI_ERR_NOT_FOUND != rc){
            return rc;
        }
    }


    if(rc_op != OMPI_PSETOP_NULL && incl){


        ts.tv_nsec = 10000;
        opal_process_name_t *pset_procs;

        size_t pset_nprocs;
        if(PMIX_SUCCESS!= (rc = get_pset_membership(delta_psets[0], &pset_procs, &pset_nprocs))){
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

    if(rc_op != OMPI_PSETOP_NULL){
        for(size_t n = 0; n < ndelta_psets; n++){
            ompi_instance_free_pset_membership(delta_psets[n]);
            free(delta_psets[n]);
        }
        free(delta_psets);
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

    /* Construct predefined keyvals */

    if (OMPI_SUCCESS != (ret = ompi_attr_create_predefined_keyvals())) {
        opal_mutex_unlock (&instance_lock);
        return ompi_instance_print_error ("ompi_attr_create_predefined_keyvals() failed", ret);
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

    /* initialize partcomm */
    if (OMPI_SUCCESS != (ret = mca_base_framework_open(&ompi_part_base_framework, 0))) {
        return ompi_instance_print_error ("mca_part_base_select() failed", ret);
    }

    if (OMPI_SUCCESS != (ret = mca_part_base_select (true, true))) {
        return ompi_instance_print_error ("mca_part_base_select() failed", ret);
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

    if (!ompi_singleton) {
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
            if (PMIX_SUCCESS != (rc = PMIx_Fence_nb(fence_procs, fence_nprocs, info, 1,
                                                    fence_release, (void*)&active))) {
                ret = opal_pmix_convert_status(rc);
                return ompi_instance_print_error ("PMIx_Fence_nb() failed", ret);
            }
            OMPI_LAZY_WAIT_FOR_COMPLETION(active);
        }
    }

    if(NULL != fence_procs){
        free(fence_procs);
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

int ompi_mpi_instance_init (int ts_level,  opal_info_t *info, ompi_errhandler_t *errhandler, ompi_instance_t **instance, int argc, char **argv)
{
    ompi_instance_t *new_instance;
    int ret;

    *instance = &ompi_mpi_instance_null.instance;

    /* If thread support was enabled, then setup OPAL to allow for them by default. This must be done
     * early to prevent a race condition that can occur with orte_init(). */
    if (ts_level == MPI_THREAD_MULTIPLE) {
        opal_set_using_threads(true);
    }

    opal_mutex_lock (&instance_lock);
    if (0 == opal_atomic_fetch_add_32 (&ompi_instance_count, 1)) {
        ret = ompi_mpi_instance_init_common (argc, argv);
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

int opal_pmix_lookup(pmix_key_t key, pmix_value_t *value, pmix_info_t *lookup_info, size_t ninfo){

    int rc;
    pmix_pdata_t lookup_data;

    PMIX_PDATA_CONSTRUCT(&lookup_data);
    PMIX_LOAD_KEY(lookup_data.key, key);

    rc = PMIx_Lookup(&lookup_data, 1, lookup_info, ninfo);

    PMIX_VALUE_XFER(rc, value, &lookup_data.value);

    PMIX_PDATA_DESTRUCT(&lookup_data);

    return rc;
}

int opal_pmix_lookup_string_wait(char * key, char *val, int val_length){
    int rc;
    bool wait = true;
    pmix_key_t pmix_key;
    pmix_value_t pmix_value;
    pmix_info_t info;

    if(strlen(key) > PMIX_MAX_KEYLEN){
        return OMPI_ERR_BAD_PARAM;
    }
    strcpy(pmix_key, key);

    PMIX_INFO_CONSTRUCT(&info);
    PMIX_INFO_LOAD(&info, PMIX_WAIT, &wait, PMIX_BOOL);

    PMIX_VALUE_CONSTRUCT(&pmix_value);

    rc = opal_pmix_lookup(pmix_key, &pmix_value, &info, 1);

    
    if(PMIX_SUCCESS == rc){
        strncpy(val, pmix_value.data.string, strlen(pmix_value.data.string) < (size_t) val_length ? strlen(pmix_value.data.string) + 1 : (size_t) val_length);
    }

    PMIX_INFO_DESTRUCT(&info);
    PMIX_VALUE_DESTRUCT(&pmix_value);

    return rc;
}

int opal_pmix_lookup_string(char * key, char *val, int val_length){
    int rc;
    pmix_key_t pmix_key;
    pmix_value_t pmix_value;

    if(strlen(key) > PMIX_MAX_KEYLEN){
        return OMPI_ERR_BAD_PARAM;
    }

    strcpy(pmix_key, key);

    PMIX_VALUE_CONSTRUCT(&pmix_value);

    rc = opal_pmix_lookup(pmix_key, &pmix_value, NULL, 0);
    
    if(PMIX_SUCCESS == rc){
        strncpy(val, pmix_value.data.string, strlen(pmix_value.data.string) < (size_t) val_length ? strlen(pmix_value.data.string) + 1 : (size_t) val_length);
    }

    PMIX_VALUE_DESTRUCT(&pmix_value);

    return rc;
}

/* TODO: handle PMIX_PSET_INFO on higher level */
int opal_pmix_lookup_pset_info(char **keys, size_t nkeys, pmix_info_t *info, size_t ninfo, char *pset_name, pmix_info_t **results, size_t *nresults){

    int rc;
    size_t n, i = 0, k, info_size, darray_size;
    pmix_pdata_t *lookup_data;
    pmix_info_t *lookup_info;
    pmix_info_t *_results, *darray_info;
    pmix_value_t *result_value;

    *nresults = 0;
    info_size = ninfo + (pset_name == NULL ? 0 : 1);

    PMIX_PDATA_CREATE(lookup_data, nkeys);
    PMIX_INFO_CREATE(lookup_info, info_size);
    

    for(n = 0; n < nkeys; n++){
        PMIX_LOAD_KEY(lookup_data[n].key, keys[n]); 
    }

    for(n = 0; n < ninfo; n++){
        PMIX_INFO_XFER(&lookup_info[n], &info[n]);
    }

    if(NULL != pset_name){
        PMIX_INFO_LOAD(&lookup_info[ninfo], PMIX_PSET_NAME, (void *) pset_name, PMIX_STRING);
    }

    rc = PMIx_Lookup(lookup_data, 1, lookup_info, info_size);
    if(rc != PMIX_SUCCESS && rc != PMIX_ERR_PARTIAL_SUCCESS){
        return rc;
    }

    for(n = 0; n < nkeys; n++){
        if(PMIX_UNDEF != lookup_data[n].value.type){
            if(0 == strcmp(lookup_data[n].key, PMIX_PSET_INFO)){
                *nresults += lookup_data[n].value.data.darray->size;
            }else{
                 ++*nresults;
            }
        }
    }
    PMIX_INFO_CREATE(_results, *nresults);
    *results = _results;
    for(n = 0; n < *nresults; n++){
        if(PMIX_UNDEF == lookup_data[n].value.type){
            continue;
        }
        if(0 == strcmp(lookup_data[n].key, PMIX_PSET_INFO)){
            darray_info = (pmix_info_t *) lookup_data[n].value.data.darray->array;
            darray_size = lookup_data[n].value.data.darray->size;
            for(k = 0; k < darray_size; k++){
                PMIX_INFO_XFER(&_results[i], &darray_info[k]);
                i++;
            }
            continue;
        }

        PMIX_LOAD_KEY( _results[i].key, lookup_data[n].key);
        result_value = &_results[i].value;
        PMIX_VALUE_XFER(rc, result_value, &lookup_data[n].value);
        if(PMIX_SUCCESS != rc){
            PMIX_INFO_FREE(_results, *nresults);
            break;
        }
        ++i;
    }
    PMIX_PDATA_FREE(lookup_data, nkeys);
    PMIX_INFO_FREE(lookup_info, info_size);

    return rc;
}


int opal_pmix_publish(pmix_key_t key, pmix_value_t value){
    int rc;
    pmix_info_t publish_data;

    PMIX_INFO_CONSTRUCT(&publish_data);
    PMIX_LOAD_KEY(publish_data.key, key);
    PMIX_VALUE_XFER_DIRECT(rc, &publish_data.value, &value);

    rc = PMIx_Publish(&publish_data, 1);

    PMIX_INFO_DESTRUCT(&publish_data);

    return rc;
}


int opal_pmix_publish_string(char * key, char *val, int val_length){

    int rc;
    pmix_key_t pmix_key;
    pmix_value_t pmix_value;
    PMIX_VALUE_CONSTRUCT(&pmix_value);

    strncpy(pmix_key, key, strlen(key) < PMIX_MAX_KEYLEN ? strlen(key) + 1 : PMIX_MAX_KEYLEN);
    PMIX_VALUE_LOAD(&pmix_value, (void *) val, PMIX_STRING);

    rc = opal_pmix_publish(pmix_key, pmix_value);

    PMIX_VALUE_DESTRUCT(&pmix_value);

    return rc;

}

int opal_pmix_publish_pset_info(char **keys, pmix_value_t *values, int nkv, char *pset_name){
    int rc, n, length;
    pmix_info_t *publish_data;

    length = nkv + (pset_name == NULL ? 0 : 1);

    PMIX_INFO_CREATE(publish_data, length);

    for(n = 0; n < nkv; n++){
        PMIX_LOAD_KEY(publish_data[n].key, keys[n]);
        PMIX_VALUE_XFER_DIRECT(rc, &publish_data[n].value, &values[n]);
    }

    if(NULL != pset_name){
        PMIX_LOAD_KEY(publish_data[nkv].key, PMIX_PSET_NAME);
        PMIX_VALUE_LOAD(&publish_data[nkv].value, (void *) pset_name, PMIX_STRING);
    }

    rc = PMIx_Publish(publish_data, length);

    PMIX_INFO_FREE(publish_data, (size_t) length);
    return rc;
}


static int ompi_mpi_instance_finalize_common (void)
{
    uint32_t key;
    ompi_datatype_t *datatype;
    int ret;
    opal_pmix_lock_t mylock;
    ompi_mpi_instance_pset_t *mpi_self_pset;

    OBJ_DESTRUCT(&tracking_structures_lock);

    if(NULL != (mpi_self_pset = get_pset_by_name("mpi://SELF"))){
        free(mpi_self_pset->members);
    }
    

    if (OMPI_SUCCESS != (ret = ompi_instance_psets_finalize())){
        return ret;
    }
    if (OMPI_SUCCESS != (ret = ompi_instance_res_changes_finalize())){
        return ret;
    }
    if (OMPI_SUCCESS != (ret = ompi_instance_collectives_finalize())){
        return ret;
    }

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

    if (0 != ompi_default_pmix_err_handler) {
        OPAL_PMIX_CONSTRUCT_LOCK(&mylock);
        PMIx_Deregister_event_handler(ompi_default_pmix_err_handler, evhandler_dereg_callbk, &mylock);
        OPAL_PMIX_WAIT_THREAD(&mylock);
        OPAL_PMIX_DESTRUCT_LOCK(&mylock);
        ompi_default_pmix_err_handler = 0;
    }

    if (0 != ompi_ulfm_pmix_err_handler) {
        OPAL_PMIX_CONSTRUCT_LOCK(&mylock);
        PMIx_Deregister_event_handler(ompi_ulfm_pmix_err_handler, evhandler_dereg_callbk, &mylock);
        OPAL_PMIX_WAIT_THREAD(&mylock);
        OPAL_PMIX_DESTRUCT_LOCK(&mylock);
        ompi_ulfm_pmix_err_handler = 0;
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
    }
    opal_mutex_unlock (&instance_lock);

    *instance = &ompi_mpi_instance_null.instance;

    return ret;
}



/* Query the runtime for available resource changes given either the delta PSet or the associated PSet */
int ompi_instance_get_res_change(ompi_instance_t *instance, char *input_name, ompi_psetop_type_t *type, char ***output_names, size_t *noutput_names, int *incl, ompi_rc_status_t *status, opal_info_t **info_used, bool get_by_delta_name){
    
    int rc;
    ompi_mpi_instance_pset_t *pset_ptr;

    if(PMIX_SUCCESS != (rc = refresh_pmix_psets(PMIX_QUERY_PSET_NAMES))){
        printf("refresh returned %d\n", rc);
        return rc;
    }

    if(NULL == (pset_ptr = get_pset_by_name(input_name))){
        printf("get pset by name returned NULL\n");
        return OMPI_ERR_NOT_FOUND;
    }

    rc = get_res_change_info(pset_ptr->name, type, output_names, noutput_names, incl, status, info_used, get_by_delta_name);


    return rc;
}



/* Query the runtime for available resource changes given either the delta PSet or the associated PSet */
int ompi_instance_dyn_v1_recv_res_change(ompi_instance_t *instance, char *input_name, ompi_psetop_type_t *type, char *output_name, int *incl, bool get_by_delta_name){
    
    int rc;
    char **output_names = NULL;
    size_t n, noutput_names = 0;
    ompi_rc_status_t status;

    ompi_mpi_instance_pset_t *pset_ptr;
    if(PMIX_SUCCESS != (rc = refresh_pmix_psets(PMIX_QUERY_PSET_NAMES))){
        printf("refresh returned %d\n", rc);
        return rc;
    }
    if(NULL == (pset_ptr = get_pset_by_name(input_name))){
        printf("get pset by name returned NULL\n");
        return OMPI_ERR_NOT_FOUND;
    }

    rc = get_res_change_info(pset_ptr->name, type, &output_names, &noutput_names, incl, &status, NULL, get_by_delta_name);

    if(OMPI_SUCCESS == rc && noutput_names != 0){
        strcpy(output_name, output_names[0]);
        for(n = 0; n < noutput_names; n++){
            free(output_names[n]);
        }
        free(output_names);
    }


    return rc;
}

/* Query the runtime for available resource changes given either the delta PSet or the associated PSet */
int ompi_instance_dyn_v1_recv_res_change_nb(ompi_instance_t *instance, char *input_name, int *type, char *output_name, int *incl, bool get_by_delta_name, ompi_request_t **req){
    
    int rc;
    bool refresh = true;
    ompi_mpi_instance_pset_t *pset_ptr;
    pmix_query_t query;

    if(PMIX_SUCCESS != (rc = refresh_pmix_psets(PMIX_QUERY_PSET_NAMES))){
        printf("refresh returned %d\n", rc);
        return rc;
    }
    if(NULL == (pset_ptr = get_pset_by_name(input_name))){
        printf("get pset by name returned NULL\n");
        return OMPI_ERR_NOT_FOUND;
    }

    ompi_instance_nb_req_create(req);


    ompi_mpi_instance_resource_change_t *res_change;
    /* if we don't find a valid & active res change locally, query the runtime. TODO: MPI Info directive QUERY RUNTIME */
    ompi_instance_lock_rc_and_psets();
    if(NULL == (res_change = get_res_change_active_for_name(pset_ptr->name))){

        v1_recv_rc_results *recv_results = malloc(sizeof(v1_recv_rc_results));
        nb_chain_info *chain_info = &recv_results->chain_info;

        chain_info->func = V1_RECV_RC;
        chain_info->nstages = 3;
        chain_info->cur_stage = 0;
        chain_info->stages = malloc(3 * sizeof(nb_chain_stage));
        chain_info->stages[0] = QUERY_RC_STAGE;
        chain_info->stages[1] = QUERY_MEM_STAGE;
        chain_info->stages[2] = LAST_STAGE;
        chain_info->req = *req;

        recv_results->rc_type = type;
        recv_results->input_pset = pset_ptr->name;
        recv_results->output_pset = output_name;
        recv_results->incl = incl;
        recv_results->get_by_delta_pset = get_by_delta_name;

        PMIX_QUERY_CONSTRUCT(&query);
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSETOP_TYPE);
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSETOP_INPUT);
        PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSETOP_OUTPUT);
        

        query.nqual = 3;
        PMIX_INFO_CREATE(query.qualifiers, 3);
        PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
        PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PROCID, &opal_process_info.myprocid, PMIX_PROC);
        PMIX_INFO_LOAD(&query.qualifiers[2], PMIX_PSET_NAME, pset_ptr->name, PMIX_STRING);

        
        ompi_instance_unlock_rc_and_psets();
        /*
         * TODO: need to handle this better
         */

        if (PMIX_SUCCESS != (rc = PMIx_Query_info_nb(&query, 1, pmix_info_cb_nb,(void*) recv_results))) {
           printf("PMIx_Query_info_nb failed with error %d\n", rc);                                              
           
        }
    }else{
        /* lookup requested properties of the resource change */
        *type = res_change->type;
        *incl = 0;

        if(get_by_delta_name){
            strcpy(output_name, res_change->bound_psets[0]->name);
        }else{
            strcpy(output_name, res_change->delta_psets[0]->name);
        }

        ompi_mpi_instance_pset_t *delta_pset_ptr;
        ompi_instance_unlock_rc_and_psets();
        if(NULL != (delta_pset_ptr = res_change->delta_psets[0])){
            opal_process_name_t *procs = NULL;
            size_t nprocs;
            
            rc = get_pset_membership(delta_pset_ptr->name, &procs, &nprocs);
            *incl = (opal_is_pset_member(procs, nprocs, opal_process_info.my_name) ? 1 : 0);
            rc = ompi_instance_free_pset_membership(delta_pset_ptr->name);
        }

        /* TODO: provide additional information in info object if requested */
        opal_atomic_wmb();
                    
        (*req)->req_complete = REQUEST_COMPLETED;
        (*req)->req_status.MPI_ERROR = rc;
        (*req)->req_state = OMPI_REQUEST_INVALID;
    }

    return rc;
}

/* Request a resource change of type 'rc_type' and size 'delta' from the RTE */
int ompi_instance_dyn_v1_request_res_change(MPI_Session session, int delta, char *assoc_pset, ompi_psetop_type_t rc_type, MPI_Info *info){
        
    char *delta_string;
    int length, rc;
    
    ompi_instance_rc_op_handle_t *op_handle = ompi_mpi_rc_op_handle_null_ptr;
    ompi_info_t *ompi_info = ompi_info_allocate();


    length = snprintf( NULL, 0, "%d", delta);
    delta_string = (char *) malloc( length + 1 );   
    snprintf(delta_string, length + 1, "%d", delta);
    
    
    if(OMPI_SUCCESS != (rc = ompi_info_set(ompi_info, "mpi.op_info.info.num_procs", delta_string))){
        ompi_info_free(&ompi_info);
        return rc;
    }

    if(OMPI_SUCCESS != (rc = rc_op_handle_create(&op_handle))){
        ompi_info_free(&ompi_info);
        return rc;
    }
    
    if(OMPI_SUCCESS != (rc = rc_op_handle_add_op(rc_type, &assoc_pset, 1, NULL, 0, ompi_info, op_handle))){
        rc_op_handle_free(&op_handle);
        ompi_info_free(&ompi_info);
        return rc;
    }

    rc = ompi_instance_dyn_v2b_psetop(session, op_handle);
        
    rc_op_handle_free(&op_handle);
    ompi_info_free(&ompi_info);
    
    return rc;
}

/* Request a resource change of type 'rc_type' and size 'delta' from the RTE */
int ompi_instance_dyn_v1_request_res_change_nb(MPI_Session session, int delta, char *assoc_pset, ompi_psetop_type_t rc_type, MPI_Info *info, ompi_request_t **request){
        
    char *delta_string;
    int length, rc;
    nb_chain_info *chain_info;
    pmix_info_t *pmix_info;
    
    ompi_instance_rc_op_handle_t *op_handle = ompi_mpi_rc_op_handle_null_ptr;
    ompi_info_t *ompi_info = ompi_info_allocate();


    length = snprintf( NULL, 0, "%d", delta);
    delta_string = (char *) malloc( length + 1 );   
    snprintf(delta_string, length + 1, "%d", delta);
    
    
    if(OMPI_SUCCESS != (rc = ompi_info_set(ompi_info, "mpi.op_info.info.num_procs", delta_string))){
        ompi_info_free(&ompi_info);
        return rc;
    }

    if(OMPI_SUCCESS != (rc = rc_op_handle_create(&op_handle))){
        ompi_info_free(&ompi_info);
        return rc;
    }

    if(OMPI_SUCCESS != (rc = rc_op_handle_add_op(rc_type, &assoc_pset, 1, NULL, 0, ompi_info, op_handle))){
        rc_op_handle_free(&op_handle);
        ompi_info_free(&ompi_info);
        return rc;
    }

    /* Construct the nb_chain handle. TODO: Rename fence handle. We use it for all procedures without output */
    fence_results *req_rc_results = malloc(sizeof(fence_results));

    ompi_instance_nb_req_create(request);
    chain_info = &req_rc_results->chain_info;

    chain_info->func = V1_REQ_RC;
    chain_info->nstages = 2;
    chain_info->cur_stage = 0;
    chain_info->stages = malloc(2 * sizeof(nb_chain_stage));
    chain_info->stages[0] = REQUEST_RC_STAGE;
    chain_info->stages[1] = LAST_STAGE;
    chain_info->req = *request;

    PMIX_INFO_CREATE(pmix_info, 1);
    rc_op_handle_serialize(op_handle, pmix_info);

    rc = PMIx_Allocation_request_nb(MPI_ALLOC_SET_REQUEST, pmix_info, 1, pmix_info_cb_nb, (void *) req_rc_results);

    PMIX_INFO_FREE(pmix_info, 1);
        
    rc_op_handle_free(&op_handle);
    ompi_info_free(&ompi_info);
    
    return rc;
}

/* Execute a Pset operation with two operands and one result. Possible Operations: UNION, DIFFERENCE, INTERSECTION */
int ompi_instance_dyn_v1_psetop(ompi_instance_t *instance, const char *pset1, const char *pset2, char *pref_name, char *pset_result, ompi_psetop_type_t op){

    int rc, ret, n, ninput = 2, noutput = 0;
    char **input = NULL, **output = NULL;
    ompi_instance_rc_op_handle_t *op_handle;

    if(NULL == pset1 || NULL == pset2){
        return OMPI_ERR_BAD_PARAM;
    }

    input = (char **) malloc(ninput * sizeof(char *));
    input[0] = strdup(pset1);
    input[1] = strdup(pset2);

    if(NULL != pref_name){
        noutput = 1;
        output = (char **) malloc(noutput * sizeof(char *));
        output[0] = strdup(pref_name);
    }

    if(OMPI_SUCCESS != (rc = rc_op_handle_create(&op_handle))){
        return rc;
    }
    
    if(OMPI_SUCCESS != (rc = rc_op_handle_add_op(MPI_OMPI_CONVT_PSET_OP(op), input, ninput, output, noutput, NULL, op_handle))){
        rc_op_handle_free(&op_handle);
        return rc;
    }

    rc = ompi_instance_dyn_v2b_psetop(instance, op_handle);
    if(PMIX_SUCCESS != rc){
        ret = opal_pmix_convert_status(rc);
        return ompi_instance_print_error ("Set Op failed", ret);
    }

    size_t num;
    rc_op_handle_get_num_output(op_handle, 0, &num);  
    
    strcpy(pset_result, op_handle->rc_op_info.output_names[0]);

    rc_op_handle_free(&op_handle);
    
    free(input[0]);
    free(input[1]);
    free(input);

    if(0 < noutput){
        for(n = 0; n < noutput; n++){
            free(output[n]);
        }
        free(output);
    }

    return OMPI_SUCCESS;
}

/* Execute a non-blocking Pset operation with two operands and one result. Possible Operations: UNION, DIFFERENCE, INTERSECTION */
int ompi_instance_dyn_v1_psetop_nb(ompi_instance_t *instance, const char *pset1, const char *pset2, char *pref_name, char *pset_result, ompi_psetop_type_t op, ompi_request_t **request){

    pmix_status_t rc;
    pmix_info_t *info;
    ompi_mpi_instance_pset_t *pset_ptr1, *pset_ptr2;
    ompi_instance_rc_op_handle_t *op_handle;
    char **input = NULL, **output = NULL;
    size_t ninput = 2, noutput = 0;

    if(PMIX_SUCCESS != (rc = refresh_pmix_psets(PMIX_QUERY_PSET_NAMES))){
        return rc;
    }
    if( NULL == (pset_ptr1 = get_pset_by_name((char *) pset1)) ||
        NULL == (pset_ptr2 = get_pset_by_name((char *) pset2))){
        return OMPI_ERR_NOT_FOUND;
    }

    v1_psetop_results *psetop_results = malloc(sizeof(v1_psetop_results));

    ompi_instance_nb_req_create(request);

    nb_chain_info *chain_info = &psetop_results->chain_info;

    chain_info->func = V1_PSETOP;
    chain_info->nstages = 2;
    chain_info->cur_stage = 0;
    chain_info->stages = malloc(2 * sizeof(nb_chain_stage));
    chain_info->stages[0] = QUERY_RC_STAGE;
    chain_info->stages[1] = LAST_STAGE;
    chain_info->req = *request;

    psetop_results->pset_result = pset_result;

    input = (char **) malloc(ninput * sizeof(char *));
    input[0] = strdup(pset1);
    input[1] = strdup(pset2);

    if(NULL != pref_name){
        noutput = 1;
        output = (char **) malloc(noutput * sizeof(char *));
        output[0] = strdup(pref_name);
    }

    if(OMPI_SUCCESS != (rc = rc_op_handle_create(&op_handle))){
        free(input);
        free(output);
        return rc;
    }

    if(OMPI_SUCCESS != (rc = rc_op_handle_add_op(op, input, ninput, output, noutput, NULL, op_handle))){
        rc_op_handle_free(&op_handle);
        free(input);
        free(output);
        return rc;
    }

    PMIX_INFO_CREATE(info, 1);
    rc_op_handle_serialize(op_handle, info);

    rc = PMIx_Allocation_request_nb(MPI_ALLOC_SET_REQUEST, info, 1, pmix_info_cb_nb, (void *) psetop_results);

    PMIX_INFO_FREE(info, 1);
    free(input);
    free(output);

    return OMPI_SUCCESS;
}


void ompi_instance_clear_rc_cache(char *delta_pset){
    res_change_clear_cache(delta_pset);
}

/* 
 * Collective over union of delta pset & associated Pset.
 * First, makes a Pset name available to all processes in the collective via publish/lookup.
 * Then, performs a fence across all processes in the collective.
 */
int ompi_instance_dyn_v1_integrate_res_change(ompi_instance_t *instance, char *delta_pset, char *pset_buf, int provider, int *terminate){
    int rc, incl;

    char key[PMIX_MAX_KEYLEN + 1];
    char *prefix = "mpi_integrate:";
    ompi_psetop_type_t rc_type;
    ompi_rc_status_t rc_status;
    ompi_mpi_instance_pset_t *pset_ptr;

    char ** fence_psets;
    char **associated_psets = NULL;
    size_t nassociated_psets;
    char **delta_psets = NULL;
    size_t ndelta_psets, n;

    if(PMIX_SUCCESS != (rc = refresh_pmix_psets(PMIX_QUERY_PSET_NAMES))){
        printf("refresh failed with %d\n", rc);
        return rc;
    }
    if(NULL == (pset_ptr = get_pset_by_name(delta_pset))){
        printf("get PSset by name failed with %d\n", OMPI_ERR_NOT_FOUND);
        return OMPI_ERR_NOT_FOUND;
    }

    /* Query the resource change information for the given delta_pset */
    rc = ompi_instance_get_res_change(instance, pset_ptr->name, &rc_type, &associated_psets, &nassociated_psets, &incl, &rc_status, NULL, true);
    /* Just return the error. The other procs will experience an error in Lookup/Fence */
    if(OMPI_SUCCESS != rc){
        printf("get res change for delta failed with %d\n", rc);
        return rc;
    }

    /* Query the resource change information for the given assoc_pset */
    rc = ompi_instance_get_res_change(instance, associated_psets[0], &rc_type, &delta_psets, &ndelta_psets, &incl, &rc_status, NULL, false);
    /* Just return the error. The other procs will experience an error in Lookup/Fence */
    if(OMPI_SUCCESS != rc){
        printf("get res change failed with %d\n", rc);
        return rc;
    }


    /* If the process is included in the delta PSet of a resource subtraction it is expected to terminate soon */
    *terminate = (rc_type == OMPI_PSETOP_SUB && incl);
    /* FIXME:   Unfortunately a PSet name can have size 512, which is already the max length of a PMIx key. 
     *          For now we just put in an assertion and assume PSet names to be shorter than the max length
     */
    assert(strlen(delta_psets[0]) + strlen(prefix) < PMIX_MAX_KEYLEN);

    strcpy(key, prefix);
    strcat(key, delta_psets[0]);


    /* The provider needs to publish the Pset name */
    if(provider){
        /* Just return the error. The other procs will experience an error in Lookup/Fence */
        if(NULL == pset_buf){
            return OMPI_ERR_BAD_PARAM;
        }
        /* Publish the PSet name*/
        rc = opal_pmix_publish_string(key, pset_buf, strlen(pset_buf));

        /* Just return the error. The other procs will experience an error in Lookup/Fence */
        if(OMPI_SUCCESS != rc){
            printf("publish failed with %d\n", rc);
            return rc;
        }
    /* The other processes lookup the Pset name */
    }else{

        /* if they provided a NULL pointer as buffer we skip the lookup */
        if(NULL != pset_buf){
            /* Lookup the PSet name*/
            rc = opal_pmix_lookup_string_wait(key, pset_buf, OPAL_MAX_PSET_NAME_LEN);
            /* Just return the error. The other procs will experience an error in Lookup/Fence */
            if(OMPI_SUCCESS != rc){
                printf("lookup failed with %d\n", rc);
                return rc;
            }
        }
    }

    fence_psets = malloc((ndelta_psets + nassociated_psets) * sizeof(char *));
    for(n = 0; n < ndelta_psets; n++){
        fence_psets[n] = strdup(delta_psets[n]);
    }
    for(n = ndelta_psets; n < ndelta_psets + nassociated_psets; n++){
        fence_psets[n] = strdup(associated_psets[n - ndelta_psets]);
    }

    rc = ompi_instance_pset_fence_multiple(fence_psets, ndelta_psets + nassociated_psets, NULL);
    if(OMPI_SUCCESS != rc){
        printf("fence failed with %d\n", rc);
    }
    
    /* Finalize the resource change. TODO: Find a better way. There is not always a provider. */
    if(provider && MPI_PSETOP_ADD == rc_type){
        
        bool non_default = true;
        pmix_info_t *event_info;
        PMIX_INFO_CREATE(event_info, 2);
        (void)snprintf(event_info[0].key, PMIX_MAX_KEYLEN, "%s", PMIX_EVENT_NON_DEFAULT);
        PMIX_VALUE_LOAD(&event_info[0].value, &non_default, PMIX_BOOL);
        (void)snprintf(event_info[1].key, PMIX_MAX_KEYLEN, "%s", PMIX_PSET_NAME);
        PMIX_VALUE_LOAD(&event_info[1].value, pset_ptr->name, PMIX_STRING);
        PMIx_Notify_event(PMIX_RC_FINALIZED, NULL, PMIX_RANGE_RM, event_info, 2, NULL, NULL);
        
        PMIX_INFO_FREE(event_info, 2);
    }

    ompi_instance_clear_rc_cache(pset_ptr->name);

    for(n = 0; n < ndelta_psets + nassociated_psets; n++){
        free(fence_psets[n]);
    }
    free(fence_psets);

    for(n = 0; n < ndelta_psets; n++){
        free(delta_psets[n]);
    }
    free(delta_psets);

    for(n = 0; n < nassociated_psets; n++){
        free(associated_psets[n]);
    }
    free(associated_psets);

    return rc;
}

int ompi_instance_dyn_v1_integrate_res_change_nb(ompi_instance_t *instance, char *delta_pset, char *pset_buf, int provider, int *terminate, ompi_request_t **request){
    int rc;
    ompi_mpi_instance_pset_t *pset_ptr = NULL;

    while(NULL == pset_ptr){
        if(PMIX_SUCCESS != (rc = refresh_pmix_psets(PMIX_QUERY_PSET_NAMES))){
            return rc;
        }
        if(NULL == (pset_ptr = get_pset_by_name(delta_pset))){
            printf("Proc %d: get_pset_by_name for pset %s returned NULL\n", opal_process_info.myprocid.rank, delta_pset);
            sleep(2);
        }
    }

    integrate_rc_results *int_rc_results = malloc(sizeof(integrate_rc_results));

    ompi_instance_nb_req_create(request);

    nb_chain_info *chain_info = &int_rc_results->chain_info;

    chain_info->func = INTEGRATE_RC;
    chain_info->nstages = 5;
    chain_info->cur_stage = 0;
    chain_info->stages = malloc(5 * sizeof(nb_chain_stage));
    chain_info->stages[0] = QUERY_RC_STAGE;
    chain_info->stages[1] = PUBSUB_STAGE;
    chain_info->stages[2] = QUERY_MEM_STAGE;
    chain_info->stages[3] = FENCE_STAGE;
    chain_info->stages[4] = LAST_STAGE;
    chain_info->req = *request;

    /* TODO: We need to do the query non-blocking. So for now we just use one delta pset*/
    int_rc_results->ndelta_psets = 1;
    int_rc_results->delta_psets = malloc(sizeof(char*));
    int_rc_results->delta_psets[0] = strdup(pset_ptr->name);
    int_rc_results->pset_buf = pset_buf;
    int_rc_results->provider = provider;
    int_rc_results->terminate = terminate;

    /* Query the  resource change information for the given delta_pset */
    rc = PMIX_ERR_NOT_FOUND;
    while(OMPI_SUCCESS != rc){
        rc = ompi_instance_get_res_change(instance, int_rc_results->delta_psets[0], &int_rc_results->rc_type, &int_rc_results->assoc_psets, &int_rc_results->nassoc_psets, &int_rc_results->incl, &int_rc_results->rc_status, NULL, true);
        sleep(1);
    }
    /* Just return the error. The other procs will experience an error in Lookup/Fence */
    pmix_info_cb_nb(rc, NULL, 0, (void*) int_rc_results, NULL, NULL); 

    return rc;
    
}


int ompi_instance_dyn_v2a_query_psetop(ompi_instance_t *instance, char *coll_pset_name, char *input_name, ompi_psetop_type_t *type, char ***output_names, size_t *noutputs, bool get_by_delta_name){
    int rc, incl;
    size_t n_coll_procs;
    ompi_rc_status_t status;
    pmix_proc_t *coll_procs;
    opal_process_name_t *opal_coll_procs;
    ompi_mpi_instance_pset_t *pset_ptr;
    
    /* TODO!!*/
    size_t dummy_noutput;

    if(0 == strcmp(coll_pset_name, "mpi://SELF")){
        rc = get_res_change_info(input_name, type, output_names, &dummy_noutput, &incl, &status, NULL, get_by_delta_name);
    }else{

        if(NULL == (pset_ptr = get_pset_by_name(coll_pset_name))){
            return OMPI_ERR_NOT_FOUND;
        }

        rc = get_pset_membership(pset_ptr->name, &opal_coll_procs, &n_coll_procs);
        if(rc != OMPI_SUCCESS){
            return rc;
        }
        
        PMIX_PROC_CREATE(coll_procs, n_coll_procs);
        opal_pmix_proc_array_conv(opal_coll_procs, &coll_procs, n_coll_procs);

        ompi_instance_free_pset_membership(coll_pset_name);

        if(NULL == (pset_ptr = get_pset_by_name(input_name))){
            free(coll_procs);
            return OMPI_ERR_NOT_FOUND;
        }
        
        rc = get_res_change_info_collective(coll_procs, n_coll_procs, pset_ptr->name, type, output_names, noutputs, &incl, &status, NULL, get_by_delta_name);

        free(coll_procs);
    }
    if(rc != OMPI_SUCCESS && rc != OMPI_ERR_NOT_FOUND){
        return rc;
    }
    return OMPI_SUCCESS;
}

int ompi_instance_dyn_v2a_query_psetop_nb(ompi_instance_t *instance, char *coll_pset_name, char *input_name, int *type, char ***output_names, int *noutputs, bool get_by_delta_name, ompi_request_t **request){
    int rc;
    size_t n_coll_procs;
    pmix_proc_t *coll_procs;
    opal_process_name_t *opal_coll_procs;
    ompi_mpi_instance_pset_t *pset_ptr;
    nb_chain_info *chain_info;

    if(NULL == (pset_ptr = get_pset_by_name(coll_pset_name))){
        return OMPI_ERR_NOT_FOUND;
    }
    rc = get_pset_membership(pset_ptr->name, &opal_coll_procs, &n_coll_procs);
    if(rc != OMPI_SUCCESS){
        return rc;
    }
    
    PMIX_PROC_CREATE(coll_procs, n_coll_procs);
    opal_pmix_proc_array_conv(opal_coll_procs, &coll_procs, n_coll_procs);
    ompi_instance_free_pset_membership(coll_pset_name);

    if(NULL == (pset_ptr = get_pset_by_name(input_name))){
        free(coll_procs);
        return OMPI_ERR_NOT_FOUND;
    }

    /* Construct the nb_chain handle */
    v2a_query_psetop_results *query_psetop_res = malloc(sizeof(v2a_query_psetop_results));
    ompi_instance_nb_req_create(request);
    chain_info = &query_psetop_res->chain_info;
    chain_info->func = V2A_QUERY_PSETOP;
    chain_info->nstages = 2;
    chain_info->cur_stage = 0;
    chain_info->stages = malloc(2 * sizeof(nb_chain_stage));
    chain_info->stages[0] = QUERY_PSETOP_STAGE;
    chain_info->stages[1] = LAST_STAGE;
    chain_info->req = *request;

    query_psetop_res->get_by_delta_name = get_by_delta_name;
    query_psetop_res->input_name = pset_ptr->name;
    query_psetop_res->output = output_names;
    query_psetop_res->noutput = noutputs;
    query_psetop_res->type = type;
    
    
    rc = get_res_change_info_collective_nb(coll_procs, n_coll_procs, pset_ptr->name, pmix_info_cb_nb, (void *) query_psetop_res);
    free(coll_procs);
    
    if(rc != OMPI_SUCCESS && rc != OMPI_ERR_NOT_FOUND){
        return rc;
    }

    return OMPI_SUCCESS;
}

int ompi_instance_dyn_v2a_pset_op(ompi_instance_t *session, int *op, char **input_sets, int ninput, char *** output_sets, int *noutput, ompi_info_t *info){
    pmix_status_t rc;
    pmix_info_t *results, *pmix_info, *info_ptr;
    pmix_value_t *out_name_vals = NULL;
    size_t nresults, noutput_names = 0, n;

    ompi_instance_rc_op_handle_t *rc_op_handle;
    rc_op_handle_create(&rc_op_handle);

    if(0 == *noutput){
        rc_op_handle_add_op(MPI_OMPI_CONV_PSET_OP(*op), input_sets, ninput, NULL, 0, info, rc_op_handle);
    }else{
        rc_op_handle_add_op(MPI_OMPI_CONV_PSET_OP(*op), input_sets, ninput, *output_sets, *noutput, info, rc_op_handle);
    }

    PMIX_INFO_CREATE(pmix_info, 1);
    rc_op_handle_serialize(rc_op_handle, pmix_info);

    rc = PMIx_Allocation_request(MPI_ALLOC_SET_REQUEST, pmix_info, 1, &results, &nresults);

    rc_op_handle_free(&rc_op_handle);

    if(PMIX_SUCCESS == rc){
        /* Get the array of pmix_value_t containing the output names*/
        for(n = 0; n < nresults; n++){
            if(PMIX_CHECK_KEY(&results[n], "mpi.set_info.output")){
                out_name_vals = results[n].value.data.darray->array;
                noutput_names = results[n].value.data.darray->size;
            }

            if(PMIX_CHECK_KEY(&results[n], "mpi.rc_op_handle")){
                info_ptr = (pmix_info_t *) results[n].value.data.darray->array;
                *op = MPI_OMPI_CONVT_PSET_OP(info_ptr[0].value.data.uint8);
            }
        }
        if(0 == noutput_names || NULL == out_name_vals){
            rc = OMPI_ERR_BAD_PARAM;
            goto CLEANUP;
        }

        /* Fill in the output for the "resource operation" */
        if(0 == *noutput){
            *noutput = noutput_names;
            *output_sets = (char **) malloc(noutput_names * sizeof(char *));
        }
        for(n = 0; n < noutput_names; n++){
            (*output_sets)[n] = strdup(out_name_vals[n].data.string);
        }
    /* Indicate pending PSet operation or failue to aquire enough resources with noutput = 0 instead of an error code */
    }else if(PMIX_ERR_EXISTS == rc || PMIX_ERR_OUT_OF_RESOURCE == rc){
        *op = MPI_PSETOP_NULL;
        *noutput = 0;
        rc = OMPI_SUCCESS;
    }

CLEANUP:
    PMIX_INFO_FREE(pmix_info, 1);
    if(0 < nresults){
        PMIX_INFO_FREE(results, nresults);
    }

    return rc;
}

int ompi_instance_dyn_v2a_pset_op_nb(ompi_instance_t *session, int *op, char **input_sets, int ninput, char *** output_sets, int *noutput, ompi_info_t *info, ompi_request_t **request){
    
    pmix_status_t rc;
    pmix_info_t *pmix_info;
    ompi_instance_rc_op_handle_t *rc_op_handle;
    v2a_psetop_results *req_rc_results = malloc(sizeof(v2a_psetop_results));

    ompi_instance_nb_req_create(request);
    nb_chain_info *chain_info = &req_rc_results->chain_info;

    chain_info->func = V2A_PSETOP;
    chain_info->nstages = 2;
    chain_info->cur_stage = 0;
    chain_info->stages = malloc(2 * sizeof(nb_chain_stage));
    chain_info->stages[0] = PSETOP_STAGE;
    chain_info->stages[1] = LAST_STAGE;
    chain_info->req = *request;

    req_rc_results->output = output_sets;
    req_rc_results->noutput = noutput;
    req_rc_results->psetop = op;

    PMIX_INFO_CREATE(pmix_info, 1);
    rc_op_handle_create(&rc_op_handle);
    if(0 == *noutput){
        rc_op_handle_add_op(MPI_OMPI_CONV_PSET_OP(*op), input_sets, ninput, NULL, 0, info, rc_op_handle);
    }else{
        rc_op_handle_add_op(MPI_OMPI_CONV_PSET_OP(*op), input_sets, ninput, *output_sets, *noutput, info, rc_op_handle);
    }
    
    rc_op_handle_serialize(rc_op_handle, pmix_info);

    rc = PMIx_Allocation_request_nb(MPI_ALLOC_SET_REQUEST, pmix_info, 1, pmix_info_cb_nb, (void *) req_rc_results);

    PMIX_INFO_FREE(pmix_info, 1);

    return rc;    
}

int ompi_instance_dyn_v2b_rc_op_handle_create(ompi_instance_t *instance, ompi_instance_rc_op_handle_t **rc_op_handle){
    int rc;

    rc = rc_op_handle_create(rc_op_handle);

    return rc;
}

int ompi_instance_dyn_v2b_rc_op_handle_add_op(ompi_instance_t *instance, ompi_psetop_type_t rc_type, char **input_names, size_t n_input_names, char **output_names, size_t n_output_names, ompi_info_t *info, ompi_instance_rc_op_handle_t *rc_op_handle){
    int rc;
    rc = rc_op_handle_add_op(rc_type, input_names, n_input_names, output_names, n_output_names, info, rc_op_handle);

    return rc;
}

int ompi_instance_dyn_v2b_rc_op_handle_add_pset_info(ompi_instance_t *instance, ompi_instance_rc_op_handle_t * rc_op_handle, char *pset_name, ompi_info_t * info){
    int n, nkeys, rc, val_len, flag;
    pmix_info_t *infos;
    opal_cstring_t *key, *value;

    if(OMPI_SUCCESS != (rc = ompi_info_get_nkeys(info, &nkeys)) || 1 > nkeys ){
        printf("get nkeys failed with %d\n", rc);
        return rc;
    }

    PMIX_INFO_CREATE(infos, nkeys);

    for(n = 0; n < nkeys; n++){
        if(OMPI_SUCCESS != (rc = ompi_info_get_nthkey(info, n, &key))){
            printf("get nthkey failed with %d\n", rc);
            PMIX_INFO_FREE(infos, (size_t) nkeys);
            return rc;
        }

        if(OMPI_SUCCESS != (rc = ompi_info_get_valuelen(info, key->string, &val_len, &flag))){
            printf("get val len failed with %d\n", rc);
            OBJ_RELEASE(key);
            PMIX_INFO_FREE(infos, (size_t) nkeys);
            return rc;
        }

        if(!flag){
            printf("key not found\n");
            OBJ_RELEASE(key);
            PMIX_INFO_FREE(infos, (size_t) nkeys);
            return OMPI_ERR_BAD_PARAM;
        }

        if(OMPI_SUCCESS != (rc = ompi_info_get(info, key->string, &value, &flag))){
            printf("info get failed with %d\n", rc);
            OBJ_RELEASE(key);
            PMIX_INFO_FREE(infos, (size_t) nkeys);
            return rc;
        }

        if(!flag){
            printf("value not found for key '%s'\n", key->string);
            OBJ_RELEASE(key);
            PMIX_INFO_FREE(infos, (size_t) nkeys);
            return OMPI_ERR_BAD_PARAM;
        }

        PMIX_INFO_LOAD(&infos[n], key->string, value->string, PMIX_STRING);

    }

    rc = rc_op_handle_add_pset_infos(rc_op_handle, pset_name, infos, nkeys);

    PMIX_INFO_FREE(infos, (size_t) nkeys);

    return rc;

}

int ompi_instance_dyn_v2b_rc_op_handle_free(ompi_instance_t * instance, ompi_instance_rc_op_handle_t ** rc_op_handle){
    int rc;

    rc = rc_op_handle_free(rc_op_handle);

    return rc;
}

int ompi_instance_dyn_v2b_rc_op_handle_get_op_type(ompi_instance_t * instance, ompi_instance_rc_op_handle_t * rc_op_handle, int op_index, int *op_type){
    int rc;
    ompi_psetop_type_t op;

    if(0 > op_index){
        return OMPI_ERR_BAD_PARAM;
    }
    
    if(OMPI_SUCCESS != (rc = rc_op_handle_get_get_op_type(rc_op_handle, (size_t) op_index, &op))){
        return rc;
    }

    *op_type = MPI_OMPI_CONVT_PSET_OP(op);

    return OMPI_SUCCESS;
}

int ompi_instance_dyn_v2b_rc_op_handle_get_num_ops(ompi_instance_t * instance, ompi_instance_rc_op_handle_t * rc_op_handle, size_t *num_ops){

    *num_ops = rc_op_handle_get_num_ops(rc_op_handle);

    return OMPI_SUCCESS;
}

int ompi_instance_dyn_v2b_rc_op_handle_get_num_output(ompi_instance_t * instance, ompi_instance_rc_op_handle_t * rc_op_handle, size_t op_index, size_t *num_output){
    int rc;

    rc = rc_op_handle_get_num_output(rc_op_handle, op_index, num_output);

    return rc;
}

int ompi_instance_dyn_v2b_rc_op_handle_get_ouput_name(ompi_instance_t * instance, ompi_instance_rc_op_handle_t *rc_op_handle, size_t op_index, size_t name_index, int *pset_len, char *pset_name){
    int rc;

    rc = rc_op_handle_get_ouput_name(rc_op_handle, op_index, name_index, pset_len, pset_name);

    return rc;
}

/* setops */
int ompi_instance_dyn_v2b_query_psetop(ompi_instance_t * instance, char *coll_pset_name, char *input_name, ompi_instance_rc_op_handle_t **rc_op_handle){
    int rc, incl;
    size_t n, n_coll_procs, ninput = 0, noutput = 0;
    char **input_names = NULL, **output_names = NULL;
    pmix_proc_t *coll_procs;
    opal_process_name_t *opal_coll_procs;
    ompi_mpi_instance_pset_t *pset_ptr;
    ompi_psetop_type_t type;
    ompi_rc_status_t status;


    if(NULL == (pset_ptr = get_pset_by_name(coll_pset_name))){
        return OMPI_ERR_NOT_FOUND;
    }

    rc = get_pset_membership(pset_ptr->name, &opal_coll_procs, &n_coll_procs);
    if(rc != OMPI_SUCCESS){
        return rc;
    }
    
    PMIX_PROC_CREATE(coll_procs, n_coll_procs);
    opal_pmix_proc_array_conv(opal_coll_procs, &coll_procs, n_coll_procs);
    ompi_instance_free_pset_membership(coll_pset_name);
    
    if(NULL == (pset_ptr = get_pset_by_name(input_name))){
        free(coll_procs);
        return OMPI_ERR_NOT_FOUND;
    }

    if(OMPI_SUCCESS != (rc = rc_op_handle_create(rc_op_handle))){
        *rc_op_handle = MPI_RC_HANDLE_NULL;
        return rc;
    }
    
    rc = get_res_change_info_collective(coll_procs, n_coll_procs, pset_ptr->name, &type, &output_names, &noutput, &incl, &status, NULL, false);
    if(rc != OMPI_SUCCESS){
        rc_op_handle_free(rc_op_handle);
        *rc_op_handle = MPI_RC_HANDLE_NULL;
        if(rc == OPAL_ERR_NOT_FOUND){
            return OMPI_SUCCESS;
        }
        return rc;
    }

    rc = get_res_change_info_collective(coll_procs, n_coll_procs, pset_ptr->name, &type, &input_names, &ninput, &incl, &status, NULL, true);
    if(rc != OMPI_SUCCESS){
        rc_op_handle_free(rc_op_handle);
        *rc_op_handle = MPI_RC_HANDLE_NULL;
        if(rc == OPAL_ERR_NOT_FOUND){
            return OMPI_SUCCESS;
        }
        return rc;
    }

    if(OMPI_SUCCESS != (rc = rc_op_handle_add_op(type, input_names, ninput, output_names, noutput, NULL, *rc_op_handle))){
        rc_op_handle_free(rc_op_handle);
        *rc_op_handle = MPI_RC_HANDLE_NULL;
        return rc;
    }
    free(coll_procs);

    for(n = 0; n < ninput; n++){
        free(input_names[n]);
    }
    free(input_names);

    for(n = 0; n < noutput; n++){
        free(output_names[n]);
    }
    free(output_names);

    
    return OMPI_SUCCESS;
}

int ompi_instance_dyn_v2b_query_psetop_nb(ompi_instance_t * instance, char *coll_pset_name, char *input_name, ompi_instance_rc_op_handle_t **rc_op_handle, ompi_request_t **request){
    int rc;
    size_t n_coll_procs;
    pmix_proc_t *coll_procs;
    opal_process_name_t *opal_coll_procs;
    ompi_mpi_instance_pset_t *pset_ptr;
    nb_chain_info *chain_info;

    if(NULL == (pset_ptr = get_pset_by_name(coll_pset_name))){
        return OMPI_ERR_NOT_FOUND;
    }
    rc = get_pset_membership(pset_ptr->name, &opal_coll_procs, &n_coll_procs);
    if(rc != OMPI_SUCCESS){
        return rc;
    }
    
    PMIX_PROC_CREATE(coll_procs, n_coll_procs);
    opal_pmix_proc_array_conv(opal_coll_procs, &coll_procs, n_coll_procs);
    ompi_instance_free_pset_membership(coll_pset_name);

    if(NULL == (pset_ptr = get_pset_by_name(input_name))){
        free(coll_procs);
        return OMPI_ERR_NOT_FOUND;
    }

    /* Construct the nb_chain handle */
    v2b_query_psetop_results *query_psetop_res = malloc(sizeof(v2b_query_psetop_results));
    ompi_instance_nb_req_create(request);
    chain_info = &query_psetop_res->chain_info;
    chain_info->func = V2B_QUERY_PSETOP;
    chain_info->nstages = 2;
    chain_info->cur_stage = 0;
    chain_info->stages = malloc(2 * sizeof(nb_chain_stage));
    chain_info->stages[0] = QUERY_PSETOP_STAGE;
    chain_info->stages[1] = LAST_STAGE;
    chain_info->req = *request;

    query_psetop_res->input_name = pset_ptr->name;
    query_psetop_res->rc_op_handle = rc_op_handle;
    
    
    rc = get_res_change_info_collective_nb(coll_procs, n_coll_procs, pset_ptr->name, pmix_info_cb_nb, (void *) query_psetop_res);
    free(coll_procs);
    
    if(rc != OMPI_SUCCESS && rc != OMPI_ERR_NOT_FOUND){
        return rc;
    }

    return OMPI_SUCCESS;
}

int ompi_instance_dyn_v2b_psetop(ompi_instance_t * instance, ompi_instance_rc_op_handle_t * rc_op_handle){

    pmix_status_t rc;
    pmix_info_t *info, *results, *info_ptr;
    pmix_value_t *out_name_vals = NULL;
    ompi_instance_set_op_handle_t *setop;
    size_t nresults, n, k = 0;

    PMIX_INFO_CREATE(info, 1);
    rc_op_handle_serialize(rc_op_handle, info);

    rc = PMIx_Allocation_request(MPI_ALLOC_SET_REQUEST, info, 1, &results, &nresults);

    if(PMIX_SUCCESS == rc){
        
        /* Get the array of pmix_value_t containing the output names*/
        for(n = 0; n < nresults; n++){
            if(PMIX_CHECK_KEY(&results[n], "mpi.set_info.output")){
                out_name_vals = results[n].value.data.darray->array;
            }
            
            if(PMIX_CHECK_KEY(&results[n], "mpi.rc_op_handle")){
                info_ptr = (pmix_info_t *) results[n].value.data.darray->array;
                rc_op_handle->rc_type = MPI_OMPI_CONVT_PSET_OP(info_ptr[0].value.data.uint8);
            }
            
        }

        if(NULL == out_name_vals){
            rc = OMPI_ERR_BAD_PARAM;
            goto CLEANUP;
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
        OPAL_LIST_FOREACH(setop, &rc_op_handle->set_ops, ompi_instance_set_op_handle_t){
            if(0 == setop->set_op_info.n_output_names){
                rc_op_handle_init_output(setop->psetop, &setop->set_op_info.output_names, &setop->set_op_info.n_output_names);
            }
            for(k = 0; k < setop->set_op_info.n_output_names; k++){
                free(setop->set_op_info.output_names[k]);
                setop->set_op_info.output_names[k] = strdup(out_name_vals[n++].data.string);
            }
        }
    }else{
        if(PMIX_ERR_EXISTS == rc || PMIX_ERR_OUT_OF_RESOURCE == rc){
            rc = OMPI_SUCCESS;
            rc_op_handle->rc_type = MPI_PSETOP_NULL;
            rc_op_handle->rc_op_info.n_output_names = 0;
            OPAL_LIST_FOREACH(setop, &rc_op_handle->set_ops, ompi_instance_set_op_handle_t){
                setop->psetop = MPI_PSETOP_NULL;
                setop->set_op_info.n_output_names = 0;
            }
        }
    }

CLEANUP:
    PMIX_INFO_FREE(info, 1);
    if(0 < nresults){
        PMIX_INFO_FREE(results, nresults);
    }

    return rc;
}

int ompi_instance_dyn_v2b_psetop_nb(ompi_instance_t * instance, ompi_instance_rc_op_handle_t * rc_op_handle, ompi_request_t **request){

    pmix_status_t rc;
    pmix_info_t *info;

    v2b_psetop_results *req_rc_results = malloc(sizeof(v2b_psetop_results));

    ompi_instance_nb_req_create(request);
    nb_chain_info *chain_info = &req_rc_results->chain_info;

    chain_info->func = V2B_PSETOP;
    chain_info->nstages = 2;
    chain_info->cur_stage = 0;
    chain_info->stages = malloc(2 * sizeof(nb_chain_stage));
    chain_info->stages[0] = PSETOP_STAGE;
    chain_info->stages[1] = LAST_STAGE;
    chain_info->req = *request;

    req_rc_results->rc_op_handle = rc_op_handle;

    PMIX_INFO_CREATE(info, 1);
    rc_op_handle_serialize(rc_op_handle, info);

    rc = PMIx_Allocation_request_nb(MPI_ALLOC_SET_REQUEST, info, 1, pmix_info_cb_nb, (void *) req_rc_results);

    PMIX_INFO_FREE(info, 1);

    return rc;
}

int ompi_instance_dyn_v2c_query_psetop(ompi_instance_t * instance, char *coll_pset_name, char *pset_name, ompi_info_t **setop_info){

    int rc;
    ompi_instance_rc_op_handle_t *op_handle = MPI_RC_HANDLE_NULL;
    ompi_info_t **info;
    size_t ninfo = 0;

    if(OMPI_SUCCESS != (rc = ompi_instance_dyn_v2b_query_psetop(instance, coll_pset_name, pset_name, &op_handle))){
        return rc;
    }

    if(MPI_RC_HANDLE_NULL == op_handle){
        *setop_info = MPI_INFO_NULL;
        return OMPI_SUCCESS;
    }

    if(OMPI_SUCCESS != (rc = rc_op_handle_to_info(op_handle, &info, &ninfo)) || 0 == ninfo){
        rc_op_handle_free(&op_handle);
        return rc;
    }
    
    *setop_info = info[0];
    
    free(info);

    return OMPI_SUCCESS;
}

int ompi_instance_dyn_v2c_query_psetop_nb(ompi_instance_t * instance, char *coll_pset_name, char *pset_name, ompi_info_t **setop_info, ompi_request_t **request){
    return OMPI_ERR_NOT_IMPLEMENTED;
}

int ompi_instance_dyn_v2c_psetop(ompi_instance_t * instance, ompi_info_t **setop_info, size_t ninfo, int *flag){
    ompi_instance_rc_op_handle_t * rc_op_handle;
    ompi_info_t **result_infos;
    pmix_info_t *info, *results;
    size_t nresults, n, k, n_result_infos;
    int rc;

    *flag = 0;

    if(OMPI_SUCCESS != (rc = rc_op_handle_from_info(setop_info, ninfo, &rc_op_handle))){
        return rc;
    }

    PMIX_INFO_CREATE(info, 1);
    if(OMPI_SUCCESS != (rc = rc_op_handle_serialize(rc_op_handle, info))){
        rc_op_handle_free(&rc_op_handle);
        return rc;
    }

    if(OMPI_SUCCESS != (rc = rc_op_handle_free(&rc_op_handle))){
        PMIX_INFO_FREE(info, 1);
        return rc;
    }

    if(OMPI_SUCCESS != (rc = PMIx_Allocation_request(MPI_ALLOC_SET_REQUEST, info, 1, &results, &nresults))){
        PMIX_INFO_FREE(info, 1);
        return OMPI_SUCCESS;
    }

    PMIX_INFO_FREE(info, 1);

    for(n = 0; n < nresults; n++){
        if(PMIX_CHECK_KEY(&results[n], "mpi.rc_op_handle")){
            if(OMPI_SUCCESS != (rc = rc_op_handle_deserialize(&results[n], &rc_op_handle))){
                PMIX_INFO_FREE(results, ninfo);
                return rc;
            }

            if(OMPI_SUCCESS != (rc = rc_op_handle_to_info(rc_op_handle, &result_infos, &n_result_infos))){
                PMIX_INFO_FREE(results, ninfo);
                return rc;
            }

            for(k = 0; k < ninfo; k++){
                ompi_info_free(&setop_info[k]);
                setop_info[k] = result_infos[k];
            }

            free(result_infos);
            *flag = 1;
            return OMPI_SUCCESS;
        }
    }

    return OMPI_ERR_REQUEST;
}

int ompi_instance_dyn_v2c_psetop_nb(ompi_instance_t * instance, ompi_info_t **info, size_t ninfo, int *flag, ompi_request_t **request){

    return OMPI_ERR_NOT_IMPLEMENTED;

}

int ompi_instance_dyn_finalize_psetop(ompi_instance_t *instance, char *pset_name){
    ompi_mpi_instance_pset_t *pset;
    int rc;

    if(NULL == (pset = get_pset_by_name(pset_name))){
        if(OMPI_SUCCESS != (rc = refresh_pmix_psets(PMIX_PSET_NAMES))){
            return rc;
        }

        if(NULL == (pset = get_pset_by_name(pset_name))){
            return OMPI_ERR_NOT_FOUND;
        }
    }
        
    bool non_default = true;
    pmix_info_t *event_info;
    PMIX_INFO_CREATE(event_info, 2);
    (void)snprintf(event_info[0].key, PMIX_MAX_KEYLEN, "%s", PMIX_EVENT_NON_DEFAULT);
    PMIX_VALUE_LOAD(&event_info[0].value, &non_default, PMIX_BOOL);
    (void)snprintf(event_info[1].key, PMIX_MAX_KEYLEN, "%s", PMIX_PSET_NAME);
    PMIX_VALUE_LOAD(&event_info[1].value, pset->name, PMIX_STRING);
    PMIx_Notify_event(PMIX_RC_FINALIZED, NULL, PMIX_RANGE_RM, event_info, 2, NULL, NULL);
    
    PMIX_INFO_FREE(event_info, 2);


    //ompi_instance_clear_rc_cache(pset->name);

    return OMPI_SUCCESS;
}

/* TODO: Triple pointer
 * Collectively query the runtime for available resource changes given either the delta PSet or the associated PSet.
 * The returned info is guranteed to be equal for all processes in the collective PSet
 */
int ompi_instance_get_res_change_collective(ompi_instance_t *instance, char *coll_pset_name, char *input_name, ompi_psetop_type_t *type, char ***output_names, size_t *noutputs, int *incl, ompi_rc_status_t *status, opal_info_t **info_used, bool get_by_delta_name){
    int rc;
    size_t n_coll_procs;
    pmix_proc_t *coll_procs;
    opal_process_name_t *opal_coll_procs;
    ompi_mpi_instance_pset_t *pset_ptr;
    
    /* TODO!!*/
    size_t dummy_noutput;

    if(0 == strcmp(coll_pset_name, "mpi://SELF")){
        rc = get_res_change_info(input_name, type, output_names, &dummy_noutput, incl, status, info_used, get_by_delta_name);
    }else{

        if(NULL == (pset_ptr = get_pset_by_name(coll_pset_name))){
            return OMPI_ERR_NOT_FOUND;
        }

        rc = get_pset_membership(pset_ptr->name, &opal_coll_procs, &n_coll_procs);
        if(rc != OMPI_SUCCESS){
            return rc;
        }
        
        PMIX_PROC_CREATE(coll_procs, n_coll_procs);
        opal_pmix_proc_array_conv(opal_coll_procs, &coll_procs, n_coll_procs);

        ompi_instance_free_pset_membership(coll_pset_name);

        if(NULL == (pset_ptr = get_pset_by_name(input_name))){
            free(coll_procs);
            return OMPI_ERR_NOT_FOUND;
        }
        
        rc = get_res_change_info_collective(coll_procs, n_coll_procs, pset_ptr->name, type, output_names, noutputs, incl, status, info_used, get_by_delta_name);

        free(coll_procs);
    }
    
    return rc;
}

int ompi_instance_pset_op(ompi_instance_t *session, int op, char **input_sets, int ninput, char *** output_sets, int *noutput, ompi_info_t *info){
    pmix_status_t rc;
    pmix_info_t *pmix_info, *results;
    pmix_data_array_t darray_in, darray_out;
    pmix_value_t *values;
    size_t n, k, ninfo, nresults;
    bool output_provided;
    int ret;
    ompi_mpi_instance_pset_t *pset_ptr;

    refresh_pmix_psets(PMIX_QUERY_PSET_NAMES);

    PMIX_DATA_ARRAY_CONSTRUCT(&darray_in, (size_t) ninput, PMIX_VALUE);
    values = (pmix_value_t *) darray_in.array;
    for(n = 0; n < (size_t) ninput; n++){
        if(NULL == (pset_ptr = get_pset_by_name(input_sets[n]))){
            PMIX_DATA_ARRAY_DESTRUCT(&darray_in);
            return OMPI_ERR_NOT_FOUND;
        }
        PMIX_VALUE_LOAD(&values[n], pset_ptr->name, PMIX_STRING);
    }

    PMIX_DATA_ARRAY_CONSTRUCT(&darray_out, (size_t) *noutput, PMIX_VALUE);
    values = (pmix_value_t *) darray_out.array;
    for(n = 0; n < (size_t) *noutput; n++){
        PMIX_VALUE_LOAD(&values[n], (*output_sets)[n], PMIX_STRING);
    }

    ninfo = 2;
    PMIX_INFO_CREATE(pmix_info, ninfo);
    PMIX_INFO_LOAD(&(pmix_info[0]), PMIX_PSETOP_INPUT, &darray_in, PMIX_DATA_ARRAY);
    PMIX_INFO_LOAD(&(pmix_info[1]), PMIX_PSETOP_OUTPUT, &darray_out, PMIX_DATA_ARRAY);

    PMIX_DATA_ARRAY_DESTRUCT(&darray_in);
    PMIX_DATA_ARRAY_DESTRUCT(&darray_out);

    /*
     * TODO: need to handle this better
     */
    if (PMIX_SUCCESS != (rc = PMIx_Pset_Op_request(op,
                                            pmix_info, ninfo, 
                                            &results, &nresults))) {
        PMIX_INFO_FREE(pmix_info, ninfo);                                             
        ret = opal_pmix_convert_status(rc);
        printf("PMIx_Pset_op_request failed with %d\n", ret);
        return ompi_instance_print_error ("PMIx_Fence_nb() failed", ret);
    }

    for(n=0; n < nresults; n++){
        if(PMIX_CHECK_KEY(&results[n], PMIX_PSETOP_OUTPUT)){
            values = (pmix_value_t *) results[n].value.data.darray->array;
            if(!(output_provided = noutput == 0)){
                *noutput = results[n].value.data.darray->size;
                *output_sets = malloc(*noutput * sizeof(char *));
            }

            for(k = 0; k < (size_t) *noutput; k++){
                if(output_provided){
                    free((*output_sets)[k]);
                }
                (*output_sets)[k] = strdup(values[k].data.string);
            }
        }
    }

    PMIX_INFO_FREE(pmix_info, ninfo);
    PMIX_INFO_FREE(results, nresults);

    return OMPI_SUCCESS;
}

int ompi_instance_set_pset_info(ompi_instance_t *instance, char *pset_name, ompi_info_t *info){
    
    int n, nkeys, rc, val_len, flag;
    pmix_value_t *values;
    opal_cstring_t *key, *value;
    char **keys = NULL;
    ompi_mpi_instance_pset_t *pset_ptr;

    refresh_pmix_psets(PMIX_QUERY_PSET_NAMES);
    if(NULL == (pset_ptr = get_pset_by_name(pset_name))){
        /* The specified PSet does not exist */
        return OMPI_ERR_NOT_FOUND;
    }

    if(OMPI_SUCCESS != (rc = ompi_info_get_nkeys(info, &nkeys)) || 1 > nkeys ){
        return rc;
    }

    PMIX_VALUE_CREATE(values, nkeys);
    keys = malloc( nkeys * sizeof(char *) );
    for(n = 0; n < nkeys; n++){
        if(OMPI_SUCCESS != (rc = ompi_info_get_nthkey(info, n, &key))){
            goto CLEANUP;
        }

        keys[n] = strdup(key->string);
        OBJ_RELEASE(key);

        if(OMPI_SUCCESS != (rc = ompi_info_get_valuelen(info, key->string, &val_len, &flag))){
            goto CLEANUP;
        }

        if(!flag){
            rc = OMPI_ERR_BAD_PARAM;
            goto CLEANUP;
        }

        if(OMPI_SUCCESS != (rc = ompi_info_get(info, key->string, &value, &flag))){
            goto CLEANUP;
        }

        if(!flag){
            rc = OMPI_ERR_BAD_PARAM;
            goto CLEANUP;
        }

        PMIX_VALUE_LOAD(&values[n], value->string, PMIX_STRING);
        OBJ_RELEASE(value);

    }

    rc = opal_pmix_publish_pset_info(keys, values, nkeys, pset_ptr->name);

CLEANUP:
    PMIX_VALUE_FREE(values, (size_t) nkeys);
    if(NULL != keys){
        for(n = 0; n < nkeys; n++){
            if(NULL != keys[n]){
                free(keys[n]);
            }
        }
        free(keys);
    }
    return rc;    
}

int ompi_instance_pset_barrier(char **pset_names, int num_psets, ompi_info_t *info){
    return ompi_instance_pset_fence_multiple(pset_names, num_psets, info);
}

int ompi_instance_pset_barrier_nb(char ** pset_names, int num_psets, ompi_info_t *info, ompi_request_t **request){
    fence_results *f_results = malloc(sizeof(v2b_psetop_results));

    ompi_instance_nb_req_create(request);
    nb_chain_info *chain_info = &f_results->chain_info;

    chain_info->func = PSET_FENCE;
    chain_info->nstages = 2;
    chain_info->cur_stage = 0;
    chain_info->stages = malloc(5 * sizeof(nb_chain_stage));
    chain_info->stages[0] = FENCE_STAGE;
    chain_info->stages[1] = LAST_STAGE;
    chain_info->req = *request;

    return pset_fence_multiple_nb(pset_names, num_psets, info, pmix_op_cb_nb, f_results);
}


int ompi_instance_get_num_psets (ompi_instance_t *instance, int *npset_names)
{
    int rc;
    if(OMPI_SUCCESS != ( rc = refresh_pmix_psets (PMIX_QUERY_NUM_PSETS))){
        return rc;
    }
    *npset_names = get_num_builtin_psets() + get_num_pmix_psets();

    return OMPI_SUCCESS;
}

/* TODO: Search in list*/
int ompi_instance_get_nth_pset (ompi_instance_t *instance, int n, int *len, char *pset_name)
{
    int rc;
    if (n >= (int) (get_num_builtin_psets() + get_num_pmix_psets())) {
        if(OMPI_SUCCESS != ( rc = refresh_pmix_psets (PMIX_QUERY_PSET_NAMES))){
            return rc;
        }
    }

    if ((size_t) n >= (get_num_builtin_psets() + get_num_pmix_psets()) || n < 0) {
        return OMPI_ERR_BAD_PARAM;
    }


    if (0 == *len) {
        *len = get_nth_pset_name_length(n);
        return OMPI_SUCCESS;
    }

    get_nth_pset_name(n, pset_name, *len);

    return OMPI_SUCCESS;
}

static int ompi_instance_group_pmix_pset (ompi_instance_t *instance, const char *pset_name, ompi_group_t **group_out)
{
    pmix_status_t rc = OPAL_ERR_NOT_FOUND;
    ompi_group_t *group;
    size_t size = 0;
    opal_process_name_t *pset_members = NULL;
    size_t pset_nmembers = 0;

    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 100000000;

    while (pset_members == NULL){
        if(OPAL_SUCCESS != (rc = get_pset_membership((char *) pset_name, &pset_members, &pset_nmembers))){
            return OPAL_ERR_BAD_PARAM;
        }
        if(pset_members == NULL){
            printf("Pset membership not yet defined. Need to sleep.");
            nanosleep(&ts, NULL);
        }
    }
    
    opal_mutex_lock (&tracking_structures_lock);
    if(PMIX_SUCCESS != rc){
        opal_mutex_unlock (&tracking_structures_lock);
        if(PMIX_ERR_NOT_FOUND == rc){
            return MPI_ERR_ARG;
        }
        return MPI_ERR_INTERN;
    }
    group = ompi_group_allocate (NULL, pset_nmembers);
    if (OPAL_UNLIKELY(NULL == group)) {
         opal_mutex_unlock (&tracking_structures_lock);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    for (size_t i = 0 ; i < pset_nmembers ; ++i) {
        opal_process_name_t name = pset_members[i];
        /* look for existing ompi_proc_t that matches this name */
        group->grp_proc_pointers[size] = (ompi_proc_t *) ompi_proc_lookup (name);
        if (NULL == group->grp_proc_pointers[size]) {
            /* set sentinel value */
            group->grp_proc_pointers[size] = (ompi_proc_t *) ompi_proc_name_to_sentinel (name);
        } else {
            OBJ_RETAIN (group->grp_proc_pointers[size]);
        }  
        ++size;
    }
    

    ompi_set_group_rank (group, ompi_proc_local());

    group->grp_instance = instance;

    *group_out = group;
    ompi_instance_free_pset_membership((char *) pset_name);
    opal_mutex_unlock (&tracking_structures_lock);
    return OMPI_SUCCESS;
}

static int ompi_instance_group_world (ompi_instance_t *instance, ompi_group_t **group_out)
{
    pmix_status_t rc;
    ompi_mpi_instance_pset_t *pset_ptr;
    char *pmix_pset;

    /* Get the locally stored alias PSET for mpi://WORLD */
    pset_ptr = get_pset_by_name("mpi://WORLD");

    /* If alias is not stored locally, try to get the launch Pset from the RTE. This should not happen but we nethertheless provide this fallback */
    if(NULL != pset_ptr){
        pmix_pset = strdup(pset_ptr->name);       
    }else{
        rc = ompi_instance_get_launch_pset(&pmix_pset, &opal_process_info.myprocid);
        if(PMIX_SUCCESS != rc){
            return rc;
        }
    }

    ompi_instance_group_pmix_pset(instance, pmix_pset, group_out);
    free(pmix_pset);

    return rc;
}

//static int ompi_instance_group_world (ompi_instance_t *instance, ompi_group_t **group_out)
//{
//    ompi_group_t *group;
//    size_t size;
//
//    size = ompi_process_info.num_procs;
//
//    group = ompi_group_allocate (NULL,size);
//    if (OPAL_UNLIKELY(NULL == group)) {
//        return OMPI_ERR_OUT_OF_RESOURCE;
//    }
//
//    for (size_t i = 0 ; i < size ; ++i) {
//        opal_process_name_t name = {.vpid = i, .jobid = OMPI_PROC_MY_NAME->jobid};
//        /* look for existing ompi_proc_t that matches this name */
//        group->grp_proc_pointers[i] = (ompi_proc_t *) ompi_proc_lookup (name);
//        if (NULL == group->grp_proc_pointers[i]) {
//            /* set sentinel value */
//            group->grp_proc_pointers[i] = (ompi_proc_t *) ompi_proc_name_to_sentinel (name);
//        } else {
//            OBJ_RETAIN (group->grp_proc_pointers[i]);
//        }
//    }
//
//    ompi_set_group_rank (group, ompi_proc_local());
//
//    group->grp_instance = instance;
//
//    *group_out = group;
//    return OMPI_SUCCESS;
//}


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

    group = ompi_group_allocate (NULL,size);
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

/* TODO: Need to introduce aliasing */
int ompi_group_from_pset (ompi_instance_t *instance, const char *pset_name, ompi_group_t **group_out)
{
    if (NULL == group_out) {
        return OMPI_ERR_BAD_PARAM;
    }

    if (0 == strncmp (pset_name, "mpi://", 6)) {
        pset_name += 6;
        if (0 == strcasecmp (pset_name, "WORLD")) {
            return ompi_instance_group_world (instance, group_out);
        }
        if (0 == strcasecmp (pset_name, "SELF")) {
            return ompi_instance_group_self (instance, group_out);
        }
    }

    if (0 == strncmp (pset_name, "mpix://", 7)) {
        pset_name += 7;
        if (0 == strcasecmp (pset_name, "SHARED")) {
            return ompi_instance_group_shared (instance, group_out);
        }
    }

    return ompi_instance_group_pmix_pset (instance, pset_name, group_out);
}

int ompi_instance_get_pset_info_by_keys (ompi_instance_t *instance, const char *pset_name, char **keys, int nkeys, int wait, opal_info_t **info_used){
    
    ompi_info_t *info = NULL;
    ompi_mpi_instance_pset_t *pset_ptr;
    pmix_info_t *pmix_info = NULL, *results = NULL, *results_info = NULL;
    ompi_psetop_type_t op_type;
    pmix_proc_t *pset_members;
    size_t ninfo = 0, nresults = 0, _nkeys = 0, n, k, i, j, num_mpi_keys = 0, pset_size, nresult_infos = 0;
    char **_keys;
    char *mpi_value;
    int mpi_val_length, rc = OMPI_SUCCESS;
    pmix_query_t query;

    bool refresh = true, b_wait = (1 == wait);

    refresh_pmix_psets(PMIX_QUERY_PSET_NAMES);
    if(NULL == (pset_ptr = get_pset_by_name((char *) pset_name))){
        return OMPI_ERR_NOT_FOUND;
    }

    *info_used = (opal_info_t *) MPI_INFO_NULL;

    if(nkeys < 0){
        return OMPI_ERR_BAD_PARAM;
    }

    info = ompi_info_allocate ();

    ninfo = 1 + wait;
    PMIX_INFO_CREATE(pmix_info, ninfo);
    PMIX_INFO_LOAD(&pmix_info[0], PMIX_PSET_NAME, pset_ptr->name, PMIX_STRING);
    if(wait){
        PMIX_INFO_LOAD(&pmix_info[1], PMIX_WAIT, &b_wait, PMIX_BOOL);
    }
    if(0 == nkeys || NULL == keys){
        _nkeys = 1;
        _keys = malloc(sizeof(char *));
        _keys[0] = strdup(PMIX_PSET_INFO);
    }else{
        _keys = malloc(nkeys * sizeof(char *));
        for(n = 0; n < (size_t) nkeys; n++){
            if(0 == strncmp(keys[n], "mpi_", 4)){
                num_mpi_keys++;
                continue;
            }
            _keys[_nkeys] = strdup(keys[n]);
            _nkeys++;
        }
    }


    if(num_mpi_keys != (size_t) nkeys){

        if(OMPI_SUCCESS != (rc = opal_pmix_lookup_pset_info(_keys, _nkeys, pmix_info, ninfo, pset_ptr->name, &results, &nresults))){
            ompi_info_free(&info);
            goto CLEANUP;
        }

        /* For now we only support string values */
        for(n = 0; n < nresults; n++){
            if(results[n].value.type != PMIX_STRING){
                continue;
            }
            rc = ompi_info_set(info, results[n].key, results[n].value.data.string);
            if(rc != OMPI_SUCCESS){
                printf("error in ompi_info_set\n");
                ompi_info_free(&info);
                goto CLEANUP;
            }
        }
    }

    /* Now handle the MPI specific attributes */
    /* TODO: */
    if(0 < num_mpi_keys){

        for(n = 0; n < (size_t) nkeys; n++){

            /* "mpi_size"*/
            if(0 == strcmp(keys[n], "mpi_size")){
                if(OMPI_SUCCESS != (rc = get_pset_size(pset_ptr->name, &pset_size))){
                    /* "mpi_size" is madatory, so this is an error */
                    ompi_info_free(&info);
                    goto CLEANUP;
                }

                mpi_val_length = snprintf(NULL, 0, "%zu", pset_size);
                mpi_value = malloc(mpi_val_length + 1);
                sprintf(mpi_value, "%zu", pset_size);
                if(OMPI_SUCCESS != (rc = opal_info_set(&info->super, "mpi_size", mpi_value))){
                    free(mpi_value);
                    ompi_info_free(&info);
                    goto CLEANUP;
                }

                free(mpi_value);

            }else if (0 == strncmp(keys[n], "mpi_", 4) && !OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_INIT)){
                PMIX_QUERY_CONSTRUCT(&query);
                PMIX_INFO_CREATE(query.qualifiers, 2);
                query.nqual = 2;
                PMIX_INFO_LOAD(&query.qualifiers[0], PMIX_QUERY_REFRESH_CACHE, &refresh, PMIX_BOOL);
                PMIX_INFO_LOAD(&query.qualifiers[1], PMIX_PSET_NAME, pset_ptr->name, PMIX_STRING);
                PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSET_MEMBERSHIP);
                PMIX_ARGV_APPEND(rc, query.keys, PMIX_QUERY_PSET_SOURCE_OP);

                if(PMIX_SUCCESS != (rc = PMIx_Query_info(&query, 1, &results, &nresults))){
                    return rc;
                }
                OMPI_PSET_FLAG_SET(pset_ptr, OMPI_PSET_FLAG_INIT);

                for(k = 0; k < nresults; k++){
                    if(0 == strcmp(results[k].key, PMIX_QUERY_RESULTS)){
                        
                        results_info = results[k].value.data.darray->array;
                        nresult_infos = results[k].value.data.darray->size;
                        if(nresult_infos >= 2){

                            ompi_instance_lock_rc_and_psets();

                            for (i = 0; i < nresult_infos; i++) {

                                if (0 == strcmp (results_info[i].key, PMIX_QUERY_PSET_MEMBERSHIP)) {
                                    pset_ptr->size = results_info[i].value.data.darray->size;
                                    pset_members = (pmix_proc_t *) results_info[i].value.data.darray->array;

                                    for(j = 0; j < pset_ptr->size; j++){
                                        if(PMIX_CHECK_PROCID(&pset_members[j], &opal_process_info.myprocid)){
                                            OMPI_PSET_FLAG_SET(pset_ptr, OMPI_PSET_FLAG_INCLUDED);
                                            if(j == 0){
                                                OMPI_PSET_FLAG_SET(pset_ptr, OMPI_PSET_FLAG_PRIMARY);
                                            }
                                        }
                                    }

                                } else if (0 == strcmp(results_info[i].key, PMIX_QUERY_PSET_SOURCE_OP)) {
                                    op_type = results_info[i].value.data.uint8;
                                    if(MPI_PSETOP_ADD == op_type){
                                        OMPI_PSET_FLAG_SET(pset_ptr, OMPI_PSET_FLAG_DYN);
                                    }
                                }
                            }
                            ompi_instance_unlock_rc_and_psets();
                        }
                    }
                }
            }
                
            /* ... */
            if(0 == strcmp(keys[n], "mpi_dyn")){
                rc = ompi_info_set(info, "mpi_dyn", OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_DYN) ? "True" : "False");
                if(rc != OMPI_SUCCESS){
                    printf("error in ompi_info_set\n");
                    ompi_info_free(&info);
                    goto CLEANUP;
                }
            }
            else if(0 == strcmp(keys[n], "mpi_included")){
                rc = ompi_info_set(info, "mpi_included", OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_INCLUDED) ? "True" : "False");
                if(rc != OMPI_SUCCESS){
                    printf("error in ompi_info_set\n");
                    ompi_info_free(&info);
                    goto CLEANUP;
                }
            }
            else if(0 == strcmp(keys[n], "mpi_primary")){
                rc = ompi_info_set(info, "mpi_primary", OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_PRIMARY) ? "True" : "False");
                if(rc != OMPI_SUCCESS){
                    printf("error in ompi_info_set\n");
                    ompi_info_free(&info);
                    goto CLEANUP;
                }
            }
        }
    }


    *info_used = &info->super;

    
CLEANUP:
    if(0 != ninfo){
        PMIX_INFO_FREE(pmix_info, ninfo);
    }
    if(0 != nresults){
        PMIX_INFO_FREE(results, nresults);
    }
    if(NULL != _keys){
        for(n = 0; n < _nkeys; n++){
            if(NULL != _keys[n]){
                free(_keys[n]);
            }
            free(_keys);
        }
    }
    return rc;

}

typedef struct _lookup_results{
    pmix_status_t status;
    pmix_info_t *results;
    size_t nresults;
}lookup_results_t;

void coll_lookup_cb( pmix_status_t status, pmix_info_t *info, size_t ninfo, void *cbdata, pmix_release_cbfunc_t release_fn, void *release_cbdata){
    size_t n;
    lookup_results_t *lookup_results = (lookup_results_t *) cbdata;
    
    lookup_results->status = status;
    lookup_results->nresults = ninfo;

    if(0 < ninfo){
        PMIX_INFO_CREATE(lookup_results->results, ninfo);
        for(n = 0; n < ninfo; n++){
            PMIX_INFO_XFER(&lookup_results->results[n], &info[n]);
        }
    }

    if(NULL != release_fn){
        release_fn(cbdata);
    }
    
}

int ompi_instance_get_pset_data (ompi_instance_t *instance, char *coll_pset, char *pset_name, char **keys, int nkeys, int wait, opal_info_t **info_used){
    
    ompi_info_t *info = NULL;
    ompi_mpi_instance_pset_t *pset_ptr, *coll_pset_ptr;
    pmix_info_t *pmix_info = NULL, *results = NULL;
    pmix_proc_t *pset_members = NULL;
    pmix_pdata_t *pdata;
    lookup_results_t lookup_results;
    size_t ninfo = 0, nresults = 0, _nkeys = 0, n, num_mpi_keys = 0, pset_size = 0;
    char **_keys;
    char *mpi_value;
    int mpi_val_length, rc = OMPI_SUCCESS;

    bool b_wait = (1 == wait);

    refresh_pmix_psets(PMIX_QUERY_PSET_NAMES);
    if(NULL == (pset_ptr = get_pset_by_name(pset_name))){
        return OMPI_ERR_NOT_FOUND;
    }


    *info_used = (opal_info_t *) MPI_INFO_NULL;

    if(nkeys < 0){
        return OMPI_ERR_BAD_PARAM;
    }

    info = ompi_info_allocate ();


    if(0 == nkeys || NULL == keys){
        _nkeys = 1;
        _keys = malloc(sizeof(char *));
        _keys[0] = strdup(PMIX_PSET_INFO);
    }else{
        _keys = malloc(nkeys * sizeof(char *));
        for(n = 0; n < (size_t) nkeys; n++){
            if(0 == strncmp(keys[n], "mpi_", 4)){
                num_mpi_keys++;
                continue;
            }
            _keys[_nkeys] = strdup(keys[n]);
            _nkeys++;
        }
    }

    /* Now handle the MPI specific attributes */
    /* TODO: */
    if(!OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_INIT)){
        if(OMPI_SUCCESS != (rc = pset_init_flags(pset_ptr->name))){
            ompi_info_free(&info);
            goto CLEANUP;
        }
    }
    if(0 < num_mpi_keys){

        for(n = 0; n < (size_t) nkeys; n++){

            /* "mpi_size"*/
            if(0 == strcmp(keys[n], "mpi_size")){
                if(OMPI_SUCCESS != (rc = get_pset_size(pset_ptr->name, &pset_size))){
                    /* "mpi_size" is madatory, so this is an error */
                    ompi_info_free(&info);
                    goto CLEANUP;
                }

                mpi_val_length = snprintf(NULL, 0, "%zu", pset_size);
                mpi_value = malloc(mpi_val_length + 1);
                sprintf(mpi_value, "%zu", pset_size);
                if(OMPI_SUCCESS != (rc = opal_info_set(&info->super, "mpi_size", mpi_value))){
                    free(mpi_value);
                    ompi_info_free(&info);
                    goto CLEANUP;
                }

                free(mpi_value);

            }else if(0 == strcmp(keys[n], "mpi_dyn")){
                rc = ompi_info_set(info, "mpi_dyn", OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_DYN) ? "True" : "False");
                if(rc != OMPI_SUCCESS){
                    printf("error in ompi_info_set\n");
                    ompi_info_free(&info);
                    goto CLEANUP;
                }
            }
            else if(0 == strcmp(keys[n], "mpi_included")){
                rc = ompi_info_set(info, "mpi_included", OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_INCLUDED) ? "True" : "False");
                if(rc != OMPI_SUCCESS){
                    printf("error in ompi_info_set\n");
                    ompi_info_free(&info);
                    goto CLEANUP;
                }
            }
            else if(0 == strcmp(keys[n], "mpi_primary")){
                rc = ompi_info_set(info, "mpi_primary", OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_PRIMARY) ? "True" : "False");
                if(rc != OMPI_SUCCESS){
                    printf("error in ompi_info_set\n");
                    ompi_info_free(&info);
                    goto CLEANUP;
                }
            }
        }
    }

    if(0 < _nkeys){

        if(NULL == (coll_pset_ptr = get_pset_by_name(coll_pset))){
            ompi_info_free(&info);
            goto CLEANUP;
        }

        if(!OMPI_PSET_FLAG_TEST(coll_pset_ptr, OMPI_PSET_FLAG_INIT)){
            if(OMPI_SUCCESS != (rc = pset_init_flags(coll_pset_ptr->name))){
                ompi_info_free(&info);
                goto CLEANUP;
            }
        }


        ninfo = 1 + wait;
        PMIX_INFO_CREATE(pmix_info, ninfo);
        PMIX_INFO_LOAD(&pmix_info[0], PMIX_PSET_NAME, pset_ptr->name, PMIX_STRING);
        if(wait){
            PMIX_INFO_LOAD(&pmix_info[1], PMIX_WAIT, &b_wait, PMIX_BOOL);
        }
        PMIX_PDATA_CREATE(pdata, _nkeys);
        for(n = 0; n < _nkeys; n++){
            PMIX_LOAD_KEY(&pdata[n], _keys[n]);
        }


        if(OMPI_PSET_FLAG_TEST(coll_pset_ptr, OMPI_PSET_FLAG_PRIMARY)){
            rc = opal_pmix_lookup_pset_info(_keys, _nkeys, &pmix_info[wait], wait, pset_ptr->name, &results, &nresults);
            if(0 != strcmp(coll_pset, "mpi://SELF")){
                if(OMPI_SUCCESS != (rc = get_pset_members(coll_pset, &pset_members, &pset_size))) {
                    ompi_info_free(&info);
                    goto CLEANUP;            
                }
                send_collective_data_lookup(pset_members, rc, pset_size, pdata, _nkeys, pmix_info, ninfo, results, nresults);
            }
            
            if(OMPI_SUCCESS != rc){
                ompi_info_free(&info);
                goto CLEANUP;
            }
        }else{
            if(OMPI_SUCCESS != (rc = get_pset_members(coll_pset, &pset_members, &pset_size))) {
                ompi_info_free(&info);
                goto CLEANUP;            
            }
            
            rc = recv_collective_data_lookup(pset_members, pset_size, pdata, _nkeys, pmix_info, ninfo, coll_lookup_cb, &lookup_results);
            if(OMPI_SUCCESS != rc || OMPI_SUCCESS != lookup_results.status){
                ompi_info_free(&info);
                goto CLEANUP;
            }
            results = lookup_results.results;
            nresults = lookup_results.nresults;
        }

        /* For now we only support string values */
        for(n = 0; n < nresults; n++){
            if(results[n].value.type != PMIX_STRING){
                continue;
            }
            rc = ompi_info_set(info, results[n].key, results[n].value.data.string);
            if(rc != OMPI_SUCCESS){
                printf("error in ompi_info_set\n");
                ompi_info_free(&info);
                goto CLEANUP;
            }
        }
    }


    *info_used = &info->super;

CLEANUP:
    if(0 != ninfo){
        PMIX_INFO_FREE(pmix_info, ninfo);
    }
    if(0 != nresults){
        PMIX_INFO_FREE(results, nresults);
    }
    if(0 != pset_size && NULL != pset_members){
        PMIX_PROC_FREE(pset_members, pset_size);
    }
    if(NULL != _keys){
        for(n = 0; n < _nkeys; n++){
            if(NULL != _keys[n]){
                free(_keys[n]);
            }
            free(_keys);
        }
    }
    return rc;
}



int ompi_instance_get_pset_data_nb (ompi_instance_t *instance, char *coll_pset, char *pset_name, char **keys, int nkeys, int wait, opal_info_t **info_used, ompi_request_t **req){
    
    ompi_info_t *info = NULL;
    ompi_mpi_instance_pset_t *pset_ptr, *coll_pset_ptr;
    pmix_info_t *pmix_info = NULL;
    pmix_proc_t *pset_members = NULL;
    pmix_pdata_t *pdata;
    pset_data_results *pdata_results;
    size_t ninfo = 0, _nkeys = 0, n, num_mpi_keys = 0, pset_size = 0;
    char **_keys;
    char ** argv = NULL;
    char *mpi_value;
    int mpi_val_length, rc = OMPI_SUCCESS;

    bool b_wait = (1 == wait);

    refresh_pmix_psets(PMIX_QUERY_PSET_NAMES);
    if(NULL == (pset_ptr = get_pset_by_name(pset_name))){
        return OMPI_ERR_NOT_FOUND;
    }

    *info_used = (opal_info_t *) MPI_INFO_NULL;

    if(nkeys < 0){
        return OMPI_ERR_BAD_PARAM;
    }

    info = ompi_info_allocate ();


    if(0 == nkeys || NULL == keys){
        _nkeys = 1;
        _keys = malloc(sizeof(char *));
        _keys[0] = strdup(PMIX_PSET_INFO);
    }else{
        _keys = malloc(nkeys * sizeof(char *));
        for(n = 0; n < (size_t) nkeys; n++){
            if(0 == strncmp(keys[n], "mpi_", 4)){
                num_mpi_keys++;
                continue;
            }
            _keys[_nkeys] = strdup(keys[n]);
            _nkeys++;
        }
    }

    /* Now handle the MPI specific attributes */
    /* TODO: */

    if(!OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_INIT)){
        if(OMPI_SUCCESS != (rc = pset_init_flags(pset_ptr->name))){
            ompi_info_free(&info);
            goto CLEANUP;
        }
    }

    if(0 < num_mpi_keys){

        for(n = 0; n < (size_t) nkeys; n++){

            /* "mpi_size"*/
            if(0 == strcmp(keys[n], "mpi_size")){
                if(OMPI_SUCCESS != (rc = get_pset_size(pset_ptr->name, &pset_size))){
                    /* "mpi_size" is madatory, so this is an error */
                    ompi_info_free(&info);
                    goto CLEANUP;
                }

                mpi_val_length = snprintf(NULL, 0, "%zu", pset_size);
                mpi_value = malloc(mpi_val_length + 1);
                sprintf(mpi_value, "%zu", pset_size);
                if(OMPI_SUCCESS != (rc = opal_info_set(&info->super, "mpi_size", mpi_value))){
                    free(mpi_value);
                    ompi_info_free(&info);
                    goto CLEANUP;
                }

                free(mpi_value);

            }else if(0 == strcmp(keys[n], "mpi_dyn")){
                rc = ompi_info_set(info, "mpi_dyn", OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_DYN) ? "True" : "False");
                if(rc != OMPI_SUCCESS){
                    printf("error in ompi_info_set\n");
                    ompi_info_free(&info);
                    goto CLEANUP;
                }
            }
            else if(0 == strcmp(keys[n], "mpi_included")){
                rc = ompi_info_set(info, "mpi_included", OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_INCLUDED) ? "True" : "False");
                if(rc != OMPI_SUCCESS){
                    printf("error in ompi_info_set\n");
                    ompi_info_free(&info);
                    goto CLEANUP;
                }
            }
            else if(0 == strcmp(keys[n], "mpi_primary")){
                rc = ompi_info_set(info, "mpi_primary", OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_PRIMARY) ? "True" : "False");
                if(rc != OMPI_SUCCESS){
                    printf("error in ompi_info_set\n");
                    ompi_info_free(&info);
                    goto CLEANUP;
                }
            }
        }
    }

    if(NULL == (coll_pset_ptr = get_pset_by_name(coll_pset))){
        ompi_info_free(&info);
        goto CLEANUP;  
    }
    
    if(!OMPI_PSET_FLAG_TEST(coll_pset_ptr, OMPI_PSET_FLAG_INIT)){
        if(OMPI_SUCCESS != (rc = pset_init_flags(coll_pset_ptr->name))){
            ompi_info_free(&info);
            goto CLEANUP;
        }
    }

    /* Get the procs in the collective PSet */
    if(OMPI_SUCCESS != (rc = get_pset_members(coll_pset_ptr->name, &pset_members, &pset_size))) {
        ompi_info_free(&info);
        goto CLEANUP;            
    }

    ompi_instance_nb_req_create(req);
    

    if(0 < _nkeys){

        pdata_results = (pset_data_results *) malloc(sizeof(pset_data_results));
        nb_chain_info *chain_info = &pdata_results->chain_info;
        chain_info->func = GET_PSET_DATA;
        chain_info->nstages = 2;
        chain_info->cur_stage = 0;
        chain_info->stages = malloc(2 * sizeof(nb_chain_stage));
        chain_info->stages[0] = LOOKUP_STAGE;
        chain_info->stages[1] = LAST_STAGE;
        chain_info->req = *req;
        pdata_results->coll_pset = coll_pset_ptr->name;
        pdata_results->coll_procs = pset_members;
        pdata_results->n_coll_procs = pset_size;
        pdata_results->nkeys = _nkeys;
        pdata_results->info = info;
        pdata_results->info_used = (ompi_info_t **) info_used;

        ninfo = 1 + wait;
        PMIX_INFO_CREATE(pmix_info, ninfo);
        PMIX_INFO_LOAD(&pmix_info[0], PMIX_PSET_NAME, pset_ptr->name, PMIX_STRING);
        if(wait){
            PMIX_INFO_LOAD(&pmix_info[1], PMIX_WAIT, &b_wait, PMIX_BOOL);
        }
        PMIX_PDATA_CREATE(pdata, _nkeys);
        for(n = 0; n < _nkeys; n++){
            PMIX_LOAD_KEY(&pdata[n], _keys[n]);
            PMIX_ARGV_APPEND(rc, argv, _keys[n]);
        }

        pdata_results->pdata = pdata;
        pdata_results->pmix_info = pmix_info;
        pdata_results->ninfo = ninfo;

        if(OMPI_PSET_FLAG_TEST(coll_pset_ptr, OMPI_PSET_FLAG_PRIMARY)){

            if(PMIX_SUCCESS != (rc = PMIx_Lookup_nb(argv, pmix_info, ninfo, pmix_lookup_cb_nb, (void *) pdata_results))){
                free(pdata_results);
                ompi_info_free(&info);
                goto CLEANUP;
            }
        }else{

            if(OMPI_SUCCESS != (rc = recv_collective_data_lookup_nb(pset_members, pset_size, pdata, _nkeys, pmix_info, ninfo, pmix_info_cb_nb, (void *) pdata_results))){
                free(pdata_results);
                ompi_info_free(&info);
                goto CLEANUP;
            }
        
        }
    }else{

        (*req)->req_status.MPI_ERROR = OMPI_SUCCESS;                    
        (*req)->req_complete = REQUEST_COMPLETED;
        (*req)->req_state = OMPI_REQUEST_INVALID;
        *info_used = &info->super;
    }

CLEANUP:
    if(OMPI_SUCCESS != rc && 0 != ninfo){
        PMIX_INFO_FREE(pmix_info, ninfo);
    }

    if(OMPI_SUCCESS != rc && 0 != pset_size && NULL != pset_members){
        PMIX_PROC_FREE(pset_members, pset_size);
    }
    if(NULL != _keys){
        for(n = 0; n < _nkeys; n++){
            if(NULL != _keys[n]){
                free(_keys[n]);
            }
            free(_keys);
        }
    }
    PMIX_ARGV_FREE(argv);
    return rc;
}

/* TODO: Need to get info from PMIx */
int ompi_instance_get_pset_info (ompi_instance_t *instance, const char *pset_name, opal_info_t **info_used)
{
    ompi_info_t *info = ompi_info_allocate ();
    ompi_mpi_instance_pset_t *pset_ptr;
    char tmp[16];
    size_t size = 0UL;
    int ret;

    *info_used = (opal_info_t *) MPI_INFO_NULL;

    if (OPAL_UNLIKELY(NULL == info)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    
    if(NULL == (pset_ptr = get_pset_by_name((char *) pset_name))){
        refresh_pmix_psets(PMIX_QUERY_PSET_NAMES);
        if(NULL == (pset_ptr = get_pset_by_name((char *) pset_name))){
            return OMPI_ERR_NOT_FOUND;
        }
    }


    if(!OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_INIT)){
        if(OMPI_SUCCESS != (ret = pset_init_flags(pset_ptr->name))){
            ompi_info_free(&info);
            return ret;
        }
    }

    if (0 == strcmp (pset_name, "mpi://self")) {
        size = 1;
    } else if (0 == strcmp (pset_name, "mpi://shared")) {
        size = ompi_process_info.num_local_peers + 1;
    }else{
         if(OMPI_SUCCESS != (ret = get_pset_size(pset_ptr->name, &size))){
            /* "mpi_size" is madatory, so this is an error */
            ompi_info_free(&info);
            return ret;
        }
    }
    

    snprintf (tmp, 16, "%" PRIsize_t, size);
    ret = opal_info_set (&info->super, MPI_INFO_KEY_SESSION_PSET_SIZE, tmp);
    if (OPAL_UNLIKELY(OPAL_SUCCESS != ret)) {
        ompi_info_free (&info);
        return ret;
    }

    ret = ompi_info_set(info, "mpi_dyn", OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_DYN) ? "True" : "False");
    if(ret != OMPI_SUCCESS){
        printf("error in ompi_info_set\n");
        ompi_info_free(&info);
        return ret;
    }

    ret = ompi_info_set(info, "mpi_included", OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_INCLUDED) ? "True" : "False");
    if(ret != OMPI_SUCCESS){
        printf("error in ompi_info_set\n");
        ompi_info_free(&info);
        return ret;
    }

    ret = ompi_info_set(info, "mpi_primary", OMPI_PSET_FLAG_TEST(pset_ptr, OMPI_PSET_FLAG_PRIMARY) ? "True" : "False");
    if(ret != OMPI_SUCCESS){
        printf("error in ompi_info_set\n");
        ompi_info_free(&info);
        return ret;
    }


    *info_used = &info->super;

    return OMPI_SUCCESS;
}

