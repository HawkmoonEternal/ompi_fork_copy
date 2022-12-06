/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2018      Triad National Security, LLC.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OMPI_INSTANCE_H)
#define OMPI_INSTANCE_H

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
#include "ompi/instance/instance_psets.h"
#include "ompi/instance/instance_res_changes.h"
#include "ompi/instance/instance_nb.h"
#include "ompi/instance/instance_collective.h"

#define MPI_ALL_ASYNC 1

struct ompi_group_t;

struct ompi_instance_t {
    opal_infosubscriber_t  super;
    opal_mutex_t           s_lock;
    int                    i_thread_level;
    char                   i_name[MPI_MAX_OBJECT_NAME];
    uint32_t               i_flags;

    /* Attributes */
    opal_hash_table_t     *i_keyhash;

    /* index in Fortran <-> C translation array (for when I get around
     * to implementing fortran support-- UGH) */
    int                    i_f_to_c_index;

    ompi_errhandler_t     *error_handler;
    ompi_errhandler_type_t errhandler_type;
};

typedef struct ompi_instance_t ompi_instance_t;

OBJ_CLASS_DECLARATION(ompi_instance_t);

/* Define for the preallocated size of the predefined handle.
 * Note that we are using a pointer type as the base memory chunk
 * size so when the bitness changes the size of the handle changes.
 * This is done so we don't end up needing a structure that is
 * incredibly larger than necessary because of the bitness.
 *
 * This padding mechanism works as a (likely) compile time check for when the
 * size of the ompi_communicator_t exceeds the predetermined size of the
 * ompi_predefined_communicator_t. It also allows us to change the size of
 * the ompi_communicator_t without impacting the size of the
 * ompi_predefined_communicator_t structure for some number of additions.
 *
 * Note: we used to define the PAD as a multiple of sizeof(void*).
 * However, this makes a different size PAD, depending on
 * sizeof(void*).  In some cases
 * (https://github.com/open-mpi/ompi/issues/3610), 32 bit builds can
 * run out of space when 64 bit builds are still ok.  So we changed to
 * use just a naked byte size.  As a rule of thumb, however, the size
 * should probably still be a multiple of 8 so that it has the
 * possibility of being nicely aligned.
 *
 * As an example:
 * If the size of ompi_communicator_t is less than the size of the _PAD then
 * the _PAD ensures that the size of the ompi_predefined_communicator_t is
 * whatever size is defined below in the _PAD macro.
 * However, if the size of the ompi_communicator_t grows larger than the _PAD
 * (say by adding a few more function pointers to the structure) then the
 * 'padding' variable will be initialized to a large number often triggering
 * a 'array is too large' compile time error. This signals two things:
 * 1) That the _PAD should be increased.
 * 2) That users need to be made aware of the size change for the
 *    ompi_predefined_communicator_t structure.
 *
 * Q: So you just made a change to communicator structure, do you need to adjust
 * the PREDEFINED_COMMUNICATOR_PAD macro?
 * A: Most likely not, but it would be good to check.
 */
#define PREDEFINED_INSTANCE_PAD 512

struct ompi_predefined_instance_t {
    ompi_instance_t instance;
    char padding[PREDEFINED_INSTANCE_PAD - sizeof(ompi_instance_t)];
};
typedef struct ompi_predefined_instance_t ompi_predefined_instance_t;

/**
 * @brief NULL instance
 */
OMPI_DECLSPEC extern ompi_predefined_instance_t ompi_mpi_instance_null;

OMPI_DECLSPEC extern opal_pointer_array_t ompi_instance_f_to_c_table;

extern ompi_instance_t *ompi_mpi_instance_default;

/**
 * @brief Bring up the bare minimum infrastructure to support pre-session_init functions.
 *
 * List of subsystems initialized:
 *  - OPAL (including class system)
 *  - Error handlers
 *  - MPI Info
 */
int ompi_mpi_instance_retain (void);

/**
 * @brief Release (and possibly teardown) pre-session_init infrastructure.
 */
void ompi_mpi_instance_release (void);

/**
 * @brief Create a new MPI instance
 *
 * @param[in]    ts_level  thread support level (see mpi.h)
 * @param[in]    info      info object
 * @param[in]    errhander errhandler to set on the instance
 */
OMPI_DECLSPEC int ompi_mpi_instance_init (int ts_level, opal_info_t *info, ompi_errhandler_t *errhandler,
                                          ompi_instance_t **instance, int argc, char **argv);

/**
 * @brief Destroy an MPI instance and set it to MPI_SESSION_NULL
 */
OMPI_DECLSPEC int ompi_mpi_instance_finalize (ompi_instance_t **instance);


/**
 * @brief Add a function to the finalize chain. Note this function will be called
 *        when the last instance has been destroyed.
 */
#define ompi_mpi_instance_append_finalize opal_finalize_register_cleanup

/**
 * @brief Get an MPI group object for a named process set.
 *
 * @param[in] instance   MPI instance (session)
 * @param[in] pset_name  Name of process set (includes mpi://world, mpi://self)
 * @param[out group_out  New MPI group
 */
OMPI_DECLSPEC int ompi_group_from_pset (ompi_instance_t *instance, const char *pset_name, struct ompi_group_t **group_out);

OMPI_DECLSPEC int ompi_instance_get_num_psets (ompi_instance_t *instance, int *npset_names);
OMPI_DECLSPEC int ompi_instance_get_nth_pset (ompi_instance_t *instance, int n, int *len, char *pset_name);
OMPI_DECLSPEC int ompi_instance_get_pset_info (ompi_instance_t *instance, const char *pset_name, opal_info_t **info_used);

/* Dyn Sessions */

OMPI_DECLSPEC int ompi_instance_request_res_change(MPI_Session session, int delta, char *delta_pset, ompi_rc_op_type_t rc_type, MPI_Info *info);

OMPI_DECLSPEC int ompi_instance_get_res_change(ompi_instance_t *instance,char *pset_name, ompi_rc_op_type_t *type, char ***delta_psets, size_t *ndelta_psets, int *incl, ompi_rc_status_t *status, opal_info_t **info_used, bool return_info);

OMPI_DECLSPEC int ompi_instance_pset_create_op(ompi_instance_t *instance, const char *pset1, const char *pset2, char *pref_name, char *pset_result, ompi_psetop_type_t op);

OMPI_DECLSPEC int ompi_instance_accept_res_change(ompi_instance_t *instance, opal_info_t **info_used, char *delta_pset, char* new_pset, bool blocking);
OMPI_DECLSPEC int ompi_instance_confirm_res_change(ompi_instance_t *instance, opal_info_t **info_used, char *delta_pset, char **new_pset);

OMPI_DECLSPEC int ompi_instance_integrate_res_change(ompi_instance_t *instance, char *delta_pset, char *pset_buf, int provider, int *terminate);
OMPI_DECLSPEC int ompi_instance_integrate_res_change_nb(ompi_instance_t *instance, char *delta_pset, char *pset_buf, int provider, int *terminate, ompi_request_t **request);


OMPI_DECLSPEC int ompi_instance_pset_op(ompi_instance_t *session, int op, char **input_sets, int ninput, char *** output_sets, int *noutput, ompi_info_t *info);
OMPI_DECLSPEC int ompi_instance_rc_op_handle_create(  ompi_instance_t *instance, ompi_instance_rc_op_handle_t **rc_op_handle);
OMPI_DECLSPEC int ompi_instance_rc_op_handle_add_op(    ompi_instance_t *instance, ompi_rc_op_type_t rc_type, 
                                                        char **input_names, size_t n_input_names, 
                                                        char **ouput_names, size_t n_output_names, 
                                                        ompi_info_t *info, ompi_instance_rc_op_handle_t *rc_op_handle);
OMPI_DECLSPEC int ompi_instance_rc_op_handle_add_pset_info(ompi_instance_t *instance, ompi_instance_rc_op_handle_t * rc_op_handle, char *pset_name, ompi_info_t * info);
OMPI_DECLSPEC int ompi_instance_rc_op_handle_free(ompi_instance_t * instance, ompi_instance_rc_op_handle_t ** rc_op_handle);
OMPI_DECLSPEC int ompi_instance_rc_op_handle_get_num_ops(ompi_instance_t * instance, ompi_instance_rc_op_handle_t * rc_op_handle, size_t *num_ops);
OMPI_DECLSPEC int ompi_instance_rc_op_handle_get_num_output(ompi_instance_t * instance, ompi_instance_rc_op_handle_t * rc_op_handle, size_t op_index, size_t *num_output);
OMPI_DECLSPEC int ompi_instance_rc_op_handle_get_ouput_name(ompi_instance_t * instance, ompi_instance_rc_op_handle_t *rc_op_handle, size_t op_index, size_t name_index, int *pset_len, char *pset_name);
OMPI_DECLSPEC int ompi_instance_request_res_changes_v23(ompi_instance_t * instance, ompi_instance_rc_op_handle_t * rc_op_handle);
OMPI_DECLSPEC int ompi_instance_request_res_changes_nb_v23(ompi_instance_t * instance, ompi_instance_rc_op_handle_t * rc_op_handle, ompi_request_t **request);
OMPI_DECLSPEC int ompi_instance_get_res_change_collective(  ompi_instance_t *instance, char *coll_pset_name, char *input_name,
                                                            ompi_rc_op_type_t *type, char ***output_names, size_t *nouputs, int *incl, ompi_rc_status_t *status, 
                                                            opal_info_t **info_used, bool get_by_delta_name);

OMPI_DECLSPEC int ompi_instance_set_pset_info(ompi_instance_t *instance, char *pset_name, opal_info_t *info);
OMPI_DECLSPEC int ompi_instance_get_pset_info_by_keys (ompi_instance_t *instance, const char *pset_name, char **keys, int nkeys, int wait, opal_info_t **info_used);

OMPI_DECLSPEC int ompi_instance_pset_barrier(char **pset_names, int num_psets, ompi_info_t *info);
OMPI_DECLSPEC int ompi_instance_pset_barrier_nb(char ** pset_names, int num_psets, ompi_info_t *info, ompi_request_t **request);

extern ompi_psetop_type_t MPI_OMPI_CONV_PSET_OP(int mpi_rc_op);

pmix_proc_t ompi_intance_get_pmixid(void);

static void ompi_instance_refresh_pmix_psets (const char *key);
int ompi_mpi_instance_refresh (ompi_instance_t *instance, opal_info_t *info, char *pset_name, ompi_rc_op_type_t rc_type, char *result_pset, bool root);
void ompi_instance_clear_rc_cache(char *delta_pset);
/**
 * @brief current number of active instances
 */
extern opal_atomic_int32_t ompi_instance_count;

#endif /* !defined(OMPI_INSTANCE_H) */
