/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2018      Triad National Security, LLC.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OMPI_INSTANCE_OP_HANDLE_H)
#define OMPI_INSTANCE_OP_HANDLE_H

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

#include "ompi/instance/instance_psets.h"
#include "ompi/instance/instance_res_changes.h"

#define PREDEFINED_RC_HANDLE_PAD 512

typedef struct opal_pmix_info_list_item_t{
    opal_list_item_t super;
    pmix_info_t info;
}opal_pmix_info_list_item_t;

struct ompi_instance_set_op_info_t{
    opal_list_item_t super;
    char ** input_names;
    size_t n_input_names;
    char ** output_names;
    size_t n_output_names;

    pmix_info_t *op_info;
    size_t n_op_info;

    void ** pset_info_lists; 
    size_t n_pset_info_lists;
};

typedef struct ompi_instance_set_op_info_t ompi_instance_set_op_info_t;

OBJ_CLASS_DECLARATION(ompi_instance_set_op_info_t);

struct ompi_instance_set_op_handle_t{
    opal_list_item_t super;
    ompi_psetop_type_t psetop;
    ompi_instance_set_op_info_t set_op_info;
};
typedef struct ompi_instance_set_op_handle_t ompi_instance_set_op_handle_t;

OBJ_CLASS_DECLARATION(ompi_instance_set_op_handle_t);

struct ompi_instance_rc_op_handle_t{
    opal_list_item_t super;
    ompi_rc_op_type_t rc_type;
    ompi_instance_set_op_info_t rc_op_info;
    opal_list_t set_ops;
};

typedef struct ompi_instance_rc_op_handle_t ompi_instance_rc_op_handle_t;

OBJ_CLASS_DECLARATION(ompi_instance_rc_op_handle_t);

struct ompi_predefined_rc_op_handle_t {
    ompi_instance_rc_op_handle_t rc_op_handle;
    char padding[PREDEFINED_RC_HANDLE_PAD - sizeof(ompi_instance_rc_op_handle_t)];
};
typedef struct ompi_predefined_rc_op_handle_t ompi_predefined_rc_op_handle_t;

OMPI_DECLSPEC extern ompi_predefined_rc_op_handle_t ompi_mpi_rc_op_handle_null;

int rc_op_handle_create(ompi_instance_rc_op_handle_t **rc_op_handle);
int rc_op_handle_add_op(ompi_rc_op_type_t rc_type, 
                            char **input_names, size_t n_input_names, 
                            char **output_names, size_t n_output_names, 
                            ompi_info_t *info, ompi_instance_rc_op_handle_t *rc_op_handle
                        );
int rc_op_handle_add_pset_infos(ompi_instance_rc_op_handle_t * rc_op_handle, char * pset_name, pmix_info_t * info, int ninfo);
int rc_op_handle_free(ompi_instance_rc_op_handle_t ** rc_op_handle);

int rc_op_handle_init_output(ompi_rc_op_type_t type, char ***output_names, size_t *noutput);

size_t rc_op_handle_get_num_ops(ompi_instance_rc_op_handle_t * rc_op_handle);
int rc_op_handle_get_num_output(ompi_instance_rc_op_handle_t * rc_op_handle, size_t op_index, size_t *num_output);
int rc_op_handle_get_ouput_name(ompi_instance_rc_op_handle_t * rc_op_handle, size_t op_index, size_t name_index, int *pset_len, char* pset_name);

int rc_op_handle_serialize(ompi_instance_rc_op_handle_t *rc_op_handle, pmix_info_t *info);

#endif /* !defined(OMPI_INSTANCE_OP_HANDLE_H) */
