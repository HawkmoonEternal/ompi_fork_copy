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

#include "ompi/instance/instance_op_handle.h"

#pragma region op_info
/* Set OP info */ 
static void ompi_instance_set_op_info_constructor(ompi_instance_set_op_info_t *op_info){

    op_info->input_names = NULL;
    op_info->output_names = NULL;
    op_info->op_info = NULL;
    op_info->pset_info_lists = NULL;

    op_info->n_input_names = 0;
    op_info->n_output_names = 0;
    op_info->n_op_info = 0;
    op_info->n_pset_info_lists = 0;
}

static void ompi_instance_set_op_info_destructor(ompi_instance_set_op_info_t *op_info){

    if(NULL != op_info->input_names){
        for(int n = 0; n < op_info->n_input_names; n++){
            free(op_info->input_names[n]);
        }
        free(op_info->input_names);
    }

    if(NULL != op_info->output_names){
        for(int n = 0; n < op_info->n_output_names; n++){
            free(op_info->output_names[n]);
        }
        free(op_info->output_names);
    }

    if(NULL != op_info->op_info){
        PMIX_INFO_FREE(op_info->op_info, op_info->n_op_info);
    }

    if(NULL != op_info->pset_info_lists){
        for(int n = 0; n < op_info->n_pset_info_lists; n++){
            PMIX_INFO_LIST_RELEASE(op_info->pset_info_lists[n]);
        }
        free(op_info->pset_info_lists);
    }
}

OBJ_CLASS_INSTANCE(ompi_instance_set_op_info_t, opal_list_item_t, ompi_instance_set_op_info_constructor, ompi_instance_set_op_info_destructor);

/* Serialize a set op info object into a single nested PMIx info object:
 *  - "mpi.op_info"         :   darray(4, PMIX_Info)
 *          - Input names       ->  "mpi.op_info.input"     :   darray(n_input_names, PMIX_VALUE(PMIX_STRING))
 *          - Output names      ->  "mpi.op_info.output"    :   darray(n_output_names, PMIX_VALUE(PMIX_STRING))
 *          - Op_info           ->  "mpi.op_info.info"      :   darray(n_op_info, PMIX_INFO)
 *          - Set_infos         ->  "mpi.op_info.set_info"  :   darray(n_output_names, PMIX_VALUE(darray(n_set_infos, PMIX_INFO))) 
 */
int set_op_info_serialize(ompi_instance_set_op_info_t *op_info, pmix_info_t *info){

    int n, rc;
    size_t n_infos;
    pmix_data_array_t *darray_tmp = NULL, darray_tmp2, *darray_set_op_info;
    pmix_info_t *info_ptr, *info_ptr2;
    pmix_value_t *val_ptr;

    //n_infos =   ((0 < op_info->n_input_names)        ?   1 : 0) + 
    //            ((0 < op_info->n_output_names)       ?   1 : 0) +
    //            ((0 < op_info->n_op_info)            ?   1 : 0) +
    //            ((0 < op_info->n_pset_info_lists)    ?   1 : 0);

    n_infos = 4;
    
    PMIX_DATA_ARRAY_CREATE(darray_set_op_info, n_infos, PMIX_INFO);
    info_ptr = (pmix_info_t *) darray_set_op_info->array;

    /* Load the input names */

    PMIX_DATA_ARRAY_CREATE(darray_tmp, op_info->n_input_names, PMIX_VALUE);
    val_ptr = (pmix_value_t *) darray_tmp->array;

    for(n = 0; n < op_info->n_input_names; n++){
        PMIX_VALUE_LOAD(&val_ptr[n], op_info->input_names[n], PMIX_STRING);
    }

    PMIX_INFO_LOAD(info_ptr++, "mpi.op_info.input", darray_tmp, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_FREE(darray_tmp);



    /* Load the output names */
    PMIX_DATA_ARRAY_CREATE(darray_tmp, op_info->n_output_names, PMIX_VALUE);
    val_ptr = (pmix_value_t *) darray_tmp->array;

    for(n = 0; n < op_info->n_output_names; n++){
        PMIX_VALUE_LOAD(&val_ptr[n], op_info->output_names[n], PMIX_STRING);
    }

    PMIX_INFO_LOAD(info_ptr++, "mpi.op_info.output", darray_tmp, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_FREE(darray_tmp);


    /* Load the op_infos */

    PMIX_DATA_ARRAY_CREATE(darray_tmp, op_info->n_op_info, PMIX_INFO);
    info_ptr2 = (pmix_info_t *) darray_tmp->array;

    for(n = 0; n < op_info->n_op_info; n++){
        PMIX_INFO_XFER(info_ptr2++, &op_info->op_info[n]);
    }

    PMIX_INFO_LOAD(info_ptr++, "mpi.op_info.info", darray_tmp, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_FREE(darray_tmp);


    /* Load the set infos in a darray of PMIX_VALUE which are darrays of PMIX_INFO */
    PMIX_DATA_ARRAY_CREATE(darray_tmp, op_info->n_pset_info_lists, PMIX_VALUE);
    val_ptr = (pmix_value_t *) darray_tmp->array;

    for(n = 0; n < op_info->n_pset_info_lists; n++){
        PMIX_INFO_LIST_CONVERT(rc, op_info->pset_info_lists[n], &darray_tmp2);
        PMIX_VALUE_LOAD(&val_ptr[n], &darray_tmp2, PMIX_DATA_ARRAY);
        PMIX_DATA_ARRAY_DESTRUCT(&darray_tmp2);
    }

    PMIX_INFO_LOAD(info_ptr++, "mpi.op_info.set_info", darray_tmp, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_FREE(darray_tmp);


    PMIX_INFO_LOAD(info, "mpi.op_info", darray_set_op_info, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_FREE(darray_set_op_info);

    return OMPI_SUCCESS;

}
#pragma endregion

#pragma region set_op_handle
/* Set OP handle */
static void ompi_instance_set_op_handle_constructor(ompi_instance_set_op_handle_t *set_op){
    
    set_op->psetop = OMPI_PSETOP_NULL;
    OBJ_CONSTRUCT(&set_op->set_op_info, ompi_instance_set_op_info_t);
}

static void ompi_instance_set_op_handle_destructor(ompi_instance_set_op_handle_t *set_op){
    set_op->psetop = OMPI_PSETOP_NULL;
    OBJ_DESTRUCT(&set_op->set_op_info);
}

OBJ_CLASS_INSTANCE(ompi_instance_set_op_handle_t, opal_list_item_t, ompi_instance_set_op_handle_constructor, ompi_instance_set_op_handle_destructor);

/* Serialize a pset op handle into a single nested PMIx info object: 
 *- "mpi.set_op_handles"  :   darray(n_set_ops, PMIX_INFO)
 *          - PMIX_RC_TYPE   :   psetop_directive
 *          - "mpi.op_info"             :   darray(4, PMIX_INFO)    ** see above ** 
 */
int set_op_handle_serialize(ompi_instance_set_op_handle_t *set_op_handle, pmix_info_t *info){
    int n, k;
    pmix_data_array_t *darray_set_op;
    pmix_info_t *info_ptr;

    PMIX_DATA_ARRAY_CREATE(darray_set_op, 2, PMIX_INFO);
    info_ptr = (pmix_info_t *) darray_set_op->array;
    /* Load the op directive */
    PMIX_INFO_LOAD(info_ptr++, PMIX_RC_TYPE, &set_op_handle->psetop, PMIX_UINT8);

    /* Load the op info */
    set_op_info_serialize(&set_op_handle->set_op_info, info_ptr++);

    PMIX_INFO_LOAD(info, "mpi.setop", darray_set_op, PMIX_DATA_ARRAY);

    PMIX_DATA_ARRAY_FREE(darray_set_op);

    return OMPI_SUCCESS;
}
#pragma endregion

#pragma region rc_op_handle
/* RC OP handle */
static void ompi_instance_rc_op_handle_constructor(ompi_instance_rc_op_handle_t *rc_op){
    rc_op->rc_type = OMPI_RC_NULL;
    
    OBJ_CONSTRUCT(&rc_op->rc_op_info, ompi_instance_set_op_info_t);
    OBJ_CONSTRUCT(&rc_op->set_ops, opal_list_t);    
}

static void ompi_instance_rc_op_handle_destructor(ompi_instance_rc_op_handle_t *rc_op){
    
    rc_op->rc_type = OMPI_RC_NULL;
    OBJ_DESTRUCT(&rc_op->rc_op_info);
    OBJ_DESTRUCT(&rc_op->set_ops);
}

OBJ_CLASS_INSTANCE(ompi_instance_rc_op_handle_t, opal_list_item_t, ompi_instance_rc_op_handle_constructor, ompi_instance_rc_op_handle_destructor);

int rc_op_handle_init_output(ompi_rc_op_type_t type, char ***output_names, size_t *noutput){
    switch(type){
        case OMPI_PSETOP_UNION:
        case OMPI_PSETOP_DIFFERENCE:
        case OMPI_PSETOP_INTERSECTION:
        case OMPI_RC_ADD:
        case OMPI_RC_SUB:
            *output_names = malloc(sizeof(char *));
            (*output_names)[0] = NULL;
            *noutput = 1;
            break;
        default:
            *output_names = NULL;
            *noutput = 0;
    }
    return OMPI_SUCCESS;
}

int rc_op_handle_create(ompi_instance_rc_op_handle_t **rc_op_handle){
    
    *rc_op_handle = OBJ_NEW(ompi_instance_rc_op_handle_t);

    return OMPI_SUCCESS;
}

int rc_op_handle_add_op(ompi_rc_op_type_t rc_type, char **input_names, size_t n_input_names, char **output_names, size_t n_output_names, ompi_info_t *info, ompi_instance_rc_op_handle_t *rc_op_handle){
    
    int rc, n, nkeys, flag;
    opal_cstring_t *opal_key, *opal_value;
    ompi_instance_rc_op_handle_t * rc_op_handle_ptr;
    ompi_instance_set_op_info_t *set_op_info;
    bool append = false;
    
    if(rc_op_handle->rc_type == OMPI_RC_NULL){
        rc_op_handle_ptr = rc_op_handle;
    }else{
        /* We only touch the first three members, so we can cast the setop handle to rc op handle */
        rc_op_handle_ptr = (ompi_instance_rc_op_handle_t *) OBJ_NEW(ompi_instance_set_op_handle_t);
        append = true;
    }

    rc_op_handle_ptr->rc_type = rc_type;
    set_op_info = &rc_op_handle_ptr->rc_op_info;

    /* Copy the input names */
    set_op_info->n_input_names = n_input_names;
    if(0 < n_input_names){    
        set_op_info->input_names = malloc(n_input_names * sizeof(char*));
        for(n = 0; n < n_input_names; n++){
            set_op_info->input_names[n] = malloc(OPAL_MAX_PSET_NAME_LEN);
            strcpy(set_op_info->input_names[n], input_names[n]);
        }
    }

    /* Copy the output names */
    set_op_info->n_output_names = n_output_names;
    if(0 < n_output_names){
        set_op_info->output_names = malloc(n_output_names * sizeof(char*));
        for(n = 0; n < n_output_names; n++){
            set_op_info->output_names[n] = malloc(OPAL_MAX_PSET_NAME_LEN);
            strcpy(set_op_info->output_names[n], output_names[n]);
        }
    }

    /* If they provided operation info copy it to the handle */
    if(NULL != info && MPI_INFO_NULL != info){
    
        rc = ompi_info_get_nkeys(info, &nkeys);
        if(OMPI_SUCCESS != rc){
            OBJ_RELEASE(rc_op_handle_ptr);
            return rc;
        }

        if(0 < nkeys){

            set_op_info->n_op_info = nkeys;
            PMIX_INFO_CREATE(set_op_info->op_info, nkeys);

            for(n = 0; n < nkeys; n++){
                rc = ompi_info_get_nthkey(info, n, &opal_key);
                if(OMPI_SUCCESS != rc){
                    OBJ_RELEASE(rc_op_handle_ptr);
                    return rc;
                }

                rc = ompi_info_get(info, opal_key->string, &opal_value, &flag);
                if(OMPI_SUCCESS != rc || 0 == flag){
                    OBJ_RELEASE(rc_op_handle_ptr);
                    return rc;
                }

                PMIX_INFO_LOAD(&set_op_info->op_info[n], opal_key->string, opal_value->string, PMIX_STRING);
            }
        }
    }

    /* Initialize the pset info lists */
    rc_op_handle_ptr->rc_op_info.n_pset_info_lists = n_output_names;
    if(0 < n_output_names){
        rc_op_handle_ptr->rc_op_info.pset_info_lists = (void **) malloc(n_output_names * sizeof(void *));
    }

    for(n = 0; n < n_output_names; n++){
        PMIX_INFO_LIST_START(rc_op_handle_ptr->rc_op_info.pset_info_lists[n]);
    }

    if(append){
        opal_list_append(&rc_op_handle->set_ops, &rc_op_handle_ptr->super);
    }

    return OMPI_SUCCESS;
}

int rc_op_handle_add_pset_infos(ompi_instance_rc_op_handle_t * rc_op_handle, char * pset_name, pmix_info_t * info, int ninfo){
    int n, k, rc;
    pmix_info_t *new_info;
    bool found = false;
    ompi_instance_set_op_handle_t *set_op_handle;

    for(n = 0; n < rc_op_handle->rc_op_info.n_output_names; n++){
        if(0 == strcmp(rc_op_handle->rc_op_info.output_names[n], pset_name)){
            found = true;
            for(k = 0; k < ninfo; k++){
                PMIX_INFO_LIST_XFER(rc, rc_op_handle->rc_op_info.pset_info_lists[n], &info[k]);
                if(PMIX_SUCCESS != rc){
                    return rc;
                }
            }
            break;
        }
    }
    
    if(!found){
        OPAL_LIST_FOREACH(set_op_handle, &rc_op_handle->set_ops, ompi_instance_set_op_handle_t){
            for(n = 0; n < set_op_handle->set_op_info.n_output_names; n++){
                if(0 == strcmp(set_op_handle->set_op_info.output_names[n], pset_name)){
                    found = true;
                    for(k = 0; k < ninfo; k++){
                        PMIX_INFO_LIST_XFER(rc, set_op_handle->set_op_info.pset_info_lists[n], &info[k]);
                        if(PMIX_SUCCESS != rc){
                            return rc;
                        }
                    }
                    break;
                }
            }
        }
    }

    if(!found){
        return OMPI_ERR_BAD_PARAM;
    }

    return OMPI_SUCCESS;

}

size_t rc_op_handle_get_num_ops(ompi_instance_rc_op_handle_t * rc_op_handle){

    if(OMPI_RC_NULL == rc_op_handle->rc_type){
        return 0;
    }else{
        return 1 + rc_op_handle->set_ops.opal_list_length;
    }

}

int rc_op_handle_get_num_output(ompi_instance_rc_op_handle_t * rc_op_handle, size_t op_index, size_t *num_output){
    
    size_t num_ops;
    size_t index;
    ompi_instance_set_op_handle_t *setop;

    num_ops = rc_op_handle_get_num_ops(rc_op_handle);

    if(op_index >= num_ops){
        return OMPI_ERR_BAD_PARAM;
    }else if(0 == op_index){
        *num_output = rc_op_handle->rc_op_info.n_output_names;
    }else{
        index = 1;
        OPAL_LIST_FOREACH(setop, &rc_op_handle->set_ops, ompi_instance_set_op_handle_t){
            if(index == op_index){
                *num_output = setop->set_op_info.n_output_names;
                break;
            }
            index ++;
        }
    }

    return OMPI_SUCCESS;
}

int rc_op_handle_get_ouput_name(ompi_instance_rc_op_handle_t * rc_op_handle, size_t op_index, size_t name_index, int *pset_len, char* pset_name){
    
    size_t index;
    char *out_pset_name = NULL;
    ompi_instance_set_op_handle_t *setop;

    if(op_index >= rc_op_handle_get_num_ops(rc_op_handle)){
        return OMPI_ERR_BAD_PARAM;
    }

    if(0 == op_index){
        if(name_index >= rc_op_handle->rc_op_info.n_output_names){
            return OMPI_ERR_BAD_PARAM;
        }
        out_pset_name = rc_op_handle->rc_op_info.output_names[name_index];
    }else{
        index = 1;
        OPAL_LIST_FOREACH(setop, &rc_op_handle->set_ops, ompi_instance_set_op_handle_t){
            if(index == op_index){
                if(name_index >= setop->set_op_info.n_output_names){
                    return OMPI_ERR_BAD_PARAM;
                }
                out_pset_name = setop->set_op_info.output_names[name_index];
                break;
            }
            index ++;
        }
    }

    if(NULL == out_pset_name){
        return OMPI_ERR_NOT_FOUND;
    }

    if (0 == *pset_len) {
        *pset_len = strlen(out_pset_name) + 1;
        return OMPI_SUCCESS;
    }
    strncpy (pset_name, out_pset_name, *pset_len);

    return OMPI_SUCCESS;
}

int rc_op_handle_free(ompi_instance_rc_op_handle_t ** rc_op_handle){
    
    OBJ_RELEASE(*rc_op_handle);
    *rc_op_handle = &ompi_mpi_rc_op_handle_null.rc_op_handle;
    
    return OMPI_SUCCESS;
}

/* Serializes an rc_op_handle into a PMIx_info with the following structure:  
 * 
 * "mpi.rc_op_handle -> darray(3, PMIX_INFO) 
 *      - PMIX_RC_TYPE: rc_type
 *      - "mpi.op_info"         :   darray(4, PMIX_Info)
 *          - Input names       ->  "mpi.op_info.input"     :   darray(n_input_names, PMIX_VALUE(PMIX_STRING))
 *          - Output names      ->  "mpi.op_info.output"    :   darray(n_output_names, PMIX_VALUE(PMIX_STRING))
 *          - Op_info           ->  "mpi.op_info.info"      :   darray(n_op_info, PMIX_INFO)
 *          - Set_infos         ->  "mpi.op_info.set_info"  :   darray(n_output_names, PMIX_VALUE(darray(n_set_infos, PMIX_INFO)))
 *      - "mpi.set_op_handles"  :   darray(n_set_ops, PMIX_INFO)
 *          - "pmix.psetop.directive"   :   psetop_directive
 *          - "mpi.op_info"     :   darray(4, PMIX_INFO)    ** see above **  
 */
int rc_op_handle_serialize(ompi_instance_rc_op_handle_t *rc_op_handle, pmix_info_t *info){

    ompi_instance_set_op_info_t rc_op_info;
    ompi_instance_set_op_handle_t *set_op_handle; 
    pmix_data_array_t *darray_rc_op, *darray_set_op;
    pmix_info_t *info_ptr, *info_ptr2;

    PMIX_DATA_ARRAY_CREATE(darray_rc_op, 3, PMIX_INFO);
    info_ptr = (pmix_info_t *) darray_rc_op->array;

    /* Load the op type */
    PMIX_INFO_LOAD(&info_ptr[0], PMIX_RC_TYPE, &rc_op_handle->rc_type, PMIX_UINT8);

    /* Load the op info */
    set_op_info_serialize(&rc_op_handle->rc_op_info, &info_ptr[1]);

    /* Load the set ops */
    PMIX_DATA_ARRAY_CREATE(darray_set_op, rc_op_handle->set_ops.opal_list_length, PMIX_INFO);
    info_ptr2 = (pmix_info_t *)darray_set_op->array;
    
    OPAL_LIST_FOREACH(set_op_handle, &rc_op_handle->set_ops, ompi_instance_set_op_handle_t){
        set_op_handle_serialize(set_op_handle, info_ptr2++);
    }

    PMIX_INFO_LOAD(&info_ptr[2], "mpi.set_op_handles", darray_set_op, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_FREE(darray_set_op);


    PMIX_INFO_LOAD(info, "mpi.rc_op_handle", darray_rc_op, PMIX_DATA_ARRAY);
    PMIX_DATA_ARRAY_FREE(darray_rc_op);

    return OMPI_SUCCESS;
}

#pragma endregion