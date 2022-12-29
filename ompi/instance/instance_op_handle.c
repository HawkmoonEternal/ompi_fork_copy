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
 *          - PMIX_QUERY_PSETOP_TYPE   :   psetop_directive
 *          - "mpi.op_info"             :   darray(4, PMIX_INFO)    ** see above ** 
 */
int set_op_handle_serialize(ompi_instance_set_op_handle_t *set_op_handle, pmix_info_t *info){
    int n, k;
    pmix_data_array_t *darray_set_op;
    pmix_info_t *info_ptr;

    PMIX_DATA_ARRAY_CREATE(darray_set_op, 2, PMIX_INFO);
    info_ptr = (pmix_info_t *) darray_set_op->array;
    /* Load the op directive */
    PMIX_INFO_LOAD(info_ptr++, PMIX_PSETOP_TYPE, &set_op_handle->psetop, PMIX_UINT8);
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
    rc_op->rc_type = OMPI_PSETOP_NULL;
    
    OBJ_CONSTRUCT(&rc_op->rc_op_info, ompi_instance_set_op_info_t);
    OBJ_CONSTRUCT(&rc_op->set_ops, opal_list_t);    
}

static void ompi_instance_rc_op_handle_destructor(ompi_instance_rc_op_handle_t *rc_op){
    
    rc_op->rc_type = OMPI_PSETOP_NULL;
    OBJ_DESTRUCT(&rc_op->rc_op_info);
    OBJ_DESTRUCT(&rc_op->set_ops);
}

OBJ_CLASS_INSTANCE(ompi_instance_rc_op_handle_t, opal_list_item_t, ompi_instance_rc_op_handle_constructor, ompi_instance_rc_op_handle_destructor);

int rc_op_handle_init_output(ompi_psetop_type_t type, char ***output_names, size_t *noutput){
    switch(type){
        case OMPI_PSETOP_UNION:
        case OMPI_PSETOP_DIFFERENCE:
        case OMPI_PSETOP_INTERSECTION:
        case OMPI_PSETOP_ADD:
        case OMPI_PSETOP_SUB:
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

int rc_op_handle_add_op(ompi_psetop_type_t rc_type, char **input_names, size_t n_input_names, char **output_names, size_t n_output_names, ompi_info_t *info, ompi_instance_rc_op_handle_t *rc_op_handle){
    
    int rc, n, nkeys, flag;
    char *alias;
    opal_cstring_t *opal_key, *opal_value;
    ompi_instance_rc_op_handle_t * rc_op_handle_ptr;
    ompi_instance_set_op_info_t *set_op_info;
    ompi_mpi_instance_pset_t *pset_ptr = NULL;
    bool append = false;
    
    if(rc_op_handle->rc_type == OMPI_PSETOP_NULL){
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
            if(NULL == (pset_ptr = get_pset_by_name(input_names[n]))){
                refresh_pmix_psets(PMIX_QUERY_PSET_NAMES);
                pset_ptr = get_pset_by_name(input_names[n]);
            }
            if(NULL != pset_ptr){
                strcpy(set_op_info->input_names[n], pset_ptr->name);
            }else{
                /* We might not know about this PSet yet. So copy the provided name and let the RTE handle the issue */
                strcpy(set_op_info->input_names[n], input_names[n]);
            }
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
    rc_op_handle_ptr->rc_op_info.n_pset_info_lists = PSET_INFO_LIST_ARRAY_BASE_SIZE;
    rc_op_handle_ptr->rc_op_info.pset_info_lists = (void **) malloc(PSET_INFO_LIST_ARRAY_BASE_SIZE * sizeof(void *));

    for(n = 0; n < PSET_INFO_LIST_ARRAY_BASE_SIZE; n++){
        PMIX_INFO_LIST_START(rc_op_handle_ptr->rc_op_info.pset_info_lists[n]);
    }

    if(append){
        opal_list_append(&rc_op_handle->set_ops, &rc_op_handle_ptr->super);
    }

    return OMPI_SUCCESS;
}

int rc_op_handle_add_pset_infos(ompi_instance_rc_op_handle_t * rc_op_handle, char * pset_name, pmix_info_t * info, int ninfo){
    int n, k, rc;
    size_t old_size, new_size;
    pmix_info_t *new_info, *info_ptr;
    bool found = false;
    ompi_instance_set_op_handle_t *set_op_handle;
    ompi_mpi_instance_pset_t *pset_ptr;
    pmix_data_array_t darray;
    void **old_lists, **new_lists;

    /* Get the right PSet name in case it refers to a builtin PSet */
    refresh_pmix_psets(PMIX_QUERY_PSET_NAMES);
    
    pset_ptr = get_pset_by_name(pset_name);
    if(NULL != pset_ptr){
        pset_name = pset_ptr->name;
    }

    /* If not already done: initialize the pset info lists */
    if(0 == rc_op_handle->rc_op_info.n_pset_info_lists){

        rc_op_handle->rc_op_info.n_pset_info_lists = PSET_INFO_LIST_ARRAY_BASE_SIZE;
        rc_op_handle->rc_op_info.pset_info_lists = (void **) malloc(PSET_INFO_LIST_ARRAY_BASE_SIZE * sizeof(void *));

        for(n = 0; n < PSET_INFO_LIST_ARRAY_BASE_SIZE; n++){
            PMIX_INFO_LIST_START(rc_op_handle->rc_op_info.pset_info_lists[n]);
        }
    }

    /* Try to insert info into the info lists*/
    for(n = 0; n < rc_op_handle->rc_op_info.n_pset_info_lists; n++){

        PMIX_INFO_LIST_CONVERT(rc, rc_op_handle->rc_op_info.pset_info_lists[n], &darray);
        if(PMIX_SUCCESS != rc && PMIX_ERR_EMPTY != rc){
            return rc;
        }
        info_ptr = (pmix_info_t *) darray.array;

        /* First check if there is already an info list for this PSet */
        if( 0 < darray.size){            
            if(!PMIX_CHECK_KEY(&info_ptr[0], PMIX_PSET_NAME) ||
            0 != strcmp(info_ptr[0].value.data.string, pset_name) ){
                
                PMIX_DATA_ARRAY_DESTRUCT(&darray);
                continue;
            }
        /* If we reach a list of size 0, there was no PSet list for this name */
        }else {
            PMIX_INFO_LIST_ADD(rc, rc_op_handle->rc_op_info.pset_info_lists[n], PMIX_PSET_NAME, pset_name, PMIX_STRING);
            if(PMIX_SUCCESS != rc){
                PMIX_DATA_ARRAY_DESTRUCT(&darray);
                return rc;
            }

        }

        /* Insert the PSet infos */
        for(k = 0; k < ninfo; k++){
            PMIX_INFO_LIST_XFER(rc, rc_op_handle->rc_op_info.pset_info_lists[n], &info[k]);
            if(PMIX_SUCCESS != rc){
                PMIX_DATA_ARRAY_DESTRUCT(&darray);
                return rc;
            }
        }

        PMIX_DATA_ARRAY_DESTRUCT(&darray);
        return OMPI_SUCCESS;
    }

    /* If we reach this we need to grow the PSet_info_lists array */
    old_lists = rc_op_handle->rc_op_info.pset_info_lists;
    old_size = rc_op_handle->rc_op_info.n_pset_info_lists;
    new_size = 2 * old_size;
    new_lists = malloc(new_size * sizeof (void*));

    /* Transfer the old lists */
    for(n = 0; n < new_size; n++){
        new_lists[n] = old_lists[n];
    }

    /* Start a new one */
    PMIX_INFO_LIST_START(new_lists[old_size]);

    /* First insert the PSet name*/
    PMIX_INFO_LIST_ADD(rc, new_lists[old_size], PMIX_PSET_NAME, pset_name, PMIX_STRING);
    if(PMIX_SUCCESS != rc){
        PMIX_INFO_LIST_RELEASE(new_lists[old_size]);
        free(new_lists);
        return rc;
    }
    /* Then insert the PSet infos */
    for(k = 0; k < ninfo; k++){
        PMIX_INFO_LIST_XFER(rc, new_lists[old_size], &info[k]);
        if(PMIX_SUCCESS != rc){
            PMIX_INFO_LIST_RELEASE(new_lists[old_size]);
            free(new_lists);
            return rc;
        }
    }

    /* Now start the remaining lists */
    for(n = old_size + 1; n < new_size; n++){
        PMIX_INFO_LIST_START(new_lists[n]);
    }

    /* Update the op handle */
    rc_op_handle->rc_op_info.pset_info_lists = new_lists;
    rc_op_handle->rc_op_info.n_pset_info_lists = new_size;
    free(old_lists);

    return OMPI_SUCCESS;

}

size_t rc_op_handle_get_num_ops(ompi_instance_rc_op_handle_t * rc_op_handle){

    if(OMPI_PSETOP_NULL == rc_op_handle->rc_type){
        return 0;
    }else{
        return 1 + rc_op_handle->set_ops.opal_list_length;
    }

}

int rc_op_handle_get_get_op_type(ompi_instance_rc_op_handle_t * rc_op_handle, size_t op_index, ompi_psetop_type_t *op_type){
    
    size_t num_ops;
    size_t index;
    ompi_instance_set_op_handle_t *setop;

    num_ops = rc_op_handle_get_num_ops(rc_op_handle);

    if(op_index >= num_ops){
        return OMPI_ERR_BAD_PARAM;
    }else if(0 == op_index){
        *op_type = rc_op_handle->rc_type;
    }else{
        index = 1;
        OPAL_LIST_FOREACH(setop, &rc_op_handle->set_ops, ompi_instance_set_op_handle_t){
            if(index == op_index){
                *op_type = setop->psetop;
                break;
            }
            index ++;
        }
    }

    return OMPI_SUCCESS;
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

int rc_op_handle_get_nth_op(ompi_instance_rc_op_handle_t * rc_op_handle, size_t op_index, ompi_instance_set_op_handle_t **op){
    
    size_t index;
    char *out_pset_name = NULL;
    ompi_instance_set_op_handle_t *setop;

    if(op_index >= rc_op_handle_get_num_ops(rc_op_handle)){
        return OMPI_ERR_BAD_PARAM;
    }

    if(0 == op_index){
        *op = (ompi_instance_set_op_handle_t *) rc_op_handle;
        return OMPI_SUCCESS;
    }else{
        index = 1;
        OPAL_LIST_FOREACH(setop, &rc_op_handle->set_ops, ompi_instance_set_op_handle_t){
            if(index == op_index){

                *op = setop;
                return OMPI_SUCCESS;
            }
            index ++;
        }
    }

    return OMPI_ERR_BAD_PARAM;
}

int rc_op_handle_free(ompi_instance_rc_op_handle_t ** rc_op_handle){
    
    OBJ_RELEASE(*rc_op_handle);
    *rc_op_handle = &ompi_mpi_rc_op_handle_null.rc_op_handle;
    
    return OMPI_SUCCESS;
}

/* Serializes an rc_op_handle into a PMIx_info with the following structure:  
 * 
 * "mpi.rc_op_handle -> darray(3, PMIX_INFO) 
 *      - PMIX_QUERY_PSETOP_TYPE: rc_type
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
    PMIX_INFO_LOAD(&info_ptr[0], PMIX_PSETOP_TYPE, &rc_op_handle->rc_type, PMIX_UINT8);

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

int rc_op_handle_deserialize(pmix_info_t *rc_op_handle_info, ompi_instance_rc_op_handle_t **rc_op_handle){
    size_t n, k, m, i, ninfo, nsetop_info = 0, ninfo2, index, ninput, noutput;
    ompi_psetop_type_t setop_type = OMPI_PSETOP_NULL;
    
    size_t n_op_output = 0;
    char ** input, ** output;

    pmix_value_t * val_ptr;
    pmix_info_t * setop_info = NULL, * info, * info2, *info_ptr, **pset_info_array;

    size_t *pset_info_array_sizes;

    ompi_info_t * op_info;

    rc_op_handle_create(rc_op_handle);

    index = 0;
    while(true){

        /* Get the next set operation to be deserialized */
        for(n = 0; n < rc_op_handle_info[0].value.data.darray->size; n++){
            if(0 == index){
                setop_info = (pmix_info_t *) rc_op_handle_info[0].value.data.darray->array;
                nsetop_info = rc_op_handle_info[0].value.data.darray->size;
            }else{
                info = (pmix_info_t *) rc_op_handle_info[0].value.data.darray->array;
                ninfo = rc_op_handle_info[0].value.data.darray->size; 

                for(i = 0; i < ninfo; i++){
                    if(PMIX_CHECK_KEY(&info[i], "mpi.set_op_handles")){

                        /*set op handles */
                        info2 = (pmix_info_t *) info[i].value.data.darray->array;

                        if(index - 1 >= info[i].value.data.darray->size){
                            return OMPI_SUCCESS;
                        }

                        setop_info = (pmix_info_t *) info2[index - 1].value.data.darray->array;
                        nsetop_info = info2[index - 1].value.data.darray->size;
                    }
                }
            }
        }

        output = input = NULL;
        op_info = NULL;
        pset_info_array = NULL;
        pset_info_array_sizes = NULL;
        noutput = ninput = setop_type = 0;

        /* Retrieve all provided info about the set operation */
        for(n = 0; n < nsetop_info; n++){
            if(PMIX_CHECK_KEY(&setop_info[n], PMIX_PSETOP_TYPE)){
                setop_type = setop_info[n].value.data.uint8;
            }else if(PMIX_CHECK_KEY(&setop_info[n], "mpi.op_info")){            
                info2 = (pmix_info_t *) setop_info[n].value.data.darray->array;
                ninfo2 = setop_info[n].value.data.darray->size;
                for(k = 0; k < ninfo2; k++){
                    if(PMIX_CHECK_KEY(&info2[k], "mpi.op_info.info")){                   

                        info_ptr = (pmix_info_t *) info2[k].value.data.darray->array;
                        op_info = ompi_info_allocate();
                        for(i = 0; i < info2[k].value.data.darray->size; i++){
                            ompi_info_set(op_info, info_ptr[i].key, info_ptr[i].value.data.string);
                        }
                    }
                    /* Get the input sets */
                    else if(PMIX_CHECK_KEY(&info2[k], "mpi.op_info.input")){                    
                        val_ptr = (pmix_value_t *) info2[k].value.data.darray->array;
                        if(0 < info2[k].value.data.darray->size){
                            output = (char **) malloc(info2[k].value.data.darray->size * sizeof(char *));
                            for(i = 0; i < info2[k].value.data.darray->size; i++){
                                output[i] = strdup(val_ptr[i].data.string);
                            }
                        }
                    }
                    /* Get the output sets */
                    else if(PMIX_CHECK_KEY(&info2[k], "mpi.op_info.output")){
                        val_ptr = (pmix_value_t *) info2[k].value.data.darray->array;
                        if(0 < info2[k].value.data.darray->size){
                            input = (char **) malloc(info2[k].value.data.darray->size * sizeof(char *));
                            for(i = 0; i < info2[k].value.data.darray->size; i++){
                                input[i] = strdup(val_ptr[i].data.string);
                            }
                        }
                    }
                    /* Get the set infos */
                    else if(PMIX_CHECK_KEY(&info2[k], "mpi.op_info.set_info")){

                        val_ptr = (pmix_value_t *) info2[k].value.data.darray->array;
                        pset_info_array = (pmix_info_t **) malloc(info2[k].value.data.darray->size * sizeof(pmix_info_t *));
                        pset_info_array_sizes = (size_t *) malloc(info2[k].value.data.darray->size * sizeof(size_t));
                        for(i = 0; i < info2[k].value.data.darray->size; i++){
                            pset_info_array[i] = (pmix_info_t *) val_ptr[i].data.darray->array;
                            pset_info_array_sizes[i] = val_ptr[i].data.darray->size;
                        }
                    }
                }
            }
        }

        /* ERROR: Invalid operation */
        if(0 == ninput || OMPI_PSETOP_NULL == setop_type){
            goto ERROR;
        }

        /* Add the operation to the handle */
        rc_op_handle_add_op(setop_type, input, ninput, output, noutput, op_info, *rc_op_handle);
        
        for(n = 0; n < noutput; n++){
            rc_op_handle_add_pset_infos(*rc_op_handle, output[n], pset_info_array[n], pset_info_array_sizes[n]);
        }

        /* cleanup for next iteration */
        for(n = 0; n < ninput; n++){
            free(input[n]);
        }
        free(input);
    
        for(n = 0; n < noutput; n++){
            free(output[n]);
        }
        free(output);
    
        free(pset_info_array_sizes);
        free(pset_info_array);
    
        if(NULL != op_info){
            ompi_info_free(&op_info);
        }

        index++;

    }

SUCCESS:
    return OMPI_SUCCESS;   
ERROR:
    for(n = 0; n < ninput; n++){
        free(input[n]);
    }
    free(input);

    for(n = 0; n < ninput; n++){
        free(input[n]);
    }
    free(input);

    free(pset_info_array_sizes);
    free(pset_info_array);

    if(NULL != op_info){
        ompi_info_free(&op_info);
    }

    rc_op_handle_free(rc_op_handle);

    return OMPI_ERR_BAD_PARAM;


}

void string_array_from_comma_list(char *s, char *** str_array, size_t *count){
    int i;
    char *str;

    for (i=0, *count=0; s[i]; i++)
        *count += (s[i] == '.');
    
    if(0 == *count)
        return;

    *str_array = (char **) malloc(i * sizeof (char*));

    str = strtok(s, ",");
    while(str) {
        (*str_array)[i] = strdup(str);
        str = strtok(NULL, ",");
    }
}

char * comma_list_from_string_array(char **str_array, size_t n_str){
    size_t size, n;
    char *output;

    size = 0;
    for(n = 0; n < n_str; n++){
        size += strlen(str_array[n]) + 1;
    }
    output = (char *) malloc(size);

    strcpy(output, str_array[0]);

    for(n = 1; n < n_str; n++){
        strcat(output, ",");
        strcat(output, str_array[n]);
    }

    return output;
}

int rc_op_handle_to_info(ompi_instance_rc_op_handle_t *op_handle, ompi_info_t ***info, size_t *ninfo){
    int rc;
    size_t num_ops, index, n;
    char *comma_list;
    ompi_instance_set_op_handle_t *setop;
    
    

    num_ops = rc_op_handle_get_num_ops(op_handle);

    *info = (ompi_info_t **) malloc (num_ops * sizeof(ompi_info_t *));

    for(index = 0; index < num_ops; index++){

        if(OMPI_SUCCESS != (rc = rc_op_handle_get_nth_op(op_handle, index, &setop))){
            return rc;
        }

        (*info)[index] = ompi_info_allocate();

        if(OMPI_SUCCESS != (rc = ompi_info_set((*info)[index], MPI_KEY_PSETOP_TYPE, OMPI_PSETOP_TO_STRING(setop->psetop)))){
            goto ERROR;
        }

        comma_list = comma_list_from_string_array(setop->set_op_info.input_names, setop->set_op_info.n_input_names);
        if(OMPI_SUCCESS != (rc = ompi_info_set((*info)[index], MPI_KEY_PSETOP_INPUT, comma_list))){
            free(comma_list);
            goto ERROR;
        }
        free(comma_list);

        comma_list = comma_list_from_string_array(setop->set_op_info.output_names, setop->set_op_info.n_output_names);
        if(OMPI_SUCCESS != (rc = ompi_info_set((*info)[index], MPI_KEY_PSETOP_OUTPUT, comma_list))){
            free(comma_list);
            goto ERROR; 
        }
        free(comma_list);

        for(n = 0; n < setop->set_op_info.n_op_info; n++){
            ompi_info_set((*info)[index], setop->set_op_info.op_info[n].key, setop->set_op_info.op_info[n].value.data.string);
        }

    }

    *ninfo = num_ops;

    return OMPI_SUCCESS;

ERROR:
    for(n = 0; n <= index; n++){
        ompi_info_free(&(*info)[n]);
    }
    free(*info);

    return rc;
}

int rc_op_handle_from_info(ompi_info_t **info, size_t num_ops, ompi_instance_rc_op_handle_t **op_handle){
    int rc, flag, nkeys, n;
    size_t index, ninput, noutput;
    char **input, **output;
    ompi_psetop_type_t type;
    opal_cstring_t *value, *key;
    ompi_info_t *op_info;
    
    if(OMPI_SUCCESS != (rc = rc_op_handle_create(op_handle))){
        return rc;
    }

    for(index = 0; index < num_ops; index++){

        if(OMPI_SUCCESS != (rc = ompi_info_get_nkeys(info[index], &nkeys))){
            rc_op_handle_free(op_handle);
            return rc;
        }

        type = OMPI_PSETOP_NULL;
        input = output = NULL;
        ninput = noutput = 0;

        op_info = ompi_info_allocate();
        for(n = 0; n < nkeys; n++){

            if(OMPI_SUCCESS != (rc = ompi_info_get_nthkey(info[index], n, &key))){
                break;
            }

            if(OMPI_SUCCESS != (rc = ompi_info_get(info[index], key->string, &value, &flag))){
                OBJ_RELEASE(key);
                break;
            }
    
            if(!flag){
                OBJ_RELEASE(key);
                OBJ_RELEASE(value);
                break;
            }

            if(0 == strcmp(MPI_KEY_PSETOP_TYPE, key->string)){
                type = OMPI_PSETOP_FROM_STRING(value->string);
            }
            else if(0 == strcmp(MPI_KEY_PSETOP_INPUT, key->string)){
                string_array_from_comma_list(value->string, &input, &ninput);

            }
            else if(0 == strcmp(MPI_KEY_PSETOP_OUTPUT, key->string)){
                string_array_from_comma_list(value->string, &output, &noutput);
            }
            else{
                rc = ompi_info_set(op_info, key->string, value->string);
                if(PMIX_SUCCESS != rc){
                    OBJ_RELEASE(key);
                    OBJ_RELEASE(value);
                    break;
                }
            }

            OBJ_RELEASE(key);
            OBJ_RELEASE(value);

        }

        if(OMPI_SUCCESS != rc || NULL == input || MPI_PSETOP_NULL == type){
            free(input);
            free(output);
            ompi_info_free(&op_info);
            rc_op_handle_free(op_handle);
            return OMPI_ERR_BAD_PARAM;
        }

        rc_op_handle_add_op(type, input, ninput, output, noutput, op_info, *op_handle);

        free(input);
        free(output);

        if(OMPI_SUCCESS != (rc = ompi_info_free(&op_info))){
            return rc;
        }


    }

    return OMPI_SUCCESS;
}

#pragma endregion