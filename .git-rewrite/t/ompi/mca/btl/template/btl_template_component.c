/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */


#include "ompi_config.h"
#include "ompi/constants.h"
#include "opal/mca/event/event.h"
#include "ompi/mca/btl/btl.h"

#include "ompi/mca/mpool/base/base.h" 
#include "btl_template.h"
#include "btl_template_frag.h"
#include "btl_template_endpoint.h" 
#include "ompi/mca/btl/base/base.h" 

/**
 * Register any MCA parameters associated with this component
 */
static int mca_btl_template_component_register(void);

/**
 * Make initial determination whether this component can run or not
 */
static int mca_btl_template_component_open(void);

/**
 * Any final cleanup before being unloaded.
 */
static int mca_btl_template_component_close(void);


mca_btl_template_component_t mca_btl_template_component = {
    {
        /* First, the mca_base_component_t struct containing meta information
           about the component itself */

        {
            MCA_BTL_BASE_VERSION_2_0_0,

            "template", /* MCA component name */
            OMPI_MAJOR_VERSION,  /* MCA component major version */
            OMPI_MINOR_VERSION,  /* MCA component minor version */
            OMPI_RELEASE_VERSION,  /* MCA component release version */
            mca_btl_template_component_open,  /* component open */
            mca_btl_template_component_close,  /* component close */
            NULL, /* component query */
            mca_btl_template_component_register, /* component register */
        },
        {
            /* The component is not checkpoint ready */
            MCA_BASE_METADATA_PARAM_NONE
        },

        mca_btl_template_component_init,  
        mca_btl_template_component_progress,
    }
};

static int mca_btl_template_component_open(void)
{
    return OMPI_SUCCESS;
}

static int mca_btl_template_component_register(void)
{    
    /* initialize state */
    mca_btl_template_component.template_num_btls=0;
    mca_btl_template_component.template_btls=NULL;
    
    /* initialize objects */ 
    OBJ_CONSTRUCT(&mca_btl_template_component.template_procs, opal_list_t);

    /* register TEMPLATE component parameters */
    mca_btl_template_component.template_free_list_num = 8;
    (void) mca_base_component_var_register(&mca_btl_template_component.super.btl_version,
                                           "free_list_num", NULL, MCA_BASE_VAR_TYPE_INT,
                                           NULL, 0, 0, OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_btl_template_component.template_free_list_num);
    (void) mca_base_component_var_register(&mca_btl_template_component.super.btl_version,
                                           "free_list_max", NULL, MCA_BASE_VAR_TYPE_INT,
                                           NULL, 0, 0, OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_btl_template_component.template_free_list_max);
    (void) mca_base_component_var_register(&mca_btl_template_component.super.btl_version,
                                           "free_list_inc", NULL, MCA_BASE_VAR_TYPE_INT,
                                           NULL, 0, 0, OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_btl_template_component.template_free_list_inc);

    mca_btl_template_component.template_mpool_name = "grdma";
    (void) mca_base_component_var_register(&mca_btl_template_component.super.btl_version,
                                           "mpool", NULL, MCA_BASE_VAR_TYPE_STRING,
                                           NULL, 0, 0, OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &mca_btl_template_component.template_mpool_name);

    mca_btl_template_module.super.btl_exclusivity = 0;
    mca_btl_template_module.super.btl_eager_limit = 64*1024;
    mca_btl_template_module.super.btl_rndv_eager_limit = 64*1024;
    mca_btl_template_module.super.btl_max_send_size = 128*1024;
    mca_btl_template_module.super.btl_min_rdma_pipeline_size = 1024*1024;
    mca_btl_template_module.super.btl_rdma_pipeline_frag_size = 1024*1024;
    mca_btl_template_module.super.btl_rdma_pipeline_send_length = 1024*1024;
    mca_btl_template_module.super.btl_flags  = MCA_BTL_FLAGS_PUT;

    return mca_btl_base_param_register(&mca_btl_template_component.super.btl_version,
                                       &mca_btl_template_module.super);
}

/*
 * component cleanup - sanity checking of queue lengths
 */

static int mca_btl_template_component_close(void)
{
    return OMPI_SUCCESS;
}

/*
 *  TEMPLATE component initialization:
 *  (1) read interface list from kernel and compare against component parameters
 *      then create a BTL instance for selected interfaces
 *  (2) setup TEMPLATE listen socket for incoming connection attempts
 *  (3) register BTL parameters with the MCA
 */

mca_btl_base_module_t** mca_btl_template_component_init(int *num_btl_modules, 
                                                  bool enable_progress_threads,
                                                  bool enable_mpi_threads)
{
    return NULL;
}

/*
 *  TEMPLATE component progress.
 */


int mca_btl_template_component_progress()
{
    return 0;
}

