/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 *
 * Sample MPI "hello world" application in C
 */

#include <stdio.h>
#include "mpi.h"
#include "../ompi/include/ompi_config.h"
#include "../instance/instance.h"
#include "../opal/util/arch.h"
#include "../opal/util/show_help.h"
#include "../opal/util/argv.h"
#include "../opal/runtime/opal_params.h"
#include "../ompi/mca/pml/pml.h"
#include "../ompi/runtime/params.h"
#include "../ompi/interlib/interlib.h"
#include "../ompi/communicator/communicator.h"
#include "../ompi/errhandler/errhandler.h"
#include "../ompi/errhandler/errcode.h"
#include "../ompi/message/message.h"
#include "../ompi/info/info.h"
#include "../ompi/attribute/attribute.h"
#include "../ompi/op/op.h"
#include "../ompi/dpm/dpm.h"
#include "../ompi/file/file.h"
#include "../ompi/mpiext/mpiext.h"
#include "../ompi/mca/hook/base/base.h"
#include "../ompi/mca/op/base/base.h"
#include "../opal/mca/allocator/base/base.h"
#include "../opal/mca/rcache/base/base.h"
#include "../opal/mca/mpool/base/base.h"
#include "../ompi/mca/bml/base/base.h"
#include "../ompi/mca/pml/base/base.h"
#include "../ompi/mca/coll/base/base.h"
#include "../ompi/mca/osc/base/base.h"
#include "../ompi/mca/io/base/base.h"
#include "../ompi/mca/topo/base/base.h"
#include "../opal/mca/pmix/base/base.h"
#include "../opal/mca/mpool/base/mpool_base_tree.h"
#include "../ompi/mca/pml/base/pml_base_bsend.h"
#include "../ompi/util/timings.h"
#include "../opal/mca/pmix/pmix-internal.h"

int rank;

int MPI_Session_get_res_change(MPI_Session session, MPI_Info *info_used){
    int rc;
    //PARAM CHECK
    rc=ompi_instance_get_res_change(session, (opal_info_t**)info_used);

    //ERROR HANDLING
    
    //ERROR HANDLING
    //OMPI_ERRHANDLER_RETURN (rc, session, rc, FUNC_NAME);
    return rc;
}

int MPI_Session_pset_create_op(MPI_Session session, ompi_psetop_type_t op, char *pset1, char *pset2, char *pset_result){



    int rc;
    //PARAM CHECK

    rc=ompi_instance_pset_create_op(session, pset1, pset2, pset_result, op);

    //ERROR HANDLING
    
    //ERROR HANDLING
    //OMPI_ERRHANDLER_RETURN (rc, session, rc, FUNC_NAME);
    return rc;

}

int init(MPI_Session *session_handle, MPI_Comm *comm, char *main_pset_name){
    int rc;
    MPI_Group wgroup = MPI_GROUP_NULL;
    
    //rc= MPI_Group_from_session_pset (session_handle, main_pset_name, &wgroup);
    /* get a communicator
    rc = MPI_Comm_create_from_group(wgroup, "mpi.forum.example",
                                    MPI_INFO_NULL,
                                    MPI_ERRORS_RETURN,
                                    comm);
    MPI_Group_free(&wgroup);

    if (rc != MPI_SUCCESS) {
	 printf("MPI_comm_create_from_group failed\n");
        MPI_Session_finalize(&session_handle);
        return -1;
    }
    */
    return rc;

    
}


int fetch_resource_change(MPI_Session *session_handle, char delta_pset[], ompi_rc_op_type_t *rc_type, int *incl_flag, int *flag){
    MPI_Info info=MPI_INFO_NULL;
    MPI_Session_get_res_change(session_handle, &info);
    int valuelen;
    MPI_Info_get_valuelen(info, "MPI_INFO_KEY_RC_PSET", &valuelen, flag);
    if(flag){
        int fflag;
        MPI_Info_get(info, "MPI_INFO_KEY_RC_PSET", valuelen, delta_pset, &fflag);
        delta_pset[valuelen]='\0';

        MPI_Info_get_valuelen(info, "MPI_INFO_KEY_RC_TYPE", &valuelen, flag);
        
        MPI_Info_get(info, "MPI_INFO_KEY_RC_TYPE", valuelen, rc_type->type, flag);
        pmix_proc_t *members;
        size_t nmembers;
        ompi_instance_get_pset_membership ((ompi_instance_t *)session_handle, delta_pset, &members, &nmembers);
        *incl_flag = is_pset_member(members, nmembers, ompi_intance_get_pmixid()) ? 1 : 0;
        free(members);
    }
    if(MPI_INFO_NULL!=info){
       MPI_Info_free(&info); 
    }
}

pmix_status_t resource_change_step(MPI_Session *session_handle, MPI_Comm *lib_comm, char *pset_name, char *delta_pset, bool accept_active){
    pmix_status_t rc;
    ompi_rc_op_type_t rc_type;
    if(ompi_instance_get_vpid()==0){
        if(!accept_active){
            int new_rc_available, incl_flag;
            char delta_pset[PMIX_MAX_KEYLEN];
            fetch_resource_change(session_handle, delta_pset, &rc_type, &incl_flag, &new_rc_available);
            if(!new_rc_available)return 0;

            if(0 == strcmp(rc_type.type, MPI_RC_ADD)){
                char pset_result[PMIX_MAX_KEYLEN];
                strcpy(pset_result,pset_name);
                strcat(pset_result, delta_pset);
                MPI_Session_pset_create_op(session_handle, MPI_PSETOP_UNION, pset_name, delta_pset, pset_result);
            }
            //SUB
        }
        MPI_Info *info;
        char *pset_result=malloc(PMIX_MAX_KEYLEN);
        strcpy(pset_result,pset_name);
        strcat(pset_result, delta_pset);
        rc=ompi_instance_accept_res_change(session_handle, (opal_info_t**)info, delta_pset, pset_result);
        if(PMIX_SUCCESS==rc)strcpy(pset_name, pset_result);
        free(pset_result);
        return rc;
    }
    return PMIX_SUCCESS;

}

void work_step(){
    sleep(1);
    printf("    RANK %d: have been working\n", rank);
}

void rebalance_step(){
    printf("    RANK %d: rebalancing\n", rank);
}

int main(int argc, char* argv[])
{
    int  size, len, incl_flag, flag;
    int counter=0;
    char pset_name[PMIX_MAX_KEYLEN];
    strcpy(pset_name, "test1");
    char delta_pset[PMIX_MAX_KEYLEN];
    MPI_Session session_handle;
    MPI_Info info=MPI_INFO_NULL;
    MPI_Comm lib_comm = MPI_COMM_NULL;
    ompi_rc_op_type_t rc_type;
    pmix_status_t rc=PMIX_ERR_NOT_FOUND;
    bool accept_active=false;
    

    MPI_Session_init(MPI_INFO_NULL, MPI_ERRORS_RETURN, &session_handle);
    rank=ompi_instance_get_vpid();
    printf("    RANK %d: Hello World!\n", rank);
    //rc = MPI_Session_get_num_psets (session_handle, MPI_INFO_NULL, &npset_names);
    fetch_resource_change(&session_handle, delta_pset, &rc_type,  &incl_flag, &flag);

    /* dynamically added process, so we need to confrim the resource change */
    if(incl_flag){
        char new_pset[PMIX_MAX_KEYLEN];
        sleep(3);
        printf("    RANK %d: I was added dynamically. Need to confirm \n", rank);
        rc=ompi_instance_confirm_res_change(session_handle, (opal_info_t**)&info, delta_pset, &pset_name);
        if(PMIX_SUCCESS == rc){
            printf("    RANK %d: Confirmation succeeded. Communication will happen via pset: %s\n", rank, pset_name);
        }
        if(MPI_INFO_NULL != info){
            MPI_Info_free(&info);
        }
        MPI_Session_finalize(&session_handle);
    
        return 0;
    }

    /* initialize communication */
    init(&session_handle, &lib_comm, pset_name);

    
    /********************** START OF MAIN LOOP *******************************/
    while(counter++ < 10){
        rebalance_step(&lib_comm);
        work_step();
        rc=resource_change_step(&session_handle, &lib_comm, pset_name, delta_pset, accept_active);
        if(rc==PMIX_ERR_NOT_FOUND){
            accept_active=true;
            printf("    RANK %d: Resource change pending. Communication will happen via pset: %s\n", rank, pset_name);
        }
        else if (rc==PMIX_SUCCESS) {
            accept_active=false;
            printf("    RANK %d: Resource change succeeded. Communication will happen via pset: %s\n", rank, pset_name);
            break;
        }

    }
    /************************ END OF MAIN LOOP ************************************/

            
        
    
    if(MPI_INFO_NULL != info){
        MPI_Info_free(&info);
    }
    
    MPI_Session_finalize(&session_handle);
    
    return 0;
}
