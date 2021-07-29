/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 *
 * Sample MPI "hello world" application in C
 */

#include <stdio.h>
#include <time.h>
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

const int PROBLEM_SIZE = 1000000;
const int ITER_MAX = 200;
int num_procs;
int my_work_rank;
int start_index;
int end_index;

int rank; 

int MPI_Session_get_res_change(MPI_Session session, MPI_Info *info_used){
    int rc;
    //PARAM CHECK
    rc=ompi_instance_get_res_change(session, (opal_info_t**)info_used, true);

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

int MPI_Session_accept_res_change(MPI_Session *session, MPI_Info *info, char delta_pset[], char result_pset[] ){
    int rc;
    rc=ompi_instance_accept_res_change(session, (opal_info_t**)info, delta_pset, result_pset);
    return rc;
}

int MPI_Session_confirm_res_change(MPI_Session *session, MPI_Info *info, char delta_pset[], char *result_pset[] ){
    int rc;
    rc=ompi_instance_confirm_res_change(session, (opal_info_t**)info, delta_pset, result_pset);
    return rc;
}

/* create a communicator from the given process set */
int init(MPI_Session *session_handle, MPI_Comm *comm, char *main_pset_name){
    int rc;
    MPI_Group wgroup = MPI_GROUP_NULL;
    //ompi_instance_pset_fence(session_handle, main_pset_name);
    rc= MPI_Group_from_session_pset (session_handle, main_pset_name, &wgroup);
    size_t size;
    MPI_Group_size(wgroup, &size);

    //ompi_instance_pset_fence(session_handle, main_pset_name);
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
    num_procs=size;
    MPI_Comm_rank(*comm, &my_work_rank);

    return rc;

    
}

int fetch_resource_change(MPI_Session *session_handle, char delta_pset[], ompi_rc_op_type_t *rc_type, int *incl_flag, int *flag){
    MPI_Info info=MPI_INFO_NULL;
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 10000;

    /* request current resource change data from host */
    int rc=MPI_Session_get_res_change(session_handle, &info);
    if(rc!=OPAL_SUCCESS){
        *incl_flag=0;
        *flag=0;
        return rc;
    }

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

        do{
            rc=ompi_instance_get_pset_membership ((ompi_instance_t *)session_handle, delta_pset, &members, &nmembers);
            if(rc==PMIX_ERR_NOT_FOUND){
                nanosleep(&ts, NULL);
            }
        }while(rc==PMIX_ERR_NOT_FOUND);
        
        *incl_flag = is_pset_member(members, nmembers, ompi_intance_get_pmixid()) ? 1 : 0;
        free(members);
    }
    if(MPI_INFO_NULL!=info){
       MPI_Info_free(&info); 
    }
    return MPI_SUCCESS;
}

pmix_status_t resource_change_step(MPI_Session *session_handle, MPI_Comm *lib_comm, char *pset_name, char *delta_pset, char *pset_result, bool accept_active){
    pmix_status_t rc;
    ompi_rc_op_type_t rc_type;
    int new_rc_available, incl_flag;
    int my_rank;


    MPI_Comm_rank(*lib_comm, &my_rank);

    /*****************fetch resource change****************************/
    
    fetch_resource_change(session_handle, delta_pset, &rc_type, &incl_flag, &new_rc_available);
    if(!new_rc_available)return PMIX_ERR_NOT_FOUND;
    /*****************end of fetch resource change**********************/    
    
    if(!accept_active){

        
        /******************handle resource change***************************/

        if(my_rank==0){
            if(0 == strcmp(rc_type.type, MPI_RC_ADD)){

                strcpy(pset_result,pset_name);
                strcat(pset_result, delta_pset);
                MPI_Session_pset_create_op(session_handle, MPI_PSETOP_UNION, pset_name, delta_pset, pset_result);
                
            }else if(0 == strcmp(rc_type.type, MPI_RC_SUB)){
                if(incl_flag){
                    printf("    RANK %d preparing for termination");
                }
                
                strcpy(pset_result,pset_name);
                strcat(pset_result, delta_pset);
                MPI_Session_pset_create_op(session_handle, MPI_PSETOP_DIFFERENCE, pset_name, delta_pset, pset_result);
                
            } 
        }
        /* Broadcast the name of the new pset */        
        MPI_Bcast(pset_result, PMIX_MAX_KEYLEN, MPI_BYTE, 0, *lib_comm);

    }

    
    /***************** end of handle resource change *******************/

    /*********************** accept resource change *************************/

    if(my_rank==0){
        MPI_Info *info;
        rc=MPI_Session_accept_res_change(session_handle, info, delta_pset, pset_result);
    }
    /* broadcast the status of the accept */
    MPI_Bcast(&rc, 1, MPI_INT, 0, *lib_comm);

    if(PMIX_SUCCESS==rc){
        MPI_Info *info;
        strcpy(pset_name, pset_result);
        ompi_instance_clear_rc_cache();

        /* If we want to continue running we need to refresh the instance */ 
        if( 0!=strcmp(rc_type.type,MPI_RC_SUB) || !incl_flag){
            ompi_mpi_instance_refresh (session_handle, info, rc_type, delta_pset);
        }
    }
    else if(PMIX_ERR_NOT_FOUND == rc)return MPI_ERR_PENDING;
    /****************** end of accept resource change *************************/

    /* now we can destroy the old communicator */
    if(MPI_COMM_NULL!= lib_comm)MPI_Comm_free(lib_comm);

    if(0==strcmp(rc_type.type,MPI_RC_SUB)){
        
        /* if we were included in the resource substraction we need to finalize now */
        if(incl_flag){
            return PMIX_SUCCESS;
        }
    }

    /* construct a new communicator from the new pset */
    if(PMIX_SUCCESS==rc){
        printf("    RANK %d: creating communicator for pset %s\n", my_work_rank, pset_name);
        rc=init(session_handle, lib_comm, pset_name);
    }

    return rc;

}


int work_step(){
    int result;
    int n,i;
    for(n=start_index; n<end_index; n++){
        result=1;
        for(i=1; i<=100; i++){
            if(i%2 == 0)result*=i;
            else result/=i;
        }
    }
    printf("    RANK %d: finished work\n", my_work_rank);
    return result;
    
}

void rebalance_step(){
    int chunk_size=PROBLEM_SIZE/num_procs;
    start_index = chunk_size*my_work_rank;
    end_index   = (my_work_rank==num_procs-1)   ? PROBLEM_SIZE  : (my_work_rank+1)*chunk_size;
    printf("    RANK %d/%d: rebalancing:  range[%d,%d]\n", my_work_rank,num_procs-1,start_index, end_index);
}

int main(int argc, char* argv[])
{
    int  size, len, incl_flag, flag, counter=0;
    char pset_name[PMIX_MAX_KEYLEN];
    char delta_pset[PMIX_MAX_KEYLEN];
    char pset_result[PMIX_MAX_KEYLEN]={0};
    
    MPI_Session session_handle;
    MPI_Info info=MPI_INFO_NULL;
    MPI_Comm lib_comm = MPI_COMM_NULL;
    ompi_rc_op_type_t rc_type;
    pmix_status_t rc=PMIX_ERR_NOT_FOUND;
    bool accept_active=false;
    strcpy(pset_name, "test1");
    int init_ret=MPI_Session_init(MPI_INFO_NULL, MPI_ERRORS_RETURN, &session_handle);
    printf("MPI_SESSION_init finished: %d\n", init_ret);
    rank=ompi_proc_local_proc->super.proc_name.vpid;
    //sleep(5);    
    /* check if there is a resource change right at the beginning */
    
    int ret=fetch_resource_change(&session_handle, delta_pset, &rc_type,  &incl_flag, &flag);
    /* if we are included in the delta_pset we are a dynamically added process, so we need to confirm the resource change */
    if(flag && incl_flag){
        char new_pset[PMIX_MAX_KEYLEN];
        printf("    DELTA PSET RANK %d: I was added dynamically. Need to confirm \n", rank);
        rc=MPI_Session_confirm_res_change(session_handle, &info, delta_pset, &pset_name);
        if(PMIX_SUCCESS == rc){
            printf("    DELTA PSET RANK %d: Confirmation succeeded. Communication will happen via pset: %s\n", rank, pset_name);
        }else{
            printf("    DELTA PSET RANK %d: Confirmation failed. ERROR: %d\n", rank, rc);
        }
        init(&session_handle, &lib_comm, pset_name);
        printf("    DELTA PSET RANK %d: Init finished\n", rank);
        /* get the current iteration from the main rank */
        MPI_Bcast(&counter, 1, MPI_INT, 0, lib_comm);
        printf("    RANK %d: received counter value %d from root in new communicator\n", my_work_rank, counter);

    }else{
        /* initialize communication */
        init(&session_handle, &lib_comm, pset_name);
        rc=MPI_SUCCESS;
    }
    
    /********************** START OF MAIN LOOP *******************************/
    while(counter++< ITER_MAX){
        if(my_work_rank==0)printf("\n START OF ITERATION: %d\n", counter);
        /* Rebalance */
        if(rc == MPI_SUCCESS){
            rebalance_step(&lib_comm);
        }

        /* Work */
        MPI_Barrier(lib_comm);
        clock_t start=clock();
        work_step();
        MPI_Barrier(lib_comm);
        int msec=(clock()-start)*1000/CLOCKS_PER_SEC;
        if(my_work_rank==0)printf("--> work_step needed %d sec, %d msec\n", msec/1000, msec%1000);

        /* Resource Change */
        start=clock();
        rc=resource_change_step(&session_handle, &lib_comm, pset_name, delta_pset, pset_result, accept_active);
        msec=(clock()-start)*1000/CLOCKS_PER_SEC;
        if(my_work_rank==0)printf("--> resource_change_step needed %d sec, %d msec\n", msec/1000, msec%1000);
        if(rc==MPI_ERR_PENDING){
            accept_active=true;
            printf("    RANK %d: Resource change pending. Communication will still happen via pset: %s\n", my_work_rank, pset_name);
        }
        else if (rc==MPI_SUCCESS) {
            accept_active=false;

            /* terminate substracted processes */
            if(lib_comm==MPI_COMM_NULL){
                printf("    Old RANK %d: Resource change succeeded. I am not needed anymore. Goodbye\n", my_work_rank);
                if(MPI_INFO_NULL != info){
                        MPI_Info_free(&info);
                }
                MPI_Session_finalize(session_handle);

                return 0;
            }
            
            printf("    RANK %d: Resource change succeeded. Communication will now happen via pset: %s\n", my_work_rank, pset_name);
            MPI_Bcast(&counter, 1, MPI_INT, 0, lib_comm);
        }

    }
    /************************ END OF MAIN LOOP ************************************/

            
        
    
    if(MPI_INFO_NULL != info){
        MPI_Info_free(&info);
    }
    
    MPI_Session_finalize(&session_handle);
    
    return 0;
}
