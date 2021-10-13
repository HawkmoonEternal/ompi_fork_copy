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
#include <unistd.h>
/*
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
*/

 
#pragma region
/* New MPI API 
int MPI_Session_get_res_change(MPI_Session session, ompi_rc_op_type_t *type, char *delta_pset, bool *incl, ompi_rc_status_t *status, MPI_Info *info_used){
    int rc;
    char bound_pset[PMIX_MAX_KEYLEN];
    int flag = 0;

    //PARAM CHECK
    if(NULL != info_used && MPI_INFO_NULL != info_used){
        ompi_info_get (info_used, "MPI_RC_BOUND_PSET", bound_pset, &flag);
        printf("extracted %s from info object\n");
    }
    if (!flag) {
        rc = ompi_instance_get_res_change(session, NULL, type, delta_pset, incl, status, (opal_info_t**)info_used, true);
    }else{
        rc = ompi_instance_get_res_change(session, bound_pset, type, delta_pset, incl, status, (opal_info_t**)info_used, true);
    }


    
    //ERROR HANDLING
    
    //ERROR HANDLING
    OMPI_ERRHANDLER_RETURN (rc, session, rc, "MPI_Session_get_res_change");
}


int MPI_Session_pset_create_op(MPI_Session session, ompi_psetop_type_t op, char *pset1, char *pset2, char *pset_result, MPI_Info *info){



    int rc, flag = 0;
    char pref_name[PMIX_MAX_KEYLEN];
    //PARAM CHECK
    if(NULL != info && MPI_INFO_NULL != info){
         ompi_info_get (info, "MPI_PSETOP_PREF_NAME", pref_name, &flag);
    }
    if(!flag){
        rc = ompi_instance_pset_create_op(session, pset1, pset2, NULL, pset_result, op);
    }
    else{
        rc = ompi_instance_pset_create_op(session, pset1, pset2, pref_name, pset_result, op);
    }
    //ERROR HANDLING
    
    //ERROR HANDLING
    OMPI_ERRHANDLER_RETURN (rc, session, rc, "MPI_Session_pset_create_op");

}

int MPI_Session_accept_res_change(MPI_Session *session, MPI_Info *info, char delta_pset[], char result_pset[], int root, MPI_Comm comm){
    int rc;
    int my_rank;
    char d_pset[PMIX_MAX_KEYLEN];
    ompi_rc_op_type_t rc_type;

    MPI_Comm_rank(comm, &my_rank);

    if(my_rank == root){
        ompi_instance_get_rc_type(delta_pset,  &rc_type);
        rc = ompi_instance_accept_res_change(session, (opal_info_t**)info, delta_pset, result_pset);
    }
    MPI_Bcast(&rc, 1, MPI_INT, 0, comm);
    if(MPI_SUCCESS == rc){
        if(my_rank==root){
            strcpy(d_pset, delta_pset);
        }
        MPI_Bcast(d_pset, PMIX_MAX_KEYLEN, MPI_CHAR, root, comm);
        MPI_Bcast(&rc_type, 1, MPI_UINT8_T, root, comm);

        if(MPI_SUCCESS != (rc = ompi_mpi_instance_refresh (session, info, d_pset, rc_type))){
            return rc;
        }
        ompi_instance_clear_rc_cache(delta_pset);
    }
    else if(OPAL_ERR_NOT_FOUND == rc)return MPI_ERR_PENDING;

    return rc;
}

int MPI_Session_confirm_res_change(MPI_Session *session, MPI_Info *info, char delta_pset[], char *result_pset[] ){
    int rc;
    rc=ompi_instance_confirm_res_change(session, (opal_info_t**)info, delta_pset, result_pset);
    return rc;
}
*/
#pragma endregion

/*-------------------------------- HERE BEGINS THE MAIN FILE ------------------------------------------- */

const int PROBLEM_SIZE = 1000000;
const int ITER_MAX = 200;
int num_procs;
int my_work_rank;
int start_index;
int end_index;
int rank; 

/* create a communicator from the given process set */
#pragma region
int init(MPI_Session *session_handle, MPI_Comm *comm, char *main_pset_name){
    clock_t start;
    int rc;
    MPI_Group wgroup = MPI_GROUP_NULL;



    /* create a group from pset */
    rc= MPI_Group_from_session_pset (session_handle, main_pset_name, &wgroup);
    MPI_Group_size(wgroup, &num_procs);
    printf("group size: %d\n", num_procs);
    /* create a communicator from group */
    start=clock();
    if(MPI_SUCCESS != (rc = MPI_Comm_create_from_group(wgroup, "mpi.forum.example", MPI_INFO_NULL, MPI_ERRORS_RETURN, comm))){
        	    printf("MPI_comm_create_from_group failed\n");
        MPI_Session_finalize(&session_handle);
        return -1;
    }
    int msec=(clock()-start)*1000/CLOCKS_PER_SEC;
  
    /* new rank & size */
    MPI_Comm_size(*comm, &num_procs);
    MPI_Comm_rank(*comm, &my_work_rank);

    if(0 == my_work_rank){
        printf("--> RANK %d comm_create_from_group needed %d sec, %d msec\n",my_work_rank, msec/1000, msec%1000);
    }
    MPI_Group_free(&wgroup);
    return rc;
}

#pragma endregion
 

int resource_change_step(MPI_Session *session_handle, MPI_Comm *lib_comm, char *pset_name, char *delta_pset, char *pset_result){
    int rc;
    int rc_type;
    int new_rc_available;
    int incl_flag;
    int my_rank;
    MPI_Info info = MPI_INFO_NULL;
    int status;
    char dummy_pset_name[] = "test1";
    char prefname[MPI_MAX_PSET_NAME_LEN-1]; 

    MPI_Comm_rank(*lib_comm, &my_rank);

    /*****************fetch resource change****************************/
    if(my_rank==0){
        MPI_Info_create(&info);
        MPI_Info_set(info, "MPI_RC_BOUND_PSET", pset_name);
        rc = MPI_Session_get_res_change(session_handle, &rc_type, delta_pset, &incl_flag, &status, &info);
        if(rc_type == MPI_RC_NULL || (status != MPI_RC_ANNOUNCED && status != MPI_RC_CONFIRMATION_PENDING)){
            rc = MPI_ERR_OTHER;
        }
        MPI_Info_free(&info);
    }
    /*****************end of fetch resource change**********************/    
    
    /******************handle resource change***************************/
    if(my_rank == 0 && rc_type != MPI_RC_NULL && status == MPI_RC_ANNOUNCED){   
        /* create a new pset name */ 
        strcpy(prefname,dummy_pset_name);
        strcat(prefname, delta_pset);
        
        MPI_Info_create(&info);
        MPI_Info_set(info, "MPI_PSETOP_PREF_NAME", prefname);

        if(rc_type ==  MPI_RC_ADD){
            
            MPI_Session_pset_create_op(session_handle, MPI_PSETOP_UNION, pset_name, delta_pset, pset_result, &info);
            
        }else if(rc_type == MPI_RC_SUB){
            MPI_Session_pset_create_op(session_handle, MPI_PSETOP_DIFFERENCE, pset_name, delta_pset, pset_result, &info);
        }

        MPI_Info_free(&info);
    }
    /***************** end of handle resource change *******************/ 

    /* Root needs to tell other processes about the resource change */
    MPI_Bcast(&rc, 1, MPI_INT, 0, *lib_comm);
    if(MPI_ERR_OTHER == rc)return rc;
    MPI_Bcast(pset_result, 511, MPI_CHAR, 0 , *lib_comm);

    /*********************** accept resource change *************************/
    rc = MPI_Session_accept_res_change(session_handle, &info, delta_pset, pset_result, 0, lib_comm);
    /*********************** end of accept resource change *************************/

    /* if successful create new communicator, else need to break (here indicated by MPI_COMM_NULL)*/
    if(MPI_SUCCESS == rc){
        if(MPI_COMM_NULL != *lib_comm){
            MPI_Comm_free(lib_comm);
            strcpy(pset_name, pset_result);
            printf("reinit\n");
            rc = init(session_handle, lib_comm, pset_name);
            printf("reinit success\n");
        }else{
            return MPI_SUCCESS;
        }
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
	char host[256];
	gethostname(host, 256);
    int  size, len, flag, npsets, counter=0;
    char pset_name[MPI_MAX_PSET_NAME_LEN-1];
    char app_pset_name[MPI_MAX_PSET_NAME_LEN-1];
    char delta_pset[MPI_MAX_PSET_NAME_LEN-1];
    char pset_result[MPI_MAX_PSET_NAME_LEN-1]={0};
    int incl_flag = 0;
    
    MPI_Session session_handle;
    MPI_Info info = MPI_INFO_NULL;
    MPI_Comm lib_comm = MPI_COMM_NULL;
 
    int rc_type;
    int rc_status;
    int rc = -16;

    strcpy(pset_name, "test1");
    //printf("MPI_Session_init\n");
    /* initialize the session */
    int init_ret = MPI_Session_init(MPI_INFO_NULL, MPI_ERRORS_RETURN, &session_handle);
    rank = 0; 
    //MPI_Session_get_num_psets(session_handle, info, &npsets);
    strcpy(pset_result,pset_name);
    /*
    if(ompi_proc_local_proc->super.proc_name.vpid==0){
        printf("pset_op\n");
        MPI_Session_pset_create_op(session_handle, MPI_PSETOP_UNION, pset_name, pset_name, pset_result);
    }
    */;
    

    /* check if there is a resource change right at the beginning */
    MPI_Session_get_res_change(session_handle, &rc_type, delta_pset, &incl_flag, &rc_status, &info);

    /* if we are included in the delta_pset we are a dynamically added process, so we need to confirm the resource change */
    if(rc_type != MPI_RC_NULL && rc_status == MPI_RC_ANNOUNCED && incl_flag){
        printf("    DELTA PSET RANK %d: I was added dynamically. Need to confirm \n", rank);
        char pset_name_fresh[MPI_MAX_PSET_NAME_LEN];
        char **pset_ptr = &pset_name_fresh;
        rc = MPI_Session_confirm_res_change(session_handle, &info, delta_pset, &pset_ptr);
        strcpy(pset_name, pset_name_fresh);
        if(MPI_SUCCESS == rc){
            printf("    DELTA PSET RANK %d: Confirmation succeeded. Communication will happen via pset: %s\n", rank, pset_name);
        }else{
            printf("    DELTA PSET RANK %d: Confirmation failed. ERROR: %d\n", rank, rc);
        }
        init(&session_handle, &lib_comm, pset_name);

        /* get the current iteration from the main rank */
        MPI_Bcast(&counter, 1, MPI_INT, 0, lib_comm);
        printf("    RANK %d: received counter value %d from root in new communicator\n", my_work_rank, counter);

    }else{
        /* initialize communication */
        init(&session_handle, &lib_comm, pset_name);
        rc = MPI_SUCCESS;
    }
    
    /********************** START OF MAIN LOOP *******************************/
    while(counter++< ITER_MAX){
        if(my_work_rank==0)printf("\n START OF ITERATION: %d\n", counter);
        /* Rebalance */
        if(rc == MPI_SUCCESS){
            rebalance_step(&lib_comm);
        }

        /* Work step */
        MPI_Barrier(lib_comm);
        clock_t start=clock();
        work_step();
        MPI_Barrier(lib_comm);
        int msec=(clock()-start)*1000/CLOCKS_PER_SEC;
        if(my_work_rank==0)printf("--> work_step with %d processes needed %d sec, %d msec\n", num_procs, msec/1000, msec%1000);

        /* Resource Change step */
        start=clock();
        rc=resource_change_step(&session_handle, &lib_comm, pset_name, delta_pset, pset_result);
        msec=(clock()-start)*1000/CLOCKS_PER_SEC;
        if(my_work_rank==0)printf("--> resource_change_step needed %d sec, %d msec\n", msec/1000, msec%1000);

        /* Data redistribution */
        if(rc == MPI_ERR_PENDING){
            printf("    RANK %d: Resource change pending. Communication will still happen via pset: %s\n", my_work_rank, pset_name);
        }else if (rc == MPI_SUCCESS) {
            /* terminate substracted processes */
            if(lib_comm == MPI_COMM_NULL){
                printf("    Old RANK %d: Resource change succeeded. I am not needed anymore. Goodbye!\n", my_work_rank);
                break;
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
