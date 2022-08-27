#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/instance/instance.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/info/info.h"
#include <stdlib.h>
#include <string.h>

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Session_get_res_change = PMPI_Session_get_res_change
#endif
#define MPI_Session_get_res_change PMPI_Session_get_res_change
#endif

static const char FUNC_NAME[] = "MPI_Session_pset_create_op";

int MPI_Session_pset_create_op(MPI_Session session, int op, char *pset1, char *pset2, char *pset_result, MPI_Info *info){

    int rc, flag = 0;
    char pref_name[PMIX_MAX_KEYLEN];
    ompi_rc_op_type_t ompi_op = MPI_OMPI_CONV_PSET_OP(op);
    //PARAM CHECK
    if(NULL != info && MPI_INFO_NULL != *info){
        MPI_Info_get(*info, "MPI_PSETOP_PREF_NAME", PMIX_MAX_KEYLEN, pref_name, &flag);
    }
    if(!flag){
        rc = ompi_instance_pset_create_op(session, pset1, pset2, NULL, pset_result, ompi_op);
    }
    else{
        rc = ompi_instance_pset_create_op(session, pset1, pset2, pref_name, pset_result, ompi_op);
    }
    //ERROR HANDLING
    
    //ERROR HANDLING
    //OMPI_ERRHANDLER_RETURN (rc, session, rc, FUNC_NAME);
    return rc;

}