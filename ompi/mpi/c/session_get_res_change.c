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

static const char FUNC_NAME[] = "MPI_Session_get_res_change";


int MPI_Session_get_res_change(MPI_Session session, int *type, char *delta_pset, int *incl, int *status, MPI_Info *info_used){
    int rc;
    char bound_pset[PMIX_MAX_KEYLEN];
    int flag = 0;
    ompi_rc_op_type_t ompi_rc_op_type;
    //PARAM CHECK
    if (NULL == session || MPI_SESSION_NULL == session) {
            return MPI_ERR_ARG;
    }

    //PARAM CHECK
    
    if(NULL != info_used && MPI_INFO_NULL != *info_used){
        MPI_Info_get(*info_used, "MPI_RC_BOUND_PSET", PMIX_MAX_KEYLEN, bound_pset, &flag);
    }
    if (!flag) {
        rc = ompi_instance_get_res_change(session, NULL, &ompi_rc_op_type, delta_pset, incl, status, (opal_info_t**)info_used, true);
    }else{
        rc = ompi_instance_get_res_change(session, bound_pset, &ompi_rc_op_type, delta_pset, incl, status, (opal_info_t**)info_used, true);
    }
    if(rc == OPAL_ERR_NOT_FOUND || rc == OMPI_SUCCESS){
        rc = MPI_SUCCESS;
    }
    //ERROR HANDLING
    
    *type = MPI_OMPI_CONVT_RC_OP(ompi_rc_op_type);
    
    return rc;
}
