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
#pragma weak MPI_Session_request_res_change = PMPI_Session_request_res_change
#endif
#define MPI_Session_get_res_change PMPI_Session_get_res_change
#endif

static const char FUNC_NAME[] = "MPI_Session_request_res_change";


int MPI_Session_request_res_change(MPI_Session session, int delta, char *delta_pset, int rc_type, MPI_Info *info){
    int rc;
    int flag = 0;
    ompi_rc_op_type_t ompi_rc_op_type;
    //PARAM CHECK
    if (NULL == session || MPI_SESSION_NULL == session || NULL == delta_pset) {
            return MPI_ERR_ARG;
    }

    ompi_rc_op_type = MPI_OMPI_CONV_RC_OP(rc_type);

    rc = ompi_instance_request_res_change(session, delta, delta_pset, rc_type, info);


    //ERROR HANDLING
    
    
    
    return rc;
}
