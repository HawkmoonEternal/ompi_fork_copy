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


int MPI_Session_get_res_change(MPI_Session session, char *pset_name, MPI_RC_TYPE *type, char *delta_pset, int *incl, MPI_RC_STATUS *status, MPI_Info *info_used);{
    int rc;
    //PARAM CHECK
    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (NULL == session || MPI_SESSION_NULL == session) {
            return MPI_ERR_ARG;
        }
        if (NULL == info_used) {
            return OMPI_ERRHANDLER_INVOKE (session, MPI_ERR_INFO, FUNC_NAME);
        }
    }

    uint8_t type_unsigned;
    bool inluded;

    rc=ompi_instance_get_res_change(session, pset_name, &type_unsigned, delta_pset, &included, status, (opal_info_t**)info_used, true);
    *incl= included ? 1 : 0;
    *type=(int)type_unsigned;

    //ERROR HANDLING
    
    //ERROR HANDLING
    OMPI_ERRHANDLER_RETURN (rc, session, rc, FUNC_NAME);
}
