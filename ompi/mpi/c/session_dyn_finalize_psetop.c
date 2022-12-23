#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/instance/instance.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/info/info.h"
#include <stdlib.h>
#include <string.h>

//#if OMPI_BUILD_MPI_PROFILING
//#if OPAL_HAVE_WEAK_SYMBOLS
//#pragma weak MPI_Session_get_res_change = PMPI_Session_get_res_change
//#endif
//#define MPI_Session_get_res_change PMPI_Session_get_res_change
//#endif

static const char FUNC_NAME[] = "MPI_Session_dyn_recv_change";


int MPI_Session_dyn_finalize_psetop(MPI_Session session, char * pset_name){
    int rc;

    //PARAM CHECK
    if (NULL == session || MPI_SESSION_NULL == session || NULL == pset_name) {
            return MPI_ERR_ARG;
    }

    rc = ompi_instance_dyn_finalize_psetop((ompi_instance_t *) session, pset_name);
    
    return rc;
}
