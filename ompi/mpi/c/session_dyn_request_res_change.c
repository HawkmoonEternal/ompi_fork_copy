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
//#pragma weak MPI_Session_request_res_change = PMPI_Session_request_res_change
//#endif
//#define MPI_Session_request_res_change PMPI_Session_request_res_change
//#endif

static const char FUNC_NAME[] = "MPI_Session_dyn_request_res_change";


int MPI_Session_dyn_request_res_change(MPI_Session session, MPI_RC_handle rc_handle){
    int rc;

    rc = ompi_instance_request_res_changes_v23((ompi_instance_t *)session, rc_handle);

    //ERROR HANDLING
       
    return rc;
}
