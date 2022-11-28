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
//#pragma weak MPI_Session_confitm_res_change = PMPI_Session_comfirm_res_change
//#endif
//#define MPI_Session_confirm_res_change PMPI_Session_confirm_res_change
//#endif

static const char FUNC_NAME[] = "MPI_Session_rc_handle_create";


int MPI_Session_rc_handle_create(MPI_Session session, MPI_RC_handle *rc_op_handle){
    int rc;
    
    rc = ompi_instance_rc_op_handle_create((ompi_instance_t *) session, (ompi_instance_rc_op_handle_t **) rc_op_handle);
    
    return rc;
}