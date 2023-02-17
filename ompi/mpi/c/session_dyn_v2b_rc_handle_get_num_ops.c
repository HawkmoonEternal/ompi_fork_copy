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

static const char FUNC_NAME[] = "MPI_Session_rc_handle_get_num_ops";


int MPI_Session_dyn_v2b_rc_handle_get_num_ops(MPI_Session session, MPI_RC_handle rc_op_handle, int *num_ops){
    int rc;
    size_t n_ops;

    rc = ompi_instance_dyn_v2b_rc_op_handle_get_num_ops((ompi_instance_t *) session, rc_op_handle, &n_ops);
    
    *num_ops = (int) n_ops;

    OMPI_ERRHANDLER_RETURN (rc, (NULL == session) ? MPI_SESSION_NULL : session, rc, FUNC_NAME);
}