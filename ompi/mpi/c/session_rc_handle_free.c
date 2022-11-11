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


int MPI_Session_rc_handle_free(MPI_Session session, MPI_RC_handle *rc_op_handle){
    int rc;
    
    if(NULL == rc_op_handle){
        return OMPI_ERRHANDLER_NOHANDLE_INVOKE(MPI_ERR_ARG,
                                          FUNC_NAME);
    }
    rc = ompi_instance_rc_op_handle_free((ompi_instance_t *) session, (ompi_instance_rc_op_handle_t **) rc_op_handle);
    
    return rc;
}