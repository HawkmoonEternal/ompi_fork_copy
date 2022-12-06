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


int MPI_Session_rc_handle_add_op(MPI_Session session, int rc_type, char **input_names, int n_input_names, char **output_names, int n_output_names, MPI_Info info, MPI_RC_handle rc_op_handle){
    int rc;
    rc = ompi_instance_rc_op_handle_add_op((ompi_instance_t *) session, MPI_OMPI_CONV_PSET_OP(rc_type), input_names, (size_t) n_input_names, output_names, (size_t) n_output_names, (ompi_info_t *) info, (ompi_instance_rc_op_handle_t *) rc_op_handle);
    
    return rc;
}