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

static const char FUNC_NAME[] = "MPI_Session_dyn_integrate_res_change";


int MPI_Session_dyn_integrate_res_change_nb(MPI_Session *session, MPI_Info info, char *delta_pset, char *pset_buf, int provider, int *terminate, MPI_Request *req){
    int rc;
    *req = OBJ_NEW(ompi_request_t);
    OMPI_REQUEST_INIT(*req, false);
    (*req)->req_type = OMPI_REQUEST_DYN;
    (*req)->req_status.MPI_ERROR = MPI_SUCCESS;
    (*req)->req_status.MPI_SOURCE = 0;
    (*req)->req_status.MPI_TAG = 0;
    (*req)->req_state = OMPI_REQUEST_ACTIVE;
    (*req)->req_free = ompi_instance_nb_req_free;

    rc = ompi_instance_integrate_res_change_nb(session, delta_pset, pset_buf, provider, terminate, (ompi_request_t *)*req);
    return rc;
}