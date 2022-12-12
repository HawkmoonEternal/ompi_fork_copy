/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2018      Triad National Security, LLC.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#if !defined(OMPI_INSTANCE_PSETS_H)
#define OMPI_INSTANCE_PSETS_H

#include "opal/class/opal_object.h"
#include "opal/class/opal_hash_table.h"
#include "opal/util/info_subscriber.h"
#include "ompi/errhandler/errhandler.h"
#include "opal/mca/threads/mutex.h"
#include "ompi/communicator/comm_request.h"

#include "mpi.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/info/info.h"
#include "ompi/proc/proc.h"

#define PREDEFINED_RC_HANDLE_PAD 512



/* Psets*/
typedef uint8_t ompi_psetop_type_t;
#define OMPI_PSETOP_NULL PMIX_PSETOP_NULL
#define OMPI_PSETOP_UNION PMIX_PSETOP_UNION
#define OMPI_PSETOP_DIFFERENCE PMIX_PSETOP_DIFFERENCE
#define OMPI_PSETOP_INTERSECTION PMIX_PSETOP_INTERSECTION


struct ompi_pset_t{
    opal_list_item_t super;
    char name[PMIX_MAX_KEYLEN];
    char *alias;
    size_t size;
    bool malleable;
    bool active;
    opal_process_name_t *members;
};
typedef struct ompi_pset_t ompi_mpi_instance_pset_t;


int ompi_instance_psets_init();

int ompi_instance_get_launch_pset(char **pset_name, pmix_proc_t *proc);

int ompi_instance_builtin_psets_init(int n_builtin_psets, char **names, opal_process_name_t **members, size_t *nmembers, char **aliases);

int ompi_instance_psets_finalize();

bool ompi_instance_sets_initalized();

void ompi_instance_lock_rc_and_psets();

void ompi_instance_unlock_rc_and_psets();

void ompi_instance_lock_rc_and_psets();


void pset_define_handler(size_t evhdlr_registration_id, pmix_status_t status,
                       const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
                       pmix_info_t results[], size_t nresults,
                       pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata);

void pset_delete_handler(size_t evhdlr_registration_id, pmix_status_t status,
                       const pmix_proc_t *source, pmix_info_t info[], size_t ninfo,
                       pmix_info_t results[], size_t nresults,
                       pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata);

OBJ_CLASS_DECLARATION(ompi_mpi_instance_pset_t);

/* Util */
int opal_pmix_proc_array_conv(opal_process_name_t *opal_procs, pmix_proc_t **pmix_procs, size_t nprocs);
int pmix_opal_proc_array_conv(pmix_proc_t *pmix_procs, opal_process_name_t **opal_procs, size_t nprocs);
int refresh_pmix_psets (const char *key);

/* PSet Functions */
size_t get_num_builtin_psets(void);
size_t get_num_pmix_psets(void);
size_t get_nth_pset_name_length(int n);
char * get_nth_pset_name(int n, char *pset_name, size_t len);

int add_pset(ompi_mpi_instance_pset_t *pset);

ompi_mpi_instance_pset_t * get_pset_by_name(char *name);
ompi_mpi_instance_pset_t * get_nth_pset(int n);
bool is_pset_leader(pmix_proc_t *pset_members, size_t nmembers, pmix_proc_t proc);
bool is_pset_member(pmix_proc_t *pset_members, size_t nmembers, pmix_proc_t proc);
bool opal_is_pset_member( opal_process_name_t *procs, size_t nprocs, opal_process_name_t proc);
bool opal_is_pset_member_local( char *pset_name, opal_process_name_t proc);
int get_pset_size(char *pset_name, size_t *pset_size);

/* PSet Membership */
int get_pset_membership (char *pset_name, opal_process_name_t **members, size_t *nmembers);
void get_pset_membership_complete(pmix_status_t status, pmix_info_t *results, size_t nresults, void *cbdata, pmix_release_cbfunc_t release_fn, void *release_cbdata);
int ompi_instance_free_pset_membership (char *pset_name);

/* PSet Fence */
static void fence_release(pmix_status_t status, void *cbdata);
int ompi_instance_pset_fence(char *pset_name);
int ompi_instance_pset_fence_multiple( char **pset_names, int num_psets, ompi_info_t *info);

#endif /* !defined(OMPI_INSTANCE_PSETS_H) */
