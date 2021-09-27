/*
 * Copyright (c) 2017 Amazon.com, Inc. or its affiliates.  All Rights
 *                    reserved.
 * Copyright (c) 2018      Cisco Systems, Inc.  All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "opal_config.h"

#include "reachable_shared.h"

#include "opal/class/opal_list.h"
#include "opal/mca/reachable/reachable.h"
#include "opal/runtime/opal.h"
#include "opal/util/if.h"

/*
 * Creates list of remote interfaces for testing reachability.
 * Only minimum information is filled out.
 */
opal_list_t *build_if_list(void)
{
    /* Allocate memory for and create interface list */
    opal_list_t *if_list = OBJ_NEW(opal_list_t);
    opal_if_t *intf;

    /*
     * Add localhost to list
     */
    intf = create_if(AF_INET, "127.0.0.1", 8, 0);
    opal_list_append(if_list, &(intf->super));

    /*
     * Add localhost with non-standard address
     */
    intf = create_if(AF_INET, "127.31.41.59", 8, 0);
    opal_list_append(if_list, &(intf->super));

    /*
     * Add another localhost with non-standard address
     */
    intf = create_if(AF_INET, "127.26.53.58", 8, 0);
    opal_list_append(if_list, &(intf->super));

    /*
     * Google's public DNS
     */
    intf = create_if(AF_INET, "8.8.8.8", 16, 0);
    opal_list_append(if_list, &(intf->super));

    /*
     * Google's public DNS (2)
     */
    intf = create_if(AF_INET, "8.8.4.4", 16, 0);
    opal_list_append(if_list, &(intf->super));

    /*
     * IPv6: Google's public DNS (IPv6)
     */
    intf = create_if(AF_INET6, "2001:4860:4860::8888", 64, 0);
    opal_list_append(if_list, &(intf->super));

    /*
     * IPv6: Google's public DNS 2 (IPv6)
     */
    intf = create_if(AF_INET6, "2001:4860:4860::8844", 128, 0);
    opal_list_append(if_list, &(intf->super));

    /*
     * IPv6: Google's public DNS 1 (IPv6) EXPLICIT ADDRESS
     */
    intf = create_if(AF_INET6, "2001:4860:4860:0:0:0:0:8888", 64, 0);
    opal_list_append(if_list, &(intf->super));

    /*
     * IPv6: Google's public DNS 2 (IPv6) EXPLICIT ADDRESS
     */
    intf = create_if(AF_INET6, "2001:4860:4860:0:0:0:0:8844", 64, 0);
    opal_list_append(if_list, &(intf->super));

    /*
     * IPv6: something that should be on the same link local...
     */
    intf = create_if(AF_INET6, "fe80::0001", 64, 0);
    opal_list_append(if_list, &(intf->super));

    return if_list;
}

int main(int argc, char **argv)
{
    opal_list_t *local_list, *remote_list;
    opal_reachable_t *results;
    uint32_t i, j;
    int successful_connections = 0;
    int local_ifs;
    int remote_ifs;
    opal_if_t *local_if;

    opal_init(&argc, &argv);

    /* List of interfaces generated by opal */
    local_list = &opal_if_list;
    /* Create test interfaces */
    remote_list = build_if_list();

    local_ifs = opal_list_get_size(local_list);
    remote_ifs = opal_list_get_size(remote_list);

    /* Tests reachability by looking up entries in routing table.
     * Tests routes to localhost and google's nameservers.
     */
    results = opal_reachable.reachable(local_list, remote_list);

    printf("Local interfaces:\n");
    i = 0;
    OPAL_LIST_FOREACH (local_if, local_list, opal_if_t) {
        char addr[128];
        char *family;

        switch (local_if->af_family) {
        case AF_INET:
            family = "IPv4";
            inet_ntop(AF_INET, &(((struct sockaddr_in *) &local_if->if_addr))->sin_addr, addr,
                      sizeof(addr));
            break;
        case AF_INET6:
            family = "IPv6";
            inet_ntop(AF_INET6, &(((struct sockaddr_in6 *) &local_if->if_addr))->sin6_addr, addr,
                      sizeof(addr));
            break;
        default:
            family = "Unknown";
            opal_string_copy(addr, "Unknown", sizeof(addr));
            break;
        }

        printf("  %3d: %s\t%s\t%s/%d\n", i, local_if->if_name, family, addr, local_if->if_mask);
        i++;
    }

    printf("\nRemote interfaces:\n");
    i = 0;
    OPAL_LIST_FOREACH (local_if, remote_list, opal_if_t) {
        char addr[128];
        char *family;

        switch (local_if->af_family) {
        case AF_INET:
            family = "IPv4";
            inet_ntop(AF_INET, &(((struct sockaddr_in *) &local_if->if_addr))->sin_addr, addr,
                      sizeof(addr));
            break;
        case AF_INET6:
            family = "IPv6";
            inet_ntop(AF_INET6, &(((struct sockaddr_in6 *) &local_if->if_addr))->sin6_addr, addr,
                      sizeof(addr));
            break;
        default:
            family = "Unknown";
            opal_string_copy(addr, "Unknown", sizeof(addr));
            break;
        }

        printf("  %3d: %s\t%s\t%s/%d\n", i, local_if->if_name, family, addr, local_if->if_mask);
        i++;
    }

    printf("\nConnectivity Table:\n       ");
    for (j = 0; j < remote_ifs; j++) {
        printf("%3d ", j);
    }
    printf("\n");

    for (i = 0; i < local_ifs; i++) {
        printf("  %3d: ", i);
        for (j = 0; j < remote_ifs; j++) {
            printf("%3d ", results->weights[i][j]);
        }
        printf("\n");
    }
    printf("\n");

    OBJ_RELEASE(remote_list);

    opal_output(0, "Passed all tests!\n");
    return 0;
}
