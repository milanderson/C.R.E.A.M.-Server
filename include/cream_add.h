#ifndef CREAM_ADD_H
#define CREAM_ADD_H

#define LISTENQ 40
#define CMSGSIZE MAX_KEY_SIZE + MAX_VALUE_SIZE + sizeof(request_header_t)
#define DBGON 0
#define DBGPRINT(x); if(DBGON){ printf(x); }
#define DBGPRINT2(x, y); if(DBGON){ printf(x, y); }
#define DBGPRINT3(x, y, z); if(DBGON){ printf(x, y, z); }


#include "cream.h"
#include "queue.h"
#include "utils.h"
#include <sys/time.h>

#define CREAMTTL (struct timeval) {.tv_sec = 2, .tv_usec = 500}

struct cmsg{
    union{
        struct {
            struct request_header_t header;
            char data[CMSGSIZE - sizeof(request_header_t)];
        } req;
        struct {
            struct response_header_t header;
            char data[CMSGSIZE - sizeof(response_header_t)];
        } resp;
    };
};

typedef struct cmsg cmsg;

// hashmap helper methods
bool nullcheck_map(hashmap_t *self);
bool keycmp(map_key_t keyA, map_key_t keyB);

// EC hashmap helper methods
void remfromputlist(hashmap_t *self, int index);
int addtoputlist(hashmap_t *self, int index);

// cream server helper methods
void parseargs(int argc, char *argv[], int *num_workers, int *port, int *hash_size);
void creamsockinit(int *sockfd, int port);
void creamworker(void *arg);
void destroymapnode(map_key_t key, map_val_t val);

#define USAGE();                                                                \
fprintf(stderr, "USAGE: %s\n",                                                  \
"./cream [-h] NUM_WORKERS PORT_NUMBER MAX_ENTRIES\n"                            \
"-h                 Displays this help menu and returns EXIT_SUCCESS.\n"          \
"NUM_WORKERS        The number of worker threads used to service requests.\n"     \
"PORT_NUMBER        Port number to listen on for incoming connections.\n"         \
"MAX_ENTRIES        The maximum number of entries that can be stored in"        \
" `cream`'s underlying data store.\n");                                           \
exit(EXIT_FAILURE);

#endif //CREAM_ADD_H