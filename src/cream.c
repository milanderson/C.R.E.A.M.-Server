#include "cream.h"
#include "utils.h"
#include "queue.h"
#include "cream_add.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <arpa/inet.h>

hashmap_t *resp_hash;
queue_t *con_que;

void null_handler(int signo);
void null_handler(int signo){
}

int main(int argc, char *argv[]) {
    // declare arg vars
    int num_workers, port, hash_size;
    // declare socket vars
    int *connfd, sockfd;
    // declare thread vars
    pthread_t threadID;

    // setup signal handlers
    signal(SIGINT, null_handler);
    signal(SIGPIPE, null_handler);

    // initialize global vars using input values
    parseargs(argc, argv, &num_workers, &port, &hash_size);
    resp_hash = create_map(hash_size, jenkins_one_at_a_time_hash, destroymapnode);
    con_que = create_queue();

    // create worker threads
    for(long i = 0; i < num_workers; i ++){
        // TODO create worker threads
        pthread_create(&threadID, NULL, (void *)creamworker, (void *)i);
    }

    // init socket
    creamsockinit(&sockfd, port);

    for(;;){
        connfd = calloc(1, sizeof(int));
        if((*connfd = accept(sockfd, NULL, NULL)) < 0){
            free(connfd);
            continue;
        }
        enqueue(con_que, connfd);
    }

    exit(EXIT_SUCCESS);
}

void parseargs(int argc, char *argv[], int *num_workers, int *port, int *hash_size){
    if(argc != 4){
        USAGE();
    }

    if((*num_workers = atoi(argv[1])) == 0){
        USAGE();
    }

    if((*port = atoi(argv[2])) == 0){
        USAGE();
    }

    if((*hash_size = atoi(argv[3])) == 0){
        USAGE();
    }
}

void creamsockinit(int *sockfd, int port){
    struct sockaddr_in servaddr;

    if((*sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0){
        perror("socket");
        exit(EXIT_FAILURE);
    }

    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);

    if(bind(*sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0){
        perror("bind");
        exit(EXIT_FAILURE);
    }

    if(listen(*sockfd, LISTENQ) < 0){
        perror("listen");
        exit(EXIT_FAILURE);
    }
}

void creamworker(void *arg){
    bool handled;
    int *item, connfd, retry;
    map_val_t val_node;
    map_key_t key_node;
    cmsg msg;

    for(;;){
        retry = 0;

        // pull item from que and dealloc the item
        if((item = dequeue(con_que)) == NULL){
            perror("deque");
            continue;
        }
        connfd = *item;
        free(item);

        // read from the socket
        reread:
        bzero(&msg, CMSGSIZE);
        if(read(connfd, &msg, CMSGSIZE) < sizeof(request_header_t)){
            if(errno == EINTR){
                retry ++;
                if(retry > 10) { continue; }
                goto reread;
            }
        }
        retry = 0;
        handled = false;

        // handle get requests
        if(!handled && msg.req.header.request_code == GET){
            DBGPRINT("get req\n");
            handled = true;
            // check request validity
            if(msg.req.header.key_size < 1 || msg.req.header.key_size > MAX_KEY_SIZE){
                DBGPRINT("bad get req\n");
                msg.resp.header.response_code = BAD_REQUEST;
                msg.resp.header.value_size = 0;
            } else {
                // search for key in hashmap
                key_node.key_len = msg.req.header.key_size;
                key_node.key_base = msg.req.data;
                val_node = get(resp_hash, key_node);
                // if not found set appropriate header info
                if(val_node.val_base == NULL){
                    DBGPRINT("get req key not found\n");
                    msg.resp.header.response_code = NOT_FOUND;
                    msg.resp.header.value_size = 0;
                } else{
                // if found set appropriate header info
                    DBGPRINT("get req key found\n");
                    msg.resp.header.response_code = OK;
                    msg.resp.header.value_size = val_node.val_len;
                    memcpy(msg.resp.data, val_node.val_base, val_node.val_len);
                    // free allocated memory from get call
                    free(val_node.val_base);
                }
            }
        }

        // handle put requests
        if(!handled && msg.req.header.request_code == PUT){
            DBGPRINT("put req\n");
            handled = true;
            // validity check
            if(msg.req.header.key_size < MIN_KEY_SIZE || msg.req.header.key_size > MAX_KEY_SIZE ||
                msg.req.header.value_size < MIN_VALUE_SIZE || msg.req.header.value_size > MAX_VALUE_SIZE){
                DBGPRINT("bad put req\n");
                msg.resp.header.response_code = BAD_REQUEST;
                msg.resp.header.value_size = 0;
            } else {
                // create key nodes
                key_node.key_len = msg.req.header.key_size;
                if((key_node.key_base = malloc(key_node.key_len)) == NULL){
                    perror("malloc");
                    msg.resp.header.response_code = BAD_REQUEST;
                    msg.resp.header.value_size = 0;
                    break;
                }
                memcpy(key_node.key_base, msg.req.data, key_node.key_len);

                // create map node
                val_node.val_len = msg.req.header.value_size;
                if((val_node.val_base = malloc(val_node.val_len)) == NULL){
                    perror("malloc");
                    free(key_node.key_base);
                    msg.resp.header.response_code = BAD_REQUEST;
                    msg.resp.header.value_size = 0;
                    break;
                }
                memcpy(val_node.val_base, msg.req.data + key_node.key_len, val_node.val_len);

                // pass nodes to hashmap
                bool worked = put(resp_hash, key_node, val_node, true);
                // set appropriate header
                if(worked){
                    DBGPRINT("put req success\n");
                    msg.resp.header.response_code = OK;
                    msg.resp.header.value_size = 0;
                }else{
                    DBGPRINT("put req failure\n");
                    free(key_node.key_base);
                    free(val_node.val_base);
                    msg.resp.header.response_code = BAD_REQUEST;
                    msg.resp.header.value_size = 0;
                }
            }
        }

        // handle evict requests
        if(!handled && msg.req.header.request_code == EVICT){
            DBGPRINT("evict req\n");
            handled = true;
            // test request validity
            if(msg.req.header.key_size < MIN_KEY_SIZE || msg.req.header.key_size > MAX_KEY_SIZE){
                DBGPRINT("bad evict req\n");
                msg.resp.header.response_code = BAD_REQUEST;
                msg.resp.header.value_size = 0;
            } else {
                DBGPRINT("executing evict req\n");
                // if valid, delete key
                key_node.key_len = msg.req.header.key_size;
                key_node.key_base = msg.req.data;
                delete(resp_hash, key_node);

                // set response header
                msg.resp.header.response_code = OK;
                msg.resp.header.value_size = 0;
            }
        }

        // handle clear requests
        if(!handled && msg.req.header.request_code == CLEAR){
            DBGPRINT("clear req\n");
            handled = true;

            // clear hash
            clear_map(resp_hash);

            // set response header
            msg.resp.header.response_code = OK;
            msg.resp.header.value_size = 0;
        }

        // handle misc requests
        if(!handled){
            DBGPRINT("unknown req\n");
            handled = true;
            msg.resp.header.response_code = UNSUPPORTED;
            msg.resp.header.value_size = 0;
        }

        resend:
        DBGPRINT("sending resp\n");
        if(send(connfd, &msg, msg.resp.header.value_size + sizeof(struct response_header_t), 0) < 0){
            perror("send");
            if(errno == EINTR){
                retry ++;
                if (retry > 10) { continue; }
                goto resend;
            } else if(errno == EPIPE){
                continue;
            }
        }
    }
}

void destroymapnode(map_key_t key, map_val_t val){
    free(val.val_base);
    free(key.key_base);
}