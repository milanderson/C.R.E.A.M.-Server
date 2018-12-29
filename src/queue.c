#include "queue.h"
#include <errno.h>

queue_t *create_queue(void) {
    queue_t *new_q;
    pthread_mutexattr_t attr;

    if((new_q = calloc(1, sizeof(queue_t))) == NULL){
        return NULL;
    }

    new_q->front = NULL;
    new_q->rear = NULL;
    sem_init(&new_q->items, 0, 0);

    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
    pthread_mutex_init(&new_q->lock, &attr);
    new_q->invalid = false;

    return new_q;
}

bool invalidate_queue(queue_t *self, item_destructor_f destroy_function) {
    queue_node_t *nextitem, *previtem;

    // lock que for editing
    if(pthread_mutex_lock(&self->lock) != 0){
        // set errno and exit
        errno = EINVAL;
        return false;
    }

    nextitem = self->front;
    while(nextitem != NULL){
        destroy_function(&nextitem->item);
        // get next item and free previous
        previtem = nextitem;
        nextitem = nextitem->next;
        free(previtem);

        // dec. item count and exit on error
        if(sem_trywait(&self->items) < 0){
            pthread_mutex_unlock(&self->lock);
            errno == EINVAL;
            return false;
        }
    }

    // unlock and return
    self->invalid = true;
    pthread_mutex_unlock(&self->lock);
    return true;
}

bool enqueue(queue_t *self, void *item) {
    queue_node_t *new_nd;

    // check que validity
    if(self->invalid){
         // set errno and exit
        errno = EINVAL;
        return item;
    }

    // lock que for editing, and inc. item count
    if(pthread_mutex_lock(&self->lock) != 0){
        // set errno and exit
        errno = EINVAL;
        return false;
    }
    if(sem_post(&self->items) != 0){
        // release lock
        pthread_mutex_unlock(&self->lock);
        // set errno and exit
        errno = EINVAL;
        return false;
    }

    // init new node
    if((new_nd = calloc(1, sizeof(queue_node_t))) == NULL){
        // dec. item count
        sem_wait(&self->items);
        // release lock and exit
        pthread_mutex_unlock(&self->lock);
        return false;
    }
    new_nd->item = item;
    new_nd->next = NULL;

    // add node to que
    if(self->front == NULL){
        self->front = new_nd;
    } else {
        if (self->rear == NULL){
            self->rear = self->front;
        }
        self->rear->next = new_nd;
        self->rear = new_nd;
    }

    // release lock
    pthread_mutex_unlock(&self->lock);

    return true;
}

void *dequeue(queue_t *self) {
    void *item = NULL;

    // check que validity
    if(self->invalid){
         // set errno and exit
        errno = EINVAL;
        return item;
    }

    // wait for pos. item count
    if(sem_wait(&self->items) != 0){
        // set errno and exit
        errno = EINVAL;
        return item;
    }
    // lock que for editing
    if(pthread_mutex_lock(&self->lock) != 0){
        // inc. item count
        sem_post(&self->items);
        // set errno and exit
        errno = EINVAL;
        return item;
    }

    // free previous head and move list head pointer forward
    if(self->front != NULL) {
        //store value of head pointer
        queue_node_t *head_nd;
        head_nd = self->front;
        // get item at head pointer
        item = self->front->item;
        // shift head pointer to next item in list and free old head
        self->front = self->front->next;
        free(head_nd);
    }

    // unlock and return
    pthread_mutex_unlock(&self->lock);
    return item;
}
