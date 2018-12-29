#include "utils.h"
#include "cream_add.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#define MAP5_NODE(key_arg, val_arg, prev_arg, next_arg, tombstone_arg) (map_node_t) {.key = key_arg, .val = val_arg, .prev = prev_arg, .next = next_arg, .tombstone = tombstone_arg}

hashmap_t *create_map(uint32_t capacity, hash_func_f hash_function, destructor_f destroy_function) {
    hashmap_t *new_hmap;
    if((new_hmap = calloc(1, sizeof(hashmap_t))) == NULL){
        return NULL;
    }

    new_hmap->capacity = capacity;
    new_hmap->size = 0;
    new_hmap->nodes = calloc(capacity, sizeof(map_node_t));
    new_hmap->oldest = -1;
    new_hmap->newest = -1;
    new_hmap->hash_function = hash_function;
    new_hmap->destroy_function = destroy_function;
    new_hmap->num_readers = 0;
    new_hmap->invalid = false;

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
    pthread_mutex_init(&new_hmap->write_lock, &attr);
    pthread_mutex_init(&new_hmap->fields_lock, &attr);

    return new_hmap;
}

bool put(hashmap_t *self, map_key_t key, map_val_t val, bool force) {
    int index, prev;

    // lock hashmap for editing
    if(pthread_mutex_lock(&self->write_lock) != 0){
        DBGPRINT("put: lock failed\n");
        errno = EINVAL;
        return false;
    }

    if(!nullcheck_map(self) || key.key_base == NULL || val.val_base == NULL){
        DBGPRINT("put: invalid key or hashmap\n");
        errno = EINVAL;
        pthread_mutex_unlock(&self->write_lock);
        return false;
    }

    // get hash index and search from index for key
    index = self->hash_function(key) % self->capacity;

    // search if key exists, or if empty slot to store key exists
    int curindex;
    bool added = false;
    for(int i = 0; i < self->capacity; i++){
        curindex = index + i;
        // search to see if key exists and replace old val
        if(!self->nodes[curindex%self->capacity].tombstone && keycmp(self->nodes[curindex%self->capacity].key, key)){
            DBGPRINT2("dupe node found at %i\n", curindex%self->capacity);

            // destroy old val
            remfromputlist(self, curindex%self->capacity);
            self->destroy_function(self->nodes[curindex%self->capacity].key, self->nodes[curindex%self->capacity].val);
            self->size--;

            // add new val
            prev = addtoputlist(self, curindex%self->capacity);
            self->nodes[curindex%self->capacity] = MAP5_NODE(key, val, prev, -1, false);
            DBGPRINT3("added node with prev %i next %i\n", self->nodes[curindex%self->capacity].prev, self->nodes[curindex%self->capacity].next);
            // set TTL value
            gettimeofday(&self->nodes[curindex%self->capacity].key.put_tstamp, NULL);

            added = true;
            break;
        }

    }

    if(!added){
        for(int i = 0; i < self->capacity; i++){
            curindex = index + i;
            // if no dupe found, add key/val to first available slot
            if((size_t)self->nodes[curindex%self->capacity].key.key_len == (size_t)0 || self->nodes[curindex%self->capacity].tombstone){
                DBGPRINT2("empty node found at index: %i\n", curindex%self->capacity);

                // add node
                prev = addtoputlist(self, curindex%self->capacity);
                self->nodes[curindex%self->capacity] = MAP5_NODE(key, val, prev, -1, false);
                DBGPRINT3("added node with prev %i next %i\n", self->nodes[curindex%self->capacity].prev, self->nodes[curindex%self->capacity].next);
                // set TTL value
                gettimeofday(&self->nodes[curindex%self->capacity].key.put_tstamp, NULL);

                added = true;
                break;
            }
        }
    }

    // if no dupe key or available slot, try to force
    // delete the node at the hash index, and add current key/val
    if(!added && self->size == self->capacity){
        if(force){
            DBGPRINT2("forcing node: deleting at %i\n", self->oldest);

            // destroy old val
            int oldest = self->oldest;
            remfromputlist(self, oldest);
            self->destroy_function(self->nodes[oldest].key, self->nodes[oldest].val);
            self->size--;

            prev = addtoputlist(self, oldest);
            self->nodes[oldest] = MAP5_NODE(key, val, prev, -1, false);
            DBGPRINT3("added node with prev %i next %i\n", self->nodes[curindex%self->capacity].prev, self->nodes[curindex%self->capacity].next);
            // set TTL info
            gettimeofday(&self->nodes[oldest].key.put_tstamp, NULL);
        } else {
            DBGPRINT("put: no memory failed\n");
            errno = ENOMEM;
            pthread_mutex_unlock(&self->write_lock);
            return false;
        }
    }

    self->size++;

    pthread_mutex_unlock(&self->write_lock);
    return true;
}

map_val_t get(hashmap_t *self, map_key_t key) {
    int index;
    struct timeval time_sitting;
    map_val_t outval = MAP_VAL(NULL, 0);

    // lock hashmap for editing
    if(pthread_mutex_lock(&self->fields_lock) != 0){
        errno = EINVAL;
        return outval;
    }

    if(!nullcheck_map(self) || key.key_base == NULL){
        errno = EINVAL;
        pthread_mutex_unlock(&self->fields_lock);
        return outval;
    }

    self->num_readers++;
    if(self->num_readers == 1){
        if(pthread_mutex_lock(&self->write_lock) != 0){
            pthread_mutex_unlock(&self->fields_lock);
            errno = EINVAL;
            return outval;
        }
    }
    pthread_mutex_unlock(&self->fields_lock);

    // get hash index and search from index for key
    index = self->hash_function(key) % self->capacity;
    int curindex;
    for(int i = 0; i < self->capacity; i++){
        curindex = index + i;
        if(self->nodes[curindex % self->capacity].key.key_len == 0){
            break;
        }
        if(!self->nodes[curindex % self->capacity].tombstone && keycmp(self->nodes[curindex % self->capacity].key, key)){
            // TTL Eviction
            // calc time of day
            DBGPRINT("Checking entry time\n");
            gettimeofday(&time_sitting, NULL);
            time_t sec = time_sitting.tv_sec - self->nodes[curindex%self->capacity].key.put_tstamp.tv_sec;
            suseconds_t msec = time_sitting.tv_usec - self->nodes[curindex%self->capacity].key.put_tstamp.tv_usec;

            // if time is past, EVICT node and previous nodes
            if(sec > CREAMTTL.tv_sec || (sec == CREAMTTL.tv_sec && msec > CREAMTTL.tv_usec)){
                // init TTL vars lock hashmap for writing
                DBGPRINT("Removing old entries\n");
                int previndex;
                pthread_mutex_lock(&self->fields_lock);

                do {
                    DBGPRINT2("Removing item %i\n", curindex%self->capacity);
                    // get earlier element
                    previndex = self->nodes[curindex%self->capacity].prev;

                    // remove outdated entry
                    remfromputlist(self, curindex%self->capacity);
                    self->destroy_function(self->nodes[curindex%self->capacity].key, self->nodes[curindex%self->capacity].val);
                    self->nodes[curindex%self->capacity].tombstone = true;
                    self->size--;

                    // iterate over prev element
                    curindex = previndex;
                } while(previndex != -1);

                // unlock hashmap
                pthread_mutex_unlock(&self->fields_lock);
                break;
            }

            outval = self->nodes[curindex % self->capacity].val;
            break;
        }
    }

    // copy value over to protect from future overwrites
    if(outval.val_len > 0){
        void *safespace = calloc(outval.val_len, sizeof(char));
        memcpy(safespace, outval.val_base, outval.val_len);
        outval.val_base = safespace;
    }

    // unlock hashmap for editing
    pthread_mutex_lock(&self->fields_lock);

    self->num_readers--;
    if(self->num_readers == 0){
        pthread_mutex_unlock(&self->write_lock);
    }

    pthread_mutex_unlock(&self->fields_lock);

    return outval;
}

map_node_t delete(hashmap_t *self, map_key_t key) {
    int index;
    map_node_t outval = MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);

    // lock hashmap for editing
    if(pthread_mutex_lock(&self->write_lock) != 0){
        errno = EINVAL;
        return outval;
    }

    if(!nullcheck_map(self) || key.key_base == NULL){
        errno = EINVAL;
        pthread_mutex_unlock(&self->write_lock);
        return outval;
    }

    // look for key
    int curindex;
    index = self->hash_function(key) % self->capacity;
    for(int i = 0; i < self->capacity; i++){
        curindex = index + i;
        if(self->nodes[curindex % self->capacity].key.key_len == 0){
            break;
        }
        if(keycmp(self->nodes[curindex % self->capacity].key, key) && !self->nodes[curindex % self->capacity].tombstone){
            remfromputlist(self, curindex % self->capacity);
            self->nodes[curindex % self->capacity].tombstone = true;
            self->size--;
            outval = MAP_NODE(self->nodes[curindex % self->capacity].key, self->nodes[curindex % self->capacity].val, true);
            break;
        }
    }

    // unlock and return
    pthread_mutex_unlock(&self->write_lock);
    return outval;
}

bool clear_map(hashmap_t *self) {

    // lock hashmap for editing
    if(pthread_mutex_lock(&self->write_lock) != 0){
        errno = EINVAL;
        return false;
    }

    // check if hashmap is valid
    if(!nullcheck_map(self)){
        errno = EINVAL;
        pthread_mutex_unlock(&self->write_lock);
        return false;
    }

    // call destroy on all nodes
    for(int i = 0; i < self->capacity; i++){
        if(self->nodes[i].key.key_len != 0) {
            self->destroy_function(self->nodes[i].key, self->nodes[i].val);
            self->nodes[i].key.key_len = 0;
            self->nodes[i].key.key_base = 0;
            self->nodes[i].val.val_len = 0;
            self->nodes[i].val.val_base = 0;
        }
    }

    self->size = 0;

    // unlock and return
    pthread_mutex_unlock(&self->write_lock);
    return true;
}

bool invalidate_map(hashmap_t *self) {

    // lock hashmap for editing
    if(pthread_mutex_lock(&self->write_lock) != 0){
        errno = EINVAL;
        return false;
    }

    if(!nullcheck_map(self)){
        errno = EINVAL;
        pthread_mutex_unlock(&self->write_lock);
        return false;
    }

    // call destroy on all nodes
    for(int i = 0; i < self->capacity; i++){
        if(self->nodes[i].key.key_len != 0) {
            self->destroy_function(self->nodes[i].key, self->nodes[i].val);
        }
    }

    // free the calloc'd space and set invalid
    free(self->nodes);
    self->invalid = true;

    // unlock and return
    pthread_mutex_unlock(&self->write_lock);

    return false;
}

void remfromputlist(hashmap_t *self, int index){
    int prev, next;
    // update replace list
    if(self->oldest == index){
        self->oldest = self->nodes[self->oldest].next;
    }
    if(self->newest == index){
        self->newest = self->nodes[self->newest].prev;
    }
    //update linked list
    if((prev = self->nodes[index].prev) != -1){
        self->nodes[prev].next = self->nodes[index].next;
    }
    if((next = self->nodes[index].next) != -1){
        self->nodes[next].prev = self->nodes[index].prev;
    }
}

int addtoputlist(hashmap_t *self, int index){
    int prev;

    // update replace list
    if(self->oldest == -1){
        prev = -1;
        self->oldest = index;
    } else {
        if(index != self->newest){
            prev = self->newest;
            self->nodes[self->newest].next = index;
        } else {
            prev = self->nodes[self->newest].prev;
        }
    }
    self->newest = index;

    return prev;
}

bool nullcheck_map(hashmap_t *self) {
    if(self->capacity < 1 ||
        self->size < 0 || self->size > self->capacity ||
        self->hash_function == NULL ||
        self->destroy_function == NULL ||
        self->num_readers < 0 ||
        self->invalid) {
        return false;
    }

    return true;
}

bool keycmp(map_key_t keyA, map_key_t keyB){
    DBGPRINT3("comparing key sizes %li and %li\n", keyA.key_len, keyB.key_len);
    if(keyA.key_len != keyB.key_len){
        return false;
    }


    for(int i = 0; i < keyA.key_len; i++){
        DBGPRINT3("comparing %c and %c\n", ((char *)keyA.key_base)[i], ((char *)keyB.key_base)[i]);
        if( ((char *)keyA.key_base)[i] != ((char *)keyB.key_base)[i]){
            return false;
        }
    }

    return true;
}
