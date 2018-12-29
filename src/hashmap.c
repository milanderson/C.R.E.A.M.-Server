#include "utils.h"
#include "cream_add.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>

#define MAP_KEY(base, len) (map_key_t) {.key_base = base, .key_len = len}
#define MAP_VAL(base, len) (map_val_t) {.val_base = base, .val_len = len}
#define MAP_NODE(key_arg, val_arg, tombstone_arg) (map_node_t) {.key = key_arg, .val = val_arg, .tombstone = tombstone_arg}

hashmap_t *create_map(uint32_t capacity, hash_func_f hash_function, destructor_f destroy_function) {
    hashmap_t *new_hmap;
    if((new_hmap = calloc(1, sizeof(hashmap_t))) == NULL){
        return NULL;
    }

    new_hmap->capacity = capacity;
    new_hmap->size = 0;
    new_hmap->nodes = calloc(capacity, sizeof(map_node_t));
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
    int index;

    // lock hashmap for editing
    if(pthread_mutex_lock(&self->write_lock) != 0){
        DBGPRINT("put: lock failed\n");
        errno = EINVAL;
        return false;
    }

    // null check args
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
            self->destroy_function(self->nodes[curindex%self->capacity].key, self->nodes[curindex%self->capacity].val);
            self->size--;
            self->nodes[curindex%self->capacity] = MAP_NODE(key, val, false);
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
                self->nodes[curindex%self->capacity] = MAP_NODE(key, val, false);
                added = true;
                break;
            }
        }
    }

    // if no dupe key or available slot, try to force
    // delete the node at the hash index, and add current key/val
    if(!added && self->size == self->capacity){
        if(force){
            DBGPRINT3("forcing node: deleting %s at %i\n", (char *)self->nodes[index].key.key_base, index);
            // remove index and insert value
            self->destroy_function(self->nodes[index].key, self->nodes[index].val);
            self->size--;
            self->nodes[index] = MAP_NODE(key, val, false);
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
    map_val_t outval = MAP_VAL(NULL, 0);

    // lock hashmap for editing
    if(pthread_mutex_lock(&self->fields_lock) != 0){
        errno = EINVAL;
        return outval;
    }

    // null check args
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
        if(keycmp(self->nodes[curindex % self->capacity].key, key) && !self->nodes[curindex % self->capacity].tombstone){
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

    // null check hashmap
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

    // null check map
    if(!nullcheck_map(self)){
        errno = EINVAL;
        pthread_mutex_lock(&self->write_lock);
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
        pthread_mutex_lock(&self->write_lock);
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
    if(keyA.key_len != keyB.key_len){
        return false;
    }

    for(int i = 0; i < keyA.key_len; i++){
        if( ((char *)keyA.key_base)[i] != ((char *)keyB.key_base)[i]){
            return false;
        }
    }

    return true;
}
