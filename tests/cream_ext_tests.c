#include <criterion/criterion.h>
#include <criterion/logging.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cream_ext.h"

#define NUM_THREADS 5
#define MAP_KEY(kbase, klen) (map_key_t) {.key_base = kbase, .key_len = klen}
#define MAP_VAL(vbase, vlen) (map_val_t) {.val_base = vbase, .val_len = vlen}

hashmap_t *global_map;

typedef struct map_insert_t {
    void *key_ptr;
    void *val_ptr;
} map_insert_t;

/* Used in item destruction */
void map_free_function(map_key_t key, map_val_t val) {
    free(key.key_base);
    free(val.val_base);
}

uint32_t jenkins_hash(map_key_t map_key) {
    const uint8_t *key = map_key.key_base;
    size_t length = map_key.key_len;
    size_t i = 0;
    uint32_t hash = 0;

    while (i != length) {
        hash += key[i++];
        hash += hash << 10;
        hash ^= hash >> 6;
    }

    hash += hash << 3;
    hash ^= hash >> 11;
    hash += hash << 15;
    return hash;
}

void map_init(void) {
    global_map = create_map(NUM_THREADS, jenkins_hash, map_free_function);
}

void *test_thread(void *arg) {
    map_key_t key;
    map_val_t val, getval;

    //******************* put 1
    key.key_len = 1;
    key.key_base = calloc(key.key_len, sizeof(char));
    memcpy(key.key_base, "a", key.key_len);

    val.val_len = 5;
    val.val_base = calloc(val.val_len, sizeof(char));
    memcpy(val.val_base, "test0", val.val_len);

    put(global_map, key, val, true);

    //******************* put 2
    key.key_len = 1;
    key.key_base = calloc(key.key_len, sizeof(char));
    memcpy(key.key_base, "b", key.key_len);

    val.val_len = 5;
    val.val_base = calloc(val.val_len, sizeof(char));
    memcpy(val.val_base, "test1", val.val_len);

    put(global_map, key, val, true);

    sleep(1);

    //******************* put 3
    key.key_len = 1;
    key.key_base = calloc(key.key_len, sizeof(char));
    memcpy(key.key_base, "c", key.key_len);

    val.val_len = 5;
    val.val_base = calloc(val.val_len, sizeof(char));
    memcpy(val.val_base, "test2", val.val_len);

    put(global_map, key, val, true);

    //******************* get 1
    key.key_len = 1;
    key.key_base = "b";

    getval = get(global_map, key);
    printf("get1 b %s\n", (char *)getval.val_base);
    free(getval.val_base);

    sleep(1);

    //******************* put 4
    key.key_len = 1;
    key.key_base = calloc(key.key_len, sizeof(char));
    memcpy(key.key_base, "b", key.key_len);

    val.val_len = 5;
    val.val_base = calloc(val.val_len, sizeof(char));
    memcpy(val.val_base, "test3", val.val_len);

    put(global_map, key, val, true);

    //******************* put 5
    key.key_len = 1;
    key.key_base = calloc(key.key_len, sizeof(char));
    memcpy(key.key_base, "d", key.key_len);

    val.val_len = 5;
    val.val_base = calloc(val.val_len, sizeof(char));
    memcpy(val.val_base, "test4", val.val_len);

    put(global_map, key, val, true);

    //******************* put 6
    key.key_len = 1;
    key.key_base = calloc(key.key_len, sizeof(char));
    memcpy(key.key_base, "e", key.key_len);

    val.val_len = 5;
    val.val_base = calloc(val.val_len, sizeof(char));
    memcpy(val.val_base, "test5", val.val_len);

    put(global_map, key, val, true);

    //******************* get 2
    key.key_len = 1;
    key.key_base = "b";

    getval = get(global_map, key);
    printf("get2 b %s\n", (char *)getval.val_base);
    free(getval.val_base);

    sleep(1);

    //******************* put 4
    key.key_len = 1;
    key.key_base = calloc(key.key_len, sizeof(char));
    memcpy(key.key_base, "f", key.key_len);

    val.val_len = 5;
    val.val_base = calloc(val.val_len, sizeof(char));
    memcpy(val.val_base, "test6", val.val_len);

    put(global_map, key, val, true);

    //******************* put 7
    key.key_len = 1;
    key.key_base = calloc(key.key_len, sizeof(char));
    memcpy(key.key_base, "g", key.key_len);

    val.val_len = 5;
    val.val_base = calloc(val.val_len, sizeof(char));
    memcpy(val.val_base, "test7", val.val_len);

    put(global_map, key, val, true);

    //******************* put 8
    key.key_len = 1;
    key.key_base = calloc(key.key_len, sizeof(char));
    memcpy(key.key_base, "h", key.key_len);

    val.val_len = 5;
    val.val_base = calloc(val.val_len, sizeof(char));
    memcpy(val.val_base, "test8", val.val_len);

    put(global_map, key, val, true);

    //******************* put 9
    key.key_len = 1;
    key.key_base = calloc(key.key_len, sizeof(char));
    memcpy(key.key_base, "i", key.key_len);

    val.val_len = 5;
    val.val_base = calloc(val.val_len, sizeof(char));
    memcpy(val.val_base, "test9", val.val_len);

    put(global_map, key, val, true);

    //******************* put 10
    key.key_len = 1;
    key.key_base = calloc(key.key_len, sizeof(char));
    memcpy(key.key_base, "j", key.key_len);

    val.val_len = 6;
    val.val_base = calloc(val.val_len, sizeof(char));
    memcpy(val.val_base, "test10", val.val_len);

    put(global_map, key, val, true);

    //******************* get 3
    key.key_len = 1;
    key.key_base = "b";

    getval = get(global_map, key);
    printf("get3 b %s\n", (char *)getval.val_base);
    free(getval.val_base);

    //******************* get 4
    key.key_len = 1;
    key.key_base = "f";

    getval = get(global_map, key);
    printf("get4 f %s\n", (char *)getval.val_base);
    free(getval.val_base);

    //******************* get 5
    key.key_len = 1;
    key.key_base = "c";

    getval = get(global_map, key);
    printf("get5 b %s\n", (char *)getval.val_base);
    free(getval.val_base);

    return NULL;
}

void map_fini(void) {
    invalidate_map(global_map);
}

Test(map_suite, 02_multithreaded, .timeout = 20, .init = map_init, .fini = map_fini) {
    pthread_t thread_ids[NUM_THREADS];

    // spawn NUM_THREADS threads to put elements
    for(int index = 0; index < NUM_THREADS; index++) {

        if(pthread_create(&thread_ids[index], NULL, test_thread, NULL) != 0)
            exit(EXIT_FAILURE);
    }

    // wait for threads to die before checking queue
    for(int index = 0; index < NUM_THREADS; index++) {
        pthread_join(thread_ids[index], NULL);
    }

    int num_items = global_map->size;
    cr_assert_eq(num_items, NUM_THREADS, "Had %d items in map. Expected %d", num_items, NUM_THREADS);
}
