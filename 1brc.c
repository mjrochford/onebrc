#include <assert.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "queue.h"
#include "vendor/xxHash/xxhash.h" // TODO
/* #include <linux/aio_abi.h> */
struct Stats {
    float min;
    float max;
    float total;
    size_t n;
};

#define N_BUCKETS 1000
struct StatMap {
    pthread_mutex_t locks[N_BUCKETS];
    atomic_bool occupied[N_BUCKETS];
    struct Stats data[N_BUCKETS];
    char *names[N_BUCKETS];
};

struct StatMap stat_map = {
    .occupied = {0},
    .data = {{0}},
    .locks = {{{0}}},
    .names = {0},
};

size_t stat_map_hash(char *name, size_t name_len) {
    XXH3_state_t *state = XXH3_createState();
    assert(state != NULL && "Out of memory!");
    XXH3_64bits_reset(state);
    XXH3_64bits_update(state, name, name_len);
    XXH64_hash_t result = XXH3_64bits_digest(state);
    size_t bucket_index = result % N_BUCKETS;
    XXH3_freeState(state);
    return bucket_index;
}

// find open bucket index with linear scanning
size_t stat_map_scan_forward(char *name, size_t name_len, size_t bucket_index) {
    // break IF names [ bucket_index ] is null or name matches names [ bucket_index ]
    size_t count = 0;
    while (stat_map.names[bucket_index] != NULL &&
            0 != strncmp(name, stat_map.names[bucket_index], name_len)) {
        bucket_index += 1; // TODO: quadratic scanning? geometric?
        count += 1;
    }
    if (count > 1) {
        /* printf("name :%.*s: took %lu iterations to find a bucket\n", (int)name_len, name, count); */
    }
    return bucket_index;
}

#define max(a, b) ((a > b) ? a : b)
#define min(a, b) ((a < b) ? a : b)
void stat_map_put(char *name, size_t name_len, float value) {
    size_t bucket_index = stat_map_hash(name, name_len);
    bucket_index = stat_map_scan_forward(name, name_len, bucket_index);
    if (NULL != stat_map.names[bucket_index]) {
        pthread_mutex_lock(&stat_map.locks[bucket_index]);
        struct Stats current = stat_map.data[bucket_index];
        stat_map.data[bucket_index] = (struct Stats){
            .max = max(current.max, value),
            .min = min(current.min, value),
            .total = current.total + value,
            .n = current.n + 1,
        };
        pthread_mutex_unlock(&stat_map.locks[bucket_index]);
    } else {
        pthread_mutex_lock(&stat_map.locks[bucket_index]);
        stat_map.names[bucket_index] = malloc(name_len + 1);
        memcpy(stat_map.names[bucket_index], name, name_len);
        stat_map.names[bucket_index][name_len] = 0;

        atomic_store(&stat_map.occupied[bucket_index], true);
        stat_map.data[bucket_index] = (struct Stats){
            .max = value,
            .min = value,
            .total = value,
            .n = 1,
        };
        pthread_mutex_unlock(&stat_map.locks[bucket_index]);
    }
}

struct ThreadData {
    char *buf;
    size_t buf_size;
};

atomic_size_t line_count = 0;
void *process_chunk(void *ud) {
    struct ThreadData td = *(struct ThreadData *)ud;
    char *buf = td.buf;
    const size_t buf_size = td.buf_size;
    size_t delim_index = 0;
    size_t name_len = 0;
    for (size_t cursor = 0; cursor < buf_size; cursor++) {
        if (buf[cursor] == '\n') {
            assert(delim_index != 0);
            assert(buf[delim_index] == ';');
            // Parse digit
            char *dig_s = &buf[delim_index + 1];
            float dig = strtof(dig_s, NULL);

            // Update stats
            stat_map_put(&buf[delim_index - name_len], name_len, dig);

            // reset
            delim_index = 0;
            name_len = 0;
            atomic_fetch_add(&line_count, 1);
        } else if (buf[cursor] == ';') {
            delim_index = cursor;
        } else if (delim_index == 0) {
            name_len += 1;
        }
    }

    return NULL;
}

void print_stats();
int main(int argc, char *argv[]) {
    /* FILE *f = fopen("10mil.txt", "r"); */
    /* fopen("/home/mrochford/Projects/src/1brc/1brc/measurements.txt", "r"); */
    if (argc < 2) {
        fprintf(stderr, "ERROR: Provied an input file\n");
        return 1;
    }
    FILE *f = fopen(argv[1], "r");
    if (f == NULL) {
        fprintf(stderr, "ERROR: Failed to open %s\n", argv[1]);
        return 1;
    }

    for (int i = 0; i < N_BUCKETS; i++) {
        pthread_mutex_init(&stat_map.locks[i], NULL);
    }
    memset(stat_map.occupied, false, N_BUCKETS);
    clock_t c_start = clock();
    const size_t n_thread = 8;
    const size_t buf_size = 4096;
    /* const size_t n_lines = 1e7; */
    int current_worker = 0;
    char buf[n_thread][buf_size]; // TODO front and back buffers
    pthread_t threads[n_thread];
    struct ThreadData args[n_thread];
    memset(threads, (pthread_t){0}, sizeof(pthread_t) * n_thread);
    size_t line_fragment_len = 0;
    size_t total_read = 0;
    /* while (atomic_load(&line_count) < n_lines) { */
    size_t chunk_count = 0;
    size_t chunk_size_total = 0;
    size_t fragment_count = 0;
    while (true) {
        pthread_join(threads[current_worker], NULL);
        // Read from the file offset by whatever is already in our segment
        size_t n_read = fread(&buf[current_worker][line_fragment_len], 1,
                              buf_size - line_fragment_len, f);
        total_read += n_read;
        if (n_read == 0) {
            break;
        }
        size_t last_newline = min(line_fragment_len + n_read, buf_size - 1);
        line_fragment_len = 0;
        while (buf[current_worker][last_newline] != '\n') {
            line_fragment_len += 1;
            last_newline -= 1;
        }
        const size_t chunk_size = last_newline + 1;
        chunk_size_total += chunk_size;
        args[current_worker] = (struct ThreadData){
            .buf = buf[current_worker],
            .buf_size = chunk_size,
        };

        /* printf("read %lu bytes\n", n_read); */
        /* printf("dispatch worker %d with segment sized %lu\n", current_worker,
         * last_newline + 1); */
        pthread_create(&threads[current_worker], NULL, process_chunk,
                       &args[current_worker]);
        chunk_count += 1;
        const size_t next_worker = (current_worker + 1) % n_thread;
        // Copy partial line into the next worker's segment
        if (line_fragment_len > 0) {
            fragment_count += 1;
            assert(last_newline < buf_size);
            assert(buf[current_worker][last_newline] == '\n');
            pthread_join(threads[next_worker], NULL);
            memcpy(buf[next_worker], &buf[current_worker][chunk_size],
                   line_fragment_len);
        }
        current_worker = (current_worker + 1) % n_thread;
    }
    /* for (int i = 0; i < n_thread; i++) */
    /*     pthread_join(threads[i], NULL); */
    clock_t c_end = clock();

    print_stats();
    printf("took %g cycles to parse %lu lines\n", (float)(c_end - c_start),
           atomic_load(&line_count));
}

void print_stats() {
    size_t total_names = 0;
    for (size_t bucket_index = 0; bucket_index < N_BUCKETS; bucket_index++) {
        if (stat_map.occupied[bucket_index]) {
            total_names += 1;
            struct Stats current = stat_map.data[bucket_index];
            printf("name: %s, max: %f, min: %f, mean: %f\n",
                   stat_map.names[bucket_index], current.max, current.min,
                   current.total / current.n);
            printf("-------------------------\n");
        }
    }
    printf("total names: %lu\n", total_names);
}
