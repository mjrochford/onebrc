#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <time.h>
/* #include <linux/aio_abi.h> */
struct Stats {
    double min;
    double max;
    double total;
    size_t n;
};

#define N_BUCKETS 10000
struct StatMap {
    bool occupied[N_BUCKETS];
    struct Stats data[N_BUCKETS];
};

struct StatMap stat_map = {
    .occupied = {0},
    .data = {0},
};

void parse_row(char * row, size_t row_len);
void print_stats();
int main() {
    memset(stat_map.occupied, false, N_BUCKETS);
    FILE *f = fopen("/home/mrochford/Projects/src/1brc/1brc/measurements.txt", "r");
    struct timespec start;
    clockid_t cid = CLOCK_PROCESS_CPUTIME_ID;
    if (0 != clock_gettime(cid, &start)) {
        perror("can't get start time");
    }
    size_t line_count = 0;
    const size_t buf_size = 4096;
    size_t line_start = buf_size;
    char buf[buf_size];
    const size_t n_lines = 10000000;
    while (line_count < n_lines) { // TODO: parse in place
        size_t n_read = fread(&buf[sizeof(buf) - line_start], 1, line_start, f);
        if (n_read == 0) {
            break;
        }
        line_start = 0;
        for (size_t cursor = 0; cursor < sizeof(buf); cursor++) {
            if (buf[cursor] == '\n') {
                parse_row(&buf[line_start], cursor - line_start);
                line_start = cursor + 1;
                line_count += 1;
            }
        }
        assert(buf[line_start - 1] == '\n');
        memcpy(&buf[0], &buf[line_start], sizeof(buf) - line_start);
    }
    struct timespec end;
    if (0 != clock_gettime(cid, &end)) {
        perror("can't get end time");
    }

    print_stats();
    printf("took %fms to parse %u lines\n", (end.tv_nsec - start.tv_nsec) / 1e6f, n_lines);
}
#define max(a, b) ((a > b) ? a : b)
#define min(a, b) ((a < b) ? a : b)

void parse_row(char * row, size_t row_len) {
    size_t bucket_index = 0;
    size_t i = 0;
    for (; row[i] != ';'; i++) {
        bucket_index += row[i];
    }
    bucket_index = (bucket_index * 128) % N_BUCKETS;
    double dig = strtod(row + i + 1, NULL);
    if (stat_map.occupied[bucket_index]) {
        struct Stats current = stat_map.data[bucket_index];
        stat_map.data[bucket_index] = (struct Stats){
            .max = max(current.max, dig),
            .min = min(current.min, dig),
            .total = current.total + dig,
            .n = current.n + 1,
        };
    } else {
        stat_map.data[bucket_index] = (struct Stats){
            .max = dig,
            .min = dig,
            .total = dig,
            .n = 1,
        };
        stat_map.occupied[bucket_index] = true;
    }
    struct Stats current = stat_map.data[bucket_index];

}


void print_stats() {
    for (size_t bucket_index = 0; bucket_index < N_BUCKETS; bucket_index++) {
        if (stat_map.occupied[bucket_index]) {
            struct Stats current = stat_map.data[bucket_index];
            printf("bucket: %u, max: %f, min: %f, mean: %f\n", bucket_index, current.max, current.min, current.total / current.n);
            printf("-------------------------\n");
        }
    }
}
