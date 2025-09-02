CFLAGS := -Wall -Wextra -Werror -g -O3
CLIBS := -Lvendor/xxHash -l:libxxhash.a

.PHONY: submodules

main: 1brc.c vendor/xxHash/libxxhash.a
	gcc -o $@ 1brc.c ${CFLAGS} ${CLIBS}

vendor/xxHash/libxxhash.a: vendor/xxHash/xxhash.c
	cd vendor/xxHash && make
