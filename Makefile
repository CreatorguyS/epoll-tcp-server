# Makefile for tcp_server
#
# Targets:
#   make            — build the server (optimised)
#   make debug      — build with AddressSanitizer + debug info
#   make valgrind   — run server under valgrind for leak check
#   make clean      — remove build artefacts

CC      = gcc
CFLAGS  = -std=c11 -Wall -Wextra -Wpedantic -D_GNU_SOURCE
SRC     = src/server.c
BIN     = server

.PHONY: all debug valgrind clean

all: $(BIN)

$(BIN): $(SRC)
	$(CC) $(CFLAGS) -O2 -o $@ $^
	@echo "Built: ./$@"

debug: $(SRC)
	$(CC) $(CFLAGS) -g -fsanitize=address,undefined -o $(BIN)_asan $^
	@echo "Built: ./$(BIN)_asan  (AddressSanitizer enabled)"

valgrind: $(BIN)
	valgrind --leak-check=full --track-origins=yes \
	         --error-exitcode=1 ./$(BIN) 8080 &
	sleep 1
	python3 tests/client_test.py
	pkill -f "valgrind.*server" || true

clean:
	rm -f $(BIN) $(BIN)_asan
