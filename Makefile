CC=gcc
VPATH=./lib
CFLAGS=-I$(VPATH) -DHAVE_READLINE  -lpthread -ldl -lreadline -g -O2 -Wall

all: sqlite3_stream sqlite3_stream_np sqlite3 generator clean

sqlite3_stream: sqlite3_modified.o shell.o
	$(CC)  shell.o sqlite3_modified.o -o sqlite3_stream $(CFLAGS)

sqlite3_stream_np: sqlite3_modified.o shell_noprint.o
	$(CC)  shell_noprint.o sqlite3_modified.o -o sqlite3_stream_np $(CFLAGS)

sqlite3: sqlite3.o shell.o
	$(CC)  shell.o sqlite3.o -o sqlite3 $(CFLAGS)

generator: Client.o
	$(CC)  Client.o -o generator 


$(VPATH)/%.o: %.c
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	rm *.o 
