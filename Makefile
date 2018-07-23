.PHONY: release debug clean

CFLAGS := -Wall -lsqlite3 -ltraildb -shared -fPIC

release: CFLAGS += -O2
release: sqlite3traildb.so

debug: CFLAGS += -Og -g3
debug: sqlite3traildb.so

sqlite3traildb.so: sqlite3_traildb.c
	$(CC) -o sqlite3traildb.so sqlite3_traildb.c $(CFLAGS)

clean:
	$(RM) sqlite3traildb.so
