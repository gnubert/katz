debug : DEBUG=-g -pg
debug : DEFINES=-DDEBUG


LIBS=-lrt
CFLAGS=-Wall

CFLAGS+=$(DEBUG) ${PROFILE} $(DEFINES) $(LIBS) 

katz: katz.c katz.h
	$(CC) -o katz katz.c $(CFLAGS)

debug: katz

clean:
	rm katz
