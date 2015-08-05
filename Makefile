COMPILER=dmd

all: app examples doc test

test: source/fio/fio.d source/fio/poll.d
	$(COMPILER) -main -unittest source/fio/fio.d source/fio/poll.d -gc -oftest -Isource/fio/

forked_server: examples/forked_server.d
	$(COMPILER) examples/forked_server.d source/fio/fio.d source/fio/poll.d -gc -offorked_server -Isource/fio/

examples: forked_server

app: source/app.d
	$(COMPILER) source/app.d source/fio/fio.d source/fio/poll.d -gc -ofapp -Isource/fio/

clean:
	rm -f test test.o app forked_server forked_server *.o

doc: source/fio/fio.d docs/fio.html
	$(COMPILER) -Dfdocs/fio.html source/app.d  source/fio/poll.d source/fio/fio.d -Isource/fio/
