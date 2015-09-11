COMPILER=dmd

all: examples doc test lib

test: source/fio/fio.d source/fio/poll.d
	$(COMPILER) -main -unittest source/fio/fio.d source/fio/poll.d -gc -oftest -Isource/fio/

forked_server: source/examples/forked_server.d
	$(COMPILER) source/examples/forked_server.d source/fio/fio.d source/fio/poll.d -gc -ofsource/examples/forked_server -Isource/fio/

examples: forked_server app

app: source/examples/app.d source/fio/fio.d source/fio/poll.d
	$(COMPILER) source/examples/app.d source/fio/fio.d source/fio/poll.d -gc -ofsource/examples/app -Isource/fio/

clean:
	rm -f source/examples/*.o source/examples/app source/examples/forked_server *.a *.o test __test__fio__

doc: source/fio/fio.d
	$(COMPILER) -D -o- -Dfdocs/fio.html docs/viola.ddoc source/fio/poll.d source/fio/fio.d -Isource/fio/

lib: source/fio/fio.d source/fio/poll.d
	$(COMPILER) -c -oflibfio.a source/fio/fio.d source/fio/poll.d

