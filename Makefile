COMPILER=dmd

all: examples doc test lib

test: source/fio.d source/poll.d
	$(COMPILER) -main -unittest source/fio.d source/poll.d -gc -oftest -Isource/fio/

forked_server: source/examples/forked_server.d
	$(COMPILER) source/examples/forked_server.d source/fio.d source/poll.d -gc -ofsource/examples/forked_server -Isource/fio/

examples: forked_server app

app: source/examples/app.d source/fio.d source/poll.d
	$(COMPILER) source/examples/app.d source/fio.d source/poll.d -gc -ofsource/examples/app -Isource/fio/

clean:
	rm -f source/examples/*.o source/examples/app source/examples/forked_server *.a *.o test __test__fio__

doc: source/fio.d
	$(COMPILER) -D -o- -Dfdocs/fio.html docs/viola.ddoc source/poll.d source/fio.d -Isource/fio/

lib: source/fio.d source/poll.d
	$(COMPILER) -c -oflibfio.a source/fio.d source/poll.d

