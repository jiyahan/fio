module fio;

private import std.stdio: writeln;
private import std.datetime;
private import std.experimental.logger;
private import std.socket;
private import std.format;
private import std.conv;
private import core.thread;
private import core.memory;
private import core.sys.posix.unistd : pipe, write;
private import poll;
private import ipaddr;

template frm(alias v) {
    string frm() {
        string frm = v.stringof;
        frm ~= "=" ~ to!string(v);
        return frm;
    }
}


//private import libasync;

private EventLoop 	evl;
private bool      	stopped = false;
private fioFiber  	loop;

private Fiber[]					runnables;
private static bool[fioTask]	started;
private static bool[fioTask]	zombie;

enum {
    ERROR = -1,
    TIMEOUT = -2
}

enum BACKLOG = 1024;

static this() {
    evl = new EventLoop;
    loop = new fioFiber;
}

void fioSleep(in Duration d) {
    auto t = new AsyncTimer(evl, __FILE__, __LINE__);
    auto f = Fiber.getThis();
    t.duration = d;
    t.run({
        t.kill();		// release timer file descriptor and other resources
        runnables ~= f; // continue task
    });
    Fiber.yield(); // pass control to main
}

class fioTCPListener {
    string 	host;
    ushort 	port;
    void 	delegate(fioTCPConnection) server;
    Address _address;
    Socket 	so;
    asyncAccept acceptor;

    this(string host, ushort port, void delegate(fioTCPConnection) d) {
        server = d;
        _address = getAddress(host, port)[0];
        so = new Socket(_address.addressFamily, SocketType.STREAM, ProtocolType.TCP);
        so.bind(_address);
        so.listen(BACKLOG);
        acceptor = new asyncAccept(evl, so, &run);
        trace("Listener started");
    }

    void run(Event e) {
        trace("handle new incoming connection");
        auto newSo = so.accept();
        auto fio_connection = new fioTCPConnection(newSo);
        auto server_task = new fioTask((){
            trace("calling incoming server");
            scope(exit) {
                fio_connection.close();
                fio_connection = null;
                newSo = null; // socket closed in fioConnection.close
            }
            server(fio_connection);
        });
    }

    void  close() {
        if ( acceptor !is null ) {
            acceptor.close();
            acceptor = null;
        }
        if ( so !is null ) {
            so.close();
        }
        so = null;
    }
}

class fioTCPConnection {
    string          host;
    ushort          port;
    Socket          so;
    AsyncTimer      timer;
    Fiber           thisFiber;
    bool            _timedout;
    Address         _address;
    asyncConnection _async_connection;

    this(Socket so) {
        this.so = so;
        thisFiber = Fiber.getThis();
        this._async_connection = new asyncConnection(evl, so);
    }

    this(in string host, in ushort port, in Duration timeout = 0.seconds ) {
        this.host = host;
        this.port = port;
        _timedout = false;
        _address = getAddress(host, port)[0];
        thisFiber = Fiber.getThis();

        so = new Socket(_address.addressFamily, SocketType.STREAM, ProtocolType.TCP);
        if ( timeout != 0.seconds ) {
            timer = new AsyncTimer(evl,__FILE__,__LINE__);
            timer.duration = timeout;
            timer.run({
                timer.kill();
                timer = null;
                _timedout = true;
                runnables ~= thisFiber;
            });
        }
        _async_connection = new asyncConnection(evl, so, _address, (Event e){
            if ( timer ) {
                timer.kill();
                timer = null;
            }
            runnables ~= thisFiber;
        });
        Fiber.yield();
    }

    bool connected() const pure nothrow @property {
        return _async_connection && _async_connection.connected();
    }

    bool error() const pure nothrow @property {
        return !_async_connection || _async_connection.error();
    }

    bool instream_closed() const pure nothrow @property {
        return !_async_connection || _async_connection.instream_closed();
    }

    bool outstream_closed() const pure nothrow @property {
        return !_async_connection || _async_connection.outstream_closed();
    }

    int send(const void[] buff, Duration timeout = 60.seconds)
    in {
        assert( buff.length, "You can't send from empty buffer");
    }
    body {
        uint _sent = 0;
        _timedout = false;
        thisFiber = Fiber.getThis();

        if ( _async_connection is null ) {
            // closed or disconnected
            return ERROR;
        }

        if ( _async_connection.error ) {
            trace("trying to send to error-ed socket");
            return ERROR;
        }

        scope(exit) {
            _async_connection.on_send = null;
            if ( timer ) {
                timer.kill();
            }
        }

        if ( timeout != 0.seconds ) {
            timer = new AsyncTimer(evl, __FILE__, __LINE__);
            timer.duration = timeout;
            timer.run({
                timer.kill();
                timer = null;
                _timedout = true;
                runnables ~= thisFiber;
            });
        }
        void __send__(Event e) {
            tracef("event %0x on %s", e.events, this.host);
            while ( _sent < buff.length ) {
                auto rc = so.send(buff[_sent..$]);
                tracef("so.send() = %d", rc);
                if ( rc == Socket.ERROR && wouldHaveBlocked() ) {
                    // will restart when ready
                    trace("block");
                    return;
                }
                if ( rc == Socket.ERROR ) {
                    // failure
                    trace("error");
                    break;
                }
                _sent += rc;
            }
            if ( timer ) {
                this.timer.kill();
                destroy(this.timer);
                this.timer = null;
            }
            _async_connection.on_send = null;
            runnables ~= thisFiber;
        }
        _async_connection.on_send = &__send__;
        Fiber.yield();
        if ( !_sent ) {
            if ( _timedout ) {
                return TIMEOUT;
            }
            if ( _async_connection.error ) {
                return ERROR;
            }
        }
        return _sent;
    }

    int recv(byte[] buff, Duration timeout=0.seconds, bool partial=true)
    /***********************************************
    * Receive data from socket
    * when data received from low level:
    *   if partial is true, return immediately
    *    else continue waiting for data.
    *   if timeout:
    *     return as many data as we can.
    *   if error on socket:
    *   	return and set error state.
    ***********************************************/
    in {
        assert( buff.length, "You can't recv to zero-length buffer");
    }
    body {
        int	received = 0;
        _timedout = false;
        thisFiber = Fiber.getThis();


        if ( _async_connection is null ) {
            // closed or disconnected
            return ERROR;
        }

        if ( _async_connection.instream_closed ) {
            return 0; /// closed already
        }

        scope(exit) {
            _async_connection.on_recv = null;
            if ( timer ) {
                timer.kill();
            }
        }

        if ( timeout != 0.seconds ) {
            timer = new AsyncTimer(evl, __FILE__, __LINE__);
            timer.duration = timeout;
            timer.run({
                timer.kill();
                timer = null;

                _timedout = true;
                runnables ~= thisFiber;
            });
        }

        void __recv__(Event e) {
            tracef("received event %02x", e.events);
//			if ( e.events & Event.HUP ) {
//			}
            if ( e.events & Event.IN ) {
                auto rc = so.receive(buff[received..$]);
                tracef("received from so.receive: %d", rc);
                switch (rc) {
                case Socket.ERROR:
                    goto case; // fall-through
                case 0:
                    // connection closed
                    runnables ~= thisFiber;
                    return;
                default:
                    received += rc;
                    if ( partial || ( received >= buff.length)) { /// return to caller
                        runnables ~= thisFiber;
                    }
                    /// continue to receive
                    return;
                }
            }
        }
        _async_connection.on_recv = &__recv__;
        Fiber.yield();
//		foreach(v;[frm!(received),frm!(_timedout)]){
//			writeln(v);
//		}
        if ( received > 0 ) {
            if ( !partial && received < buff.length ) {
                return TIMEOUT;
            }
            return received;
        }
        if ( _timedout ) {
            return TIMEOUT;
        }
        if ( _async_connection.error ) {
            return ERROR;
        }
        return 0;
    }
    void close() {
        if (timer) {
            timer.kill();
            destroy(timer);
            timer = null;
        }
        if ( so ) {
            so.close();
            destroy(so);
            so = null;
        }
        if ( _async_connection ) {
            destroy(_async_connection);
            _async_connection = null;
        }
    }
}

class fioTask : Fiber {
    enum states {
        init = 0,
        run  = 1,
        fin  = 2
    }
    int 			state;
    void delegate() f;
    Fiber[]			in_wait;

    this(void delegate() f) {
        super(&run, 64*1024);
        this.f = f;
        this.state = states.init;
        runnables ~= this;
//		started[this] = true;
    }
    void wait() {
        auto f = Fiber.getThis();
        if ( this.state == states.run ) {
            return;
        }
        in_wait ~= f;
        Fiber.yield();
    }
private:
    void run() {
        this.state = states.run;
        f();
        this.state = states.fin;
        trace("fioTask Finished");
        if ( in_wait.length ) {
            foreach(ref f; this.in_wait) {
                runnables ~= f;
            }
            Fiber.yield();
        }
    }
}

class fioFiber: Fiber {
    // this is master of all fibers
    this() {
        super(&run);
    }
private:
    void run() {
        // This is fiber's main coordination loop
        while ( !stopped ) {
            do {
                foreach(ref f; runnables) {
                    try {
                        f.call(Rethrow.yes);
                    } catch (Exception e) {
                        error("fio catched Exception: " ~ e.toString);
                    }
                    runnables = runnables[1..$];
                }
            } while (runnables.length); // run again if runnable expanded from inside fiber

            if ( stopped ) {
                break;
            }

            evl.loop(60.seconds);
            trace("ev loop wakeup");
        }
        //destroyAsyncThreads();
    }
}

void runEventLoop() {
    loop.call();
}
void stopEventLoop() {
    stopped = true;
}

unittest {
    globalLogLevel(LogLevel.info);
    auto l_evl = new EventLoop();
    //auto clock = new Clock();
    //info("Phase 1");
    //auto t = new AsyncTimer(l_evl);
    //t.duration = 1.seconds;
    //SysTime start = Clock.currTime(), stop;
    //t.run({
    //	stop = Clock.currTime();
    //});
    //l_evl.loop(5.seconds);
    //assert(990.msecs < stop - start && stop - start < 1010.msecs);
    //info("Phase 1 - ok");
    //info("Phase 2");
    void testAll() {
        info("Test wait");
        void t() {
            info("t started");
            fioSleep(1.seconds);
            info("t finished");
        }
        auto task = new fioTask(&t);
        info("wait for t");
        auto start = Clock.currTime();
        task.wait();
        auto stop = Clock.currTime();
        assert(990.msecs < stop - start && stop - start < 1010.msecs);
        info("Test wait - ok");
        info("Test sleeps");
        void test0() {
            fioSleep(1.seconds);
            int loops = 50;
            foreach(i; 0 .. loops ) {
                start = Clock.currTime();
                fioSleep(50.msecs);
                stop = Clock.currTime();
                assert(45.msecs < stop - start && stop - start < 55.msecs, "bad sleep time, shoul be 50 ms");
                if ( i> 0 && (i % 10 == 0) ) {
                    info(format("%d iterations out of %d", i, loops));
                }
            }
        }
        task = new fioTask(&test0);
        task.wait();
        info("Test sleeps - ok");
        void test1() {
            int loops = 5000;
            infof("%s:%d, tmo=%s - wait", "1.1.1.1", 9998, to!string(10.msecs));
            foreach(i; 0..loops) {
                start = Clock.currTime();
                auto conn = new fioTCPConnection("1.1.1.1", 9998, 10.msecs);
                stop = Clock.currTime();
                assert( !conn.connected );
                conn.close();
                assert(5.msecs < stop - start && stop - start < 20.msecs, format("connect took %s", to!string(stop-start)));
                if ( i> 0 && (i % 1000 == 0) ) {
                    info(format("%d iterations out of %d", i, loops));
                }
            }
            infof("%s:%d, tmo=%s passed", "1.1.1.1", 9998, to!string(10.msecs));
            infof("%s:%d, tmo=%s - wait", "localhost", 9998, to!string(10.msecs));
            loops = 10000;
            foreach(i; 0..loops) {
                start = Clock.currTime();
                auto conn = new fioTCPConnection("localhost", 9998, 10.msecs);
                stop = Clock.currTime();
                assert( !conn.connected );
                assert( stop - start < 5.msecs, "Connection to localhost should return instantly" );
                conn.close();
            }
            infof("%s:%d, tmo=%s passed", "localhost", 9998, to!string(10.msecs));
            infof("%s:%d, tmo=%s - wait", "localhost", 9998, to!string(0.seconds));
            loops = 10000;
            foreach(i; 0..loops) {
                start = Clock.currTime();
                auto conn = new fioTCPConnection("localhost", 9998, 0.seconds);
                stop = Clock.currTime();
                assert( !conn.connected );
                assert( stop - start < 10.msecs,
                    format("Connection to localhost should return instantly, but it took %s", to!string(stop-start))
                );
                conn.close();
            }
            infof("%s:%d, tmo=%s passed", "localhost", 9998, to!string(0.seconds));
            info("test dumb listener");
            void dumb_server(fioTCPConnection c) {
                // nothing
            }
            auto dumb_server_listener = new fioTCPListener("localhost", 9997, &dumb_server);
            loops = 100;
            foreach(i;0..loops) {
                auto c = new fioTCPConnection("localhost", 9997, 10.seconds);
                assert(c.connected);
                //info("connected");
                c.close();
                if ( i> 0 && (i % 10 == 0) ) {
                    info(format("%d iterations out of %d", i, loops));
                }
            }
            dumb_server_listener.close();
            info("test dumb listener - done");
            globalLogLevel(LogLevel.info);
            void test_server(fioTCPConnection c) {
                ///
                /// simple server receive and execute commands
                /// 'q' in fist position in line - exit from processing loop
                /// digit in first position - sleep that number of seconds and echo line
                /// anything else - just echo
                ///
                import std.ascii;
                byte[1024] server_buff;
                int rc;
                info("test echo server started");
                trace("incoming connection");
                do {
                    rc = c.recv(server_buff);
                    tracef("echo server received %d", rc);
                    if ( rc > 0 ) {
                        if ( server_buff[0] == 'q' ) {
                            break;
                        }
                        if ( isDigit(server_buff[0]) ) {
                            int s = to!int(server_buff[0]-'0');
                            tracef("sleeping for %d sec, then send buffer", s);
                            fioSleep(dur!"seconds"(s));
                        }
                       rc = c.send(server_buff[0..rc]);
                       tracef("echoserver sent %d", rc);
                    }
                    //fioSleep(1.seconds);
                } while (rc > 0);
                info("echo server loop done");
            }
            auto listener = new fioTCPListener("localhost", 9999, &test_server);
            string host = "localhost";
            ushort port = 9999;
            Duration t = 5.seconds;
            loops = 1;
            infof("%s:%d, tmo=%s - wait", host, port, to!string(t));
            foreach(i; 0..loops) {
                byte[10] buff;
                int send_rc, recv_rc;
                char[32000] c = 'c';
                start = Clock.currTime();
                auto conn = new fioTCPConnection(host, port, t);
                assert(conn.connected);
                assert(!conn.instream_closed);
                assert(!conn.outstream_closed);
                assert(!conn.error);
                stop = Clock.currTime();
                assert( conn.connected );
                trace("connected");
                conn.send("abc");
                start = Clock.currTime();
                recv_rc = conn.recv(buff);
                stop = Clock.currTime();
                infof("receive any input, default timeout (should receive 3 bytes immediately)");
                infof("received %d bytes in %s", recv_rc, to!string(stop-start));
                assert(recv_rc == 3);
                conn.send("1abc");
                start = Clock.currTime();
                recv_rc = conn.recv(buff, 3.seconds); // receive anything in 3 seconds
                stop = Clock.currTime();
                infof("receive any input, 3 sec timeout (should receive 4 bytes in 1sec)");
                infof("received %d bytes in %s", recv_rc, to!string(stop-start));
                assert(recv_rc == 4);
                conn.send("1abc");
                start = Clock.currTime();
                recv_rc = conn.recv(buff, 3.seconds, false); // wait 3 sec, require full buff -> should be timeout
                stop = Clock.currTime();
                infof("receive exactly %d bytes with 3 sec timeout(should timeout)", buff.length);
                infof("received %d bytes in %s", recv_rc, to!string(stop-start));
                assert(recv_rc == TIMEOUT);
                conn.send("1234567890");
                start = Clock.currTime();
                recv_rc = conn.recv(buff, 3.seconds, false); // wait 3 sec, require full buff
                stop = Clock.currTime();
                infof("receive exactly %d bytes with 3 sec timeout(should succeed)", buff.length);
                infof("received %d bytes in %s", recv_rc, to!string(stop-start));
                assert(recv_rc == 10);
                conn.send("3abc");
                start = Clock.currTime();
                recv_rc = conn.recv(buff, 1.seconds); // wait 1 sec, should be timeout
                stop = Clock.currTime();
                infof("receive any input, 1 sec timeout (should timeout)");
                infof("received %d bytes in %s", recv_rc, to!string(stop-start));
                assert(recv_rc == TIMEOUT);
                conn.recv(buff, 5.seconds); // eat all input
                info("stop echo server");
                conn.send("q");
                start = Clock.currTime();
                recv_rc = conn.recv(buff);
                stop = Clock.currTime();
                infof("received %d bytes in %s", recv_rc, to!string(stop-start));
                recv_rc = conn.recv(buff);
                do {
                    send_rc = conn.send(buff);
                    fioSleep(100.msecs);
                } while (send_rc > 0);
                infof("recv_rc: %d, send_rc: %d", recv_rc, send_rc);
                conn.close();
                // verify that closed connection behave correctly
                recv_rc = conn.recv(buff);
                assert(recv_rc == ERROR);
                send_rc = conn.send(buff);
                assert(send_rc == ERROR);
                assert(!conn.connected);
                assert(conn.instream_closed);
                assert(conn.outstream_closed);
                assert(conn.error);
            }
            infof("%s:%d, tmo=%s passed", host, port, to!string(t));
            globalLogLevel(LogLevel.info);
        }
        info("Test connection");
        task = new fioTask(&test1);
        task.wait();
        info("Test connection - ok");
        stopEventLoop();
    }
    new fioTask(&testAll);
    //void testf2() {
    //	foreach(i; 0 .. 100_000) {
    //		trace("connect ", i);
    //		auto conn = new fioConnection("1.1.1.1", 9999, 50.msecs);
    //		if ( !conn || !conn.connected ) {
    //			trace("Can't connect");
    //		} else{
    //		}
    //		conn.close();
    //		destroy(conn);
    //		trace("---");
    //		//GC.collect();
    //		//GC.minimize();
    //	}
    //	stopEventLoop();
    //}
    //new fioTask(&testf2);
    runEventLoop();
    writeln("Test finished");
}