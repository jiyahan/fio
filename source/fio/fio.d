module fio;

private import std.stdio: writeln;
private import std.datetime;
private import std.experimental.logger;
private import std.socket;
private import std.format;
private import std.range;
private import std.conv;
private import std.typecons;
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

static Exception RingEmpty;
static Exception RingFull;

static this() {
    RingEmpty = new Exception("Ring empty");
    RingFull = new Exception("Ring full");
}

struct Ring {
    ///
    /// cyclic buffer for runnable fibers
    ///
    int _front;
    int _back;
    Fiber[1024] holder;

    int length() pure const nothrow @nogc @property {
        return (_back-_front)%holder.length;
    }
    ulong capacity() pure const nothrow @nogc @property {
        return holder.length - this.length;
    }
    void opOpAssign(string s: "~", T)(T x) @nogc {
        if (this.length >= holder.length ) {
            throw RingFull;
        }
        holder[_back] = x;
        _back = (_back+1) % holder.length;
    }
    bool empty() @property @nogc pure const nothrow {
        return this.length == 0;
    }
    Fiber front() @property @nogc {
        if ( empty ) throw RingEmpty;
        return holder[_front];
    }
    void popFront() @nogc {
        if ( empty ) throw RingEmpty;
        _front =( _front+1)%holder.length;
    }
}

private static EventLoop 	evl;
private static bool      	stopped = false;
private static fioFiber  	loop;

private static Ring          runnables;
private static bool[fioTask]    started;
private static bool[fioTask]    zombie;

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
    auto t = scoped!AsyncTimer(evl);
    auto f = Fiber.getThis();
    t.duration = d;
    t.run({
        runnables ~= f; // continue task
    });
    Fiber.yield();  // pass control to main
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
        so.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, 1);
        so.bind(_address);
        so.listen(BACKLOG);
        acceptor = new asyncAccept(evl, so, &run);
        trace("Listener started");
    }

    void run(Event e) {
        trace("handle new incoming connection");
        void run() {
            auto newSo = so.accept();
            auto fio_connection = scoped!fioTCPConnection(newSo);
            scope(exit) {
                fio_connection.close();
                destroy(newSo);
            }
            server(fio_connection);
        }
        auto server_task = new fioDaemonTask(&run);
    }

    void  close() {
        if ( acceptor !is null ) {
            acceptor.close();
            acceptor = null;
        }
        if ( so !is null ) {
            so.close();
            destroy(so);
            so = null;
        }
    }
}

class fioTCPConnection {
    string          host;
    ushort          port;
    Socket          so;
    Fiber           thisFiber;
    bool            _timedout;
    Address[]       _address;
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
        _address = getAddress(host, port);
        auto t = scoped!AsyncTimer(evl);
        thisFiber = Fiber.getThis();

        so = new Socket(_address[0].addressFamily, SocketType.STREAM, ProtocolType.TCP);
        if ( timeout != 0.seconds ) {
            t.duration = timeout;
            t.run({
                _timedout = true;
                runnables ~= thisFiber;
            });
        }
        _async_connection = new asyncConnection(evl, so, _address[0], (Event e){
            runnables ~= thisFiber;
        });
        Fiber.yield();
    }

    void close() {
        if ( so ) {
            //so.close(); // destroy will close
            destroy(so);
            so = null;
        }
        if ( _async_connection ) {
            destroy(_async_connection);
            _async_connection = null;
        }
        destroy(_address);
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

    int send(const void[] buff, in Duration timeout = 60.seconds)
    in {
        assert( buff.length, "You can't send from empty buffer");
    }
    body {
        uint _sent = 0;
        _timedout = false;
        auto timer = scoped!AsyncTimer(evl);
        thisFiber = Fiber.getThis();

        if ( _async_connection is null ) {
            // closed or disconnected
            return ERROR;
        }

        if ( _async_connection.error ) {
            std.experimental.logger.error("trying to send to error-ed socket");
            return ERROR;
        }

        scope(exit) {
            _async_connection.on_send = null;
        }

        if ( timeout != 0.seconds ) {
            timer.duration = timeout;
            timer.run({
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

    int recv(byte[] buff, in Duration timeout=0.seconds, bool partial=true)
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
        auto timer = scoped!AsyncTimer(evl);


        if ( _async_connection is null ) {
            // closed or disconnected
            return ERROR;
        }

        if ( _async_connection.instream_closed ) {
            return 0; /// closed already
        }

        scope(exit) {
            _async_connection.on_recv = null;
        }

        if ( timeout != 0.seconds ) {
            timer.duration = timeout;
            timer.run({
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
}

class fioDaemonTask: fioTask {
    @disable override void wait() {};
    this(void delegate() f) {
        super(f, true);
    }
}

class fioTask : Fiber {
    void delegate() f;
    Fiber[]			in_wait;
    bool            daemon;

    this(void delegate() f, bool daemon=false) {
        super(&run, 64*1024);
        this.f = f;
        this.daemon = daemon;
        runnables ~= this;
    }

    void wait() {
        if ( state == Fiber.State.TERM ) {
            return;
        }
        auto f = Fiber.getThis();
        in_wait ~= f;
        Fiber.yield();
    }

private:
    void run() {
        started[this] = true;
        f();
        if ( daemon ) {
            zombie[this]=true;
            assert(started[this] == true);
            auto removed = started.remove(this);
            assert(removed);
            return;
        }
        if ( in_wait.length ) {
            runnables ~= this;
            foreach(ref f; in_wait) {
                runnables ~= f;
            }
            Fiber.yield();
        }
        started.remove(this);
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
            while ( runnables.length ) {
                auto f = runnables.front();
                runnables.popFront();
                try {
                    f.call(Rethrow.yes);
                } catch (Exception e) {
                    error("fio catched Exception: " ~ e.toString);
                }
            }
            if ( stopped ) {
                break;
            }
            foreach(ref z; zombie.byKey) {
                zombie.remove(z);
                destroy(z);
            }
            evl.loop(60.seconds);
            trace("ev loop wakeup");
        }
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
    void testAll() {
        info("Test wait");
        void t() {
            info("t started");
            fioSleep(1.seconds);
            info("t finished");
        }
        auto task = new fioTask(&t);
        auto start = Clock.currTime();
        task.wait();
        auto stop = Clock.currTime();
        assert(990.msecs < stop - start && stop - start < 1010.msecs);
        info("Test wait - ok");

        globalLogLevel(LogLevel.info);
        info("Test task create/destroy");
        int loops = 50000;
        int c = 0;
        void empty() {
            c++;
        }
        info("Test non-daemon task wait() after some delay");
        foreach(i; 0..loops) {
            auto z = new fioTask(&empty);
            fioSleep(1.msecs);
            z.wait();
            destroy(z);
        }
        info("Test non-daemon task wait() right after start");
        foreach(i; 0..loops) {
            auto z = new fioTask(&empty);
            z.wait();
            destroy(z);
        }
        info("Test daemon task");
        foreach(i; 0..loops) {
            auto z = new fioDaemonTask(&empty);
            fioSleep(1.msecs);
        }
        info("Test task create/destroy - ok");
        fioSleep(5.seconds);
        writeln(c, " s:", started.length," z:", zombie.length);
        globalLogLevel(LogLevel.info);


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
        writeln(c, " started=", started.length, " zombie=", zombie.length);

        void test1() {
            int loops = 5000;
            infof("%s:%d, tmo=%s - wait", "1.1.1.1", 9998, to!string(10.msecs));
            foreach(i; 0..loops) {
                start = Clock.currTime();
                auto conn = new fioTCPConnection("1.1.1.1", 9998, 10.msecs);
                stop = Clock.currTime();
                assert( !conn.connected );
                conn.close();
                destroy(conn);
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
                destroy(conn);
            }
            infof("%s:%d, tmo=%s passed", "localhost", 9998, to!string(10.msecs));
            infof("%s:%d, tmo=%s - wait", "localhost", 9998, to!string(0.seconds));
            loops = 10000;
            foreach(i; 0..loops) {
                start = Clock.currTime();
                auto conn = new fioTCPConnection("localhost", 9998, 0.seconds);
                stop = Clock.currTime();
                assert( !conn.connected );
                assert( stop - start < 5.msecs,
                    format("Connection to localhost should return instantly, but it took %s", to!string(stop-start))
                );
                conn.close();
                destroy(conn);
            }
            infof("%s:%d, tmo=%s passed", "localhost", 9998, to!string(0.seconds));
            info("test dumb listener");
            void dumb_server(fioTCPConnection c) {
                // nothing
            }
            foreach(int j; 0..10000) {
                auto dumb_server_listener = scoped!fioTCPListener("localhost", cast(ushort)9997, &dumb_server);
                loops = 100;
                foreach(i;0..loops) {
                    auto c = scoped!fioTCPConnection("localhost", cast(ushort)9997, 10.seconds);
                    assert(c.connected);
                    c.close();
                }
                dumb_server_listener.close();
                if ( j > 0 && (j % 1000 == 0) ) {
                    info(format("%d iterations out of %d", j, 1000));
                }
            }
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
                destroy(conn);
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
    runEventLoop();
    writeln("Test finished");
}