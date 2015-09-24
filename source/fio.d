module fio;

private import std.stdio: writeln;
private import std.datetime;
private import std.experimental.logger;
private import std.socket;
private import std.format;
private import std.algorithm;
private import std.array;
private import std.conv;
private import std.traits;
private import std.variant;
private import std.exception;
private import std.container.dlist;
private import core.sys.posix.sys.wait: wait;
private import std.typecons;
private import std.algorithm: remove, countUntil, map, each;
private import core.thread;
private import core.memory;
private import core.exception;
private import core.sys.posix.unistd: fork;

private import poll;

alias Partial = Flag!"Partial";

template frm(alias v) {
    string frm() {
        string frm = v.stringof;
        frm ~= "=" ~ to!string(v);
        return frm;
    }
}

static Exception RingEmpty;
static Exception RingFull;
static Exception ResultNotReady;
static Exception TimeoutException;

static this() {
    RingEmpty = new Exception("Ring empty");
    RingFull = new Exception("Ring full");
    ResultNotReady = new Exception("Task not ready");
    TimeoutException = new Exception("Timeout");

    loop = new fioFiber;
    _STACKSIZE = 64*1024; // default stacksize
}

///
/// cyclic buffer for runnable fibers
///
private struct Ring {
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

private static EventLoop 	   evl;
private static fioFiber  	   loop;
private static int             _STACKSIZE;
private static Ring            runnables;
private static bool[fioTask]   started;
private static bool[fioTask]   zombie;

enum {
    ERROR = -1,
    TIMEOUT = -2
}

enum BACKLOG = 1024;

///
/// Get stack size for new fibers. Default value is 64k.
///
int stacksize() @property {
    return _STACKSIZE;
}
///
/// Set stack size for new fibers
///
void stacksize(int s) @property {
    _STACKSIZE = s;
}
///
/// Syncronization between fibers.
///
class MsgBox {
    private DList!Variant msgList;
    private DList!Fiber   waitList;
    ///
    /// Post message to box.
    /// Parameters:
    ///    msg = data to post. Can be empty if you just need to wake up some fiber.
    /// Return:
    ///		void
    void post(in Variant msg = Variant.init) @trusted {
        msgList.insertBack(msg);
        if ( !waitList.empty ) {
            tracef("Wake up first");
            // wake up first
            auto r = waitList.front();
            waitList.removeFront();
            runnables ~= r;
        } else {
            tracef("Nobody wait on this mbox");
        }
    }
    ///
    /// Wait and get message from box
    /// Parameters:
    ///		timeout = how long to wait
    ///	Returns:
    ///		message posted.
    /// Can throw exception TimeoutException.
    ///
    Variant waitAndGet(Duration timeout = 0.seconds) @trusted {
        auto thisFiber = Fiber.getThis();
        bool _timedout;

        auto t = scoped!AsyncTimer(evl);
        if ( timeout != 0.seconds ) {
            t.duration = timeout;
            t.run({
                trace("Waiting on mbox timedout");
                _timedout = true;
                runnables ~= thisFiber;
            });
        }

        while ( msgList.empty ) {
            waitList ~= thisFiber;
            trace("Waiting on emty msgbox");
            Fiber.yield();
            if ( !msgList.empty || _timedout ) {
                break;
            }
        }
        if ( _timedout ) {
            throw TimeoutException;
        }
        auto msg = msgList.front();
        msgList.removeFront();
        return msg;
    }
}
///
unittest {
    globalLogLevel(LogLevel.info);
    info("Test msgBox");
    makeApp((){
        auto box = new MsgBox;

        auto f1 = makeFuture((int arg){
                Variant msg = arg;
                info("post 1st msg");
                box.post(msg);
                fioSleep(500.msecs);
                msg = arg + 1;
                info("post 2nd msg");
                box.post(msg);
                info("post empty msg");
                box.post();
            }, 1).start();
        auto f2 = makeFuture((){
                infof("Wait for 1st msg");
                auto i = box.waitAndGet(1.seconds);
                assert(i == 1);
                infof("Wait for 2nd msg");
                i = box.waitAndGet(1.seconds);
                assert(i == 2);
                auto e = box.waitAndGet();
                info("Check timeouts on waiting for msg");
                assertThrown(box.waitAndGet(1.seconds));
            }).start();
        auto t = tuple(f1,f2);
        t.waitAll(10.seconds);
    });
    runEventLoop();
    info("MsgBox tests ok");
}

///
/// Excecution unit that run in "background". You can't wait for its completion or get result.
/// To run asyncronous code that will return any result use Future
///
class Daemon(F, A...) {
  private:
    F           g;
    A           a;
    fioDaemonTask    _task;
  public:
    ///
    /// Constructor
    /// Params:
    /// f = function or generator
    /// a = args to call f
    ///
    this(F f, A a) @safe pure {
        this.g = f;
        this.a = a;
    }
    ///
    /// You have to call start() method to start daemon running
    /// Params:
    ///
    /// Return:
    ///  Daemon object (you can chain calls)
    auto start() {
        _task = new fioDaemonTask({
                g(a);
        });
        return this;
    }
}

///
/// Execution unit that run in "foreground" - you can wait for completion and get results
///
class Future(F, A...) {
  private:
    F             g;
    A             a;
    fioTask _task;
  public:
    ///
    /// Constructor
    /// Params:
    /// f = function or generator
    /// a = args to call f
    ///
    this(F f, A a) @safe pure nothrow @nogc {
        this.g = f;
        this.a = a;
    }
    ///
    /// You have to call start() to start Future execution
    ///
    static if ( is (ReturnType!F==void) ) {
        private auto _result = null;
        auto start() @safe {
            _task = new fioTask({
                g(a);
            });
            return this;
        }
    } else {
        private ReturnType!F _result;
        auto start() @safe {
            _task = new fioTask({
                _result = g(a);
            });
            return this;
        }
    }
    ///
    /// Get return value of f when f completes.
    ///
    auto get() @property @safe {
        if ( ready ) {
            return _result;
        }
        throw ResultNotReady;
    }
    ///
    /// Get completion status and result readiness
    ///
    /// Return:
    ///   True if task were started and were finished
    ///
    bool ready() @property @safe {
        return _task !is null && _task.ready;
    }
    ///
    /// Wait for task completion. Also start task if you forget to call method start()
    /// Params:
    ///		d = how long to wait (forewer by default)
    /// Return:
    ///		status of wait (SUCCESS or TIMEDOUT)
    ///
    int wait(Duration d = 0.seconds) {
        if ( _task is null ) {
            start();
        }
        return _task.wait(d);
    }

    ///
    /// Wait for task completion and return result. Also start task if you forget to call method start()
    /// Params:
    ///		d = how long to wait (forewer by default)
    /// Return:
    ///		result of f, or exceptioin ResultNotReady if result is not ready after timeout d expired.
    ///
    auto waitAndGet(Duration d = 0.seconds) @property {
        if ( _task is null ) {
            start();
        }
        _task.wait(d);
        return get();
    }
}

///////////////////////////////////////////////////////////
///
/// makeFuture create "Future" - execution unit that you
/// can start, wait for completion and get result
/// Params:
/// 	f = function or delegate to run
///  a = args for calling f
///
///////////////////////////////////////////////////////////
auto makeFuture(F, A...)(F f, A a) @safe pure nothrow {
  return new Future!(F, A)(f, a);
}
///
unittest {
    globalLogLevel(LogLevel.info);
    info("Test Future");
    new fioTask((){
        int base = 1;
        void f0() {
        }
        int f1(int a) {
            return a;
        }
        int f2(int a, int b) {
            infof("f2 sleep, wait %d seconds, please.", a);
            fioSleep(dur!"seconds"(a));
            return b+base;
        }
        auto t0 = makeFuture(&f0).start();
        auto t1 = makeFuture(&f1, 1).start();
        auto t2 = makeFuture(&f2, 5, 1).start();
        auto t3 = makeFuture((string s) {
                assert(s == "hello");
            }, "hello").start();
        auto t = tuple(t0, t1, t2, t3);
        assert(t.waitAll(1.seconds) == [0,0,TIMEOUT,0]);
        info("Future waitAll - ok");
        auto tasks = map!(a => makeFuture(&f1, a).start)([1, 2, 3]).array();
        tasks ~= makeFuture(&f1, 4).start;
        //fioSleep(1.seconds);
        assert(tasks.map!(a => a.waitAndGet).array() == [1,2,3,4]);
        info("Future waitAndGet - ok");
        auto d = makeDaemon(
                (int a) {
                    infof("Daemon got %d and return", a);
                }, 1
            ).start();
        stopEventLoop();
    });
    runEventLoop();
    info("Test Future! Done");
}

//////////////////////////////////////////////////////////////////////////
///
/// makeDaemon create "Daemon" - execution unit that you
/// can start but can't wait for completion or get result.
/// All you can do with daemons - wait when all daemons finish with call
/// waitForAllDaemons()
/// Params:
/// 	f = function or delegate to run
///  a = args for calling f
///
//////////////////////////////////////////////////////////////////////////
auto makeDaemon(F, A...)(F f, A a) @safe pure nothrow {
    return new Daemon!(F, A)(f, a);
}

/**
 *
 * makeApp create "Future" that
 * will stop eventLoop when completed
 *
 * Params:
 * 	f = function or delegate to run
 *  a = args for calling f
 * Return:
 *  Nothing
 *
 **/
auto makeApp(F, A...)(F f, A a) {
    makeFuture({
        auto w = makeFuture(f, a);
        w.wait();
        stopEventLoop();
    }).start();
}
///
unittest {
    globalLogLevel(LogLevel.info);
    info("Test makeApp");
    makeApp((){
        void f0() {
            auto aa = [1:1];
        }
        auto t0 = makeFuture(&f0).start();
        t0.wait();
    });
    runEventLoop();
    info("App finished");
}

/////////////////////////////////////////////////////////////////
///
/// wait(polling used) until all started daemon tasks finish
/// Params:
/// timeout = ,how long to wait
/// Return:
/// TIMEOUT or 0
///
/////////////////////////////////////////////////////////////////
int waitForAllDaemons(Duration timeout = 0.seconds) {
    //
    // wait(polling) until all started daemon tasks finish
    //
    auto deadline = Clock.currTime + timeout;
    while ( !started.keys.filter!(t => t.daemon).empty ) {
        fioSleep(100.msecs);
        if ( timeout > 0.seconds && Clock.currTime > deadline ) {
            return TIMEOUT;
        }
    }
    return 0;
}

/////////////////////////////////////////////////////////////////
///
/// Wait until all tasks in array finish
/// Params:
///		tasks = range of objects, supporting wait()
///		d = time to wait
/// Return:
///		array of the 0(success) and TIMEOUT(failed to wait)
///
/////////////////////////////////////////////////////////////////
int[] waitAll(W)(W tasks, Duration d = 0.seconds) {
    int[]   result;
    Duration timeleft = d;
    foreach(t; tasks) {
        if ( timeleft > 0.seconds ) {
            auto start = Clock.currTime;
            auto r = t.wait(timeleft);
            result ~= r;
            auto stop = Clock.currTime;
            timeleft -= stop - start;
        } else {
            if ( t.ready ) {
                result ~= 0;
            } else {
                result ~= TIMEOUT;
            }
        }
    }
    return result;
}

//////////////////////////////////////////////////////////////
/// sleep for some duration.
///		you have to use this sleep as it allow concurrency
///	Params:
///		d = duration
///	Return:
///		void
///	Examples:
///		`fioSleep(1.seconds)`
///////////////////////////////////////////////////////////////
void fioSleep(in Duration d) {
    auto t = scoped!AsyncTimer(evl);
    auto f = Fiber.getThis();
    t.duration = d;
    t.run({
        runnables ~= f; // continue task
    });
    Fiber.yield();  // pass control to main
}
///////////////////////////////////////////////////////////////
///
/// Create TCPListener instance.
/// Note:
///   1. Listener will accept connections only after call to method start().
///   2. After start() you may want to use method serve() to accept and serve requests
///      until method stop is called()
///	Params:
///		host = interface to listen
///		port = port to listen
///		f = function or delegate to execute on each connectiom
/// Return:
///		instance of class fioTCPListener
///
///////////////////////////////////////////////////////////////
auto makeTCPListener(F)(string host, ushort port, F f) {
    return new fioTCPListener!(F)(host, port, f);
}
///
/// Incoming TCP connections handler
/// Common 
///
class fioTCPListener(F) {
  private:
    F       server;

    string 	host;
    ushort 	port;
    Address _address;
    Socket 	so;
    asyncAccept acceptor;
    uint    _childs;
    bool    stopped;
    Fiber   servingFiber;

    static if ( is(SocketOption.REUSEPORT) ) {
        enum So_REUSEPORT = SocketOption.REUSEPORT;
    } else {
        version(linux) {
            enum So_REUSEPORT = cast(SocketOption)15;
        }
        version(FreeBSD) {
            enum So_REUSEPORT = cast(SocketOption)0x200;
        }
    }

  public:
    this(string host, ushort port, F d) @safe {
        server = d;
        _address = getAddress(host, port)[0];
        _childs = 0;
        so = new Socket(_address.addressFamily, SocketType.STREAM, ProtocolType.TCP);
        so.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, 1);
        so.setOption(SocketOptionLevel.SOCKET, So_REUSEPORT, 1);
        so.blocking(false);
        so.bind(_address);
        so.listen(BACKLOG);
        trace("Listener started");
    }
    ///
    /// Start accepting connections
    ///
    /// Returns:
    ///		this
    auto start() {
        if ( evl is null ) {
            evl = new EventLoop;
        }
        acceptor = new asyncAccept(evl, so, &run);
        return this;
    }
    ///
    /// Implement loop (also start listening if not done yet).
    /// ---
    /// while ( ! stopped ) { accept-and-handle;}
    /// ---
    ///
    auto serve() in {
        assert(servingFiber is null);
    } body {
        if ( evl is null ) {
            evl = new EventLoop;
        }
        if ( acceptor is null ) {
            acceptor = new asyncAccept(evl, so, &run);
        }
        // just sleep while not stopped
        servingFiber = Fiber.getThis();
        while ( !stopped ) {
            Fiber.yield();
        }
    }
    ///
    /// Break serve() loop.
    /// Note: Connection is not closed and still can accept (call close() to close socket)
    ///
    auto stop() {
        if ( stopped || servingFiber is null ) {
            return;
        }    
        stopped = true;
        runnables ~= servingFiber;
        servingFiber = null;
    }
    ///
    /// Before call to start() you can fork Listener, it wil accept connectons
    /// in several processes. See forked_server in examples.
    ///
    /// Params:
    ///		n = how many processes to fork
    /// Return:
    ///		int 0 in parent, 1 in child
    ///
    auto fork(int n) {
        while(n) {
            auto pid = .fork();
            if ( pid ) {
                n--;
                _childs++;
            } else if ( pid == 0 ) {
                return 0;
            }
        }
        return 1;
    }
    ///
    /// Parent can call for forked childs
    ///
    auto waitForForkedChilds() {
        while( _childs ) {
            int s;
            .wait(&s);
            _childs--;
        }
    }
    void run(Event e) {
        trace("handle new incoming connection");
        auto server_task = new fioDaemonTask({
            try {
                auto newSo = so.accept();
                auto fio_connection = scoped!fioTCPConnection(newSo);
                scope(exit) {
                    fio_connection.close();
                    destroy(newSo);
                }
                server(fio_connection);
            } catch (SocketAcceptException e) {
                error("SocketAcceptException");
            }
        });
    }
    ///
    /// stop listening and close socket.
    ///
    void  close() {
        trace("close listener");
        if ( acceptor !is null ) {
            acceptor.close();
            acceptor = null;
        }
        if ( so !is null ) {
            so.close();
            destroy(so);
            so = null;
        }
        stop();
    }
}
///
/// Handle connect(for outgoing connections),send,receive.
///
class fioTCPConnection {
    string          host;
    ushort          port;
    Socket          so;
    Fiber           thisFiber;
    bool            _timedout;
    Address[]       _address;
    asyncConnection _async_connection;

    ///
    /// Constructor for pre-created sockets (accepted connection for example)
    ///
    /// Params:
    ///   so = socket
    this(Socket so) {
        this.so = so;
        this._async_connection = new asyncConnection(evl, so);
    }
    ///
    ///  Constructor
    ///  Create socket, connect to remote end. You can find connection status using
    ///  connected() method
    ///
    ///  Params:
    ///     host = host (ip or name to connect)
    ///     port = port to connect
    ///     timeout = how long to wait for connection
    ///
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
        if ( _async_connection.error ) {
            return;
        }
        Fiber.yield();
    }
    ///
    /// Close socket
    ///
    void close() {
        if ( so ) {
            so.close();
            destroy(so);
            so = null;
        }
        if ( _async_connection ) {
            destroy(_async_connection);
            _async_connection = null;
        }
        destroy(_address);
    }
    ///
    /// Last operation timeout status.
    ///
    /// Return:
    ///   true or false
    ///
    bool timedout() const pure nothrow @property {
        return _timedout;
    }
    ///
    /// Status of connection.
    ///
    /// Return:
    ///   true or false
    bool connected() const pure nothrow @property {
        return _async_connection && _async_connection.connected();
    }
    ///
    /// Error state on socket
    ///
    /// Return:
    ///   true or false
    bool error() const pure nothrow @property {
        return !_async_connection || _async_connection.error();
    }

    bool instream_closed() const pure nothrow @property {
        return !_async_connection || _async_connection.instream_closed();
    }

    bool outstream_closed() const pure nothrow @property {
        return !_async_connection || _async_connection.outstream_closed();
    }
    ///
    /// Send data from buffer or timeout.
    ///
    /// Params:
    ///    buff = buffer with data
    ///	   len = range to send
    ///    timeout = timeout for data sending
    ///	Return:
    ///		number of transmitted bytes or ERROR
    ///
    int send(const void[] buff, size_t len, in Duration timeout = 60.seconds) {
        return send(buff[0..len], timeout);
    }
    ///
    /// Send data from buffer or timeout.
    ///
    /// Params:
    ///    buff = data to send
    ///    timeout = timeout for data sending
    ///	Return:
    ///		number of transmitted bytes or ERROR
    ///
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
    ///
    /// Receive data from connection.
    /// 
    /// When any data received from socket level:
    ///  if partial is true, return immediately
    ///    else continue waiting for data.
    ///  if timeout:
    ///    return as many data as we can.
    ///  if error on socket:
    ///   return and set error state.
    /// Params:
    ///    buff = buffer to receive data to.
    ///    timeout = how long to wait data.
    ///	   partial = if we allow to receive less data then buffer can accept.
    /// Return:
    ///    number of received bytes or ERROR (timedout or error on socket).
    ///	   if socket closed - return 0
    ///
    int recv(byte[] buff, in Duration timeout=0.seconds, in Partial partial=Partial.yes,
        in string __file__ = __FILE__, in size_t __line__ = __LINE__)
    in {
        assert(buff.length > 0, format("You can't recv to zero-length buffer %s:%d", __file__, __line__));
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
            return 0; // closed already
        }

        scope(exit) {
            _async_connection.on_recv = null;
        }

        auto timer = scoped!AsyncTimer(evl);
        if ( timeout != 0.seconds ) {
            timer.duration = timeout;
            timer.run({
                _timedout = true;
                runnables ~= thisFiber;
            });
        }

        void __recv__(Event e) {
            tracef("received event %02x", e.events);
//            if ( e.events & Event.HUP ) {
//                runnables ~= thisFiber;
//                return;
//            }
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
                    if ( partial || ( received >= buff.length)) { // return to caller
                        runnables ~= thisFiber;
                    }
                    // continue to receive
                    return;
                }
            }
        }
        _async_connection.on_recv = &__recv__;
        Fiber.yield();
//        tracef("fioTCPConnection:read finished with received: %d, timedout: %s, error: %s",
//                received, to!string(_timedout), to!string(_async_connection.error));
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
    @disable override int wait(Duration d) {return 0;};
    @disable override bool ready() @property @trusted nothrow const {return false;};
    this(void delegate() f) {
        super(f, true);
    }
}

class fioTask : Fiber {
    void delegate() f;
    Fiber[]			in_wait;
    bool            daemon;

    this(void delegate() f, bool daemon=false) @trusted {
        super(&run, _STACKSIZE);
        this.f = f;
        this.daemon = daemon;
        runnables ~= this;
    }

    bool ready() @property @trusted nothrow const {
        return state == Fiber.State.TERM;
    }

    int wait(Duration d=0.seconds) @trusted {
        if ( state == Fiber.State.TERM ) {
            return 0;
        }
        auto timer = scoped!AsyncTimer(evl);
        auto thisFiber = Fiber.getThis();
        bool _timedout;

        if ( d != 0.seconds ) {
            timer.duration = d;
            timer.run({
                _timedout = true;
                runnables ~= thisFiber;
                // remove this fiber from in_wait list
                auto i = countUntil(in_wait, thisFiber);
                if ( i >= 0 ) {
                    in_wait = remove(in_wait, i);
                }
            });
        }
        in_wait ~= thisFiber;
        Fiber.yield();
        if ( _timedout ) {
            return TIMEOUT;
        } else {
            return 0;
        }
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
    bool stopped = false;
    this() {
        super(&run);
    }
private:
    void run() {
        // This is fiber's main coordination loop
        trace("ioloop started");
        Throwable exception;
        while ( !stopped ) {
            while ( runnables.length ) {
                auto f = runnables.front();
                runnables.popFront();
                exception = f.call(Rethrow.no);
            }
            if ( stopped || exception ) {
                break;
            }
            foreach(ref z; zombie.byKey) {
                zombie.remove(z);
                destroy(z);
            }
            evl.loop(60.seconds);
            trace("ev loop wakeup");
        }
        if ( exception ) {
            loop.stopped = true;
            loop = null;
            throw exception;
        }
        trace("ioloop stopped");
    }
}

void runEventLoop() {
    if ( evl is null ) {
        evl = new EventLoop();
    }
    if ( loop is null ) {
        loop = new fioFiber();
    }
    loop.call();
}

void stopEventLoop() {
    loop.stopped = true;
    loop = null;
}

unittest {
    globalLogLevel(LogLevel.info);
    info("Test exception");
    makeApp((){
        void f0() {
            auto aa = [1:1];
            auto bb = aa[2]; // this will throw exception
        }
        auto t0 = makeFuture(&f0).start();
        t0.wait();
    });
    try {
        runEventLoop();
    } catch (RangeError e) {
        info("Test exception Done");
    }
}
///
/// Create signal handler
///
/// Params:
///		sig = signal number
///		f = function or delegate - handler
///	Return:
///		SignalHandler object
///
auto makeSignalHandler(F)(int sig, F f,  in string file = __FILE__ , in size_t line = __LINE__) @safe  {
    return new SignalHandler!F(evl, sig, f, file, line);
}
///
unittest {
    ///
    /// test Signal
    ///
    import core.sys.posix.signal;
    info("Test Signal");
    bool    signalled;
    makeApp((){
        auto sig = makeSignalHandler(SIGINT, (int s){
            infof("got signal %d", s);
            signalled = true;
        });
        scope(exit) {
            sig.restore();
        }
        fioSleep(1.seconds);
        kill(getpid(), SIGINT);
        fioSleep(1.seconds);
        assert(signalled);
    });
    runEventLoop();
    info("Test Signal done");
}

unittest {
    info("Test 10k connections");
    makeApp((){
        fioTCPConnection[] a;
        auto s = makeTCPListener("localhost", cast(ushort)9997, (fioTCPConnection c){
            fioSleep(60.seconds);
        }).start();
        foreach(i; 0..50) {
            auto n = new fioTCPConnection("localhost", cast(ushort)9997);
            assert(n.connected);
            a ~= n;
        }
        foreach(c; a) {
            c.close();
        }
        s.close();
    });
    runEventLoop();
    info("Test 10k connections passed");
}

unittest {
    globalLogLevel(LogLevel.info);
    void testAll() {
        info("Test wait");
        auto task = new fioTask({
            info("t started");
            fioSleep(1.seconds);
            info("t finished");
        });
        auto start = Clock.currTime();
        task.wait();
        auto stop = Clock.currTime();
        assert(990.msecs < stop - start && stop - start < 1010.msecs);
        task = new fioTask({
            info("t started");
            fioSleep(1.seconds);
            info("t finished");
        });
        start = Clock.currTime();
        auto rc = task.wait(500.msecs); // wait with timeout
        stop = Clock.currTime();
        assert(rc == TIMEOUT);
        assert(400.msecs < stop - start && stop - start < 600.msecs);
        task.wait(); // finally wait
        info("Test wait - ok");

        {
            void ff1(string s) {
                writefln("from future1: %s", s);
            }
            void ff2(int i) {
                writefln("from future2: %d", i);
            }
            int increment(int i) {
                return i+1;
            }
        }
        globalLogLevel(LogLevel.info);
        info("Test task create/destroy");
        int loops = 50000;
        int c = 0;
        void empty() {
            c++;
        }
        info("Test non-daemon task wait() after some delay.");
        info("Please, be patient, can take some time.");
        foreach(i; 0..loops) {
            auto z = new fioTask(&empty);
            fioSleep(1.msecs);
            z.wait();
            destroy(z);
        }
        info("Test non-daemon task wait() right after start.");
        info("Please, be patient, can take some time.");
        foreach(i; 0..loops) {
            auto z = new fioTask(&empty);
            z.wait();
            destroy(z);
        }
        info("Test daemon task.");
        info("Please, be patient, can take some time.");
        foreach(i; 0..loops) {
            auto z = new fioDaemonTask(&empty);
            fioSleep(1.msecs);
        }
        info("Test task create/destroy - ok");
        fioSleep(5.seconds);
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
            int loops = 500;
            infof("%s:%d, tmo=%s - wait", "1.1.1.1", 9998, to!string(10.msecs));
            foreach(i; 0..loops) {
                start = Clock.currTime();
                auto conn = new fioTCPConnection("1.1.1.1", 9998, 10.msecs);
                stop = Clock.currTime();
                assert( !conn.connected );
                conn.close();
                destroy(conn);
                assert(5.msecs < stop - start && stop - start < 100.msecs, format("connect took %s", to!string(stop-start)));
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
                assert( stop - start < 50.msecs, format("Connection to localhost should return instantly, but took %s", to!string(stop-start)));
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
                assert( stop - start < 5.seconds,
                    format("Connection to localhost should return instantly, but it took %s", to!string(stop-start))
                );
                conn.close();
                destroy(conn);
            }
            infof("%s:%d, tmo=%s passed", "localhost", 9998, to!string(0.seconds));
            info("test dumb listener");
            void dumb_server(fioTCPConnection c) {
            }
            int oloops = 10_000;
            foreach(int j; 0..oloops) {
                auto dumb_server_listener = makeTCPListener("localhost", cast(ushort)9997, &dumb_server);
                dumb_server_listener.start();
                loops = 100;
                foreach(i;0..loops) {
                    auto c = scoped!fioTCPConnection("localhost", cast(ushort)9997, 10.seconds);
                    assert(c.connected);
                    c.close();
                }
                dumb_server_listener.close();
                if ( j > 0 && (j % 1000 == 0) ) {
                    info(format("%d iterations out of %d", j, oloops));
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
                            trace("test server received quit");
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
            auto listener = makeTCPListener("localhost", 9999, &test_server).start();
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
                conn.send("abcd", 3);
                start = Clock.currTime();
                assert(buff.length>0);
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
                recv_rc = conn.recv(buff, 3.seconds, Partial.no); // wait 3 sec, require full buff -> should be timeout
                stop = Clock.currTime();
                infof("receive exactly %d bytes with 3 sec timeout(should timeout)", buff.length);
                infof("received %d bytes in %s", recv_rc, to!string(stop-start));
                assert(recv_rc == TIMEOUT);
                conn.send("1234567890");
                start = Clock.currTime();
                recv_rc = conn.recv(buff, 3.seconds, Partial.no); // wait 3 sec, require full buff
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
                tracef("wait for reaction on quit");
                start = Clock.currTime();
                recv_rc = conn.recv(buff);
                stop = Clock.currTime();
                infof("received %d bytes in %s", recv_rc, to!string(stop-start));
                //recv_rc = conn.recv(buff);
                //do {
                //    send_rc = conn.send(buff);
                //    fioSleep(100.msecs);
                //} while (send_rc > 0);
                //infof("recv_rc: %d, send_rc: %d", recv_rc, send_rc);
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
    info("tesing all");
    new fioTask(&testAll);
    runEventLoop();
    writeln("Test finished");
}
