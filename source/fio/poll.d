module poll;

private import std.datetime;
private import std.experimental.logger;
private import std.conv;
private import std.stdio;
private import std.socket;
private import std.format;
private import core.sys.posix.time : itimerspec, CLOCK_REALTIME;
private import core.sys.posix.unistd : close, read;

struct Event {
    enum {
        IN  	= 1,
        OUT 	= 4,
        CONN 	= 8,
        ERR 	= 0x10,
        HUP		= 0x20,
        TMO 	= 0x40
    };
    uint	events;
}

enum {
    EPOLLIN         = 0x001,
    EPOLLPRI        = 0x002,
    EPOLLOUT        = 0x004,
    EPOLLRDNORM 	= 0x040,
    EPOLLRDBAND 	= 0x080,
    EPOLLWRNORM 	= 0x100,
    EPOLLWRBAND 	= 0x200,
    EPOLLMSG        = 0x400,
    EPOLLERR        = 0x008,
    EPOLLHUP        = 0x010,
    EPOLLRDHUP      = 0x2000,
    EPOLLONESHOT 	= 1u << 30,
    EPOLLET         = 1u << 31,

    EPOLL_CTL_ADD  	= 1, /* Add a file descriptor to the interface. */
    EPOLL_CTL_DEL 	= 2, /* Remove a file descriptor from the interface. */
    EPOLL_CTL_MOD  	= 3 /* Change file descriptor epoll_event structure. */
}

align(1) struct epoll_event
{
align(1):
        uint events;
        epoll_data_t data;
};

union epoll_data_t
{
        void *ptr;
        int fd;
        uint u32;
        ulong u64;
        EventHandler handler;
};

extern(C) int epoll_create(int size) @safe @nogc nothrow;
extern(C) int epoll_ctl(int epfd, int op, int fd, epoll_event *event) @safe @nogc nothrow;
extern(C) int epoll_wait(int epfd, epoll_event *events, int maxevents, int timeout) @safe @nogc nothrow;
extern(C) int timerfd_create(int clockid, int flags) @safe @nogc nothrow;
extern(C) int timerfd_settime(int fd, int flags, itimerspec* new_value, itimerspec* old_value) @safe @nogc nothrow;

enum MAXEVENTS = 1024;

interface EventHandler {
    void handle(epoll_event e);
}

class EventLoop {
    int epoll_fd;
    align(1) epoll_event[MAXEVENTS] events;

    this() nothrow @safe @nogc {
        epoll_fd = epoll_create(MAXEVENTS);
        if ( epoll_fd < 0 ) {
            assert(false, "Failed to create epoll_fd");
        }
    }
    int add(int fd, epoll_event e) nothrow @trusted @nogc {
        return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &e);
    }
    int del(int fd, epoll_event e) nothrow @trusted @nogc {
        return epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, &e);
    }
    void loop(Duration d) {
        if ( d == 0.seconds ) {
            return;
        }

        uint timeout_ms = cast(int)d.total!"msecs";

        uint ready = epoll_wait(epoll_fd, cast(epoll_event*)&events[0], MAXEVENTS, timeout_ms);
//        trace("epoll_wait returned ", to!string(ready));
        if ( ready > 0 ) {
            foreach(i; 0..ready) {
                auto e = events[i];
                EventHandler handler = e.data.handler;
                handler.handle(e);
            }
        }
    }
}

class AsyncTimer : EventHandler {
    Duration    d;
    int         timer_fd;
    EventLoop   evl;
    void delegate() dg;
//    string      caller;

    this(EventLoop evl, in string file = __FILE__ , in size_t line = __LINE__) @safe @nogc {
//        caller = format("%s:%d", file, line);

        this.timer_fd = timerfd_create(CLOCK_REALTIME, 0);
        this.evl = evl;

        if ( timer_fd < 0 ) {
//            error("Failed to create timer_fd");
            return;
        }
    }

   ~this() @safe @nogc nothrow {
       kill();
//        if ( timer_fd != -1 ) {
//            close(timer_fd);
//            timer_fd = -1;
//        }
//        if ( caller !is null ) {
//            destroy(caller);
//        }
   }

    Duration duration() const @property @safe @nogc nothrow pure {
        return d;
    }

    void duration(Duration d) @property @safe @nogc nothrow pure {
        this.d = d;
    }

    void run(void delegate() dg) @trusted @nogc
    in {
        assert(d != d.init, "AsyncTimer can't run without duration");
        assert(timer_fd != -1, "AsyncTimer can't run without timer_fd");
    }
    body {
        this.dg = dg;
        itimerspec itimer;

        itimer.it_value.tv_sec = cast(typeof(itimer.it_value.tv_sec)) d.split!("seconds", "nsecs")().seconds;
        itimer.it_value.tv_nsec = cast(typeof(itimer.it_value.tv_nsec)) d.split!("seconds", "nsecs")().nsecs;
        timerfd_settime(timer_fd, 0, &itimer, null);
        auto e = epoll_event();
        e.events = EPOLLET | EPOLLIN;
        e.data.handler = this;
        evl.add(timer_fd, e);
    }

    override void handle(epoll_event e) {
        dg();
    }

    void kill() @safe @nogc nothrow {
        if ( timer_fd > 0 ) {
            auto e = epoll_event();
            e.events = EPOLLET | EPOLLIN;
            e.data.handler = this;
            evl.del(timer_fd, e);
            close(timer_fd);
            timer_fd = -1;
        }
    }
    override string toString() @safe @nogc const nothrow pure {
        return "";
    }
}

class asyncAccept : EventHandler {
    Socket                  so;
    EventLoop               evl;
    void delegate(Event)    _on_accept;

    this(EventLoop evl, Socket so, void delegate(Event) d) @trusted {
        this.so = so;
        this.evl = evl;
        this._on_accept = d;

        so.blocking(false);
        auto e = epoll_event();
        e.events = 	EPOLLIN ;
        e.data.handler = this;
        evl.add(so.handle, e);
    }

    void close() nothrow @safe @nogc {
        auto e = epoll_event();
        e.events =  EPOLLIN ;
        e.data.handler = this;
        evl.del(so.handle, e);

        if ( so !is null ) {
            so.close();
            so = null;
        }
    }

    override void handle(epoll_event e) {
        Event app_event = {events:Event.IN};
        trace("accepted");
        this._on_accept(app_event);
    }
}

class asyncConnection : EventHandler {
    bool                    _connected;
    bool                    _error;
    bool                    _instream_closed;
    bool                    _outstream_closed;
    Socket                  so;
    EventLoop               evl;
    void delegate(Event)    _on_conn;
    void delegate(Event)    _on_send;
    void delegate(Event)    _on_recv;
    void delegate(Event)    _on_err;

    this(EventLoop evl, Socket so) @safe @nogc nothrow pure {
        // create from connected socket
        this.so = so;
        this.evl = evl;
        this._connected = true;
    }

    this(EventLoop evl, Socket so, Address to, void delegate(Event) dg) @safe {
        this._on_conn = dg;
        this.so = so;
        this.evl = evl;
        so.blocking(false);
        so.connect(to);
        auto e = epoll_event();
        e.events = EPOLLOUT;
        e.data.handler = this;
        evl.add(so.handle, e);
    }

    bool connected() pure const nothrow @safe @property @nogc {
        return _connected;
    }
    bool error() pure const nothrow @safe @property @nogc {
        return _error;
    }
    bool instream_closed() pure const nothrow @safe @property @nogc {
        return _instream_closed;
    }
    bool outstream_closed() pure const nothrow @safe @property @nogc {
        return _outstream_closed;
    }
    void on_send(void delegate(Event) d) @property @safe @nogc
    in {
        assert(!_error || !d, "send to error-ed connection");
        assert(_connected, "send to disconnected" );
        assert(this._on_send is null || d is null, "send already active");
    }
    body {
        if ( d is null ) {
            // stop sending
            _on_send = null;
            epoll_event e;
            e.events = EPOLLOUT;
            evl.del(so.handle, e);
        } else {
            _on_send = d;
            epoll_event e;
            e.events = EPOLLOUT;
            e.data.handler = this;
            evl.add(so.handle, e);
        }
    }

    void on_recv(void delegate(Event) d) @property @safe
    in {
        assert(_instream_closed || !_error || d is null, "recv error-ed connection");
        assert(_connected, "recv disconnected" );
        assert(this._on_recv is null || d is null, "recv already active");
    }
    body {
        if ( d is null ) {
            // stop receiving
            trace("stop recv");
            _on_recv = null;
            epoll_event e;
            e.events = EPOLLIN|EPOLLHUP;
            evl.del(so.handle, e);
        } else {
            trace("start receiving");
            _on_recv = d;
            epoll_event e;
            e.events = EPOLLIN|EPOLLHUP;
            e.data.handler = this;
            evl.add(so.handle, e);
        }
    }

    override void handle(epoll_event e) {
        Event app_event;
        tracef("GOT EVENT %x", e.events);
        if ( _on_conn ) {
            // This is connection request
            epoll_event ev_to_del;
            ev_to_del.events = EPOLLOUT|EPOLLERR;
            evl.del(so.handle, ev_to_del);
            if ( e.events & EPOLLERR ) {
                app_event.events = Event.ERR;
                _connected = false;
                _error = true;
            } else {
                app_event.events = Event.CONN;
                _connected = true;
            }
            try {
                this._on_conn(app_event);
                this._on_conn = null;
            } catch(Exception e) {
                errorf("got exception %s", to!string(e));
            }
            return;
        }
        uint err_flags = 0;
        if ( e.events & EPOLLHUP ) {
            _instream_closed = true;
            err_flags += Event.HUP;
        }
        if ( e.events & EPOLLERR ) {
            _error = true;
            err_flags += Event.ERR;
        }
        if ( e.events & EPOLLOUT ) {
            app_event.events = Event.OUT | err_flags;
            _on_send(app_event);
        }
        if ( e.events & EPOLLIN ) {
            app_event.events = Event.IN | err_flags;
            _on_recv(app_event);
        }
        if ( e.events & (EPOLLERR|EPOLLHUP) ) {
            _error = true;
            if ( _on_err ) {
                app_event.events = Event.ERR;
                _on_err(app_event);
            }
        }
    }

}
