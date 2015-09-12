module poll;

private import std.datetime;
private import std.experimental.logger;
private import std.conv;
private import std.stdio;
private import std.socket;
private import std.format;
private import core.sys.posix.time : itimerspec, CLOCK_REALTIME, timespec;
private import core.sys.posix.unistd : close, read;
private import core.sys.posix.signal;

enum MAXEVENTS = 1024;

///
/// event interface to kernel poll implementation (epoll or kqueue)
///
struct PollingEvent {
    enum {
        IN  	= 1,
        OUT 	= 4,
        ERR 	= 0x08,
        HUP     = 0x10,
    };
    uint	     events;
    EventHandler handler;
    string toString() const nothrow @safe {
        import std.array;
        string[] bits;
        if ( events & IN ) {
            bits ~= "IN";
        }
        if ( events & OUT ) {
            bits ~= "OUT";
        }
        if ( events & ERR ) {
            bits ~= "ERR";
        }
        if ( events & HUP ) {
            bits ~= "HUP";
        }
        try {
            return format("polling events '%s'", join(bits, "+"));
        } catch (Exception e) {
            return to!string(events);
        }
    }
}

///
/// event interface to upper level application
///
struct Event {
    enum {
        IN      = 1,
        OUT     = 4,
        CONN    = 8,
        ERR     = 0x10,
        HUP     = 0x20,
        TMO     = 0x40
    };
    uint	events;
}

class FailedToCreateDescriptor: Exception {
    public
    {
        @safe pure nothrow this(string message,
                                string file =__FILE__,
                                size_t line = __LINE__,
                                Throwable next = null)
        {
            super(message, file, line, next);
        }
    }
}

class EventHandler {
    void handle(PollingEvent e) {assert(false, "Must be override this");};
}

version(FreeBSD) {
    import core.sys.freebsd.sys.event;
    import core.sys.posix.fcntl: open, O_RDONLY;
    import core.sys.posix.unistd: close;
    extern(C) int kqueue() @safe @nogc nothrow;
    extern(C) int kevent(int kqueue_fd, kevent_t *events, int ne, kevent_t *events, int ne,timespec* timeout) @safe @nogc nothrow;
    //extern(C) void EV_SET(kevent_t* kevp, typeof(kevent_t.tupleof) args) @safe @nogc nothrow;

    ///
    /// convert from platform-independent events to platform-dependent
    ///
    auto convertToImplEvents(in uint events) @safe @nogc nothrow {
        short result;
        if ( events & PollingEvent.IN )
            result |= EVFILT_READ;
        if ( events & PollingEvent.OUT )
            result |= EVFILT_WRITE;
        return result;
    }
    ///
    /// convert from platform-dependent events to platform-independent
    ///
    uint convertFromImplEvents(in kevent_t e)  @safe {
        uint result;
        auto implEvents = e.filter;
        
        tracef("implEv: %d", implEvents);
        if ( implEvents == EVFILT_TIMER ) {
            result |= PollingEvent.IN;
        }
        if ( implEvents == EVFILT_READ ) {
            result |= PollingEvent.IN;
        }
        if ( implEvents == EVFILT_WRITE ) {
            result |= PollingEvent.OUT;
        }
        if ( e.flags & EV_ERROR ) {
            result |= PollingEvent.ERR;
        }
        if ( e.flags & EV_EOF ) {
            result |= PollingEvent.HUP;
        }
        tracef("result: %0x", result);
        return result;
    }

    auto timerfd_create(int clockid, int flags) @trusted @nogc nothrow {
        return open("/dev/zero", O_RDONLY);
    }

    class EventLoop {
        private int kqueue_fd;
        private int in_index;
        private kevent_t[MAXEVENTS] in_events;
        private kevent_t[MAXEVENTS] out_events;

        this() nothrow @safe @nogc {
            kqueue_fd = kqueue();
            if ( kqueue_fd < 0 ) {
                assert(false, "Failed to create kqueue_fd");
            }
        }
        int add(int fd, PollingEvent pe) nothrow @trusted @nogc
            in {
                assert(in_index <= MAXEVENTS);
            }
            body {
                kevent_t e;
                e.filter = convertToImplEvents(pe.events);
                e.flags = EV_ADD;// | EV_ONESHOT;
                e.udata = cast(void*)pe.handler;
                add(fd, e);
                return 0;
            }
        int add(int fd, kevent_t e) nothrow @trusted @nogc
            in {
                assert(in_index <= MAXEVENTS);
            }
            body {
                e.ident = fd;
                if ( true || in_index == MAXEVENTS ) {
                    uint ready = kevent(kqueue_fd, &e, 1, null, 0, null);
                } else {
                    in_events[in_index++] = e;
                }
                return 0;
            }
        int del(int fd, PollingEvent pe) nothrow @trusted @nogc 
            in {
                assert(in_index <= MAXEVENTS);
            }
            body {
                kevent_t e;
                e.ident = fd;
                e.filter = convertToImplEvents(pe.events);
                e.flags = EV_DELETE;
                e.udata = cast(void*)pe.handler;
                if ( true || in_index == MAXEVENTS ) {
                    uint ready = kevent(kqueue_fd, &e, 1, null, 0, null);
                } else {
                    in_events[in_index++] = e;
                }
                return 0;
            }
        void loop(Duration d) 
            in {
                assert(in_index<=MAXEVENTS);
            }
            body {
                if ( d == 0.seconds ) {
                    return;
                }
        
                timespec ts = {
                    tv_sec: cast(typeof(timespec.tv_sec))d.split!("seconds", "nsecs")().seconds,
                    tv_nsec:cast(typeof(timespec.tv_nsec))d.split!("seconds", "nsecs")().nsecs
                };
                uint ready = kevent(kqueue_fd, cast(kevent_t*)&in_events[0], in_index,
                                           cast(kevent_t*)&out_events[0], MAXEVENTS, &ts);
                in_index = 0;
                if ( ready > 0 ) {
                    foreach(i; 0..ready) {
                        auto e = out_events[i];
                        EventHandler handler = cast(EventHandler)e.udata;
                        tracef("got event[%d] %s, data: %0x", i, e, e.udata);
                        auto pe = PollingEvent(convertFromImplEvents(e));
                        tracef("event %x for %s", pe.events, handler.toString());
                        handler.handle(pe);
                    }
                } else if ( ready == 0 ) {
                    error("timeout");
                } else {
                    error("kevent returned error");
                }
            }
        }
}

version(linux) {
    private import core.sys.linux.sys.signalfd;

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
    extern(C) int signalfd(int fd, const sigset_t *mask, int flags) @safe @nogc nothrow;
    extern(C) int sigaddset(const sigset_t *mask, int sig) @safe @nogc nothrow;
    extern(C) int sigprocmask(int, const sigset_t *, sigset_t *) @safe @nogc nothrow;
    
    
    ///
    /// convert from platform-independent events to platform-dependent
    ///
    uint convertToImplEvents(in uint events) @safe @nogc nothrow {
        return (events & 0x1f) | EPOLLET;
    }
    ///
    /// convert from platform-dependent events to platform-independent
    ///
    uint convertFromImplEvents(in uint implEvents)  @safe @nogc nothrow {
        return implEvents & 0x1f;
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
        int add(int fd, PollingEvent pe) nothrow @trusted @nogc {
            epoll_event e;
            e.events = convertToImplEvents(pe.events);
            e.data.handler = pe.handler;
            return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &e);
        }
        int del(int fd, PollingEvent pe) nothrow @trusted @nogc {
            epoll_event e;
            e.events = convertToImplEvents(pe.events);
            e.data.handler = pe.handler;
            return epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, &e);
        }
        void loop(Duration d) {
            if ( d == 0.seconds ) {
                return;
            }
    
            uint timeout_ms = cast(int)d.total!"msecs";
    
            uint ready = epoll_wait(epoll_fd, cast(epoll_event*)&events[0], MAXEVENTS, timeout_ms);
            if ( ready > 0 ) {
                foreach(i; 0..ready) {
                    auto e = events[i];
                    EventHandler handler = e.data.handler;
                    auto pe = PollingEvent(convertFromImplEvents(e.events));
                    handler.handle(pe);
                }
            }
        }
    }
}

class SignalHandler(F) : EventHandler {
    int                  signal_fd;
    int                  sig;
    EventLoop            evl;
    F                    dg;
    ///
    /// enable signal sig delivery to process only through signal_fd and delegate
    /// Params:
    ///		sig = signal
    ///		dg = handler
    ///
    this(EventLoop evl, int sig, F dg, in string file = __FILE__ , in size_t line = __LINE__) @trusted {
        this.dg = dg;
        static sigset_t m;
        this.sig = sig;
        this.evl = evl;
        sigemptyset(&m);
        sigaddset(&m, sig);
        sigprocmask(SIG_BLOCK, &m, null);
        version(linux) {
            this.signal_fd = signalfd(-1, &m, 0);
            if ( this.signal_fd < 0 ) {
                throw new FailedToCreateDescriptor("Failed to create signalfd");            
            }
            auto pe = PollingEvent(Event.IN, this);
            evl.add(this.signal_fd, pe);
        }
        version(FreeBSD) {
            signal(sig, SIG_IGN);
            kevent_t e;
            e.ident = sig;
            e.filter = EVFILT_SIGNAL;
            e.flags = EV_ADD|EV_ENABLE;
            e.udata = cast(void*)this;
//            EV_SET(&e, sig, EVFILT_SIGNAL, EV_ADD|EV_ENABLE, 0, 0, cast(void*)this);
            evl.add(sig, e);
        }
    }
    override string toString() const {
        return "SignalHandler";
    }

    override void handle(PollingEvent e) {
        version(linux) {
            signalfd_siginfo si;
            ssize_t res = read(this.signal_fd, &si, si.sizeof);
            if ( res != si.sizeof ) {
                errorf("Something wrong reading from signal_fd: got %d instead of %d", res, si.sizeof);
                return;
            }
        }
        dg(this.sig);
    }
    void restore() {
        static sigset_t m;
        sigaddset(&m, this.sig);
        sigprocmask(SIG_UNBLOCK, &m, null);
        version(FreeBSD) {
            signal(sig, SIG_DFL);
        }
        auto pe = PollingEvent(PollingEvent.IN, this);
        evl.del(this.signal_fd, pe);
        close(this.signal_fd);
    }
}

class AsyncTimer : EventHandler {
  private:
    Duration    d;
    int         timer_fd;
    EventLoop   evl;
    void delegate() dg;
    string      file;
    size_t      line;

  public:
    this(EventLoop evl, in string file = __FILE__ , in size_t line = __LINE__) @safe {

        this.timer_fd = timerfd_create(CLOCK_REALTIME, 0);
        this.evl = evl;
        this.file = file;
        this.line = line;

        if ( timer_fd < 0 ) {
            throw new FailedToCreateDescriptor("Failed to create timerfd");
        }
    }
    override string toString() const pure nothrow @safe {
        string name;
        try {
            name = format("SyncTimer %s:%d", file, line);
        } catch (Exception e) {
            name = "SyncTimer";
        }
        return name;
    }

   ~this() @safe @nogc nothrow {
       kill();
   }

    Duration duration() const @property @safe @nogc nothrow pure {
        return d;
    }

    void duration(Duration d) @property @safe @nogc nothrow pure {
        this.d = d;
    }

    void run(void delegate() dg) @trusted nothrow @nogc
    in {
        assert(d != d.init, "AsyncTimer can't run without duration");
        assert(timer_fd != -1, "AsyncTimer can't run without timer_fd");
    }
    body {
        this.dg = dg;
        version(linux) {
            itimerspec itimer;

            itimer.it_value.tv_sec = cast(typeof(itimer.it_value.tv_sec)) d.split!("seconds", "nsecs")().seconds;
            itimer.it_value.tv_nsec = cast(typeof(itimer.it_value.tv_nsec)) d.split!("seconds", "nsecs")().nsecs;
            timerfd_settime(timer_fd, 0, &itimer, null);
            auto pe = PollingEvent(PollingEvent.IN, this);
            evl.add(timer_fd, pe);
        }
        version(FreeBSD) {
            kevent_t e;
            int delay_ms = cast(int)d.split!"msecs"().msecs;
            e.ident = timer_fd;
            e.filter = EVFILT_TIMER;
            e.flags = EV_ADD|EV_ONESHOT;
            e.data = delay_ms;
            e.udata = cast(void*)this;
            //EV_SET(&e, 1u, EVFILT_TIMER, EV_ADD|EV_ONESHOT, 0u, delay_ms, cast(void*)this);
            evl.add(timer_fd, e);
        }
    }

    override void handle(PollingEvent e) {
        trace("timer event");
        dg();
    }

    void kill() @trusted @nogc nothrow {
        if ( timer_fd > 0 ) {
            auto pe = PollingEvent(PollingEvent.IN, this);
            version(linux) {
                evl.del(timer_fd, pe);
            }
            version(FreeBSD) {
                kevent_t e;
                e.ident = timer_fd;
                e.filter = EVFILT_TIMER;
                e.flags = EV_DELETE;
                evl.add(timer_fd, e);
            }
            close(timer_fd);
            timer_fd = -1;
        }
    }
}

class asyncAccept : EventHandler {
  private:
    Socket                  so;
    EventLoop               evl;
    void delegate(Event)    _on_accept;
    string      file;
    size_t      line;

  public:
    this(EventLoop evl, Socket so, void delegate(Event) d,
            in string file = __FILE__ , in size_t line = __LINE__) @trusted {
        this.so = so;
        this.evl = evl;
        this._on_accept = d;
        this.file = file;
        this.line = line;

        so.blocking(false);
        auto pe = PollingEvent(PollingEvent.IN, this);
        evl.add(so.handle, pe);
    }
    override string toString() const pure nothrow @safe {
        string name;
        try {
            name = format("AsyncAccept %s:%d", file, line);
        } catch (Exception e) {
            name = "AsyncAccept";
        }
        return name;
    }

    void close() nothrow @safe @nogc {
        auto pe = PollingEvent(PollingEvent.IN, this);
        evl.del(so.handle, pe);

        if ( so !is null ) {
            so.close();
            so = null;
        }
    }

    override void handle(PollingEvent e) {
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

    this(EventLoop evl, Socket so, Address to, void delegate(Event) dg) nothrow @safe {
        this._on_conn = dg;
        this.so = so;
        this.evl = evl;
        try {
            so.blocking(false);
            so.connect(to);
        } catch ( Exception e ) {
            _error = true;
            _connected = false;
            return;
        }
        auto pe = PollingEvent(PollingEvent.OUT, this);
        evl.add(so.handle, pe);
    }
    override string toString() const {
        return "asyncConnection";
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
        assert(_connected || d is null, "send to disconnected" );
        assert(this._on_send is null || d is null, "send already active");
    }
    body {
        if ( d is null ) {
            // stop sending
            _on_send = null;
            auto pe = PollingEvent(PollingEvent.OUT);
            evl.del(so.handle, pe);
        } else {
            _on_send = d;
            auto pe = PollingEvent(PollingEvent.OUT, this);
            evl.add(so.handle, pe);
        }
    }

    void on_recv(void delegate(Event) d) @property @safe
    in {
        assert(_instream_closed || !_error || d is null, "recv error-ed connection");
        assert(_connected || d is null, "recv disconnected" );
        assert(this._on_recv is null || d is null, "recv already active");
    }
    body {
        if ( d is null ) {
            // stop receiving
            trace("stop recv");
            _on_recv = null;
            auto pe = PollingEvent(PollingEvent.IN|PollingEvent.HUP);
            evl.del(so.handle, pe);
        } else {
            trace("start receiving");
            _on_recv = d;
            auto pe = PollingEvent(PollingEvent.IN|PollingEvent.HUP, this);
            evl.add(so.handle, pe);
        }
    }

    override void handle(PollingEvent e) {
        Event app_event;
        tracef("GOT EVENT %s", e.toString());
        if ( _on_conn ) {
            // This is connection request
            auto pe = PollingEvent(PollingEvent.OUT|PollingEvent.ERR);
            evl.del(so.handle, pe);
            if ( e.events & (PollingEvent.ERR|PollingEvent.HUP) ) {
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
        if ( e.events & PollingEvent.HUP ) {
            _instream_closed = true;
            _connected = false;
            err_flags += Event.HUP;
        }
        if ( e.events & PollingEvent.ERR ) {
            _error = true;
            err_flags += Event.ERR;
        }
        if ( e.events & PollingEvent.OUT ) {
            app_event.events = Event.OUT | err_flags;
            _on_send(app_event);
        }
        if ( e.events & PollingEvent.IN ) {
            app_event.events = Event.IN | err_flags;
            _on_recv(app_event);
        }
        if ( e.events & (PollingEvent.ERR) ) {
            _error = true;
            if ( _on_err ) {
                app_event.events = Event.ERR;
                _on_err(app_event);
            }
        }
    }
}
