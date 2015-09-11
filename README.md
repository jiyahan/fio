# fio

fio is small async framework. You can use as abstracted interface to OS event system (like linux epoll or FreeBSD kqueue), or you can use fio 'wrappers' which let you avoid callbacks in your code.


Current features:
* Future's - execution units you can wait and get results from.
* Daemon's - execution units for background tasks.
* TCP client and server code.
* fork for TCP server (spread connections over several processes).

See examples in _examples_ folder.

    import std.format;
    import std.experimental.logger;
    import fio: makeTCPListener, fioTCPConnection, runEventLoop, makeApp;
    
    static string host = "localhost";
    static ushort port = 9999;
    
    void server(fioTCPConnection c) {
        byte[64] buf;
    
        auto l = c.recv(buf);
        if ( l > 0 )
            infof("got '%s'", cast(string)buf[0..l]);
    }
    
    void client(string s) {
        infof("Got args %s", s);
        for(int i=0;i<10;i++) {
            auto c = new fioTCPConnection(host, port);
            assert(c.connected);
            infof("send %d", i);
            c.send(s ~ format("-%d", i));
            c.close();
        }
    }
    
    void main() {
        globalLogLevel(LogLevel.info);
        auto server = makeTCPListener(host, port, &server);
        server.start();
        makeApp(&client, "Hello");
        runEventLoop();
        info("done");
    }

* main() create socket listener, which call *server* routine for any new incoming connection, and then start small application *client*. Client send string to server.
* *server(fioTCPConnection c)* called when new connection arrive on the listening socket.
* clint(string s) is small application, connecting to server and sending string.

Current limitations:
* works only with linux epoll and FreeBSD kqueue

