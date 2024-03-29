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
