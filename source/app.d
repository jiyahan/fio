import std.stdio;
import std.format;
import std.datetime;
import std.typecons;
import core.sys.posix.unistd: fork;
import core.sys.posix.sys.wait: wait;
import std.experimental.logger;
import fio: fioTCPListener, fioTCPConnection, fioSleep, runEventLoop, makeApp, stacksize, waitForAllDaemons;


void main()
{
    globalLogLevel(LogLevel.error);
    stacksize = 64*1024;
    auto dumbServer = new fioTCPListener("localhost", 9999, (fioTCPConnection c) {
            byte[64] buf;
            auto l = c.recv(buf);
            if ( l > 0 )
                writefln("got '%s'", cast(string)buf[0..l]);
            fioSleep(3.seconds);
            c.close();
        });

    dumbServer.fork(3);

    dumbServer.start();

    makeApp((string s){
            infof("Got args %s", s);

            for(int i=0;i<10;i++) {
                auto c = scoped!fioTCPConnection("127.0.0.1", cast(ushort)9999);
                assert(c.connected);
                writefln("send %d", i);
                c.send(s ~ format("-%d", i));
                c.close();
                fioSleep(500.msecs);
            }
            writeln("App done, waiting when all daemons finish");
            waitForAllDaemons(5.seconds);
        },
    "Hello"
    );

    info("running loop");
    runEventLoop();
    info("done");
}
