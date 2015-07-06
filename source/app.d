import std.stdio;
import std.datetime;
import std.typecons;
import std.experimental.logger;

import fio: fioTCPListener, fioTCPConnection, fioSleep, runEventLoop, makeApp, stacksize;

void main()
{
    globalLogLevel(LogLevel.info);
    stacksize = 64*1024;

    makeApp((string s){
            infof("Got args %s", s);
            auto dumbServer = new fioTCPListener("localhost", 9999, (fioTCPConnection c) {
                    byte[64] buf;
                    c.recv(buf);
                    infof("got  '%s'", cast(string)buf);
                    c.close();
            });

            for(int i=0;i<10;i++) {
                auto c = scoped!fioTCPConnection("127.0.0.1", cast(ushort)9999);
                assert(c.connected);
                infof("send '%s'", s);
                c.send(s);
                fioSleep(1.msecs);
                c.close();
            }
            info("App done");
        },
    "Hello"
    );

    info("running loop");
    runEventLoop();
    info("done");
}
