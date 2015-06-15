import std.stdio;
import std.datetime;
import std.typecons;
import std.experimental.logger;

import fio: runEventLoop, stopEventLoop, fioTask, fioSleep, fioTCPListener, fioTCPConnection;

void main()
{
    globalLogLevel(LogLevel.info);
    auto dumbServer = new fioTCPListener("localhost", 9999, (fioTCPConnection c) {
            // nothing, dumb
            c.close();
        });

    auto task = new fioTask({
        info("sleep for 1 seconds");
        fioSleep(1.seconds);
        info("wakeup");
        int i;
        for(i=0;i<1_000;i++) {
            auto c = new fioTCPConnection("127.0.0.1", cast(ushort)9999);
            assert(c.connected);
            fioSleep(1.msecs);
            c.close();
        }
        writeln("DONE");
        fioSleep(100.seconds);
        stopEventLoop();
    });
    info("running loop");
    runEventLoop();
    info("done");
}
