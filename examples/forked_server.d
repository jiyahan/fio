import std.stdio;
import std.format;
import std.datetime;
import std.typecons;
import core.sys.posix.unistd: fork;
import core.sys.posix.sys.wait: wait;
import std.experimental.logger;
import fio: makeTCPListener, fioTCPConnection, fioSleep, runEventLoop, makeApp, stacksize, waitForAllDaemons;


void main()
{
    globalLogLevel(LogLevel.info);
    stacksize = 64*1024;
    auto dumbServer = makeTCPListener("localhost", 9999, (fioTCPConnection c) {
            ///
            /// This is listener wich will be created in master process and then
            /// forks
            ///
            byte[64] buf;
            auto l = c.recv(buf);
            if ( l > 0 )
                writefln("got '%s'", cast(string)buf[0..l]);
            fioSleep(3.seconds);
            c.close();
        });

    //
    // fork after Listener created, but before any start()
    //
    auto i_am_parent = dumbServer.fork(3);
    //
    // start servers after fork
    //
    dumbServer.start();

    if ( i_am_parent ) {
        //
        // parent starts client app, send 10 requests and exit
        // Hopefully each next request handled in separate process
        //
        makeApp((string s){
                infof("Got args %s", s);

                for(int i=0;i<10;i++) {
                    auto c = scoped!fioTCPConnection("127.0.0.1", cast(ushort)9999);
                    assert(c.connected);
                    writefln("send %d", i);
                    c.send(s ~ format("-%d", i));
                    c.close();
                    //fioSleep(50.msecs);
                }
                writeln("App finished, waiting for all daemons");
                waitForAllDaemons(10.seconds);
                writeln("All daemon tasks finished");
                writeln("Waiting for forked processes");
                dumbServer.waitForForkedChilds();
            },
        "Hello"
        );
     } else {
         //
         // server app just sleep 10 seconds (accepting incoming connections) and then exit
         //
         makeApp({
             fioSleep(10.seconds);
         });
     }

    writeln("running loop");
    runEventLoop();
    writeln("done");
}
