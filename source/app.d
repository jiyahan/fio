import std.stdio;
import std.datetime;
import std.experimental.logger;

import fio: runEventLoop, stopEventLoop, fioTask, fioSleep;

void main()
{
    globalLogLevel(LogLevel.info);
    auto task = new fioTask({
        info("sleep for 5 seconds");
        fioSleep(5.seconds);
        info("wakeup");
        stopEventLoop();
    });
    info("running loop");
    runEventLoop();
    info("done");
}
