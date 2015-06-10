import std.stdio;
import std.datetime;

import fio: runEventLoop, fioSleep;

void main()
{
	fioSleep(1.seconds);
	writeln("running loop");
	runEventLoop();
}
