module ipaddr;

import std.conv;
import std.array;
import std.format;
import std.exception;

enum ver = "0.0.1";
pragma(msg, "ipaddr " ~ ver);

class IPv4AddressException: Exception {
	this(string msg) @safe pure {
		super(msg);
	}
}

int _string_to_ipv4(string s) @safe pure {
	int result = 0;
	string[] octets = s.split(".");
	if ( octets.length != 4 ) {
		throw new IPv4AddressException(format("%s can't be converted to IPv4Addr", s));
	}
	foreach(o; octets) {
		int o_value = toImpl!(int, string)(o, 10);
		if ( o_value < 0 || o_value > 255 ) {
			throw new IPv4AddressException(format("%s can't be converted to IPv4Addr", s));
		}
		result = (result << 8) + o_value;
	}
	return result;
}

struct IPv4Address {
	int addr;
	this(string addr) @safe pure {
		this.addr = _string_to_ipv4(addr);
	}
	this(int addr) @safe pure {
		this.addr = addr;
	}
}

unittest {
	assert( _string_to_ipv4("0.0.0.0") == 0 );
	assert( _string_to_ipv4("0.0.1.0") == 256 );
	assertThrown!IPv4AddressException( _string_to_ipv4("500.0.1.0") == 256 );
}