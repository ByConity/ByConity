#pragma once

#include <map>
#include <string>
namespace cpputil {
namespace consul {

typedef std::map<std::string, std::string> Tags;

struct ServiceEndpoint {
	std::string host;
	int port;
	Tags tags;
};

}

}
