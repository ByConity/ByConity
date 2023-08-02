#pragma once
#include <functional>
#include <string>

void handle_pyerror(std::function<void(const std::string &msg)> setError);
