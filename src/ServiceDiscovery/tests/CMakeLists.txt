add_executable(fake_endpoints_registration fake_endpoints_registration.cpp)
add_executable(test_service_discovery_dns test_service_discovery_dns.cpp)
target_link_libraries(test_service_discovery_dns dbms)
