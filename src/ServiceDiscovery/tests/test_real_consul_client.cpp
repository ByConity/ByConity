#include <consul/bridge.h>
#include <iostream>
#include <chrono>
#include <Common/ThreadPool.h>
#include <cstdlib>

cpputil::consul::ServiceDiscovery get_sd() {
    static cpputil::consul::ServiceDiscovery sd;
    int i = 0;
    while (sd.init_failed() && i++ < 3) {
        sd = cpputil::consul::ServiceDiscovery();
        if (sd.init_failed()) {
            usleep(1500 * 1000);
        }
    }
    return sd;
}

void run(std::string psm, int expected_num)
{
    try
    {
        auto endpoints = get_sd().translateName(psm);
        if(endpoints.size() != static_cast<unsigned>(expected_num))
            std::cout<<"endpoints size incorrect: got "<<endpoints.size()<<" expect "<<expected_num<<std::endl;
    }
    catch (const std::exception & e)
    {
        std::cerr << e.what() << std::endl;
    }
}

int main(int argc, char** argv)
{
    int expected_num;
    int concurrency;
    std::string psm;
    if(argc==4)
    {
        concurrency = atoi(argv[1]);
        expected_num = atoi(argv[2]);
        psm = argv[3];
    }
    else
    {
        std::cout<<"Usage:  ./test_real_consul_client [concurrency] [expected_result] [psm]"<<std::endl;
        return -1;
    }

    std::function<void()> f = std::bind(run, psm, expected_num);
    auto thread_pool = std::make_unique<ThreadPool>(concurrency);
    auto t1 = std::chrono::high_resolution_clock::now();
    for(int i=0; i<concurrency; i++)
    {
        thread_pool->trySchedule(f);
    }
    thread_pool->wait();
    auto t2 = std::chrono::high_resolution_clock::now();
    std::cout<<"concurrency: "<<concurrency<<std::endl<<"total time: "<<std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()<<" ms"<<std::endl; 
}

