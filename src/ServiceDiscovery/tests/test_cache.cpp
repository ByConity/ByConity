#include <ServiceDiscovery/ServiceDiscoveryCache.h>
#include <consul/discovery.h>
#include <memory>
#include <iostream>

int main()
{
    using namespace DB;
    using Endpoint = cpputil::consul::ServiceEndpoint;

    struct CacheEntry
    {
        SDCacheKey key;
        SDCacheValue<Endpoint> value;
    };
    
    std::vector<CacheEntry> entryList =
    {
        {
            {"data.cnch.server",""},
            {{{"127.0.0.1",9000,{{"cluster","default"}}}},1}
        },
        {
            {"data.cnch.tso",""},
            {{{"127.0.0.1",8080,{{"cluster","default"}}}},2}
        },
        {
            {"data.cnch.vw","vw_write"},
            {{{"127.0.0.1",8080,{{"cluster","default"},{"vw_name","vw_write"}}}},2}
        }
    };

    using ServiceDiscoveryCachePtr = std::shared_ptr<ServiceDiscoveryCache<Endpoint>>;
    ServiceDiscoveryCachePtr pcache = std::make_shared<ServiceDiscoveryCache<Endpoint>>();

    // populate cache
    for (auto & entry: entryList)
        pcache->put(entry.key,entry.value);
    
    SDCacheValue<Endpoint> result;

    // Test1: cache hit
    SDCacheKey key1 = {"data.cnch.server", ""};
    if(pcache->get(key1,result))
        std::cout<<"cache hit - "<<key1.psm<<"("<<result.endpoints[0].host<<":"<<result.endpoints[0].port<<")"<<std::endl;
    else
        std::cout<<"cache miss"<<std::endl;
    
    // Test2: cache hit
    SDCacheKey key2 = {"data.cnch.tso", ""};
    if(pcache->get(key2,result))
        std::cout<<"cache hit - "<<key2.psm<<"("<<result.endpoints[0].host<<":"<<result.endpoints[0].port<<")"<<std::endl;
    else
        std::cout<<"cache miss"<<std::endl;

    // Test3: cache hit
    SDCacheKey key3 = {"data.cnch.vw", "vw_write"};
    if(pcache->get(key3,result))
        std::cout<<"cache hit - "<<key3.psm<<"("<<result.endpoints[0].host<<":"<<result.endpoints[0].port<<")"<<std::endl;
    else
        std::cout<<"cache miss"<<std::endl;

    // Test4: cache miss
    SDCacheKey key4 = {"data.cnch.vw", "vw_default"};
    if(pcache->get(key4,result))
        std::cout<<"cache hit - "<<key4.psm<<"("<<result.endpoints[0].host<<":"<<result.endpoints[0].port<<")"<<std::endl;
    else
        std::cout<<"cache miss"<<std::endl;
}
