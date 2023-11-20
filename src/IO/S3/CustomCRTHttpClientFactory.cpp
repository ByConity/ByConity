#include "CustomCRTHttpClientFactory.h"

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <IO/S3/CustomCRTHttpClient.h>
#include <aws/core/Globals.h>


namespace DB::S3
{

// See HttpClientFactory.cpp::DefaultHttpClientFactory
std::shared_ptr<Aws::Http::HttpClient>
CustomCRTHttpClientFactory::CreateHttpClient(const Aws::Client::ClientConfiguration & clientConfiguration) const
{
    return std::make_shared<CustomCRTHttpClient>(
        static_cast<const CustomCRTHttpClientConfiguration &>(clientConfiguration), *Aws::GetDefaultClientBootstrap());
}


std::shared_ptr<Aws::Http::HttpRequest> CustomCRTHttpClientFactory::CreateHttpRequest(const Aws::String &uri, Aws::Http::HttpMethod method,
                                                const Aws::IOStreamFactory &streamFactory) const
{
    return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
}

std::shared_ptr<Aws::Http::HttpRequest> CustomCRTHttpClientFactory::CreateHttpRequest(const Aws::Http::URI& uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory& streamFactory) const
{
    auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>("CustomCRTHttpClientFactory", uri, method);
    request->SetResponseStreamFactory(streamFactory);
    return request;
}

}
