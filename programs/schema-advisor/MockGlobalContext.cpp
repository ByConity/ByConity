#include "MockGlobalContext.h"

#include <Common/Config/ConfigProcessor.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/AutoPtr.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/Text.h>
#include <Poco/Util/XMLConfiguration.h>

namespace DB
{
ContextMutablePtr MockGlobalContext::createSessionContext()
{
    ContextMutablePtr session_context = Context::createCopy(context);
    session_context->makeSessionContext();
    return session_context;
}

MockGlobalContext::MockGlobalContext()
{
    shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();
    ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(MockGlobalContext::mockConfig()));
    context->setConfig(configuration);
}

XMLDocumentPtr MockGlobalContext::mockConfig()
{
    XMLDocumentPtr document = new Poco::XML::Document();
    Poco::AutoPtr<Poco::XML::Element> yandex = document->createElement("yandex");
    Poco::AutoPtr<Poco::XML::Element> remote_servers = document->createElement("remote_servers");
    Poco::AutoPtr<Poco::XML::Element> advisor_shard = document->createElement(ADVISOR_SHARD);
    Poco::AutoPtr<Poco::XML::Element> shard = document->createElement("shard");
    Poco::AutoPtr<Poco::XML::Element> replica = document->createElement("replica");

    Poco::AutoPtr<Poco::XML::Element> host = document->createElement("host");
    Poco::AutoPtr<Poco::XML::Text> host_text = document->createTextNode("localhost");
    host->appendChild(host_text);
    replica->appendChild(host);

    Poco::AutoPtr<Poco::XML::Element> port = document->createElement("port");
    Poco::AutoPtr<Poco::XML::Text> port_text = document->createTextNode("9000");
    port->appendChild(port_text);
    replica->appendChild(port);

    Poco::AutoPtr<Poco::XML::Element> exchange_port = document->createElement("exchange_port");
    Poco::AutoPtr<Poco::XML::Text> exchange_port_text = document->createTextNode("9300");
    exchange_port->appendChild(exchange_port_text);
    replica->appendChild(exchange_port);

    Poco::AutoPtr<Poco::XML::Element> exchange_status_port = document->createElement("exchange_status_port");
    Poco::AutoPtr<Poco::XML::Text> exchange_status_port_text = document->createTextNode("9400");
    exchange_status_port->appendChild(exchange_status_port_text);
    replica->appendChild(exchange_status_port);

    shard->appendChild(replica);
    advisor_shard->appendChild(shard);
    remote_servers->appendChild(advisor_shard);
    yandex->appendChild(remote_servers);
    document->appendChild(yandex);

    return document;
}

}
