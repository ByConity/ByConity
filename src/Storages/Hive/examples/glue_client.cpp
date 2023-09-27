#include <stdio.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <boost/program_options.hpp>
#include <Storages/Hive/Metastore/GlueMetastore.h>
#include <boost/program_options/variables_map.hpp>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3Common.h>
#include "ExternalCatalog/IExternalCatalogMgr.h"
using std::string;
namespace po = boost::program_options;

po::variables_map parseOptions(int argc, char ** argv)
{
        po::options_description desc("Allowed options");
         desc.add_options()
        ("ak", po::value<string>(), "ak")
        ("sk", po::value<string>(), "sk")
        ("session", po::value<string>(), "session")
        ("endpoint", po::value<string>(), "endpoint")
        ("region", po::value<string>(), "region")
        ("catalog_id", po::value<string>(), "catalog_id")
        ("logging_level", po::value<string>()->default_value("debug"), "logging level")
        ("desc", po::value<std::vector<std::string>>()->multitoken(), "describe table")
        ("list", po::value<std::vector<std::string>>()->multitoken(), "list db/table")
        ("stat", po::value<std::vector<std::string>>()->multitoken(), "get table statistics");


    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);
    return options;
}


Aws::Auth::AWSCredentials getCredentials(po::variables_map& opts)
{
  if(opts.count("session"))
  {
  return Aws::Auth::AWSCredentials(opts["ak"].as<std::string>(), opts["sk"].as<std::string>(),opts["session"].as<std::string>());
  } else
  {
    return Aws::Auth::AWSCredentials(opts["ak"].as<std::string>(), opts["sk"].as<std::string>());
  }
}

Aws::Client::ClientConfiguration getClientConfig(po::variables_map& opts)
{

  DB::S3::PocoHTTPClientConfiguration conf
            = DB::S3::ClientFactory::instance().createClientConfiguration(opts["endpoint"].as<std::string>(), DB::RemoteHostFilter(), 3, 10*1000, 1024, false);
  conf.endpointOverride = opts["endpoint"].as<std::string>();
  conf.region= opts["region"].as<std::string>();
  return std::move(conf);
}

DB::GlueMetastoreClientPtr getClient(po::variables_map& opts)
{
    DB::ExternalCatalog::PlainConfigsPtr config(new DB::ExternalCatalog::PlainConfigs());
    config->setString( DB::GlueConfigKey::aws_glue_ak, opts["ak"].as<String>());
    config->setString( DB::GlueConfigKey::aws_glue_sk, opts["sk"].as<String>());
    config->setString( DB::GlueConfigKey::aws_glue_catalog_id, opts["catalog_id"].as<String>());
    config->setString( DB::GlueConfigKey::aws_glue_region, opts["region"].as<String>());
    config->setString( DB::GlueConfigKey::aws_glue_endpoint, opts["endpoint"].as<String>());
>>>>>>> 302194daa09 (Merge branch 'cnch2-external_catalog_trans' into 'cnch-ce-merge')
    return DB::GlueMetastoreClientFactory::instance().getOrCreate(config);
}

static void describe_table(DB::GlueMetastoreClient & client, const String & db_name, const String & table_name)
{
    auto table = client.getTable(db_name, table_name);
    table->printTo(std::cout);
}

static void listAllDBs(DB::GlueMetastoreClient & client)
{
    std::vector<std::string> databases = client.getAllDatabases();
    for (const auto & database : databases)
    {
        std::cout << database << std::endl;
    }
}

static void listAllTables(DB::GlueMetastoreClient & client, const String & db)
{
    auto tables = client.getAllTables(db);
    for (const auto & table : tables)
    {
        std::cout << table << std::endl;
    }
}

static void show_stat(DB::GlueMetastoreClient & client, const String & db_name, const String & table_name)
{
    auto table = client.getTable(db_name, table_name);
    std::vector<std::string> column_names;
    for (const auto & c : table->sd.cols)
    {
        column_names.emplace_back(c.name);
    }
    auto stat = client.getTableStats(db_name, table_name, column_names,true);
    stat.table_stats.printTo(std::cout);
}

int main(int argc, char ** argv)
{
  auto opts = parseOptions(argc, argv);
  opts.notify();

//   auto credentials = getCredentials(opts);
//   auto conf = getClientConfig(opts);
//   auto client = DB::GlueMetastoreClient(credentials,conf,{.catalog_id = opts["catalog_id"].as<String>()});
  auto client_ptr = getClient(opts);
  auto client = *client_ptr;
  if (opts.count("desc"))
  {
      const auto & params = opts["desc"].as<std::vector<std::string>>();
      if (params.size() != 2)
      {
          std::cout << "--desc db_name table_name" << std::endl;
          return -1;
      }
      describe_table(client, params[0], params[1]);
      return 0;
  }

  if (opts.count("list"))
  {
      const auto & params = opts["list"].as<std::vector<std::string>>();
      switch (params.size())
      {
          case 0:
              listAllDBs(client);
              return 0;
          case 1:
              listAllTables(client, params[0]);
              return 0;
          default:
              std::cout << "--list [db_name]" << std::endl;
              return -1;
      }
  }

  if (opts.count("stat"))
  {
      const auto & params = opts["stat"].as<std::vector<std::string>>();
      if (params.size() != 2)
      {
          std::cout << "--stat db_name table_name" << std::endl;
          return -1;
      }
      show_stat(client, params[0], params[1]);

      return 0;
  }
}
