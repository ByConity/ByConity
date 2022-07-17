#include <Core/Types.h>
#include <CloudServices/CnchBGThreadCommon.h>
#include <Catalog/Catalog.h>

namespace DB
{
namespace BGJobStatusInCatalog
{
    CnchBGThreadStatus deserializeFromChar(char c);
    CnchBGThreadStatus deserializeFromString(const std::string & str);
    char serializeToChar(CnchBGThreadStatus status);

    /// This class is used to inject dependency of catalog into BackgroundJob,
    /// so that write unit testing are easier
    class IBGJobStatusPersistentStoreProxy
    {
    public:
        /// clear the statuses_cache in dtor
        class CacheClearer
        {
            public:
                CacheClearer() = default;
                CacheClearer(IBGJobStatusPersistentStoreProxy *);
                CacheClearer(const CacheClearer &) = delete;
                CacheClearer(CacheClearer &&);
                CacheClearer & operator=(CacheClearer &&);
                CacheClearer & operator=(const CacheClearer &) = delete;
                ~CacheClearer();
            private:
                IBGJobStatusPersistentStoreProxy * proxy = nullptr;
        };

        virtual CnchBGThreadStatus createStatusIfNotExist(const StorageID & storage_id, CnchBGThreadStatus init_status) const = 0;
        virtual void setStatus(const UUID & table_uuid, CnchBGThreadStatus status) const = 0;
        virtual CnchBGThreadStatus getStatus(const UUID & table_uuid,  bool use_cache) const = 0;
        virtual CacheClearer fetchStatusesIntoCache() = 0;
        virtual ~IBGJobStatusPersistentStoreProxy() = default;
    protected:
        virtual void clearCache() {}
    };

    /// this class support get/setStatus operator for each table by calling coresponding methods in Catalog,
    /// it also support batch getStatus by fetch statuses of all tables
    /// into cache , after that the getStatus will use cache data instead of fetching data from Catalog
    class CatalogBGJobStatusPersistentStoreProxy : public IBGJobStatusPersistentStoreProxy
    {
    public:
        CatalogBGJobStatusPersistentStoreProxy(std::shared_ptr<Catalog::Catalog>, CnchBGThreadType);

        CnchBGThreadStatus createStatusIfNotExist(const StorageID & storage_id, CnchBGThreadStatus init_status) const override;
        void setStatus(const UUID & table_uuid, CnchBGThreadStatus status) const override;
        CnchBGThreadStatus getStatus(const UUID & table_uuid, bool use_cache) const override;
        CacheClearer fetchStatusesIntoCache() override;
    protected:
        void clearCache() override;
        /// keep member variables protected for testing
        std::shared_ptr<Catalog::Catalog> catalog;
        std::unordered_map<UUID, CnchBGThreadStatus> statuses_cache;
        CnchBGThreadType type;
    };
}

}
