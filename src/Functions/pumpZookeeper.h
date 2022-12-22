#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{
/// This function is used to print hosts from zookeeper
template <typename Impl>
class FunctionGetHosts : public IFunction
{
    public:
        static constexpr auto name = Impl::name;

        FunctionGetHosts(const ContextPtr & context_):context(context_){}

        static FunctionPtr create(const ContextPtr & context_)
        {
            return std::make_shared<FunctionGetHosts>(context_);
        }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 0; }

        DataTypePtr getReturnTypeImpl(const DataTypes &) const override
        {
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
        {
            auto array = handleHosts();
            auto column = ColumnArray::create(ColumnString::create());
            column->insert(array);
            return ColumnConst::create(std::move(column), input_rows_count);
        }

    private:
        const ContextPtr context;
        /// Store the hosts into array, where array consists Field.
        Array handleHosts() const
        {
            Array array;
            Strings hosts;
            if(context->hasZooKeeper())
                hosts = context->getZooKeeper()->getHosts();
            for (auto & host : hosts)
                array.emplace_back(host);

            return array;
        }
};

}
