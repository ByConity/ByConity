#include <Processors/Formats/IInputFormat.h>
#include <IO/ReadBuffer.h>


namespace DB
{

void ColumnMapping::addConstColumn(Block & block, const Names & required_columns) const
{
    for (const auto & column_name : required_columns)
    {
        if (auto it = name_of_default_columns.find(column_name); it != name_of_default_columns.end() && !block.has(column_name))
        {
            const auto & default_desc = it->second;
            auto column = default_desc.type->createColumnConst(block.rows(), default_desc.default_value);
            block.insert(ColumnWithTypeAndName(std::move(column), default_desc.type, column_name));
        }
    }
}

IInputFormat::IInputFormat(Block header, ReadBuffer & in_)
    : ISource(std::move(header)), in(in_)
{
    column_mapping = std::make_shared<ColumnMapping>();
}

void IInputFormat::resetParser()
{
    in.ignoreAll();
    // those are protected attributes from ISource (I didn't want to propagate resetParser up there)
    finished = false;
    got_exception = false;

    getPort().getInputPort().reopen();
}

void IInputFormat::setReadBuffer(ReadBuffer & in_)
{
    in = in_;
}

}
