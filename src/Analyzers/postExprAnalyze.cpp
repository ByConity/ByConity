#include <Analyzers/postExprAnalyze.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/MapHelpers.h>
#include <Common/FieldVisitorToString.h>

namespace DB
{

void postExprAnalyze(ASTFunctionPtr & function, const ColumnsWithTypeAndName & processed_arguments, Analysis & analysis, ContextPtr context)
{
    String func_name_lowercase = Poco::toLower(function->name);

    auto check_origin_column = [](const FieldDescription & field, auto && pred) {
        if (!field.hasOriginInfo())
            return false;

        for (const auto & origin_col : field.origin_columns)
            if (!pred(origin_col))
                return false;

        return true;
    };

    auto register_subcolumn = [&](const ASTPtr & ast, const ResolvedField & column_ref, const SubColumnID & sub_column_id) {
        analysis.setSubColumnReference(ast, SubColumnReference{column_ref, sub_column_id});

        for (const auto & origin_col : column_ref.getFieldDescription().origin_columns)
            analysis.addReadSubColumn(origin_col.table_ast, origin_col.index_of_scope, sub_column_id);
    };

    do
    {
        if (func_name_lowercase == "mapelement")
        {
            auto column_reference = analysis.tryGetColumnReference(function->arguments->children[0]);
            if (!column_reference)
                break;

            const auto & resolved_field = column_reference->getFieldDescription();
            const auto map_type = std::dynamic_pointer_cast<const DataTypeMap>(resolved_field.type);
            if (!map_type)
                break;

            String column_name;
            if (processed_arguments[1].column)
            {
                auto argument_value = std::make_shared<ASTLiteral>((*processed_arguments[1].column)[0]);
                column_name = argument_value->getColumnName();
            }
            if (column_name.empty())
                break;

            if (!check_origin_column(resolved_field, [](const auto & origin) -> bool {
                    if (!origin.storage->supportsMapImplicitColumn())
                        return false;

                    DataTypePtr type = origin.metadata_snapshot->columns.getPhysical(origin.column).type;
                    return type->isByteMap();
                }))
                break;

            /// Convert key according to map key type
            Field key_field = convertToMapKeyField(map_type->getKeyType(), column_name);
            auto key_name = applyVisitor(DB::FieldVisitorToString(), key_field); // convert to correct implicit key name
            auto column_id = SubColumnID::mapElement(key_name);
            register_subcolumn(function, *column_reference, column_id);
        }

        if (func_name_lowercase == "mapkeys")
        {
            auto column_reference = analysis.tryGetColumnReference(function->arguments->children[0]);
            if (!column_reference)
                break;

            const auto & resolved_field = column_reference->getFieldDescription();
            if (!check_origin_column(resolved_field, [](const auto & origin) -> bool {
                    // TODO(shiyuze): maybe we can remove this check, this rewrite is only for kv map
                    if (!origin.storage->supportsMapImplicitColumn())
                        return false;
                    DataTypePtr type = origin.metadata_snapshot->columns.getPhysical(origin.column).type;
                    return type->isKVMap();
                }))
                break;

            auto column_id = SubColumnID::mapKeys();
            register_subcolumn(function, *column_reference, column_id);
        }

        if (func_name_lowercase == "mapvalues")
        {
            auto column_reference = analysis.tryGetColumnReference(function->arguments->children[0]);
            if (!column_reference)
                break;

            const auto & resolved_field = column_reference->getFieldDescription();
            if (!check_origin_column(resolved_field, [](const auto & origin) -> bool {
                    // TODO(shiyuze): maybe we can remove this check, this rewrite is only for kv map
                    if (!origin.storage->supportsMapImplicitColumn())
                        return false;
                    DataTypePtr type = origin.metadata_snapshot->columns.getPhysical(origin.column).type;
                    return type->isKVMap();
                }))
                break;

            auto column_id = SubColumnID::mapValues();
            register_subcolumn(function, *column_reference, column_id);
        }

        if ((func_name_lowercase == "get_json_object" || func_name_lowercase == "jsonextractraw")
            && context->getSettingsRef().optimize_json_function_to_subcolumn)
        {
            auto column_reference = analysis.tryGetColumnReference(function->arguments->children[0]);
            if (!column_reference)
                break;

            const auto & resolved_field = column_reference->getFieldDescription();

            String column_name;
            if (processed_arguments[1].column)
            {
                column_name = (*processed_arguments[1].column)[0].safeGet<String>();
                if (func_name_lowercase == "get_json_object" && startsWith(column_name, "$."))
                    column_name = column_name.substr(2, column_name.length() - 2);
            }
            if (column_name.empty())
                break;

            auto column_id = SubColumnID::jsonField(column_name);

            if (!check_origin_column(resolved_field, [&](const auto & origin) -> bool {
                    auto storage_snapshot = origin.storage->getStorageSnapshot(origin.metadata_snapshot, context);
                    GetColumnsOptions options{GetColumnsOptions::All};
                    options.withSubcolumns();
                    options.withExtendedObjects();
                    return /* context->getSettingsRef().allow_nonexist_object_subcolumns || */
                        !!(storage_snapshot->tryGetColumn(options, column_id.getSubColumnName(origin.column)));
                }))
                break;

            register_subcolumn(function, *column_reference, column_id);
        }
    } while (false);
}

}
