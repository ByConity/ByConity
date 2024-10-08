#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_utils.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(AnsiSettings, TestApplyOnlyIfDialectChange)
{
    auto context = Context::createCopy(getContext().context);
    context->applySettingsChanges(SettingsChanges{{"dialect_type", "CLICKHOUSE"}});
    context->applySettingsChanges(SettingsChanges{{"cast_keep_nullable", "1"}});
    context->applySettingsChanges(SettingsChanges{{"dialect_type", "CLICKHOUSE"}});
    EXPECT_EQ(context->getSettingsRef().cast_keep_nullable.value, bool{1});
    context->applySettingsChanges(SettingsChanges{{"dialect_type", "ANSI"}});
    context->applySettingsChanges(SettingsChanges{{"dialect_type", "CLICKHOUSE"}});
    EXPECT_EQ(context->getSettingsRef().cast_keep_nullable.value, bool{0});
}
