#include <Optimizer/Rule/Patterns.h>
#include <Optimizer/tests/gtest_optimizer_test_utils.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(OptimizerPatternMatchTest, DISABLED_captures)
{
    Captures captures;
    Capture uniqueCap;
    Capture multiCap;
    Capture emptyCap;

    std::string sFoo = "foo";
    std::shared_ptr<std::string> sPtrBar = std::make_shared<std::string>("bar");
    int i57 = 57;
    double d6_5 = 6.5;

    captures.insert(std::make_pair(uniqueCap, sFoo));
    captures.insert(std::make_pair(multiCap, sPtrBar));
    captures.insert(std::make_pair(multiCap, i57));
    captures.insert(std::make_pair(multiCap, d6_5));

    ASSERT_TRUE(captures.size() == 4);

    ASSERT_TRUE(captures.at<std::string>(uniqueCap) == "foo");
    ASSERT_THROW(captures.at<int>(uniqueCap), std::bad_any_cast);
    ASSERT_THROW_DB_EXCEPTION_WITH_MESSAGE_PREFIX(captures.at<int>(multiCap), "Not unique capture");
    ASSERT_THROW_DB_EXCEPTION_WITH_MESSAGE_PREFIX(captures.at<int>(emptyCap), "Not unique capture");

    bool sPtrBarChecked = false;
    bool i57Checked = false;
    bool d6_5Checked = false;
    int containedValues = 0;

    for (auto iters = captures.equal_range(multiCap); iters.first != iters.second; ++iters.first)
    {
        if (std::string(typeid(int).name()) == iters.first->second.type().name())
        {
            ASSERT_TRUE(std::any_cast<int>(iters.first->second) == i57);
            i57Checked = true;
        }
        else if (std::string(typeid(double).name()) == iters.first->second.type().name())
        {
            ASSERT_TRUE(std::any_cast<double>(iters.first->second) == d6_5);
            d6_5Checked = true;
        }
        else
        {
            ASSERT_TRUE(std::any_cast<std::shared_ptr<std::string>>(iters.first->second) == sPtrBar);
            sPtrBarChecked = true;
        }

        ++containedValues;
    }

    ASSERT_TRUE(containedValues == 3 && i57Checked && d6_5Checked && sPtrBarChecked);
}
