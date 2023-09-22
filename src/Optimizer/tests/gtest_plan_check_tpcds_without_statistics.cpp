/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <QueryPlan/PlanPrinter.h>
#include <Optimizer/tests/gtest_base_tpcds_plan_test.h>
#include <Optimizer/Iterative/IterativeRewriter.h>
#include <gtest/gtest.h>

using namespace DB;

class PlanCheckTpcdsWihtoutStatistics : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        std::unordered_map<std::string, DB::Field> settings = BasePlanTest::getDefaultOptimizerSettings();
        tester = std::make_shared<DB::BaseTpcdsPlanTest>(settings);
    }

    static std::string explain(const std::string & name)
    {
        return tester->explain(name);
    }

    static std::shared_ptr<DB::BaseTpcdsPlanTest> tester;
};

std::shared_ptr<DB::BaseTpcdsPlanTest> PlanCheckTpcdsWihtoutStatistics::tester;

TEST_F(PlanCheckTpcdsWihtoutStatistics, q1)
{
    explain("q1");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q2)
{
    explain("q2");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q3)
{
    explain("q3");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q4)
{
    explain("q4");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q5)
{
    explain("q5");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q6)
{
    explain("q6");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q7)
{
    explain("q7");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q8)
{
    explain("q8");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q9)
{
    explain("q9");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q10)
{
    explain("q10");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q11)
{
    explain("q11");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q12)
{
    explain("q12");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q13)
{
    explain("q13");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q14)
{
    explain("q14");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q15)
{
    explain("q15");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q16)
{
    explain("q16");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q17)
{
    explain("q17");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q18)
{
    explain("q18");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q19)
{
    explain("q19");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q20)
{
    explain("q20");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q21)
{
    explain("q21");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q22)
{
    explain("q22");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q23)
{
    explain("q23");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q24)
{
    explain("q24");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q25)
{
    explain("q25");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q26)
{
    explain("q26");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q27)
{
    explain("q27");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q28)
{
    explain("q28");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q29)
{
    explain("q29");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q30)
{
    explain("q30");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q31)
{
    explain("q31");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q32)
{
    explain("q32");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q33)
{
    explain("q33");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q34)
{
    explain("q34");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q35)
{
    explain("q35");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q36)
{
    explain("q36");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q37)
{
    explain("q37");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q38)
{
    explain("q38");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q39)
{
    explain("q39");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q40)
{
    explain("q40");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q41)
{
    explain("q41");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q42)
{
    explain("q42");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q43)
{
    explain("q43");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q44)
{
    explain("q44");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q45)
{
    explain("q45");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q46)
{
    explain("q46");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q47)
{
    explain("q47");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q48)
{
    explain("q48");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q49)
{
    explain("q49");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q50)
{
    explain("q50");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q51)
{
    explain("q51");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q52)
{
    explain("q52");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q53)
{
    explain("q53");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q54)
{
    explain("q54");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q55)
{
    explain("q55");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q56)
{
    explain("q56");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q57)
{
    explain("q57");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q58)
{
    explain("q58");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q59)
{
    explain("q59");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q60)
{
    explain("q60");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q61)
{
    explain("q61");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q62)
{
    explain("q62");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q63)
{
    explain("q63");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q64)
{
    explain("q64");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q65)
{
    explain("q65");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q66)
{
    explain("q66");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q67)
{
    explain("q67");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q68)
{
    explain("q68");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q69)
{
    explain("q69");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q70)
{
    explain("q70");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q71)
{
    explain("q71");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q72)
{
    explain("q72");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q73)
{
    explain("q73");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q74)
{
    explain("q74");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q75)
{
    explain("q75");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q76)
{
    explain("q76");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q77)
{
    explain("q77");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q78)
{
    explain("q78");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q79)
{
    explain("q79");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q80)
{
    explain("q80");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q81)
{
    explain("q81");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q82)
{
    explain("q82");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q83)
{
    explain("q83");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q84)
{
    explain("q84");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q85)
{
    explain("q85");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q86)
{
    explain("q86");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q87)
{
    explain("q87");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q88)
{
    explain("q88");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q89)
{
    explain("q89");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q90)
{
    explain("q90");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q91)
{
    explain("q91");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q92)
{
    explain("q92");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q93)
{
    explain("q93");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q94)
{
    explain("q94");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q95)
{
    explain("q95");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q96)
{
    explain("q96");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q97)
{
    explain("q97");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q98)
{
    explain("q98");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, q99)
{
    explain("q99");
}

TEST_F(PlanCheckTpcdsWihtoutStatistics, summary)
{
    std::cout << "rule call times:" << std::endl;
    for (const auto & x: IterativeRewriter::getRuleCallTimes())
        std::cout << x.first << ": " << x.second << std::endl;
}
