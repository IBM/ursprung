/**
 * Copyright 2020 IBM
 *
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

#include <fstream>

#include "gtest/gtest.h"
#include "condition.h"
#include "logger.h"

TEST(condition_expr_test, test1) {
  ConditionExpr e1("f1>0 & f2<10 & f3=5");
  ConditionExpr e2("f1>1 & f2<9 & f3=5");
  ConditionExpr e3("(f1>1 & f2<9) | f3=5");
  ConditionExpr e4("(f1>0 & f2<10) | f3=4");
  std::string msg_str1 = "1,9,5";
  TestIntermediateMessage test_msg1(CS_PROV_AUDITD, msg_str1);
  EXPECT_TRUE(e1.eval(test_msg1));
  LOG_INFO("1 done");
  EXPECT_FALSE(e2.eval(test_msg1));
  LOG_INFO("2 done");
  EXPECT_TRUE(e3.eval(test_msg1));
  LOG_INFO("3 done");
  EXPECT_TRUE(e4.eval(test_msg1));
  LOG_INFO("4 done");
}
TEST(condition_expr_test, test2) {
  ConditionExpr e1("f1@s1[.*] & f2@s2[.*]");
  ConditionExpr e2("f1@s1 & f2@s2[.*]");
  ConditionExpr e3("(f1@s1 | f2@s2[.*]) & f3=5");
  std::string msg_str1 = "s1 field,s2 field,5";
  TestIntermediateMessage test_msg1(CS_PROV_AUDITD, msg_str1);
  LOG_INFO(test_msg1.normalize(CD_DB2));
  EXPECT_TRUE(e1.eval(test_msg1));
  EXPECT_FALSE(e2.eval(test_msg1));
  EXPECT_TRUE(e3.eval(test_msg1));
}
