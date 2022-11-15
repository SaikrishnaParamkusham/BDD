from behave import *
from MarketcapModule import MarketCapEligibility
from spark import *
import findspark
from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal


@given("an input data frame")
def step_impl(context):
    findspark.init()
    context.spark = SparkSession.builder \
        .appName('behave') \
        .master('local') \
        .getOrCreate()
    context.main_table = table_to_spark(context.spark, context.table)


@when('parameters passed are {field_str},{operator},{threshold}')
def step_impl(context, field_str, operator, threshold):
    context.EligibleDF, context.IneligibleDF = MarketCapEligibility(context.main_table).\
        filter_marketcap(field_str, operator, int(threshold))


@then("the eligible dataframes is")
def step_impl(context):
    context.test_table = table_to_spark(context.spark, context.table)
    assert_pyspark_df_equal(context.EligibleDF, context.test_table)


@then("the ineligible dataframe is")
def step_impl(context):
    context.test1_table = table_to_spark(context.spark, context.table)
    assert_pyspark_df_equal(context.IneligibleDF, context.test1_table)


@given("an input data frame with fullmarketcap values greater than 50")
def step_impl(context):
    findspark.init()
    context.spark = SparkSession.builder \
        .appName('behave') \
        .master('local') \
        .getOrCreate()
    context.main_table = table_to_spark(context.spark, context.table)


@when('the parameters passed to the function are {field_str}, {operator}, and {threshold}')
def step_impl(context, field_str, operator, threshold):
    context.EligibleDF, context.IneligibleDF = MarketCapEligibility(context.main_table).\
        filter_marketcap(field_str, operator, int(threshold))


@then("the eligible dataframe should have the below fullmarketcap values")
def step_impl(context):
    context.test_table = table_to_spark(context.spark, context.table)
    assert_pyspark_df_equal(context.EligibleDF, context.test_table)


@then("the ineligible dataframe should be an empty dataframe with below column names")
def step_impl(context):
    context.test1_table = table_to_spark(context.spark, context.table)
    assert_pyspark_df_equal(context.IneligibleDF, context.test1_table)


@given("an input data frame with missing values")
def step_impl(context):
    findspark.init()
    context.spark = SparkSession.builder \
        .appName('behave') \
        .master('local') \
        .getOrCreate()
    context.main_table = table_to_spark(context.spark, context.table)


@when('the given input parameters are {field_str}, {operator}, and {threshold}')
def step_impl(context, field_str, operator, threshold):
    context.EligibleDF, context.IneligibleDF = MarketCapEligibility(context.main_table).\
        filter_marketcap(field_str, operator, int(threshold))


@then("the eligible data frame should be")
def step_impl(context):
    context.test_table = table_to_spark(context.spark, context.table)
    assert_pyspark_df_equal(context.EligibleDF, context.test_table)


@then("the ineligible data frame should be")
def step_impl(context):
    context.test1_table = table_to_spark(context.spark, context.table)
    assert_pyspark_df_equal(context.IneligibleDF, context.test1_table)


@given("an input data frame for empty data frame exception")
def step_impl(context):
    findspark.init()
    context.spark = SparkSession.builder \
        .appName('behave') \
        .master('local') \
        .getOrCreate()
    context.main_table = table_to_spark(context.spark, context.table)


@when('the parameters passed for empty Data frame exception are {field_str}, {operator}, and {threshold}')
def step_impl(context, field_str, operator, threshold):
    context.field_str = field_str
    context.operator = operator
    context.threshold = threshold


@then("the exception raised for empty input data frame is {expected_exception}")
def step_impl(context, expected_exception):
    try:
        MarketCapEligibility(context.main_table). \
            filter_marketcap(context.field_str, context.operator, int(context.threshold))
    except Exception as e:
        actual_exception = str(e)
    assert actual_exception == expected_exception


@given("an input data frame for incorrect field exception")
def step_impl(context):
    findspark.init()
    context.spark = SparkSession.builder \
        .appName('behave') \
        .master('local') \
        .getOrCreate()
    context.main_table = table_to_spark(context.spark, context.table)


@when('the parameters passed for incorrect field name exception are {field_str}, {operator}, and {threshold}')
def step_impl(context, field_str, operator, threshold):
    context.field_str = field_str
    context.operator = operator
    context.threshold = threshold


@then("the exception for incorrect field name is {expected_exception}")
def step_impl(context, expected_exception):
    try:
        MarketCapEligibility(context.main_table). \
            filter_marketcap(context.field_str, context.operator, int(context.threshold))
    except Exception as e:
        actual_exception = str(e)
    assert expected_exception == actual_exception


@given("an input data frame for incorrect Operator exception")
def step_impl(context):
    findspark.init()
    context.spark = SparkSession.builder \
        .appName('behave') \
        .master('local') \
        .getOrCreate()
    context.main_table = table_to_spark(context.spark, context.table)


@when('the parameters passed for incorrect operator name exception are {field_str}, {operator}, and {threshold}')
def step_impl(context, field_str, operator, threshold):
    context.field_str = field_str
    context.operator = operator
    context.threshold = threshold


@then("the exception for incorrect operator is {expected_exception}")
def step_impl(context, expected_exception):
    try:
        MarketCapEligibility(context.main_table). \
            filter_marketcap(context.field_str, context.operator, int(context.threshold))
    except Exception as e:
        actual_exception = str(e)
    assert expected_exception == actual_exception


@given("an input data frame for incorrect threshold datatype exception")
def step_impl(context):
    findspark.init()
    context.spark = SparkSession.builder \
        .appName('behave') \
        .master('local') \
        .getOrCreate()
    context.main_table = table_to_spark(context.spark, context.table)


@when('the parameters passed for incorrect threshold datatype exception are {field_str}, {operator}, and {threshold}')
def step_impl(context, field_str, operator, threshold):
    context.field_str = field_str
    context.operator = operator
    context.threshold = threshold


@then("the exception for incorrect threshold datatype is {expected_exception}")
def step_impl(context, expected_exception):
    try:
        MarketCapEligibility(context.main_table). \
            filter_marketcap(context.field_str, context.operator, context.threshold)
    except Exception as e:
        actual_exception = str(e)
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n", actual_exception)
        print("######################################\n", expected_exception)
    assert expected_exception == actual_exception

