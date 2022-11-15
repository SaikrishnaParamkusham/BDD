Feature: MarketCap validation
   # Validating eligible and ineligible dataframes
   Scenario: Data validation for filtering eligible and ineligible dataframes
      Given an input data frame:
         | Id:int | fullmarketcap:int |
         | 1      | 10000000  |
         | 2      | 10000010  |
         | 3      | 10000     |
         | 4      | 10        |
         | 5      | 1000      |
         | 6      | 23948     |
       When parameters passed are fullmarketcap,>=,2345612
       Then the eligible dataframes is:
         | fullmarketcap:int |
         | 10000000  |
         | 10000010  |
       And the ineligible dataframe is:
         | fullmarketcap_reason:str | fullmarketcap:int |
         | failed due to fullmarketcap | 10000     |
         | failed due to fullmarketcap | 10        |
         | failed due to fullmarketcap | 1000      |
         | failed due to fullmarketcap | 23948     |

   # This scenario is used to validate,
   # 1. if the module is able to filter all values as eligible given a dataframe
   # 2. And, able create an empty ineligible dataframe.
   Scenario: Data validation for all eligible values and empty ineligible dataframe
      Given an input data frame with fullmarketcap values greater than 50:
         | Id:int | fullmarketcap:int |
         | 1      | 10000000  |
         | 2      | 10000010  |
         | 3      | 10000     |
         | 4      | 100       |
         | 5      | 1000      |
         | 6      | 23948     |
      When the parameters passed to the function are fullmarketcap, >, and 50
      Then the eligible dataframe should have the below fullmarketcap values:
         | fullmarketcap:int |
         | 10000000  |
         | 10000010  |
         | 10000     |
         | 100       |
         | 1000      |
         | 23948     |
      And the ineligible dataframe should be an empty dataframe with below column names:
         | fullmarketcap_reason:str | fullmarketcap:int |

   # This scenario is used to validate missing values in the input data frame
   Scenario: Data Validation for missing values
      Given an input data frame with missing values:
         | Id:int | fullmarketcap:int |
         | 1      | 10000000  |
         | 2      | 10000010  |
         | 3      | 10000     |
         | 4      |           |
         | 5      | 1000      |
         | 6      |           |
      When the given input parameters are fullmarketcap, >, and 1200
      Then the eligible data frame should be:
         | fullmarketcap:int |
         | 10000000  |
         | 10000010  |
         | 10000     |
      And the ineligible data frame should be:
         | fullmarketcap_reason:str | fullmarketcap:int |
         | failed due to fullmarketcap |  1000    |

   # This scenario is used to validate empty data frame exception
   Scenario: Data Validation for Data Frame Exceptions
      Given an input data frame for empty data frame exception:
         | Id:int | fullmarketcap:int |
      When the parameters passed for empty Data frame exception are fullmarketcap, >, and 1200
      Then the exception raised for empty input data frame is Dataframe or Table returned ZERO records.

   # This scenario is used to validating exception when passing incorrect field name as parameter
   Scenario: Data Validation for Field Parameter Exception
      Given an input data frame for incorrect field exception:
         | Id:int | fullmarketcap:int |
         | 1      | 10000000  |
         | 2      | 10000010  |
         | 3      | 10000     |
         | 4      | 100       |
      When the parameters passed for incorrect field name exception are fullmarket, >, and 1200
      Then the exception for incorrect field name is 'fullmarket' provided was/were not found in the 'Dataframe or Table'.

   # This scenario is used to validating exception when passing incorrect operator as parameter
   Scenario: Data Validation for Operator Parameter Exception
      Given an input data frame for incorrect Operator exception:
         | Id:int | fullmarketcap:int |
         | 1      | 10000000  |
         | 2      | 10000010  |
         | 3      | 10000     |
         | 4      | 100       |
      When the parameters passed for incorrect operator name exception are fullmarketcap, !, and 1200
      Then the exception for incorrect operator is filter_type entered is invalid. Please enter the valid ['>=', '>', '<=', '<', '==', '!='].

   # This scenario is used to validating exception when passing incorrect datatype threshold as parameter
   Scenario: Data Validation for threshold Parameter Exception
      Given an input data frame for incorrect threshold datatype exception:
         | Id:int | fullmarketcap:int |
         | 1      | 10000000  |
         | 2      | 10000010  |
         | 3      | 10000     |
         | 4      | 100       |
      When the parameters passed for incorrect threshold datatype exception are fullmarketcap, >=, and 1Million
      Then the exception for incorrect threshold datatype is Invalid Datatype. Please provide 'threshold' in the format of list, int or float.
