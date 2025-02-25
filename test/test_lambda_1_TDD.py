import pytest
import boto3
from moto import mock_aws
import json
from unittest.mock import Mock, patch
from pprint import pprint
from src.lambda_ingest import write_variable_to_s3, write_table_to_s3, connect_to_db, close_db_connection, get_rows_and_columns_from_table, return_object_s3_key_injection_bucket, lambda_handler

"""
test_lambda_1_TDD.py
Author: Fabio Greenwood

Intro:
    This module contains unit tests for the lambda_ingest.py module.
    It uses pytest for testing.

    Currently the first draft of tests are being written, the major point of this file is to lay out a set of habits like the use of fixtures


Functions tested:
    - write_file_to_s3
        test this!!!

    - connect_to_db?
    - close_db_connection?
        unsure how and if I should test these (maybe check what was done on other exercises)

    - get_rows_and_columns_from_table??
        maybe

PS:
    Please check the Trello for the over arching To-Do list!

Actions:
    - Remove the hardcoding
"""



#%% Placeholder Variables and Functions - These are hard-coded currently and may need to be made more programmic

target_bucket_name = "injestion zone"
list_of_toteSys_tables = ["tableA", "tableB"] # Action: obviously this isn't the list and should be replaced with an postGRESS function that lists all the tables in a database


def some_function_that_returns_a_table_from_Postgres():
    pass

class DummyContext:
    pass

MOCK_ENVIROMENT = True


#%% fixtures


@pytest.fixture()
def hardcoded_variables():
    hardcoded_variables = {}
    hardcoded_variables["target_bucket_name"] = "injestion zone"
    hardcoded_variables["list_of_toteSys_tables"] = ["tableA", "tableB"]
    hardcoded_variables["AccountId"] = "AccountId"
    hardcoded_variables["list_of_tables"] = ["address", "counterparty", "currency", "department", "design", "payment_type", "payment", "purchase_order", "sales_order", "staff", "transaction"]
    return hardcoded_variables


@pytest.fixture()
def s3_client(hardcoded_variables):
    global MOCK_ENVIROMENT
    if MOCK_ENVIROMENT == True:
        with mock_aws():
            s3 = boto3.client("s3")
            s3.create_bucket(
                Bucket=target_bucket_name,
                CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
            yield s3
    elif MOCK_ENVIROMENT == False:
        s3 = boto3.client('s3')
        response = s3.get_bucket(
            AccountId='string',
            Bucket=hardcoded_variables["target_bucket_name"])
        yield s3

@pytest.fixture()
def example_table_A_from_Postgres():
    example_table_A_from_Postgres = some_function_that_returns_a_table_from_Postgres()
    return example_table_A_from_Postgres


@pytest.fixture()
def snapshot_data_dict():
    # TO DO: this will take all the snapshot data from the jsons and place them into dfs in a dict for later testing
    return None


#%% Tests


@pytest.mark.timeout(10)
class Test_write_variable_to_s3:
    def test_1_expected_file_names_are_added_to_blank_s3(self, s3_client, hardcoded_variables, example_table_A_from_Postgres):
        """
        This test verifies that the write_variable_to_s3 function adds a table to the s3 bucket.

        Expected behavior:
        - write_variable_to_s3(s3_client, variable, bucket_name, object_key)
            should:
            then populate said variable (in this case table data returned from postgres) in the object_key location specified
        """
        # assemble
        object_key = return_object_s3_key_injection_bucket(example_table_A_from_Postgres.__name__) # Action: I reckon there is a way to extract the table name from the variable metadata
        
        # act
        response = write_variable_to_s3(s3_client, example_table_A_from_Postgres, hardcoded_variables["target_bucket_name"], object_key)
        
        # assert
        file_list_response = s3_client.list_objects_v2(Bucket=hardcoded_variables["target_bucket_name"])
        actual_file_key_list = [i['Key'] for i in file_list_response['Contents']]
        pprint(response)
        assert [object_key] == actual_file_key_list


class Test_lambda_hander:
    def test_2a_all_tables_are_digested_once_mock(self, hardcoded_variables, snapshot_data_dict):
        
        with mock_aws():
            
            # assemble
            
            s3_actual_results = boto3.client("s3")
            expected_data = snapshot_data_dict
            event = {}

            # act
            lambda_handler(event, DummyContext)

            
            # assert - do all the files exist?
            actual_list_of_s3_filepaths = s3_actual_results.list_objects()
            assert actual_list_of_s3_filepaths == [return_object_s3_key_injection_bucket(table_name) for table_name in hardcoded_variables["list_of_toteSys_tables"]]
            
            # assert - does data match?
            for table_name in hardcoded_variables["list_of_toteSys_tables"]:
                assert snapshot_data_dict["table_name"].to_json() == s3_actual_results.get_object(return_object_s3_key_injection_bucket(table_name))

    @pytest.mark.skip()
    def test_2b_tables_are_refreshed_with_update():
        # this method should check that the s3 bucket is wiped clean and new data is inserted to the s3 bucket on a second calling of the method
        # to ensure this, the dummy totsys data will need to be modified between excution calls
        
        """
        Insert the previous method then:
        
        
        new_snapshot_data = add_some_data_to_snapshot(snapshot_data)
        
        """
        
        
        
        pass
    
    def test_2c_ensure_that_failures_are_logged():
        # adapt the below
        """Tests the lambda handler"""

        """@pytest.mark.it("unit test: writes to s3")
        @patch("src.quotes.get_quote")
        def test_handler_writes_quotes_to_s3(self, mock_quote, s3, bucket, caplog):
            mock_quote.return_value = (200, processed)
            event = {}
            context = DummyContext()
            with caplog.at_level(logging.INFO):
                lambda_handler(event, context)
                assert "Wrote quotes to S3" in caplog.text"""



