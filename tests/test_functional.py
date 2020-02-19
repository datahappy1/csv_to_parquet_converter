from csv_to_parquet_converter.__main__ import converter_csv_to_parquet, converter_parquet_to_csv
# set the working directory for instance like C:\csv_to_parquet_converter\tests

def test_functional_csv_to_parquet():
    """
    functional test function for local csv to parquet
    :return: assertion output
    """
    calculated_result = converter_csv_to_parquet(source_file_path='files/source.csv',
                                                 target_file_path='files/target.parquet')

    assert calculated_result == 0


def test_functional_parquet_to_csv():
    """
    functional test function for local parquet to csv
    :return: assertion output
    """
    calculated_result = converter_parquet_to_csv(source_file_path='files/source.parquet',
                                                 target_file_path='files/target.csv')

    assert calculated_result == 0
