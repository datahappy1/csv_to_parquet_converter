import argparse
import logging
import pandas as pd
# import modin.pandas as pd

logger = logging.getLogger(__name__)

def converter_csv_to_parquet(source_file_path, target_file_path, columns_subset=None, compression=None):
    """
    function to convert source csv file to target parquet file
    :param source_file_path:
    :param target_file_path:
    :param columns_subset:
    :param compression:
    :return:
    """
    try:
        df = pd.read_csv(source_file_path,
                         usecols=columns_subset)
    except Exception as error:
        logger.error(error)
        raise

    try:
        df.to_parquet(target_file_path,
                      compression=compression or 'UNCOMPRESSED',
                      engine='fastparquet')
    except Exception as error:
        logger.error(error)
        raise

    return 0


def converter_parquet_to_csv(source_file_path, target_file_path, columns_subset=None, compression=None):
    """
    function to convert source parquet file to target csv file
    :param source_file_path:
    :param target_file_path:
    :param columns_subset:
    :param compression:
    :return:
    """
    try:
        df = pd.read_parquet(source_file_path,
                             engine='fastparquet',
                             columns=columns_subset)
    except Exception as error:
        logger.error(error)
        raise

    try:
        df.to_csv(target_file_path)
    except Exception as error:
        logger.error(error)
        raise

    return 0


def main(conversion_type, source_file_path, target_file_path, columns_subset, compression):
    if conversion_type == 'csv_to_parquet':
        conversion_result = converter_csv_to_parquet(source_file_path, target_file_path, columns_subset, compression)
    elif conversion_type == 'parquet_to_csv':
        conversion_result = converter_parquet_to_csv(source_file_path, target_file_path, columns_subset, compression)
    else:
        raise NotImplementedError

    if conversion_result == 0:
        return 0


def prepare_args():
    """
    function for preparation of the CLI arguments
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-sfp', '--sourcefilepath', type=str, required=True)
    parser.add_argument('-tfp', '--targetfilepath', type=str, required=True)
    parser.add_argument('-cols', '--columnssubset', type=str, required=False)
    parser.add_argument('-comp', '--compression', type=str, required=False)
    parsed = parser.parse_args()

    source_file_path = parsed.sourcefilepath
    target_file_path = parsed.targetfilepath
    columns_subset = parsed.columnssubset  # ["col1", "col3"]
    compression = parsed.compression

    if str(source_file_path).endswith(".csv") and str(target_file_path).endswith(".parquet"):
        conversion_type = 'csv_to_parquet'
    elif str(source_file_path).endswith(".parquet") and str(target_file_path).endswith(".csv"):
        conversion_type = 'parquet_to_csv'
    else:
        raise NotImplementedError

    main(conversion_type=conversion_type,
         source_file_path=source_file_path,
         target_file_path=target_file_path,
         columns_subset=columns_subset,
         compression=compression
         )


if __name__ == '__main__':
    prepare_args()
