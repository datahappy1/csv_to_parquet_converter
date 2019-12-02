import argparse
import logging
import pandas as pd
# import modin.pandas as pd
from csv_to_parquet_converter.lib import aws


def cleanup_temp_folders():
    """
    cleanup temp folders function
    :return:
    """
    # cleanup content of /temp/source, /temp/target


def converter_csv_to_parquet(source_file_path, target_file_path, columns_subset=None, compression=None):
    """
    function to convert source csv file to target parquet file
    :param source_file_path:
    :param target_file_path:
    :param columns_subset:
    :param compression:
    :return:
    """
    df = None
    temp_source_file_path = '/temp/source/xxx.csv'
    temp_target_file_path = '/temp/target/xxx.parquet'

    if str(source_file_path).startswith("s3://"):
        s3obj = aws.init_s3()
        aws.download_from_s3(s3obj, source_file_path, temp_source_file_path)
    else:
        temp_source_file_path = source_file_path

    try:
        df = pd.read_csv(temp_source_file_path,
                         usecols=columns_subset)
    except Exception as e:
        print(str(e))

    try:
        df.to_parquet(temp_target_file_path,
                      compression=compression or 'UNCOMPRESSED',
                      engine='fastparquet')
    except Exception as e:
        print(str(e))

    if str(target_file_path).startswith("s3://"):
        s3obj = aws.init_s3()
        aws.upload_to_s3(s3obj, target_file_path, temp_target_file_path)
    else:
    # copy target file from temp_target_file_path to target_file_path

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
    df = None
    temp_source_file_path = '/temp/source/xxx.csv'
    temp_target_file_path = '/temp/target/xxx.parquet'

    if str(source_file_path).startswith("s3://"):
        s3obj = aws.init_s3()
        aws.download_from_s3(s3obj, source_file_path, temp_source_file_path)
    else:
        temp_source_file_path = source_file_path
    try:
        df = pd.read_parquet(temp_source_file_path,
                             engine='fastparquet',
                             columns=columns_subset)
    except Exception as e:
        print(str(e))

    try:
        df.to_csv(temp_target_file_path)
    except Exception as e:
        print(str(e))

    if str(target_file_path).startswith("s3://"):
        s3obj = aws.init_s3()
        aws.upload_to_s3(s3obj, target_file_path, temp_target_file_path)
    else:
    # copy target file from temp_target_file_path to target_file_path

    return 0


def main(conversion_type, source_file_path, target_file_path, columns_subset, compression):
    if conversion_type == 'csv_to_parquet':
        res = converter_csv_to_parquet(source_file_path, target_file_path, columns_subset, compression)
    elif conversion_type == 'parquet_to_csv':
        res = converter_parquet_to_csv(source_file_path, target_file_path, columns_subset, compression)
    else:
        raise NotImplementedError

    if res == 0:
        cleanup_temp_folders()


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
