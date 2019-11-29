import argparse
import pandas as pd
# import modin.pandas as pd
from csv_to_parquet_converter.lib import aws


def converter(source_file_path, target_file_path, columns_subset=None, compression=None):
    """
    function to convert source csv file to target parquet file
    :param source_file_path:
    :param target_file_path:
    :param columns_subset:
    :param compression:
    :return:
    """
    df = None

    if str(source_file_path).startswith("s3://"):
        s3obj = aws.init_s3()
        aws.download_from_s3(s3obj, source_file_path, '/temp/xxx.csv')
        source_file_path = '/temp/xxx.csv'

    try:
        df = pd.read_csv(source_file_path,
                         usecols=columns_subset)
    except Exception as e:
        print(str(e))

    try:
        df.to_parquet(target_file_path,
                      compression=compression or 'UNCOMPRESSED',
                      engine='fastparquet')
    except Exception as e:
        print(str(e))

    if str(target_file_path).startswith("s3://"):
        s3obj = aws.init_s3()
        aws.upload_to_s3(s3obj, target_file_path, '/temp/yyy.parquet')

    return 0


def prepare_args():
    """

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
    columns_subset = parsed.columnssubset
    compression = parsed.compression

    converter(source_file_path=source_file_path,
              target_file_path=target_file_path,
              columns_subset=columns_subset,
              # columns_subset=["col1", "col3"]
              compression=compression
              )


if __name__ == '__main__':
    prepare_args()
