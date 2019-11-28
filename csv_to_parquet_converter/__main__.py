import argparse

import pandas as pd
#import modin.pandas as pd


def converter(source_file_path, target_file_path, compression=None, columns_subset=None):
    """
    function to convert source csv file to target parquet file
    :param source_file_path:
    :param target_file_path:
    :param compression:
    :param columns_subset:
    :return:
    """
    df = pd.read_csv(source_file_path,
                     usecols=columns_subset)

    df.to_parquet(target_file_path, compression=compression or 'UNCOMPRESSED', engine='fastparquet')

    return 0

def prepare_args():
    ###########################################################################################
    """ 0: The arguments parser and main launcher """
    ###########################################################################################

    parser = argparse.ArgumentParser()
    parser.add_argument('-dn', '--databasename', type=str, required=True)
    parser.add_argument('-sn', '--schemaname', type=str, required=True)
    parser.add_argument('-td', '--targetdirectory', type=str, required=True)
    parser.add_argument('-dr', '--dryrun', type=str, required=True)
    parsed = parser.parse_args()

    database_name = parsed.databasename
    schema_name = parsed.schemaname

    target_directory = parsed.targetdirectory
    target_directory = target_directory.replace('\f', '\\f')

    dry_run = parsed.dryrun
    # arg parse bool data type known bug workaround
    if dry_run.lower() in ('no', 'false', 'f', 'n', 0):
        dry_run = False
        dry_run_str_prefix = ''
    else:
        dry_run = True
        dry_run_str_prefix = 'Dry run '

    # obj = Runner(database_name, schema_name, target_directory, dry_run, dry_run_str_prefix)
    # Runner.main(obj)
    converter(source_file_path="C:\GIT_PROJECTS_PERSONAL\csv_to_parquet_converter\csv_to_parquet_converter\scratch\\source.csv",
              target_file_path="C:\GIT_PROJECTS_PERSONAL\csv_to_parquet_converter\csv_to_parquet_converter\scratch\\target.parquet",
              columns_subset=["col1", "col3"])


if __name__ == '__main__':
    prepare_args()
