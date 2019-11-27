import argparse
import numpy as np
import py

# import pandas as pd
import modin.pandas as pd


def converter(source_file_path, target_file_path, compression=None, columns_subset=None):
    """

    :param source_file_path:
    :param target_file_path:
    :param compression:
    :param columns_subset:
    :return:
    """
    df = pd.read_csv(source_file_path,
                     usecols=columns_subset)

    df.to_parquet(target_file_path,
                  compression=compression or 'UNCOMPRESSED',
                  engine='fastparquet')


class AWS:
    def __init__(self):
        pass

    def s3_downloader(self):
        """

        :return:
        """
        pass

    def s3_uploader(self):
        """

        :return:
        """
        pass


def arg_parser():
    """

    :return:
    """
    pass


if __name__ == "main":
    pass
