"""
__main__ module
"""
import argparse
import logging
import traceback
import pandas as pd

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
LOGGER = logging.getLogger(__name__)


def converter_csv_to_parquet(source_file_path, target_file_path,
                             columns_subset=None, compression=None):
    """
    function to convert source csv file to target parquet file
    :param source_file_path:
    :param target_file_path:
    :param columns_subset:
    :param compression:
    :return:
    """
    try:
        data_frame = pd.read_csv(source_file_path,
                                 usecols=columns_subset)
        LOGGER.info("read_csv passed")
    except Exception:
        trace_back = traceback.format_exc()
        LOGGER.error(trace_back)
        return 1

    try:
        data_frame.to_parquet(target_file_path,
                              compression=compression or 'UNCOMPRESSED',
                              engine='fastparquet')
        LOGGER.info("to_parquet passed")
    except Exception:
        trace_back = traceback.format_exc()
        LOGGER.error(trace_back)
        return 1

    return 0


def converter_parquet_to_csv(source_file_path, target_file_path,
                             columns_subset=None):
    """
    function to convert source parquet file to target csv file
    :param source_file_path:
    :param target_file_path:
    :param columns_subset:
    :param compression:
    :return:
    """
    try:
        data_frame = pd.read_parquet(source_file_path,
                                     engine='fastparquet',
                                     columns=columns_subset)
        LOGGER.info("read_parquet passed")
    except Exception:
        trace_back = traceback.format_exc()
        LOGGER.error(trace_back)
        return 1

    try:
        data_frame.to_csv(target_file_path, index=False)
        LOGGER.info("to_csv passed")
    except Exception:
        trace_back = traceback.format_exc()
        LOGGER.error(trace_back)
        return 1

    return 0


def main(**prepared_args):
    """
    main conversion runner
    :param prepared_args kwargs:
    :return:
    """
    conversion_result = None

    if prepared_args['conversion_type'] == 'csv_to_parquet':
        conversion_result = converter_csv_to_parquet(prepared_args['source_file_path'],
                                                     prepared_args['target_file_path'],
                                                     prepared_args['columns_subset'],
                                                     prepared_args['compression'])
    elif prepared_args['conversion_type'] == 'parquet_to_csv':
        conversion_result = converter_parquet_to_csv(prepared_args['source_file_path'],
                                                     prepared_args['target_file_path'],
                                                     prepared_args['columns_subset'])

    return conversion_result


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
    columns_subset = parsed.columnssubset
    compression = parsed.compression

    if str(source_file_path).endswith(".csv") and str(target_file_path).endswith(".parquet"):
        conversion_type = 'csv_to_parquet'
    elif str(source_file_path).endswith(".parquet") and str(target_file_path).endswith(".csv"):
        conversion_type = 'parquet_to_csv'
    else:
        LOGGER.error('Not implemented conversion type, valid options: '
                     '.csv to .parquet or .parquet to .csv')
        raise NotImplementedError

    return {'conversion_type': conversion_type,
            'source_file_path': source_file_path,
            'target_file_path': target_file_path,
            'columns_subset': columns_subset,
            'compression': compression}


if __name__ == '__main__':
    prepared_args = prepare_args()
    main_result = main(**prepared_args)

    if main_result == 0:
        LOGGER.info(f'Conversion end to end passed')
    else:
        LOGGER.info(f'Conversion end to end failed')
