"""
Script to provide a CLI to set-up all the folders and manipulate files
"""
import argparse
import shutil
from os import listdir, mkdir
from os.path import dirname, exists, join, realpath
from subprocess import call

import pandas as pd

from gdelt_proxy.pre_processing.json import dict_to_json_file
from gdelt_proxy.pre_processing.run_dataset_pipeline import \
    run_dataset_pipeline

dir_path = dirname(realpath(__file__))
datasets_dir = join(dir_path, 'datasets')
datesets_zip = join(dir_path, 'datasets.zip')
cached_queries_pipelines = join(dir_path, 'cached_queries_pipelines')
datasets_pipelines = join(dir_path, 'datasets_pipelines')


def set_up_datasets_dirs():
    call(['wget', '-O', datesets_zip,
          'https://cloud.floflo.ch/index.php/s/LZ9ef7tJXwpyMHt/download'])
    call(['unzip', datesets_zip])
    if not exists(datasets_pipelines):
        mkdir(datasets_pipelines)
    genereate_pipeline_dataset()


def genereate_pipeline_dataset():
    for file in listdir(datasets_dir):
        full_df = pd.read_csv(join(datasets_dir, file))
        res = run_dataset_pipeline(full_df)
        fn = file.split('.')[0]
        dict_to_json_file(res, join(datasets_pipelines, fn + '.json'))


def init():
    if not exists(datasets_dir):
        set_up_datasets_dirs()
    if not exists(cached_queries_pipelines):
        mkdir(cached_queries_pipelines)


def empty_cache():
    shutil.rmtree(cached_queries_pipelines)
    init()


def reset_all():
    empty_cache()
    shutil.rmtree(datasets_dir)
    init()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Trajectory generation')

    parser.add_argument('--reset-all', dest='reset_all',
                        action='store_true', default=False,
                        help="Reset all the generated data stored on the server and update datasets")

    parser.add_argument('--empty-cache', dest='empty_cache',
                        action='store_true', default=False,
                        help="Empty the cached queries datasets")

    parser.add_argument('--rerun-datasets-pipeline', dest='rerun_datasets_pipelines',
                        action='store_true', default=False,
                        help="Regenerate datasets pipelines json files")

    args = parser.parse_args()

    init()

    if args.reset_all:
        reset_all()

    if args.empty_cache:
        empty_cache()

    if args.rerun_datasets_pipelines:
        genereate_pipeline_dataset()

    print("All systems are ready")
