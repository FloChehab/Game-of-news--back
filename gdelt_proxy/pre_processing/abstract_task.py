import pandas as pd
from typing import Dict


class Task(object):
    """Abstract class that handles one pipeline task.
    """

    # Holds the task name, should be overrided in subclasses
    task_name = None

    @classmethod
    def run(cls, full_df: pd.DataFrame,
            events_df: pd.DataFrame,
            mentions_df: pd.DataFrame) -> Dict:
        raise NotImplementedError(
            "This method should be implemented in subclasses")


def pandasDfToDict(df: pd.DataFrame) -> Dict:
    """Custom function for converting pandas df to dict

    Arguments:
        df {pd.DataFrame}

    Returns:
        Dict
    """

    d = df.to_dict(orient='split')
    d.pop('index')
    return d
