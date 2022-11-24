import numpy as np

from datapool_client.core.plots import format_comment


def test_format_comment():
    format_comment(np.nan)
