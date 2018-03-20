"""
detsim_functions_test.py
"""

import pytest
import os
import numpy  as np
import tables as tb
import pandas as pd

from .. io.dst_io  import load_dst
from numpy.testing import assert_allclose

@pytest.fixture(scope = 'module')
def SE_nexus_filename(ICDATADIR):
    return os.path.join(ICDATADIR, "electron_26keV_nexus.h5")

def test_diffuse_and_smear_hits(SE_nexus_filename):
    mchits_dict = load_mchits_nexus(SE_nexus_filename, (0,1))

    for evt_number, mchits in mchits_dict.items():

        dmchits,zdrift = diffuse_and_smear_hits(mchits,
                                            50,      # zmin
                                            500,     # zmax
                                            1.0,     # diff_transv
                                            0.3,     # diff_long
                                            0.8,     # resolution_FWHM
                                            2.45783) # Qbb

        assert(len(dmchits) == len(mchits))
        assert(zdrift > 50)
        assert(zdrift < 500)
