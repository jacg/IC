"""Functions manipulating sensors (PMTs and SiPMs)
JJGC January 2017
"""
from __future__ import print_function, division, absolute_import

import numpy as np

from   invisible_cities.database import load_db
import invisible_cities.core.system_of_units as units


def weighted_sum(CWF, w_vector):
    """Return the weighted sum of CWF (weights defined in w_vector)."""

    NPMT = len(CWF)
    NWF = len(CWF[0])
    assert len(w_vector) == NPMT

    csum = np.zeros(NWF, dtype=np.double)
    for j in range(NPMT):
        csum += CWF[j] * 1 / w_vector[j]
    return csum


def sipm_with_signal(sipmrwf, thr=1):
    """Find the SiPMs with signal in this event."""
    SIPML = []
    for i in range(sipmrwf.shape[0]):
        if np.sum(sipmrwf[i] > thr):
            SIPML.append(i)
    return np.array(SIPML)
