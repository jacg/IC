import numpy as np

from numpy.testing import assert_almost_equal

from hypothesis            import given
from hypothesis.strategies import lists
from hypothesis.strategies import floats
from hypothesis.strategies import builds

from . paolina import Hit
from . paolina import Voxel
from . paolina import bounding_box
from . paolina import voxelize_hits

posn = floats(min_value=0, max_value=100)
ener = posn

bunch_of_hits = lists(builds(Hit, posn, posn, posn, ener),
                      min_size = 0,
                      max_size = 30)


@given(bunch_of_hits)
def test_voxelize_hits_does_not_lose_energy(hits):
    voxels = voxelize_hits(hits, None)

    def sum_energy(seq):
        return sum(e.E for e in seq)

    assert sum_energy(hits) == sum_energy(voxels)

@given(bunch_of_hits)
def test_bounding_box(hits):
    if not len(hits): # TODO: deal with empty sequences
        return

    lo, hi = bounding_box(hits)

    mins = [float(' inf')] * 3
    maxs = [float('-inf')] * 3

    for hit in hits:
        assert lo[0] <= hit.pos[0] <= hi[0]
        assert lo[1] <= hit.pos[1] <= hi[1]
        assert lo[2] <= hit.pos[2] <= hi[2]

        for d in range(3):
            mins[d] = min(mins[d], hit.pos[d])
            maxs[d] = max(maxs[d], hit.pos[d])

    for d in range(3):
        assert_almost_equal(mins[d], lo[d])
        assert_almost_equal(maxs[d], hi[d])
