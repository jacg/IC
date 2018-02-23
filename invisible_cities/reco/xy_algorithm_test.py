import numpy as np

from pytest import fixture
from pytest import mark
from pytest import raises
parametrize = mark.parametrize

from hypothesis             import given
from hypothesis.strategies  import floats
from hypothesis.strategies  import integers
from hypothesis.strategies  import composite
from hypothesis.extra.numpy import arrays

from .. core.system_of_units_c import units
from .. core.exceptions        import SipmEmptyList
from .. core.exceptions        import ClusterEmptyList
from .. core.exceptions        import SipmZeroCharge

from .       xy_algorithms     import corona
from .       xy_algorithms     import barycenter
from .       xy_algorithms     import discard_sipms
from .       xy_algorithms     import get_nearby_sipm_inds
from .       xy_algorithms     import get_neighbours
from .       xy_algorithms     import have_same_position_in_space
from .       xy_algorithms     import is_masked


@composite
def positions_and_qs(draw, min_value=0, max_value=100):
    size = draw(integers(min_value, max_value))
    pos  = draw(arrays(float, (size, 2), floats(0.1,1)))
    qs   = draw(arrays(float, (size,  ), floats(0.1,1)))
    return pos, qs


@given(positions_and_qs(1))
def test_barycenter(p_q):
    pos, qs = p_q
    B  = barycenter(pos, qs)[0]
    assert np.allclose(B.posxy, np.average(pos, weights=qs, axis=0))
    assert np. isclose(B.Q  , qs.sum())
    assert B.nsipm == len(qs)


def test_barycenter_raises_sipm_empty_list():
    with raises(SipmEmptyList):
        barycenter(np.array([]), None)


def test_barycenter_raises_sipm_zero_charge():
    with raises(SipmZeroCharge):
        barycenter(np.array([1,2]), np.array([0,0]))


@fixture
def toy_sipm_signal():
    xs = np.array([65, -64]) * units.mm
    ys = np.array([63, -62]) * units.mm
    qs = np.array([ 6,   5]) * units.pes
    pos = np.stack((xs, ys), axis=1)
    return pos, qs


def test_corona_barycenter_are_same_with_one_cluster(toy_sipm_signal):
    pos, qs = toy_sipm_signal
    c_clusters = corona(pos, qs,
                        new_lm_radius = 10 * units.m,
                        msipm         =  1,
                        Qlm           =  4.9 * units.pes,
                        Qthr          =  0)
    c_cluster = c_clusters[0]
    b_cluster = barycenter(pos, qs)[0]

    # assert len(c_cluster)  == len(b_cluster)
    # A cluster object has no length!
    np.array_equal(c_cluster.posxy, b_cluster.posxy)
    np.array_equal(c_cluster.var.XY, b_cluster.var.XY)
    assert c_cluster.nsipm      == b_cluster.nsipm
    assert c_cluster.Q          == b_cluster.Q
    assert c_cluster.nsipm      == b_cluster.nsipm
    assert c_cluster.X          == b_cluster.X
    assert c_cluster.Y          == b_cluster.Y
    assert c_cluster.Xrms       == b_cluster.Xrms
    assert c_cluster.Yrms       == b_cluster.Yrms
    assert c_cluster.XY         == b_cluster.XY
    assert c_cluster.R          == b_cluster.R
    assert c_cluster.Phi        == b_cluster.Phi


def test_corona_multiple_clusters(toy_sipm_signal):
    """notice: cluster.xy =(x,y)
               cluster.posxy = ([x],
                              [y])
    """
    pos, qs = toy_sipm_signal
    clusters = corona(pos, qs, msipm=1, new_lm_radius=15*units.mm, Qlm=4.9*units.pes)
    assert len(clusters) == 2
    for i in range(len(pos)):
        assert np.array_equal(clusters[i].XY, pos[i])
        assert clusters[i].Q == qs[i]


def test_corona_min_threshold_Qthr():
    """notice: cluster.XY =(x,y)
               cluster.posxy = ([x],
                              [y])
    """
    xs = np.arange(100) * 10
    ys = np.zeros (100)
    qs = np.arange(100)
    pos = np.stack((xs, ys), axis=1)
    try:
        clusters = corona(pos, qs,
                        Qthr           = 99*units.pes,
                        Qlm            = 1*units.pes,
                        lm_radius  = 1*units.mm,
                        new_lm_radius  = 2*units.mm,
                        msipm          = 1)
    except ClusterEmptyList:
        pass
    assert len(clusters) ==   1
    assert clusters[0].Q ==  99
    assert clusters[0].XY == (990, 0)


def test_corona_msipm(toy_sipm_signal):
    pos, qs = toy_sipm_signal
    # this is gonna raise and error:
    try:
        cc = corona(pos, qs, msipm=2)
        assert False # otherwise make it crahs
    except SipmEmptyList:
        pass
    except ClusterEmptyList:
        pass


@parametrize(' Qlm,    rmax, nclusters',
             ((6.1,      15, 0),
              (4.9,      15, 2),
              (4.9, 1000000, 1)))
def test_corona_simple_examples(toy_sipm_signal, Qlm, rmax, nclusters):
    pos, qs = toy_sipm_signal
    try:
        assert len(corona(pos, qs,
                          msipm          =  1,
                          Qlm            =  Qlm * units.pes,
                          new_lm_radius  = rmax * units.mm )) == nclusters
    except SipmEmptyList:
        pass
    except ClusterEmptyList:
        pass


@fixture
def toy_sipm_signal_and_inds():
    k = 10000
    xs = np.arange(k)
    ys = xs + k
    pos = np.stack((xs, ys), axis=1)
    qs = xs + 2*k
    i  = np.array([0, 5, 1000, 9999])
    return k, i, pos, qs


def test_discard_sipms(toy_sipm_signal_and_inds):
    k, i, pos, qs = toy_sipm_signal_and_inds
    xysel, qsel = discard_sipms(i, pos, qs)
    xsel , ysel = xysel.T
    for ind in i:
        assert ind         not in xsel
        assert ind +     k not in ysel
        assert ind + 2 * k not in qsel


def test_get_nearby_sipm_inds():
    xs  = np.array([0,1,2,3,4,0,1,2,3,4,0,1,2,3,4])
    ys  = np.array([0,0,0,1,1,1,2,2,2,3,3,3,4,4,4])
    pos = np.stack((xs, ys), axis=1)
    qs  = np.ones (100) * 10 * units.pes
    xc, yc = (2, 2)
    c = np.array((xc, yc))
    d  = 1.5
    sis = get_nearby_sipm_inds(c, d, pos, qs)
    for i in range(len(xs)):
        if i in sis:
            assert np.sqrt((xs[i] - xc)**2 + (ys[i] - yc)**2) <= d
        else:
            assert np.sqrt((xs[i] - xc)**2 + (ys[i] - yc)**2) >  d


def test_get_neighbours():
    pos = np.array([(35, 55)])

    exp_xs = np.array([35, 35, 35, 25, 25, 25, 45, 45, 45])
    exp_ys = np.array([55, 65, 45, 55, 65, 45, 55, 65, 45])
    expected_neighbours = np.stack((exp_xs, exp_ys), axis=1)

    found_neighbours = get_neighbours(pos, pitch = 10. * units.mm)

    number_of_sipm_found_correctly = 0
    for found in found_neighbours:
        assert any(have_same_position_in_space(found, expected) for expected in expected_neighbours)
        number_of_sipm_found_correctly += 1

    assert number_of_sipm_found_correctly == 9


def test_is_masked():
    pos_masked = np.array([(0, 2),
                           (2, 1)])

    sipm_masked = [(0, 2)]
    sipm_alive  = [(3, 2)]

    assert     is_masked(sipm_masked, pos_masked)
    assert not is_masked(sipm_alive , pos_masked)


def test_masked_channels():
    """
    Scheme of SiPM positions (the numbers are the SiPM charges)
    1 1 1
    1 6 1
    1 1 1
    1 5 0
    1 1 1
    This test is meant to fail if either
    1) in the case of an empty masked channel list,
       the actual threshold in the number of SiPMs
       around the hottest one turns out to be different from msipm
    2) the masked channel is not taken properly into account
       by the code
    """
    xs         = np.array([0, 0, 0, 1, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2])
    ys         = np.array([0, 1, 2, 0, 1, 2, 0, 2, 3, 4, 3, 4, 3, 4])
    qs         = np.array([1, 1, 1, 1, 5, 1, 1, 1, 1, 1, 6, 1, 1, 1])
    pos        = np.stack((xs, ys), axis=1)
    masked_pos = np.array([(2, 1)])

    # Corona should return 1 cluster if the masked sipm is taken into account...
    expected_nclusters = 1
    found_clusters = corona(pos, qs,
                            msipm         = 6              ,
                            Qlm           = 4   * units.pes,
                            new_lm_radius = 1.5 * units.mm ,
                            pitch         = 1   * units.mm )

    assert len(found_clusters) == expected_nclusters

    # ... and two when ignored.
    expected_nclusters = 2
    found_clusters = corona(pos, qs,
                            msipm          = 6              ,
                            Qlm            = 4   * units.pes,
                            new_lm_radius  = 1.5 * units.mm ,
                            pitch          = 1   * units.mm ,
                            masked_sipm    = masked_pos     )

    assert len(found_clusters) == expected_nclusters
