from argparse import Namespace

import numpy  as np
import tables as tb
import pandas as pd


from .. database            import load_db
from .. io.dst_io           import load_dst
from .. reco.corrections    import Correction
from .. reco.corrections    import LifetimeCorrection
from .. core                import fit_functions  as fitf
from .. core.core_functions import in_range
from .. io.kdst_io          import xy_correction_writer

from .  components import city


@city
def zaira(files_in, file_out,
          event_range,   # not used, by config insists on sending it
          dst_group, dst_node,
          lifetime, u_lifetime,
          xbins, xmin, xmax,
          ybins, ymin, ymax,
          zbins, zmin, zmax,     tbins,
          fiducial_r, fiducial_z, fiducial_e):

    def get_z_and_e_fiducial():
        fiducial_z_ = ((det_geo.ZMIN[0], _geo.ZMAX[0])
                       if fiducial_z is None
                       else fiducial_z)
        fiducial_e_ = (0, np.inf) if fiducial_e is None else fiducial_e
        return fiducial_z_, fiducial_e_

    def get_lifetime_corrections():
        lifetimes   = [  lifetime] if not np.shape(  lifetime) else   lifetime
        u_lifetimes = [u_lifetime] if not np.shape(u_lifetime) else u_lifetime
        return tuple(map(LifetimeCorrection, lifetimes, u_lifetimes))

    def get_xybins_and_range():
        xmin_ = det_geo.XMIN[0] if xmin is None else xmin
        xmax_ = det_geo.XMAX[0] if xmax is None else xmax
        ymin_ = det_geo.YMIN[0] if ymin is None else ymin
        ymax_ = det_geo.YMAX[0] if ymax is None else ymax
        xrange = xmin_, xmax_
        yrange = ymin_, ymax_
        return xrange, yrange

    def xy_correction(X, Y, E):
        xs, ys, es, us = fitf.profileXY(X, Y, E,
                                        xbins,  ybins,
                                        xrange, yrange)
        return Correction((xs, ys), es, us)

    def xy_statistics(X, Y):
        return np.histogram2d(X, Y, (xbins,  ybins),
                                    (xrange, yrange))

    det_geo = load_db.DetectorGeo()

    lifetime_corrections   = get_lifetime_corrections()
    xrange, yrange         = get_xybins_and_range()
    fiducial_z, fiducial_e = get_z_and_e_fiducial()

    dsts    = [load_dst(input_file, dst_group, dst_node) for input_file in files_in]

    # Correct each dataset with the corresponding lifetime
    for dst, correct in zip(dsts, lifetime_corrections):
        dst.S2e *= correct(dst.Z.values).value

    # Join datasets
    dst = pd.concat(dsts)
    events_in = len(dst)

    # select fiducial region
    dst = dst[in_range(dst.S2e.values, * fiducial_e)]
    dst = dst[in_range(dst.Z  .values, * fiducial_z)]
    dst = dst[in_range(dst.X  .values, * xrange    )]
    dst = dst[in_range(dst.Y  .values, * yrange    )]

    # Compute corrections and stats
    xycorr = xy_correction(dst.X.values, dst.Y.values, dst.S2e.values)
    nevt   = xy_statistics(dst.X.values, dst.Y.values)[0]

    with tb.open_file(file_out, 'w') as h5out:
        write_xy = xy_correction_writer(h5out)
        write_xy(*xycorr._xs, xycorr._fs, xycorr._us, nevt)

    return Namespace(events_in  = events_in,
                     events_out = len(dst))

