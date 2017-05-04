"""
Monte Carlo tracks functions
"""
from   invisible_cities.reco.nh5 import MCTrack
import invisible_cities.reco.tbl_functions as tbl

class MCTrackWriter:
    """Write MCTracks to file."""
    def __init__(self, h5file, compression = 'ZLIB4'):

        self.h5file      = h5file
        self.compression = compression
        self._create_mctracks_table()
        # last visited row
        self.last_row = 0

    def _create_mctracks_table(self):
        """Create MCTracks table in MC group in file h5file."""
        if '/MC' in self.h5file:
            MC = self.h5file.root.MC
        else:
            MC = self.h5file.create_group(self.h5file.root, "MC")

        self.mc_table = self.h5file.create_table(MC, "MCTracks",
                        description = MCTrack,
                        title       = "MCTracks",
                        filters     = tbl.filters(self.compression))

    def copy_mctracks(self, mctracks, evt_number, offset=0):
        for r in mctracks.iterrows(start=self.last_row):
            if r['event_indx'] == evt_number:
                self.last_row += 1
                evt = (r['event_indx']+ offset,)+ r[1:]
                self.mc_table.append([evt])
            else:
                break
        self.mc_table.flush()
