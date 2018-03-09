
import tables
from ..evm.event_model     import MCParticle
from ..evm.event_model     import MCHit
from typing import Mapping

# use Mapping (duck type) rather than dict

def load_mchits(file_name: str, max_events:int =1e+9) -> Mapping[int, MCHit]:

    with tables.open_file(file_name,mode='r') as h5in:
        mctable = h5in.root.MC.MCTracks
        mcevents = read_mcinfo (mctable, max_events)
        mchits_dict = compute_mchits_dict(mcevents)
    return mchits_dict

def load_mchits_nexus(file_name: str,
                      event_range=(0,int(1e9))) -> Mapping[int, MCHit]:

    with tables.open_file(file_name,mode='r') as h5in:
        mcevents = read_mcinfo_nexus(h5in, event_range)
        mchits_dict = compute_mchits_dict(mcevents)

    return mchits_dict

def load_mcparticles(file_name: str, max_events:int =1e+9) -> Mapping[int, MCParticle]:

    with tables.open_file(file_name,mode='r') as h5in:
        mctable = h5in.root.MC.MCTracks
        return read_mcinfo (mctable, max_events)

def load_mcparticles_nexus(file_name: str,
                               event_range=(0,int(1e9))) -> Mapping[int, MCParticle]:

    with tables.open_file(file_name,mode='r') as h5in:
        return read_mcinfo_nexus(h5in, event_range)

def load_mcsensor_response_nexus(file_name: str,
                               event_range=(0,int(1e9))) -> Mapping[int, MCParticle]:

    with tables.open_file(file_name,mode='r') as h5in:
        return read_mcsns_response_nexus(h5in, event_range)

def read_mcinfo (mc_table: tables.table.Table,
                   max_events:int =1e+9) ->Mapping[int, Mapping[int, MCParticle]]:

    all_events = {}
    current_event = {}
#    convert table to numpy.ndarray
    data       = mc_table[:]
    data_size  = len(data)

    event            =  data["event_indx"]
    particle         =  data["mctrk_indx"]
    particle_name    =  data["particle_name"]
    pdg_code         =  data["pdg_code"]
    initial_vertex   =  data["initial_vertex"]
    final_vertex     =  data["final_vertex"]
    momentum         =  data["momentum"]
    energy           =  data["energy"]
    nof_hits         =  data["nof_hits"]
    hit              =  data["hit_indx"]
    hit_position     =  data["hit_position"]
    hit_time         =  data["hit_time"]
    hit_energy       =  data["hit_energy"]

    for i in range(data_size):
        if event[i] >= max_events:
            break

        current_event = all_events.setdefault(event[i], {})

        current_particle = current_event.setdefault( particle[i],
                                                    MCParticle(particle_name[i],
                                                               pdg_code[i],
                                                               initial_vertex[i],
                                                               final_vertex[i],
                                                               momentum[i],
                                                               energy[i]))
        hit = MCHit(hit_position[i], hit_time[i], hit_energy[i])
        current_particle.hits.append(hit)

    return all_events

def read_mcinfo_nexus (h5f, event_range=(0,int(1e9))) ->Mapping[int, Mapping[int, MCParticle]]:
    h5extents   = h5f.root.Run.extents
    h5hits      = h5f.root.Run.hits
    h5particles = h5f.root.Run.particles

    all_events = {}
    particles  = {}

    # the indices of the first hit and particle are 0 unless the first event
    #  written is to be skipped: in this case they must be read from the extents
    ihit = 0; ipart = 0
    if event_range[0] > 0:
        ihit  = h5extents[event_range[0]-1]['last_hit']+1
        ipart = h5extents[event_range[0]-1]['last_particle']+1

    for iext in range(*event_range):
        if iext >= len(h5extents):
            break

        current_event = {}

        ipart_end = h5extents[iext]['last_particle']
        ihit_end  = h5extents[iext]['last_hit']

        while ipart <= ipart_end:
            h5particle = h5particles[ipart]
            itrack     = h5particle['particle_indx']

            current_event[itrack] = MCParticle(h5particle['particle_name'].decode('utf-8','ignore'),
                                               0, # PDG code not currently stored
                                               h5particle['initial_vertex'],
                                               h5particle['final_vertex'],
                                               h5particle['momentum'],
                                               h5particle['kin_energy'])
            ipart += 1

        while ihit <= ihit_end:
            h5hit  = h5hits[ihit]
            itrack = h5hit['particle_indx']

            # in case the hit does not belong to a particle, create one
            # This shouldn't happen!
            current_particle = current_event.setdefault(itrack,
                                   MCParticle('unknown', 0, [0.,0.,0.],
                                              [0.,0.,0.], [0.,0.,0.], 0.))

            hit = MCHit(h5hit['hit_position'],h5hit['hit_time'],
                          h5hit['hit_energy'])
            # for now, only keep the ACTIVE hits
            if(h5hit['label'].decode('utf-8','ignore') == 'ACTIVE'):
                current_particle.hits.append(hit)
            ihit += 1

        evt_number             = h5extents[iext]['evt_number']
        all_events[evt_number] = current_event

    return all_events

def compute_mchits_dict(mcevents:Mapping[int, Mapping[int, MCParticle]])->Mapping[int, MCHit]:
    """Returns all hits in the event"""
    mchits_dict = {}
    for event_no, particle_dict in mcevents.items():
        hits = []
        for particle_no in particle_dict.keys():
            particle = particle_dict[particle_no]
            hits.extend(particle.hits)
        mchits_dict[event_no] = hits
    return mchits_dict


def read_mcsns_response_nexus (h5f, event_range=(0,1e9)) ->Mapping[int, Mapping[int, MCParticle]]:

#    h5config = h5f.get('Run/configuration')
#    for row in h5config:
#        if row['param_key']

    h5extents = h5f.root.Run.extents
    h5waveforms = h5f.root.Run.waveforms

#    if table_name == 'waveforms':
    last_line_of_event = 'last_sns_data'
#    elif table_name == 'tof_waveforms':
#        last_line_of_event = 'last_sns_tof'

    all_events = {}

    iwvf = 0
    if(event_range[0] > 0):
        iwvf = h5extents[event_range[0]-1][last_line_of_event]+1

    for iext in range(*event_range):
        if (iext >= len(h5extents)):
            break

        current_event = {}

        iwvf_end = h5extents[iext][last_line_of_event]
        current_sensor_id = h5waveforms[iwvf]['sensor_id']
        times = []
        charges = []
        while (iwvf <= iwvf_end):
            wvf_row = h5waveforms[iwvf]
            sensor_id = wvf_row['sensor_id']

            if (sensor_id == current_sensor_id):
                times.append(wvf_row['time_bin'])
                charges.append(wvf_row['charge'])
            else:
                current_event[current_sensor_id] = zip(times, charges)
                times = []
                charges = []
                times.append(wvf_row['time_bin'])
                charges.append(wvf_row['charge'])
                current_sensor_id = sensor_id

            iwvf += 1

        evt_number = h5extents[iext]['evt_number']
        all_events[evt_number] = current_event

    return all_events
