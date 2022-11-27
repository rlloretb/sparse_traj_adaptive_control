#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 15:41:32 2020
@author: Roger Lloret-Batlle rogerm8@gmail.com
# TODO: clean trajectories which did not cross the junction
# TODO: create dep_time feature
"""

#%%
import time
import os, sys
import subprocess
from pathlib import Path
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import traci

from dict2xml import dict2xml

# sys.path.append(str(Path.cwd().parent / 'data_processing' / 'inference'))
# from traj_plotting import *
# from shockwave import *

#%% Functions

def create_SUMO_grid_network(root_sim, pars_net, net_filename=''):
    '''Creates SUMO grid network'''
    if net_filename == '':
        network_filename = compose_network_filename(pars_net)
    else:
        network_filename = net_filename
    full_path_sumo_net = root_sim / network_filename
    listt = ['netgenerate', '--grid',
            f'--grid.x-number={pars_net["n_x"]}', f'--grid.y-number={pars_net["n_y"]}',
            f'--grid.x-length={pars_net["l_x"]}', f'--grid.y-length={pars_net["l_y"]}',
            f'--grid.attach-length={pars_net["l_out"]}', 
            f'-L={pars_net["n_lanes"]}',
            "--tls.guess=true",
            '--tls.guess.threshold=10',
            f'-S={pars_net["speed_ms"]}',
            f'--no-turnarounds=true',
            f'--output-file={str(full_path_sumo_net)}']
    # listt.append(f'--default.junctions.keep-clear={str(pars_net["keep_clear"]).lower()}')
    # listt.append(f'--remove-edges.explicit={pars_net["remove_edges_list"]}')
    # listt.append(f'--turn-lanes=1')
    # listt.append(f'--turn-lanes.length=75')
    # listt.append(f'--tls.left-green.time=0')
    listt.append(f'--check-lane-foes.all=true')
    listt.append(f'--opposites.guess=true')
    # listt.append(f'--tls.cycle.time={pars_net["cycle_time"]}')
    listt.append(f'--tls.default-type={pars_net["tls_type"]}')
    # if len(pars_net["half_offset_list"]) > 0:
    #     listt.append(f'--tls.half-offset={pars_net["half_offset_list"]}')
    print(" ".join(listt))
    print(subprocess.check_output(listt))


def shp2net():
    '''Transforms shapefile to SUMO network'''
    print(subprocess.check_output(['netconvert', '-v',
                                    '--shapefile-prefix', 'test_area',
                                    '-o', 'net.net.xml',
                                    '--shapefile.from-id', 'start_junc',
                                    '--shapefile.to-id', 'end_junc_i',
                                    '--shapefile.street-id', 'segment_id',
                                    '--shapefile.use-defaults-on-failure']))


def check_sumo_path():
    sumo_path = Path(os.environ['SUMO_HOME'])
    if 'SUMO_HOME' in os.environ:
        tools = os.path.join(str(sumo_path), 'tools')
        sys.path.append(tools)
    else:
        sys.exit("please declare environment variable 'SUMO_HOME'")


def change_jam_density_new_vehicles(mu_min_gap = 3, scale_min_gap=np.sqrt(0.5)):
    for vehicle_id in traci.simulation.getDepartedIDList():
        min_gap = max(0.5,np.random.normal(mu_min_gap, scale_min_gap))
        vehicle_length = max(3,np.random.normal(4, np.sqrt(0.5)))
        traci.vehicle.setMinGap(vehicle_id, min_gap)
        traci.vehicle.setLength(vehicle_id, vehicle_length)


def change_stopbar():
    for vehicle_id in traci.simulation.getDepartedIDList():
        stopbar_offset = np.random.normal(5, 0.2)
        traci.vehicle.setParameter(vehicle_id, 'jmStoplineGap', str(stopbar_offset))


def get_connection_df(tls_id):
    controlled_links = traci.trafficlight.getControlledLinks(tls_id)
    connection_dict = {}
    for i, el in enumerate(controlled_links):
        first_edge = el[0][0].split('_')[0]
        last_edge = el[0][1].split('_')[0]
        connection_dict[i] = {'movement_id': first_edge + '_' + last_edge}
    return pd.DataFrame(connection_dict).T.reset_index()


def run_sumo_simulation(pars, binary ='sumo'):
    '''Runs SUMO simulation, transforms XML output to CSV.
        Assumes config filename'''
    root_sim = pars['root_sim']
    # output_filename_no_ext = output_filename.split('.')[0]
    sumo_path = Path(os.environ['SUMO_HOME'])
    print('START SUMO simulation')
    a = time.perf_counter()

    sumoCmd = [binary, '-c', str(root_sim / 'config.sumocfg')]
    # subprocess.run(sumoCmd, capture_output=True).stdout

    traci.start(sumoCmd)

    tls_list = traci.trafficlight.getIDList()
    tls_id = tls_list[0]
    logics = traci.trafficlight.getAllProgramLogics(tls_id)[0]
    print(logics)

    connection_df = get_connection_df(tls_id)
    print(connection_df)
    signal_plan_list = []
    step = 0
    while step < pars['tod_length_s']:
        traci.simulationStep()
        change_jam_density_new_vehicles()

        # if step % 100 == 0:
        #     change_stopbar()
        
        phase = traci.trafficlight.getPhase(tls_id)
        # state = traci.trafficlight.getRedYellowGreenState(tls_id)
        # phase_name = traci.trafficlight.getPhaseName(tls_id)
        program_id = None # traci.trafficlight.getProgram(tls_id)
        print(f"{step=}, {phase=}")
        signal_plan_list.append({'step': step, 'phase': phase})
        step += 1
    traci.close()

    signal_plan_df = pd.DataFrame(signal_plan_list)
    
    print(f'END SUMO simulation {np.round(time.perf_counter()-a,2)} s')


    print('START SUMO data export')
    xml2csv_path = str(sumo_path / 'tools' / 'xml'/ 'xml2csv.py')
    a = time.perf_counter()
    # subprocess.run(['python', xml2csv_path, str(root_sim / 'output' / f'output.xml')], 
    #                    capture_output=True).stdout
    # os.rename(str(root_sim / 'output' / 'output.csv'),
    #           str(root_sim / 'output' / f'{output_filename_no_ext}.csv'))
    subprocess.run(['python', xml2csv_path, str(root_sim / 'output' / 'output_fcd.xml')], 
                    capture_output=True).stdout
    # subprocess.run(['python', xml2csv_path, str(root_sim / network_filename)], 
    #                 capture_output=True).stdout
    print(f'END SUMO data export {np.round(time.perf_counter()-a,2)} s')

    return signal_plan_df, connection_df


def run_sumo_data_export(root_sim, network_filename, output_filename):
    output_filename_no_ext = output_filename.split('.')[0]
    sumo_path = Path(os.environ['SUMO_HOME'])
    # if 'SUMO_HOME' in os.environ:
    #     tools = os.path.join(str(sumo_path), 'tools')
    #     sys.path.append(tools)
    # else:
    #     sys.exit("please declare environment variable 'SUMO_HOME'")
    print('START SUMO data export')
    a = time.perf_counter()
    xml2csv_path = str(sumo_path / 'tools' / 'xml'/ 'xml2csv.py')
    # subprocess.run(['python', xml2csv_path, str(root_sim / 'output' / 'output.xml')], 
    #                 capture_output=True).stdout
    # os.rename(str(root_sim / 'output' / 'output.csv'),
    #           str(root_sim / 'output' / f'{output_filename_no_ext}.csv'))
    subprocess.run(['python', xml2csv_path, str(root_sim / 'output' / 'output_fcd.xml')], 
                    capture_output=True).stdout
    # subprocess.run(['python', xml2csv_path, str(root_sim / network_filename)], 
    #                 capture_output=True).stdout
    print(f'END SUMO data export {np.round(time.perf_counter()-a,2)} s')    


def create_config_dict(pars):
    root_sim = pars['root_sim']
    net_filename = pars['net_filename']
    route_filename = pars['route_filename']
    seed = pars['seed']
    tod_length_s = pars['tod_length_s']
    visualization_filename = pars['visualization_filename']
    det_filename = pars['det_filename']
    tls_filename = pars['tls_filename']

    config_dict = dict()
    config_dict['input'] = {'net-file': [{'value': f'{root_sim / net_filename}'}],
                            'route-files': [{'value': f'{root_sim / route_filename}'}]}
    if visualization_filename != None:
        config_dict['input']['gui-settings-file'] = [{'value': f'{root_sim / visualization_filename}'}]
    if det_filename != None:
        config_dict['input'].update({'additional-files': [{'value': f'{root_sim / det_filename}'}]})
    if tls_filename != None:
        config_dict['input'].update({'additional-files': [{'value': f'{root_sim / tls_filename}'}]})

    config_dict['output'] = {'fcd-output': [{'value': f'{root_sim / "output" / "output_fcd.xml"}'}],
                            'fcd-output.attributes': [{'value': 'lane,odometer'}],
                            'fcd-output.distance': [{'value': 'true'}],
                            'device.fcd.period': [{'value': '1'}]} # id,lane,pos,distance,
    # config_dict['output'] = {'netstate-dump': [{'value': f'{root_sim / "output" / "output.xml"}'}],
    #                         'netstate-dump.empty-edges': [{'value': "true"}]}
    config_dict['time'] = {'begin': [{'value': 0}],
                            'end': [{'value': tod_length_s}]}
    config_dict['random_state'] = {'seed': [{'value': seed}]}
    config_dict['processing'] = {'step-method.ballistic': [{'value': "true"}],
                                'ignore-junction-blocker': [{'value': f"{int(tod_length_s)}"}],
                                'collision.action': [{'value': 'warn'}],
                                'lateral-resolution': [{'value': '1.6'}],
                                'time-to-teleport': [{'value': '-1'}]}
    return config_dict


def create_config_file(pars):
    config_dict = create_config_dict(pars)
    config_xml = dict2xml(config_dict, 'configuration')
    config_xml = "<?xml version='1.0' encoding='utf8'?>" + config_xml
    text_file = open(pars['root_sim'] / "config.sumocfg", "w")
    text_file.write(config_xml)
    text_file.close()


def add_vType_to_dict(dict_):
    dict_['routes']['vType'] = [{'accel': "2.6",
                                'carFollowModel': "Krauss", 
                                'decel': "3.0",
                                'id': "type1",
                                'length': "5",
                                'maxSpeed': "55.55",
                                'sigma': "0.5",
                                'jmStoplineGap':"1.0"}]
    return dict_


def create_route_dict(root_sim, lambda_vph, tod_length_s, n_lanes):
    routes_dict = dict()
    routes_dict['routes'] = {}
    # create all vtypes
    routes_dict = add_vType_to_dict(routes_dict)
    # add a line per vtype
    routes_dict['routes']['flow'] = [{'begin': "0.00", 
                                    'end': tod_length_s, 
                                    'from': 'left0A0',
                                    'departLane': i,
                                    'id': f'flow_6_{i}',
                                    'probability': f'{lambda_vph/3600}',
                                    'to': 'A0right0',
                                    'type': 'type1'} for i in range(n_lanes)]
    return routes_dict


def create_flow_dict(lambda_vph, tod_length_s, n_lanes):
    routes_dict = dict()
    routes_dict['routes'] = {}
    routes_dict = add_vType_to_dict(routes_dict)
    routes_dict['routes']['flow'] = [{'begin': "0.00", 
                                        'end': tod_length_s, 
                                        'from': 'left0A0',
                                        'departLane': i,
                                        'id': f'flow_6_{i}',
                                        'probability': f'{lambda_vph/3600}',
                                        'to': 'A0right0',
                                        'type': 'type1'} for i in range(n_lanes)]
    return routes_dict


def create_trip_dict(ar_vec_dict):
    routes_dict = dict()
    routes_dict['routes'] = {}
    routes_dict = add_vType_to_dict(routes_dict)
    routes_dict['routes']['route'] = [ {'id': route_id, 'edges': ' '.join(route_id.split(sep='_'))} 
                                            for route_id in ar_vec_dict.keys()]
    routes_dict['routes']['vehicle'] = [{'id': route_id + '_' + lane + '_' + str(i), 
                                        'route': route_id, 
                                        'type': 'type1',
                                        'depart': np.round(depart,1),
                                        'departLane': str(lane)}
                                        for route_id in ar_vec_dict.keys() 
                                        for lane in ar_vec_dict[route_id].keys()
                                        for i, depart in enumerate(ar_vec_dict[route_id][lane])]
    routes_dict['routes']['vehicle'] = sorted(routes_dict['routes']['vehicle'], key= lambda k : k['depart'])                      
    return routes_dict


def delete_file_extension(filename):
    return filename.split('.')[0]


def compose_network_filename(p):
    return f"grid_{p['n_x']}_{p['n_y']}_{p['l_x']}_{p['l_y']}_{p['l_out']}_{p['n_lanes']}_{p['speed_ms']}.net.xml"


def compose_routes_filename(net_filename, lambda_vph, tod_length_s):
    net_filename_no_ext = delete_file_extension(net_filename)
    return f'routes_{net_filename_no_ext}_{lambda_vph}_{tod_length_s}.rou.xml'


def compose_output_filename(pars):
    net_filename_no_ext = delete_file_extension(pars['net_filename'])
    return f"output_{net_filename_no_ext}_{pars['tod_length_s']}_{pars['seed']}.xml"


def fix_xml_headers(xml):
    xml = xml.split('<objects>')[1]
    xml = xml.split('</objects>')[0]
    return "<?xml version='1.0' encoding='utf8'?>" + xml


def create_routes_xml(root_sim, net_filename, lambda_vph, tod_length_s, n_lanes):
    routes_dict = create_route_dict(root_sim, lambda_vph, tod_length_s, n_lanes)
    routes_xml = dict2xml(routes_dict, None)
    routes_xml = fix_xml_headers(routes_xml)
    net_filename_no_ext = delete_file_extension(net_filename)
    routes_filename = compose_routes_filename(net_filename_no_ext, lambda_vph, tod_length_s)
    text_file = open(root_sim / routes_filename, "w")
    text_file.write(routes_xml)
    text_file.close()


def create_trip_xml(trip_dict, root_sim, net_filename):
    routes_xml = dict2xml(trip_dict, None)
    routes_xml = fix_xml_headers(routes_xml)
    net_filename_no_ext = delete_file_extension(net_filename)
    routes_filename = net_filename_no_ext + ".rou.xml"
    # routes_filename = compose_routes_filename(net_filename_no_ext, lambda_vph, tod_length_s)
    text_file = open(root_sim / routes_filename, "w")
    text_file.write(routes_xml)
    text_file.close()


def extract_net_filename_from_config_file(config_file):
    root = ET.parse(config_file).getroot()
    net_filename = None
    for i, child in enumerate(root):
        if child.tag == 'input':
            for j, subchild in enumerate(root[i]):
                if subchild.tag == 'net-file':
                    net_filename = root[i][j].attrib['value']
    return net_filename.split('/')[-1]


# def SUMO_sim_loop(root_sim, net_geom_dict, n_lanes_vec, lambda_vph_vec, 
#                 tod_length_s_vec, seed_vec):
#     n_x, n_y, = net_geom_dict['n_x'], net_geom_dict['n_y']
#     l_x, l_y, l_out = net_geom_dict['l_x'], net_geom_dict['l_y'], net_geom_dict['l_out']
#     for n_lanes in n_lanes_vec:
#         create_SUMO_grid_network(root_sim, n_x, n_y, l_x, l_y, l_out, n_lanes)
#         net_filename = compose_network_filename(n_x, n_y, l_x, l_y, l_out, n_lanes)
#         for lambda_vph in lambda_vph_vec:
#             for tod_length_s in tod_length_s_vec:
#                 for seed in seed_vec:
#                     print(f'{n_lanes=}, {lambda_vph=}, {tod_length_s=}, {seed=}')
#                     create_routes_xml(root_sim, net_filename, lambda_vph, tod_length_s, n_lanes)
#                     routes_filename = compose_routes_filename(net_filename, lambda_vph, tod_length_s)
#                     create_config_file(root_sim, net_filename, routes_filename, seed, tod_length_s)
#                     output_filename = compose_output_filename(net_filename, lambda_vph, 
#                                                             tod_length_s, seed)
#                     run_sumo_simulation(root_sim, net_filename, output_filename)


def run_jtrrouter(pars: dict):
    listt = ['jtrrouter',
            f"--flow-files={pars['root_sim'] / pars['flow_filename']}", 
            f"--net-file={pars['root_sim'] / pars['net_filename']}",
            f"--begin={pars['begin']}",
            f"--end={pars['tod_length_s']}",
            '--turn-defaults=0,100,0',
            f"--seed={pars['seed']}",
            f"--output-file={pars['root_sim'] / pars['route_filename']}"]
    # listt.append('--accept-all-destinations')
    listt.append(f"--sink-edges={pars['sink_list']}")
    print(' '.join(listt))
    print(subprocess.check_output(listt))


def create_route_dict_corridor(vol_dict, tod_length_s: int, n_lanes: int):
    flows_dict = dict()
    flows_dict['routes'] = {}
    flows_dict = add_vType_to_dict(flows_dict)
    flow_list = []
    for node, vol in vol_dict.items():
        for lane in range(n_lanes):
            flow_list.append([{'begin': "0.00", 'end': str(tod_length_s), 'from': node,
                                'departLane': lane, 'id': f'{node}_{lane}',
                                'probability': f'{vol/3600/n_lanes}',
                                'type': 'type1'}])
    flows_dict['routes']['flow'] = flow_list
    return flows_dict


def create_flows_xml_corridor(root_sim: Path, flows_filename: str, vol_dict, tod_length_s, pars_net):
    flows_dict = create_route_dict_corridor(vol_dict, tod_length_s, pars_net['n_lanes'])
    flows_xml = dict2xml(flows_dict, None)
    flows_xml = fix_xml_headers(flows_xml)
    text_file = open(root_sim / flows_filename, "w")
    text_file.write(flows_xml)
    text_file.close()


def linear_interpolation(point_1_t, point_1_x, point_2_t, point_2_x, x_value):
    '''Interpolates the x dimension of two points'''
    slope = (point_2_x - point_1_x)/(point_2_t - point_1_t)
    if slope != 0:
        t_crossing = point_1_t + np.abs(x_value - point_1_x)/slope
    else:
        t_crossing = float(point_1_t)
    return t_crossing


def calc_dep_time(traj):
    sb_x = traj['stopbar_location']
    i = np.where(np.array(traj['points_distance']) < sb_x)[0][-1]
    # print(traj['points_distance'],i)
    t_1 = traj['points_timestamp'][i]
    x_1 = traj['points_distance'][i]
    t_2 = traj['points_timestamp'][i+1]
    x_2 = traj['points_distance'][i+1]
    traj['dep_time'] = linear_interpolation(t_1, x_1, t_2, x_2, sb_x)
    return traj


def add_stop_info_SUMO(traj):
    dt = 1
    speed_thres = 0.1  # 0.6 originally
    t_i = traj['points_timestamp'][1:]  # to avoid error of initial stop
    x_i = traj['points_distance'][1:]   # to avoid error of initial stop
    asign = np.sign(np.diff(x_i) - speed_thres*dt)
    signchange = ((np.roll(asign,1) - asign) != 0).astype(int)
    signchange_loc_vec = np.where(signchange == 1)[0]
    stop_dict_list = []
    total_stop_delay = 0
    if len(signchange_loc_vec) >= 2:
        for j in range(0,len(signchange_loc_vec),2):
            a = signchange_loc_vec[j]
            b = signchange_loc_vec[j+1]
            t_stop = t_i[a]
            x_stop = x_i[a]
            stop_dict_list.append({'movement_id': traj['movement_id'],
                                    'driver_id': traj['driver_id'],
                                    'distance': x_stop,
                                    'timestamp': t_stop,
                                    'timestamp_shockwave': t_i[b],
                                    'stop_delay': t_i[b] - t_i[a]})
            assert(t_i[b] - t_i[a] >= 0)
            total_stop_delay += t_i[b] - t_i[a]
    traj['stop_dict_list'] = stop_dict_list
    traj['n_stops'] = len(stop_dict_list)
    traj['total_delay'] = total_stop_delay
    return traj


def traj_is_complete(traj):
    '''ALERT: Easy, simple version.'''
    traj['is_complete'] = 1 if max(traj['points_distance']) > traj['stopbar_location'] else 0
    return traj


def create_stop_df(series: pd.Series):
    return pd.DataFrame(series.explode().dropna().to_list())


def merge_stops(traj, stop_pos_threshold=16):
    if traj['n_stops'] > 1:
        print('before', traj['stop_dict_list'])
        pos_list = [stop['distance'] for stop in traj['stop_dict_list']]  # traj['pos_list']
        delay_list = [stop['stop_delay'] for stop in traj['stop_dict_list']]  # traj['delay_list']
        start_list = [stop['timestamp'] for stop in traj['stop_dict_list']]  # traj['start_list']
        new_pos_list, new_start_list, new_delay_list = [], [], []
        for i in range(traj['n_stops']-1):
            if np.abs(pos_list[i] - pos_list[i+1]) < stop_pos_threshold:
                delay_list[i+1] += (start_list[i+1] - start_list[i])
                start_list[i+1] = start_list[i]
            else:
                new_pos_list.append(pos_list[i])
                new_delay_list.append(delay_list[i])
                new_start_list.append(start_list[i])
            if i == traj['n_stops'] - 2:
                new_pos_list.append(pos_list[-1])
                new_delay_list.append(delay_list[-1])
                new_start_list.append(start_list[-1])
        traj['stop_dict_list'] = [{'movement_id': traj['movement_id'], 
                                    'driver_id': traj['driver_id'],
                                    'distance': dist,
                                    'timestamp': time_,
                                    'stop_delay': delay,
                                    'timestamp_shockwave': time_ + delay}
                                    for dist, time_, delay in zip(new_pos_list,
                                                                new_start_list,
                                                                new_delay_list)]
        print('after', traj['stop_dict_list'])
        traj['n_stops'] = len(new_pos_list)
        traj['total_delay'] = np.sum(new_delay_list)
        assert(len(new_pos_list) > 0)
    return traj


def add_is_observed(traj_df, pen_rate = 0.1):
    traj_df['is_observed'] = np.random.choice([0,1], p = [1 - pen_rate, pen_rate], 
                                                size = traj_df.shape[0])
    return traj_df

#%% Main
if __name__ == '__main__': 
    
    root_sim = Path(r'C:\Users\roger\OneDrive\RESEARCH\SUMO_simulation\isolated')
    
    # # loop over instances
    # net_geom_dict = {'n_x': 1, 'n_y': 1, 'l_x': 300, 'l_y': 300, 'l_out': 800}
    # seed_vec = list(range(5))
    # lambda_vph_vec = [300, 500, 700]
    # tod_length_s_vec = [1800, 3600]
    # n_lanes_vec = [1,3]
    # SUMO_sim_loop(root_sim, net_geom_dict, n_lanes_vec, lambda_vph_vec, 
    #                 tod_length_s_vec, seed_vec)

    flow_filename = 'corridor_flows.rou.xml'
    route_filename = 'corridor.rou.xml'
    det_filename = 'detectors.xml'

    seed = 5
    tod_length_s = 7200

    pars_net = dict()
    pars_net['n_x'] = 1
    pars_net['n_y'] = 1
    pars_net['l_x'] = 300
    pars_net['l_y'] = 300
    pars_net['l_out'] = 300
    pars_net['n_lanes'] = 2
    pars_net['speed_ms'] = 10
    pars_net['cycle_time'] = 100
    pars_net['tls_type'] = 'actuated'
    # pars_net['half_offset_list'] = ','.join(['B0','D0'])
    # pars_net['remove_edges_list'] = ','.join(['top0A0', 'A0bottom0'])
    vol_dict = {'left0A0': 900, 'right0A0': 200, 'bottom0A0': 500, 'top0A0': 200}

    create_SUMO_grid_network(root_sim, pars_net, '')
    net_filename = compose_network_filename(pars_net)
    
    create_flows_xml_corridor(root_sim, flow_filename, vol_dict, tod_length_s, pars_net)
    # routes_filename = compose_routes_filename(net_filename, lambda_vph, tod_length_s)
    
    pars = dict()
    pars['root_sim'] = root_sim
    pars['net_filename'] = net_filename
    pars['flow_filename'] = flow_filename
    pars['route_filename'] = route_filename
    pars['visualization_filename'] = None
    pars['det_filename'] = None
    pars['tls_filename'] = None
    pars['begin'] = 0
    pars['tod_length_s'] = tod_length_s
    pars['seed'] = seed
    pars['sink_list'] = ','.join(['A0right0', 'A0left0', 'A0top0', 'A0bottom0'])
    run_jtrrouter(pars)
    
    create_config_file(pars)
    output_filename = compose_output_filename(pars)
    binary = 'sumo'
    # binary = 'sumo-gui'
    signal_plan_df = run_sumo_simulation(pars, binary)

    csv_path = root_sim / 'output' / 'output_fcd.csv'

    df = pd.read_csv(csv_path, sep=';')
    df = df.dropna()
    df['timestep_time'] = df['timestep_time'].astype(int)
    df['edge'] = df['vehicle_lane'].apply(lambda x: x if x[0] == ':' else x.split('_')[0])
    df['lane'] = df['vehicle_lane'].apply(lambda x: x if x[0] == ':' else x.split('_')[1])

    # transform into traj_df normal format? basically to enjoy the plotting functions
    # easy way... filter by sink edges and keep working.

    df['first_edge'] = df.groupby(['vehicle_id'])['edge'].transform('first')
    df['last_edge'] = df.groupby(['vehicle_id'])['edge'].transform('last')
    df = df[~df.edge.isin(pars['sink_list'].split(','))]
    df['movement_id'] = df['first_edge'] + '_' + df['last_edge']
    
    distance_key = 'vehicle_odometer' # it used to be vehicle_distance
    timestep_df = df.groupby('vehicle_id')['timestep_time'].apply(list)
    distance_df = df.groupby('vehicle_id')[distance_key].apply(list)  
    movement_df = df.groupby('vehicle_id')['movement_id'].first()
    
    traj_df = pd.concat([timestep_df, distance_df, movement_df], axis = 1).reset_index()
    traj_df = traj_df.rename(columns = {'vehicle_id': 'driver_id', 
                                        'timestep_time': 'points_timestamp',
                                        distance_key: 'points_distance'})

    traj_df['stopbar_location'] = 289  # hard-coded, please check manually

    traj_df = traj_df.apply(lambda x : traj_is_complete(x), axis = 1)
    traj_df = traj_df[traj_df.is_complete == 1]
    traj_df = traj_df.apply(lambda x : calc_dep_time(x), axis = 1)
    traj_df = traj_df.apply(add_stop_info_SUMO, axis = 1)
    traj_df['first_timestamp'] = traj_df['points_timestamp'].str[0]
    traj_df = traj_df.sort_values(by='first_timestamp')
    traj_df = traj_df.apply(lambda x : merge_stops(x), axis=1)
    traj_df['stopping_distance'] = 10
    traj_df['stop_dict_list_last'] = traj_df['stop_dict_list'].apply(lambda x : [x[-1]] if len(x)> 0 else [])
    traj_df['freeflow_speed'] = 10
    pen_rate = 0.1
    traj_df['is_observed'] = np.random.choice([0,1], p = [1 - pen_rate, pen_rate], size = traj_df.shape[0])


    stop_df = create_stop_df(traj_df['stop_dict_list_last']).dropna()
    # stop_df = stop_df.groupby('driver_id').last().reset_index()
