"""
Created on 2022/04/08
@author: Roger Lloret-Batlle (rogerm8@gmail.com)
"""

#%%
import sys
from pathlib import Path

import numpy as np
import pandas as pd
%matplotlib qt

from dict2xml import dict2xml

sys.path.append(str(Path.cwd().parent / 'data_processing' / 'inference'))

import traj_plotting as tp
import shockwave as sw
from input_SUMO_functions import *


#%%

def create_green_interval(traj):
    if traj['n_stops'] == 0:
        traj['green_interval'] = [traj['dep_time'] - traj['stopping_distance']/traj['freeflow_speed'], 
                                    traj['dep_time']]
    else:
        t_sw = traj['stop_dict_list_last'][0]['timestamp_shockwave']
        x_sw = traj['stop_dict_list_last'][0]['distance']
        traj['green_interval'] = [t_sw + np.abs(x_sw - traj['stopbar_location'])/traj['shockwave_speed'], 
                                    traj['dep_time']]
    return traj


def create_red_interval(traj):
    if traj['n_stops'] > 0:
        t_sw = traj['stop_dict_list_last'][0]['timestamp_shockwave']
        x_sw = traj['stop_dict_list_last'][0]['distance']
        delay = traj['stop_dict_list_last'][0]['stop_delay']
        b = t_sw + np.abs(x_sw - traj['stopbar_location'])/traj['shockwave_speed']
        a = b - delay
        traj['red_interval'] = [a, b]
    else:
        traj['red_interval'] = []
    return traj


def create_traj_df(output_csv_path):
    df = pd.read_csv(output_csv_path, sep=';')
    df = df.dropna()
    df['timestep_time'] = df['timestep_time'].astype(int)
    df['edge'] = df['vehicle_lane'].apply(lambda x: x if x[0] == ':' else x.split('_')[0])
    df['lane'] = df['vehicle_lane'].apply(lambda x: x if x[0] == ':' else x.split('_')[1])
    df['first_edge'] = df.groupby(['vehicle_id'])['edge'].transform('first')
    df['last_edge'] = df.groupby(['vehicle_id'])['edge'].transform('last')
    df = df[~df.edge.isin(pars['sink_list'].split(','))]
    df['movement_id'] = df['first_edge'] + '_' + df['last_edge']

    timestep_df = df.groupby('vehicle_id')['timestep_time'].apply(list)
    distance_df = df.groupby('vehicle_id')['vehicle_odometer'].apply(list)
    movement_df = df.groupby('vehicle_id')['movement_id'].first()
    
    traj_df = pd.concat([timestep_df, distance_df, movement_df], axis = 1).reset_index()
    traj_df = traj_df.rename(columns = {'vehicle_id': 'driver_id', 
                                        'timestep_time': 'points_timestamp',
                                        'vehicle_odometer': 'points_distance'})

    traj_df['stopbar_location'] = 289

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
    return traj_df


def add_is_observed(traj_df, pen_rate = 0.1):
    traj_df['is_observed'] = np.random.choice([0,1], p = [1 - pen_rate, pen_rate], 
                                                size = traj_df.shape[0])
    return traj_df


#%% Main
if __name__ == '__main__':
    
    root_sim = Path('c:/RESEARCH/SUMO_simulation/corridor')

    seed = 5
    tod_length_s = 3600

    pars_net = dict()
    pars_net['n_x'] = 1
    pars_net['n_y'] = 1
    pars_net['l_x'] = 300
    pars_net['l_y'] = 300
    pars_net['l_out'] = 300
    pars_net['n_lanes'] = 2
    pars_net['speed_ms'] = 10
    pars_net['cycle_time'] = 100
    pars_net['tls_type'] = 'static'  # static, actuated, delay_based | nema
    # pars_net['half_offset_list'] = ','.join(['B0','D0'])
    
    # pars_net['remove_edges_list'] = ','.join(['top0A0', 'A0bottom0'])
    vol_dict = {'left0A0': 1800, 'right0A0': 200, 'bottom0A0': 500, 'top0A0': 200}

    flow_filename = 'corridor_flows.rou.xml'
    route_filename = 'corridor.rou.xml'
    det_filename = 'detectors.xml'

    create_SUMO_grid_network(root_sim, pars_net, '')
    net_filename = compose_network_filename(pars_net)
    
    create_flows_xml_corridor(root_sim, flow_filename, vol_dict, tod_length_s, pars_net)
    # routes_filename = compose_routes_filename(net_filename, lambda_vph, tod_length_s)
    
    pars = dict()
    pars['root_sim'] = root_sim
    pars['net_filename'] = net_filename
    pars['flow_filename'] = flow_filename
    pars['route_filename'] = route_filename
    pars['begin'] = 0
    pars['tod_length_s'] = tod_length_s
    pars['seed'] = seed
    pars['sink_list'] = ','.join(['A0right0', 'A0left0', 'A0top0', 'A0bottom0'])
    pars['output_filename'] = compose_output_filename(pars)
    pars['det_filename'] = None
    pars['visualization_filename'] = None
    pars['tls_filename'] = "reservice.add.xml" #None  # "reservice.add.xml"  # 'nema.add.xml'
    
    run_jtrrouter(pars)
    create_config_file(pars)
    binary = 'sumo'
    # binary = 'sumo-gui'
    signal_plan_df, connection_df = run_sumo_simulation(pars, binary)

    ######
    output_csv_path = root_sim / 'output' / 'output_fcd.csv'
    traj_df = create_traj_df(output_csv_path)

    pen_rate = 0.1
    traj_df = add_is_observed(traj_df, pen_rate)

    stop_df = create_stop_df(traj_df['stop_dict_list_last']).dropna()
    # stop_df = stop_df.groupby('driver_id').last().reset_index()

    cols = ['distance', 'timestamp_shockwave','movement_id']
    shockwave_df = stop_df[cols].groupby('movement_id')[['distance', 'timestamp_shockwave']].diff()
    shockwave_df['movement_id'] = stop_df['movement_id']
    mov_list = ['left0A0_A0right0', 'bottom0A0_A0top0', 'top0A0_A0bottom0', 'right0A0_A0left0']
    shockwave_df = shockwave_df[shockwave_df.movement_id.isin(mov_list)]
    shockwave_df  = calc_shockwave_loop(shockwave_df)

    sw_dict = shockwave_df.groupby('movement_id')['shockwave_speed'].first().to_dict()
    traj_df['shockwave_speed'] = traj_df['movement_id'].map(sw_dict)
    
    traj_df = traj_df.apply(create_green_interval, axis = 1)
    traj_df = traj_df.apply(create_red_interval, axis = 1)
    cond_mov = traj_df.movement_id == 'left0A0_A0right0'
    traj_df_sel = traj_df[cond_mov].sort_values(by='first_timestamp')


    # tp.plot_traj_all_features(traj_df_sel[traj_df_sel.is_observed == 1])
    # tp.plot_intervals(traj_df_sel[traj_df_sel.is_observed == 1])

    tp.plot_relative_shockwave(shockwave_df)
    for mov_id in mov_list:
        cond = shockwave_df.movement_id == mov_id
        tp.plot_shockwave_line(shockwave_df['shockwave_speed'][cond].iloc[0], movement_id=mov_id)
    plt.legend()


    df = shockwave_df[shockwave_df.movement_id == 'left0A0_A0right0'].dropna()

    def calc_rel_shockwave_constrained(df):
        '''Solved by enumeration'''
        res_dict_list = []
        for alpha_cap in range(0, 30, 1):
            for beta in range(0, 15, 0.1):
                # filter data by region
                df = df[df.]
                # do LR
                # calc R^2
                # r2 =
                # store result
                res_dict_list.append({'alpha_cap': alpha_cap, 'beta': beta, 'r2': r2})
        # get best
