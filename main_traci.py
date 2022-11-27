#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 15 17:41:32 2020
@author: Roger Lloret-Batlle rogerm8@gmail.com
"""

#%%
from pathlib import Path
import time
import os, sys

import numpy as np
import pandas as pd

# path_data_processing = '/Users/lloretroger/Documents/data_processing'
# sys.path.insert(0, path_data_processing)
# sys.path.insert(0, path_data_processing + '/gen-py')


if 'SUMO_HOME' in os.environ:
    tools = os.path.join(os.environ['SUMO_HOME'], 'tools')
    sys.path.append(tools)
else:
    sys.exit("please declare environment variable 'SUMO_HOME'")

import traci









binary = 'sumo'
# binary = 'sumo-gui'
sumoBinary = f"C:\Program Files (x86)\Eclipse\Sumo\bin\{binary}"
path_SUMO_simulation_root = Path.cwd()
project_name = 'single_junction'
root_sim = Path(str(path_SUMO_simulation_root / project_name))
config_file = str(root_sim / "config.sumocfg")
sumoCmd = [sumoBinary, "-c", config_file]
traci.start(sumoCmd)
