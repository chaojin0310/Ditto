import os
import sys
import numpy as np
import matplotlib
import matplotlib.pyplot as plt


scale = 10  # default 10 for 10G, 1000 for 1T
with open('./scale.txt', 'r') as f:
    scale = int(f.readline())

# constants
num_queries = 4
num_bars = 2
num_subfigs = 3
sysname = 'Ditto'

use_norm = 1

elastic = np.zeros((num_subfigs, num_queries))
nimble = np.zeros((num_subfigs, num_queries))

nimble_res = {1:{}, 16:{}, 94:{}, 95:{}}
ditto_res = {1:{}, 16:{}, 94:{}, 95:{}}
sched_time = []
k = ''
# read data
for i in [1, 16, 94, 95]:
    with open('../results/q{}_s3_new_nimble.log'.format(i), 'r') as f:
        s = f.readline()
        while s:
            tmps = s.strip().split(' ')
            if tmps[0] == 'Resource:':
                k = tmps[1]
                nimble_res[i][k] = {'jct': 0, 'cost': 0}
            elif tmps[0] == 'JCT:':
                nimble_res[i][k]['jct'] = float(tmps[1]) / 1000
            elif tmps[0] == 'Cost:':
                nimble_res[i][k]['cost'] = float(tmps[1])
            s = f.readline()
    with open('../results/q{}_s3_elastic_jct.log'.format(i), 'r') as f:
        s = f.readline()
        while s:
            tmps = s.strip().split(' ')
            if tmps[0] == 'Resource:':
                k = tmps[1]
                ditto_res[i][k] = {'jct': 0, 'cost': 0}
            elif tmps[0] == 'Sched_time:':
                sched_time.append(float(tmps[1]))
            elif tmps[0] == 'JCT:':
                ditto_res[i][k]['jct'] = float(tmps[1]) / 1000
            s = f.readline()
    with open('../results/q{}_s3_elastic_cost.log'.format(i), 'r') as f:
        s = f.readline()
        while s:
            tmps = s.strip().split(' ')
            if tmps[0] == 'Resource:':
                k = tmps[1]
            elif tmps[0] == 'Sched_time:':
                sched_time.append(float(tmps[1]))
            elif tmps[0] == 'Cost:':
                ditto_res[i][k]['cost'] = float(tmps[1])
            s = f.readline()

elastic[0][0] = ditto_res[1]['zipf-0.9']['cost']
elastic[0][1] = ditto_res[16]['zipf-0.9']['cost']
elastic[0][2] = ditto_res[94]['zipf-0.9']['cost']
elastic[0][3] = ditto_res[95]['zipf-0.9']['cost']
elastic[1][0] = ditto_res[95]['uniform-1']['cost']
elastic[1][1] = ditto_res[95]['uniform-75']['cost']
elastic[1][2] = ditto_res[95]['uniform-50']['cost']
elastic[1][3] = ditto_res[95]['uniform-25']['cost']
elastic[2][0] = ditto_res[95]['norm-1.0']['cost']
elastic[2][1] = ditto_res[95]['norm-0.8']['cost']
elastic[2][2] = ditto_res[95]['zipf-0.9']['cost']
elastic[2][3] = ditto_res[95]['zipf-0.99']['cost']

nimble[0][0] = nimble_res[1]['zipf-0.9']['cost']
nimble[0][1] = nimble_res[16]['zipf-0.9']['cost']
nimble[0][2] = nimble_res[94]['zipf-0.9']['cost']
nimble[0][3] = nimble_res[95]['zipf-0.9']['cost']
nimble[1][0] = nimble_res[95]['uniform-1']['cost']
nimble[1][1] = nimble_res[95]['uniform-75']['cost']
nimble[1][2] = nimble_res[95]['uniform-50']['cost']
nimble[1][3] = nimble_res[95]['uniform-25']['cost']
nimble[2][0] = nimble_res[95]['norm-1.0']['cost']
nimble[2][1] = nimble_res[95]['norm-0.8']['cost']
nimble[2][2] = nimble_res[95]['zipf-0.9']['cost']
nimble[2][3] = nimble_res[95]['zipf-0.99']['cost']

# Our paper data
# elastic = [[2708.5, 130691, 31785.7, 36265.9], [26300.7, 26813.9, 32842.5, 70611.8], [35110.6, 33785.3, 36265.9, 34819.3]]
# nimble = [[3624.17, 181062, 48885.8, 55201.9], [33329.2, 44694.2, 40056.8, 88193.1], [40806.1, 43461.7, 55201.9, 51612.8]]
# elastic = np.array(elastic)
# nimble = np.array(nimble)

# normalize by elastic
for i in range(num_subfigs):
    for j in range(num_queries):
        nimble[i][j] = nimble[i][j] / elastic[i][j]
        elastic[i][j] = 1

# Set font and figure size
font_size = 32
plt.rc('font',**{'size': font_size})
plt.rc('pdf',fonttype = 42)
fig_size = (36, 4.6)
fig, axes = plt.subplots(nrows=1, ncols=num_subfigs, sharey=False, figsize=fig_size)
matplotlib.rcParams['xtick.minor.size'] = 4.
matplotlib.rcParams['xtick.major.size'] = 8.
matplotlib.rcParams['ytick.major.size'] = 6.
matplotlib.rcParams['ytick.minor.visible'] = True
plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.14, hspace=None)

# settings
colors = {0: 'black', 1: '#EE2C2C', 2: None, 3: 'steelblue'}
edge_color = 'white'
labels = {0: 'Lower Bound', 1: sysname, 2: 'NIMBLE+BestPlace', 3: 'NIMBLE'}

# indexes for x
width = 0.06
indexes = [[0.2*i+0.1+width*j for i in range(num_queries)] for j in range(num_bars)]

# axis setting
y_ticks = [i*0.5 for i in range(0, 5)]
y_label = ['Normalized Cost']
x_ticks = []
for i in range(num_queries):
    x_ticks.append(sum(indexes[j][i] for j in range(num_bars))/num_bars)
x_ticklabels = [['Q1', 'Q16', 'Q94', 'Q95'], 
    ['100%', '75%', '50%', '25%'], 
    ['Norm-1.0', 'Norm-0.8', 'Zipf-0.9', 'Zipf-0.99']]

# Plot x ticks and label
for j in range(num_subfigs):
    axes[j].set_xticks(x_ticks)
    axes[j].set_xticklabels(x_ticklabels[j])
    axes[j].get_xaxis().set_tick_params(direction='in', pad=7)

# Plot y ticks and label
for j in range(num_subfigs):
    if j == 0:
        axes[0].set_ylabel(y_label[0])
    axes[j].set_ylim(bottom=0, top=2)
    axes[j].set_yticks(y_ticks)
    axes[j].tick_params(bottom=False, top=False, left=False, right=False)
    if use_norm:
        axes[j].minorticks_off()
    axes[j].yaxis.set_ticks_position('left')
    axes[j].get_yaxis().set_tick_params(direction='in', pad=4)
    if not use_norm:
        axes[j].get_yaxis().set_tick_params(which='minor', direction='in')
   
bars = [[] for i in range(num_bars)]
# Plot bars
for j in range(num_subfigs):
    bars[0] = axes[j].bar(indexes[0], elastic[j], width, label=labels[1], edgecolor=edge_color, zorder=3, color=colors[1])
    bars[1] = axes[j].bar(indexes[1], nimble[j], width, label=labels[3], edgecolor=edge_color, zorder=3, color=colors[2])

# Plot legend
fig.legend(handles=[bars[0], bars[1]], handlelength=1.8, 
           ncol=2, loc='upper center', bbox_to_anchor=(0.5, 1.1), frameon=False, prop={'size':font_size}, columnspacing = 1)

# Plot grid
for j in range(num_subfigs):
    axes[j].grid(axis='y', linestyle='--')

# Save the figure
file_path = '../results/fig9.png'
plt.savefig(file_path, bbox_inches='tight')