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
num_bars = 4
num_subfigs = 2
sysname = 'Ditto'

ours = np.zeros((num_subfigs, num_queries))
group = np.zeros((num_subfigs, num_queries))
elas = np.zeros((num_subfigs, num_queries))
naive = np.zeros((num_subfigs, num_queries))

nimble_res = {1:{}, 16:{}, 94:{}, 95:{}}
ditto_res = {1:{}, 16:{}, 94:{}, 95:{}}
group_res = {1:{}, 16:{}, 94:{}, 95:{}}
dop_res = {1:{}, 16:{}, 94:{}, 95:{}}
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

    with open('../results/q{}_s3_new_greedy.log'.format(i), 'r') as f:
        s = f.readline()
        while s:
            tmps = s.strip().split(' ')
            if tmps[0] == 'Resource:':
                k = tmps[1]
                group_res[i][k] = {'jct': 0, 'cost': 0}
            elif tmps[0] == 'JCT:':
                group_res[i][k]['jct'] = float(tmps[1]) / 1000
            elif tmps[0] == 'Cost:':
                group_res[i][k]['cost'] = float(tmps[1])
            s = f.readline()

    with open('../results/q{}_s3_elastic_nimble_jct.log'.format(i), 'r') as f:
        s = f.readline()
        while s:
            tmps = s.strip().split(' ')
            if tmps[0] == 'Resource:':
                k = tmps[1]
                dop_res[i][k] = {'jct': 0, 'cost': 0}
            elif tmps[0] == 'Sched_time:':
                sched_time.append(float(tmps[1]))
            elif tmps[0] == 'JCT:':
                dop_res[i][k]['jct'] = float(tmps[1]) / 1000
            s = f.readline()
    with open('../results/q{}_s3_elastic_nimble_cost.log'.format(i), 'r') as f:
        s = f.readline()
        while s:
            tmps = s.strip().split(' ')
            if tmps[0] == 'Resource:':
                k = tmps[1]
            elif tmps[0] == 'Sched_time:':
                sched_time.append(float(tmps[1]))
            elif tmps[0] == 'Cost:':
                dop_res[i][k]['cost'] = float(tmps[1])
            s = f.readline()

ours[0][0] = ditto_res[1]['zipf-0.9']['jct']
ours[0][1] = ditto_res[16]['zipf-0.9']['jct']
ours[0][2] = ditto_res[94]['zipf-0.9']['jct']
ours[0][3] = ditto_res[95]['zipf-0.9']['jct']
ours[1][0] = ditto_res[1]['zipf-0.9']['cost']
ours[1][1] = ditto_res[16]['zipf-0.9']['cost']
ours[1][2] = ditto_res[94]['zipf-0.9']['cost']
ours[1][3] = ditto_res[95]['zipf-0.9']['cost']

group[0][0] = group_res[1]['zipf-0.9']['jct']
group[0][1] = group_res[16]['zipf-0.9']['jct']
group[0][2] = group_res[94]['zipf-0.9']['jct']
group[0][3] = group_res[95]['zipf-0.9']['jct']
group[1][0] = group_res[1]['zipf-0.9']['cost']
group[1][1] = group_res[16]['zipf-0.9']['cost']
group[1][2] = group_res[94]['zipf-0.9']['cost']
group[1][3] = group_res[95]['zipf-0.9']['cost']

elas[0][0] = dop_res[1]['zipf-0.9']['jct']
elas[0][1] = dop_res[16]['zipf-0.9']['jct']
elas[0][2] = dop_res[94]['zipf-0.9']['jct']
elas[0][3] = dop_res[95]['zipf-0.9']['jct']
elas[1][0] = dop_res[1]['zipf-0.9']['cost']
elas[1][1] = dop_res[16]['zipf-0.9']['cost']
elas[1][2] = dop_res[94]['zipf-0.9']['cost']
elas[1][3] = dop_res[95]['zipf-0.9']['cost']

naive[0][0] = nimble_res[1]['zipf-0.9']['jct']
naive[0][1] = nimble_res[16]['zipf-0.9']['jct']
naive[0][2] = nimble_res[94]['zipf-0.9']['jct']
naive[0][3] = nimble_res[95]['zipf-0.9']['jct']
naive[1][0] = nimble_res[1]['zipf-0.9']['cost']
naive[1][1] = nimble_res[16]['zipf-0.9']['cost']
naive[1][2] = nimble_res[94]['zipf-0.9']['cost']
naive[1][3] = nimble_res[95]['zipf-0.9']['cost']


# Our paper data
# ours = [[222.26, 567.3, 272.38, 267.7],[2644.9, 130691, 31785.7, 36265.9]]
# group = [[254.55, 769.24, 298.85, 286.31],[3161.41, 159428, 47341.3, 49591.1]]
# elas = [[262.13, 695.5, 305.6, 308.5],[2946.2, 153310, 42894.8, 41300]]
# naive = [[279.75, 824.9, 459.83, 403.4],[3624.17, 181062, 48885.8, 55201.9]]

# ours = np.array(ours)
# group = np.array(group)
# elas = np.array(elas)
# naive = np.array(naive)

# normalize by elastic
for i in range(1, num_subfigs):
    for j in range(num_queries):
        group[i][j] = group[i][j] /ours[i][j]
        elas[i][j] = elas[i][j] / ours[i][j]
        naive[i][j] = naive[i][j] / ours[i][j]
        ours[i][j] = 1
        
# Set font and figure size
font_size = 30
plt.rc('font',**{'size': font_size})
plt.rc('pdf',fonttype = 42)
fig_size = (18, 4.6)
fig, axes = plt.subplots(nrows=1, ncols=num_subfigs, sharey=False, figsize=fig_size)
matplotlib.rcParams['xtick.minor.size'] = 4.
matplotlib.rcParams['xtick.major.size'] = 8.
matplotlib.rcParams['ytick.major.size'] = 6.
matplotlib.rcParams['ytick.minor.visible'] = True
plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.2, hspace=None)

# settings
colors = {0: 'steelblue', 1: '#FF8247', 2: '#00CD66', 3: '#EE2C2C'}
labels = {0: 'NIMBLE', 1: 'NIMBLE+Group', 2: 'NIMBLE+DoP', 3: sysname}

# indexes for x
width = 0.04
indexes = [[0.2*i+0.1+width*j for i in range(num_queries)] for j in range(num_bars)]

# y-axis setting
y_ticks = [[i*10 for i in range(0, 10)], [i*0.5 for i in range(0, 5)]]
if scale == 1000:
    y_ticks = [[i*200 for i in range(0, 6)], [i*0.5 for i in range(0, 5)]]
y_label = ['JCT (s)', 'Normalized Cost']
x_ticks = []
for i in range(num_queries):
    x_ticks.append(sum(indexes[j][i] for j in range(num_bars))/num_bars)
x_ticklabels = ['Q1', 'Q16', 'Q94', 'Q95']

# Plot x ticks and label
for j in range(num_subfigs):
    axes[j].set_xticks(x_ticks)
    axes[j].set_xticklabels(x_ticklabels)
    axes[j].get_xaxis().set_tick_params(direction='in', pad=7)

# Plot y ticks and label
for j in range(num_subfigs):
    axes[j].set_ylabel(y_label[j])
    if j == 0:
        axes[j].set_ylim(bottom=0, top=90)
        if scale == 1000:
            axes[j].set_ylim(bottom=0, top=1000)
    else:
        axes[j].set_ylim(bottom=0, top=2)
    axes[j].set_yticks(y_ticks[j])
    axes[j].tick_params(bottom=False, top=False, left=False, right=False)
    axes[j].minorticks_off()
    axes[j].yaxis.set_ticks_position('left')
    axes[j].get_yaxis().set_tick_params(direction='in', pad=4)
   
bars = [[] for i in range(num_bars)]
# Plot bars
for j in range(num_subfigs):
    if j == 0:
        bars[0] = axes[j].bar(indexes[0], naive[0], width, label=labels[0], edgecolor='white', zorder=3, color=colors[0])
        bars[1] = axes[j].bar(indexes[1], group[0], width, label=labels[1], edgecolor='white', zorder=3, color=colors[1])
        bars[2] = axes[j].bar(indexes[2], elas[0], width, label=labels[2], edgecolor='white', zorder=3, color=colors[2])
        bars[3] = axes[j].bar(indexes[3], ours[0], width, label=labels[3], edgecolor='white', zorder=3, color=colors[3])
    elif j == 1:
        bars[0] = axes[j].bar(indexes[0], naive[1], width, label=labels[0], edgecolor='white', zorder=3, color=colors[0])
        bars[1] = axes[j].bar(indexes[1], group[1], width, label=labels[1], edgecolor='white', zorder=3, color=colors[1])
        bars[2] = axes[j].bar(indexes[2], elas[1], width, label=labels[2], edgecolor='white', zorder=3, color=colors[2])
        bars[3] = axes[j].bar(indexes[3], ours[1], width, label=labels[3], edgecolor='white', zorder=3, color=colors[3])

# Plot legend
fig.legend(handles=[bars[0], bars[1], bars[2], bars[3]], handlelength=1.7, 
           ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1.1), frameon=False, prop={'size':font_size}, columnspacing = 1)

# Plot grid
for j in range(num_subfigs):
    axes[j].grid(axis='y', linestyle='--')

# Save the figure
file_path = '../results/fig12.png'
plt.savefig(file_path, bbox_inches='tight')