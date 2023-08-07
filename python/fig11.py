import os
import sys
import re
import numpy as np
import matplotlib
import matplotlib.pyplot as plt


scale = 10  # default 10 for 10G, 1000 for 1T
with open('./scale.txt', 'r') as f:
    scale = int(f.readline())

# constants
num_queries = 4
num_lines = 2
num_subfigs = 4
sysname = 'Ditto'

# Read data
data_x = np.array([2, 4, 6, 12])
if scale == 1000:
    data_x = np.array([10, 20, 30, 60, 120])
data_y = np.zeros((num_subfigs, num_lines, len(data_x)))
model_a = np.zeros((num_subfigs, num_lines))
model_b = np.zeros((num_subfigs, num_lines))

q = {1:0, 16:1, 94:2, 95:3}
qid = 0
idx = 0  # subfig index
l = 0  # line index

fit_times = {1:0, 16:0, 94:0, 95:0}

with open('../results/effect_perf.log', 'r') as f:
    s = f.readline()
    while s:
        tmps = s.strip().split(' ')
        if tmps[0] == 'Query':
            qid = int(tmps[1])
            idx = q[qid]
        elif tmps[0] == 'IO':
            l = 0
        elif tmps[0] == 'Comp':
            l = 1
        elif tmps[0] == 'Time':
            fit_times[qid] = float(tmps[1])
        
        if tmps[1] == 'a:':
            model_a[idx][l] = float(tmps[2])
            model_b[idx][l] = float(tmps[3])
        elif tmps[1] == 'actuals:':
            for i in range(len(data_x)):
                data_y[idx][l][i] = float(tmps[i+2])
        s = f.readline()

print("Model building time (ms): ", fit_times)

# ms to s
data_y = data_y / 1000
model_a = model_a / 1000
model_b = model_b / 1000

# Our paper data
# data_x = [10, 20, 30, 60, 120]
# data_y = [
#         [[147.854, 72.048, 48.341, 23.896, 14.561], [14.63, 7.29, 4.921, 2.879, 2.123]],
#         [[1059.924, 530.995, 356.537, 181.993, 88.508], [471.669, 345.522, 301.855, 256.205, 237.487]],
#         [[644.696, 311.484, 210.464, 104.482, 55.188], [229.617, 163.442, 139.133, 124.787, 118.503]],
#         [[534.318, 275.798, 183.368, 90.08, 45.236], [238.822, 169.34, 141.655, 119.983, 119.244]]
#     ]
# model_a = [[1466.45, 138.395], [10569.37, 2568.571], [6441.62, 1236.72], [5339.51, 1361.86]]
# model_b = [[0.238, 0.602], [3.202, 215.522], [-3.138, 103.57], [3.266, 101.06]]


# Set font and figure size
font_size = 30
plt.rc('font',**{'size': font_size})
plt.rc('pdf',fonttype = 42)
fig_size = (36, 4.6)
fig, axes = plt.subplots(nrows=1, ncols=4, sharey=False, figsize=fig_size)
matplotlib.rcParams['xtick.minor.size'] = 5.
matplotlib.rcParams['xtick.major.size'] = 10.
matplotlib.rcParams['ytick.major.size'] = 6.
matplotlib.rcParams['ytick.minor.visible'] = True
plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.2, hspace=None)

# settings
colors = {0: None, 1: None}
line_colors = {0: None, 1: None}
point_labels = {0: 'IO-Stage Actual', 1: 'Comp-Stage Actual'}
line_lables = {0: 'IO-Stage Model', 1: 'Comp-Stage Model'}

# axis setting
y_ticks = [[i*3 for i in range(0, 6)], [i*20 for i in range(0, 6)], 
        [i*10 for i in range(0, 5)], [i*15 for i in range(0, 5)]]
x_ticks = [2*i for i in range(1, 7)]
x_label = 'Degree of Parallelism'
y_label = 'Execution Time (s)'
x_max = 13
y_max = [15, 100, 40, 60]

if scale == 1000:
    y_ticks = [[i*40 for i in range(0, 5)], [i*300 for i in range(0, 5)], 
            [i*300 for i in range(0, 5)], [i*300 for i in range(0, 5)]]
    x_ticks = [20*i for i in range(1, 7)]
    x_max = 130

# Plot x ticks and label
for j in range(num_subfigs):
    axes[j].set_xlabel(x_label)
    axes[j].set_xlim(left=0, right=x_max)
    axes[j].xaxis.set_ticks_position('bottom')
    axes[j].set_xticks(x_ticks)
    axes[j].get_xaxis().set_tick_params(direction='in', pad=7)
    axes[j].get_xaxis().set_tick_params(which='minor', direction='in')

# Plot y ticks and label
for j in range(num_subfigs):
    if j == 0:
        axes[j].set_ylabel(y_label)
    if scale == 10:
        axes[j].set_ylim(bottom=0, top=y_max[j])
    axes[j].set_yticks(y_ticks[j])
    axes[j].tick_params(bottom=False, top=False, left=False, right=False)
    axes[j].minorticks_off()
    axes[j].yaxis.set_ticks_position('left')
    axes[j].get_yaxis().set_tick_params(direction='in', pad=4)
    axes[j].get_yaxis().set_tick_params(which='minor', direction='in')
   
lines = [None for i in range(num_lines)]
points = [None for i in range(num_lines)]
# Plot bars
for j in range(num_subfigs):
    for i in range(num_lines):
        lines[i] = axes[j].scatter(data_x, data_y[j][i], label=point_labels[i], 
            color=colors[i], marker='X', s=300, zorder=3)
        mx = np.linspace(1, 12.5, 80)
        if scale == 1000:
            mx = np.linspace(5, 125, 80)
        my = model_a[j][i] / mx + model_b[j][i]
        points[i],  = axes[j].plot(mx, my, label=line_lables[i], 
            color=line_colors[i], linewidth=3.6, zorder=1)

# Plot legend
fig.legend(handles=[lines[0], points[0], lines[1], points[1]], handlelength=1.8, 
           ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1.1), frameon=False, prop={'size':font_size}, columnspacing = 1)

# Plot grid
for j in range(num_subfigs):
    axes[j].grid(axis='y', linestyle='--')

# Save the figure
file_path = '../results/fig11.png'
plt.savefig(file_path, bbox_inches='tight')