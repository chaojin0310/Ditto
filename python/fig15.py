import os
import sys
import random
import numpy as np
import matplotlib
import matplotlib.pyplot as plt


scale = 10  # default 10 for 10G, 1000 for 1T
with open('./scale.txt', 'r') as f:
    scale = int(f.readline())

nim_para = [3, 3, 1, 3, 3, 3, 3, 3, 1]
ours_para = [12, 2, 1, 6, 1, 2, 1, 2, 1]
if scale == 1000:
    nim_para = [24, 24, 1, 24, 24, 24, 24, 24, 1]
    ours_para = [60, 15, 1, 48, 4, 24, 1, 12, 1]
nim_exec_times = [[] for _ in range(9)]
ours_exec_times = [[] for _ in range(9)]

for s in range(9):
    for i in range(nim_para[s]):
        with open('../results/fixed/q95_stage{}_parall{}_task{}.log'.format(s+1, nim_para[s], i), 'r') as f:
            lines = f.readlines()
            lines = [line.rstrip() for line in lines]
            lines = lines[:-1]
            total_time = 0
            for line in lines:
                tokens = line.split()
                total_time += float(tokens[1])
            nim_exec_times[s].append(total_time / 1000)

for s in range(9):
    for i in range(ours_para[s]):
        with open('../results/ditto/q95_stage{}_parall{}_task{}.log'.format(s+1, ours_para[s], i), 'r') as f:
            lines = f.readlines()
            lines = [line.rstrip() for line in lines]
            lines = lines[:-1]
            total_time = 0
            for line in lines:
                tokens = line.split()
                total_time += float(tokens[1])
            ours_exec_times[s].append(total_time / 1000)

nim_bar_ind = [0, 0, 0, 0, 0, 0, 0, 0, 0]
for i in range(1, 9):
    nim_bar_ind[i] = nim_bar_ind[i - 1] + nim_para[i - 1]
ours_bar_ind = [0, 0, 0, 0, 0, 0, 0, 0, 0]
for i in range(1, 9):
    ours_bar_ind[i] = ours_bar_ind[i - 1] + ours_para[i - 1]

nim_st = [0, 19.991, 22.131, 0, 0, 22.106, 0, 27.47, 27.986]
if scale == 1000:
    nim_st = [0, 225.511, 252.369, 0, 0, 270.264, 0, 423.248, 415.936]
nim_start_times = [[] for _ in range(9)]
# nim_exec_times = [[] for _ in range(9)]

ours_st = [0, 1.589, 2.745, 0, 0, 8.28, 0, 10.476, 10.656]
if scale == 1000:
    ours_st = [0, 89.087, 105.329, 86.419, 0, 119.881, 0, 260.373, 260.584]
ours_start_times = [[] for _ in range(9)]
# ours_exec_times = [[] for _ in range(9)]

for i in range(9):
    p = nim_para[i]
    for j in range(p):
        newst = nim_st[i]
        if newst != 0: 
            newst = newst + random.uniform(1, 2)  # random cold start time
        nim_start_times[i].append(newst)
    
for i in range(9):
    p = ours_para[i]
    for j in range(p):
        newst = ours_st[i]
        if newst != 0:
            newst = newst + random.uniform(1, 2)
        ours_start_times[i].append(newst)

num_slots = 30
if scale == 1000:
    num_slots = 170
num_subfigs = 2

# Set font and figure size
font_size = 30
plt.rc('font',**{'size': font_size})
plt.rc('pdf',fonttype = 42)
fig_size = (18, 6)
fig, axes = plt.subplots(nrows=1, ncols=2, sharey=False, figsize=fig_size)
matplotlib.rcParams['xtick.minor.size'] = 4.
matplotlib.rcParams['xtick.major.size'] = 8.
matplotlib.rcParams['ytick.major.size'] = 6.
matplotlib.rcParams['ytick.minor.visible'] = True
plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.24, hspace=None)

# settings##
colors = {0: '#FF7F24', 1: '#98FB98', 2: '#1E90FF', 3: '#551A8B', 4:'#FFD700', 
    5: 'red', 6: 'pink', 7:'#AB82FF', 8:'#363636'}

# indexes for x
width = 0.1
num_bars = 1
indexes = [[0.1*i for i in range(num_slots+5)]]
if scale == 1000:
    indexes = [[0.1*i for i in range(num_slots+40)]] # tmp solution

# x-axis setting
x_label = 'Time (s)'

# y-axis setting
y_ticks = [indexes[0][i] for i in range(0, num_slots+5, 5)]
y_tick_labels = [str(i*5) for i in range(7)]
if scale == 1000:
    y_ticks = [indexes[0][i] for i in range(0, num_slots+40, 50)] # tmp solution
    y_tick_labels = [str(i*50) for i in range(5)]
y_label = ['Stages & Tasks']
x_ticks = [10*i for i in range(5)]
if scale == 1000:
    x_ticks = [100*i for i in range(5)]

# Plot x ticks and label
for j in range(num_subfigs):
    axes[j].set_xlabel(x_label)
    axes[j].set_xlim(0, 45)
    if scale == 1000:
        axes[j].set_xlim(0, 450)
    axes[j].set_xticks(x_ticks)

# Plot y ticks and label
for j in range(num_subfigs):
    axes[j].set_ylabel(y_label[0])
    axes[j].set_ylim(bottom=0.5)
    axes[j].set_yticks(y_ticks)
    axes[j].set_yticklabels(y_tick_labels)
    axes[j].minorticks_off()
   
# Plot bars
for i in range(9):
    axes[0].barh(indexes[0][nim_bar_ind[i]:nim_bar_ind[i]+nim_para[i]], nim_exec_times[i], width, left=nim_start_times[i], color=colors[i])
    axes[1].barh(indexes[0][ours_bar_ind[i]:ours_bar_ind[i]+ours_para[i]], ours_exec_times[i], width, left=ours_start_times[i], color=colors[i])

# Plot grid
for j in range(num_subfigs):
    axes[j].grid(linestyle='--')

# Save the figure
file_path = '../results/fig15.png'
plt.savefig(file_path, bbox_inches='tight')