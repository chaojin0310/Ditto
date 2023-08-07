import os
import sys
import numpy as np
import matplotlib
import matplotlib.pyplot as plt


scale = 10  # default 10 for 10G, 1000 for 1T
with open('./scale.txt', 'r') as f:
    scale = int(f.readline())

times = np.zeros((5, 9))

a = []
b = []

# read data
with open('../results/time_breakdown.log', 'r') as f:
    s = f.readline()
    while s:
        tmps = s.strip().split(' ')
        if tmps[0] == 'a:':
            for i in range(5*9):
                a.append(float(tmps[i+1]))
        elif tmps[0] == 'b:':
            for i in range(5*9):
                b.append(float(tmps[i+1]))
        s = f.readline()

a = np.array(a).reshape(9, 5)
b = np.array(b).reshape(9, 5)

ra = np.zeros((9, 4))
rb = np.zeros((9, 4))
ra[:, 0] = a[:, 0]
ra[:, 1] = a[:, 1] + a[:, 2]
ra[:, 2] = a[:, 3]
ra[:, 3] = a[:, 4]
rb[:, 0] = b[:, 0]
rb[:, 1] = b[:, 1] + b[:, 2]
rb[:, 2] = b[:, 3]
rb[:, 3] = b[:, 4]

x = 4
if scale == 1000:
    x = 40

for i in range(1, 5):
    times[i] = ra[:, i-1] / x + rb[:, i-1]

times = times / 1000

# get the prefix sum of times
y = times.copy()
for i in range(1, len(times)):
    for j in range(len(times[i])):
        y[i][j] += y[i-1][j]

sysname = 'SysName'

num_queries = 9
num_bars = 1
num_subfigs = 1

# Set font and figure size
font_size = 30
plt.rc('font',**{'size': font_size})
plt.rc('pdf',fonttype = 42)
fig_size = (13, 6)
fig, axes = plt.subplots(nrows=1, ncols=num_subfigs, sharey=False, figsize=fig_size)
matplotlib.rcParams['xtick.minor.size'] = 4.
matplotlib.rcParams['xtick.major.size'] = 8.
matplotlib.rcParams['ytick.major.size'] = 6.
matplotlib.rcParams['ytick.minor.visible'] = True
plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.2, hspace=None)

# settings
colors = {0: 'gray', 1: 'darkorange', 2: 'royalblue', 3: 'palegreen'}
labels = ['setup', 'read', 'compute', 'write']

# indexes for x
width = 0.08
indexes = [[0.2*i+0.1+width*j for i in range(num_queries)] for j in range(num_bars)]

# x-axis setting
x_label = 'Time (s)'

# y-axis setting
y_ticks = [i for i in range(10)]
y_tick_labels = [str(i+1) for i in range(9)]
y_label = ['Stage Index']
x_ticks = [0, 5, 10, 15]
if scale == 1000:
    x_ticks = [0, 50, 100, 150]

# Plot x ticks and label
for j in range(1):
    axes.set_xlabel(x_label)
    if scale == 10:
        axes.set_xlim(left=0, right=16)
    axes.set_xticks(x_ticks)
    axes.get_xaxis().set_tick_params(direction='in', pad=7)

# Plot y ticks and label
for j in range(1):
    axes.set_ylabel(y_label[j])
    axes.set_yticks(indexes[0])
    axes.set_yticklabels(y_tick_labels)
    axes.minorticks_off()
   
bars = [[] for i in range(len(times) - 1)]
# Plot bars
for i in range(len(times)-1):
    bars[i] = axes.barh(indexes[0], times[i+1], width, left=y[i], label=labels[i], color=colors[i])

# Plot legend
axes.legend(handles=[bars[0], bars[1], bars[2], bars[3]], handlelength=1.2,
    ncol=1, loc='upper center', bbox_to_anchor=(0.78, 1.03), frameon=True, prop={'size':font_size})

# Plot grid
for j in range(num_subfigs):
    axes.grid(axis='x', linestyle='--')

# Save the figure
file_path = '../results/fig14.png'
plt.savefig(file_path, bbox_inches='tight')