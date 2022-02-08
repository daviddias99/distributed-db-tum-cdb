from posixpath import dirname
from matplotlib.figure import Figure
from mpl_toolkits.axisartist.axislines import Axes
import matplotlib.pyplot as plt
import pandas as pd

from os import path

plt.rcParams.update({'font.size': 22})

def plot_v_line(ax, data, color, label):
  for i in range(len(data)):
    if i == 0:
      ax.axvline(data[i], alpha=0.5, linestyle='--', color=color, label=label)
    else:
      ax.axvline(data[i], alpha=0.5, linestyle='--', color=color)

CSV_FOLDER = path.join(dirname(__file__), '..', 'stats')
CSV_FILE = 'out_behaviour_normal_20220207150925.csv'
ROLLING_AVG_WINDOW_SIZE = 10
TITLE='FIFO Cache (size=100 elements)'

dataframe = pd.read_csv(path.join(CSV_FOLDER, CSV_FILE))
dataframe = dataframe.head(1239)
dataframe['MA'] = dataframe['totalSucc'].rolling(window=10).mean()
server_start = dataframe.loc[dataframe['event'] == 'START_SERVER']['timeStep'].tolist()
server_stop = dataframe.loc[dataframe['event'] == 'STOP_SERVER']['timeStep'].tolist()
client_start = dataframe.loc[dataframe['event'] == 'START_CLIENT']['timeStep'].tolist()

fig: Figure = plt.figure()

ax = fig.add_subplot(111, axes_class=Axes)
fig.set_size_inches(12, 7)

# plt.plot(dataframe['timeStep'], dataframe['totalSucc'])
ax.plot(dataframe['timeStep'], dataframe['MA'], label=f'Successful ops.')
ax.plot(dataframe['timeStep'], dataframe['getFailCount'] + dataframe['putFailCount'] + dataframe['deleteFailCount'], label="Unsuccessful ops.")

plot_v_line(ax, server_start, 'green', 'Server start')
plot_v_line(ax, server_stop, 'red', 'Server stop')
plot_v_line(ax, client_start, 'black', 'Client start')

# ax.set_ylim(0, 300)
ax.set_xlabel('Time (s)')
ax.set_ylabel('Operations')
# ax.set_title(TITLE)
ax.legend(
#  loc="upper left"
)
for direction in ["left", "bottom"]:
  # adds arrows at the ends of each axis
  ax.axis[direction].set_axisline_style("->")

  # adds X and Y-axis from the origin
  ax.axis[direction].set_visible(True)

for direction in ["right", "top"]:
  # hides borders
  ax.axis[direction].set_visible(False)
# ax.show()
ax.margins(0.03)
ax.grid(linestyle="dotted")
fig.tight_layout(pad=1.5)
fig.savefig(path.join('plots', CSV_FILE[:-4] + '.pdf'))