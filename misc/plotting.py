from posixpath import dirname
import matplotlib.pyplot as plt
import pandas as pd

from os import path

def plot_v_line(data, color, label):
  for i in range(len(data)):
    if i == 0:
      plt.axvline(data[i], alpha=0.3, linestyle='--', color=color, label=label)
    else:
      plt.axvline(data[i], alpha=0.3, linestyle='--', color=color)

CSV_FOLDER = path.join(dirname(__file__), '..', 'stats')
CSV_FILE = 'out_-1032586642.csv'
ROLLING_AVG_WINDOW_SIZE = 10
TITLE='LFU Cache'

dataframe = pd.read_csv(path.join(CSV_FOLDER, CSV_FILE))

dataframe['MA'] = dataframe['totalSucc'].rolling(window=10).mean()
server_start = dataframe.loc[dataframe['event'] == 'START_SERVER']['timeStep'].tolist()
server_stop = dataframe.loc[dataframe['event'] == 'STOP_SERVER']['timeStep'].tolist()
client_start = dataframe.loc[dataframe['event'] == 'START_CLIENT']['timeStep'].tolist()

# plt.plot(dataframe['timeStep'], dataframe['totalSucc'])
plt.plot(dataframe['timeStep'], dataframe['MA'], label=f'Successful ops. (avg., window={ROLLING_AVG_WINDOW_SIZE})')
plt.plot(dataframe['timeStep'], dataframe['getFailCount'] + dataframe['putFailCount'] + dataframe['deleteFailCount'], label="Unsuccessful ops.")

plot_v_line(server_start, 'green', 'Server start')
plot_v_line(server_stop, 'red', 'Server stop')
plot_v_line(client_start, 'black', 'Client start')

plt.xlabel('Time (s)')
plt.ylabel('Operations')
plt.title(TITLE)
plt.legend()
plt.show()