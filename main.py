import json
import numpy as np


language = 'python-multithreaded'
corrupt_files = set()

extract_thingies_path = f'{language}/benchmarks-client/total_time.txt'

def get_mean_throughput(file_path: str):
    throughput_entries = []
    with open(file_path, 'r') as file:
        for line in file.readlines():
            throughput_entries.append(line)
    return len(throughput_entries) / (float(throughput_entries[-1]) - float(throughput_entries[0]) + 1)

def get_mean_consumer_latency(file_path: str):
    latency_entries = []
    with open(file_path, 'r') as file:
        for line in file.readlines():
            latency_entries.append(float(line))
    return np.mean(latency_entries)

def get_mean_producer_latency(broker_timestamps_file: str, client_timestamps_file: str):
    broker_timestamps_entries = {}
    with open(broker_timestamps_file, 'r') as file:
        for line in file.readlines():
            key, value = line.split(',')[1], float(line.split(',')[0])
            broker_timestamps_entries[key] = max(broker_timestamps_entries.get(key, 0), value)

    client_timestamps_entries = {}
    with open(client_timestamps_file, 'r') as file:
        for line in file.readlines():
            key, value = line.split(',')[1], float(line.split(',')[0])
            client_timestamps_entries[key] = max(client_timestamps_entries.get(key, 0), value)

    latency_entries = []
    for key, broker_timestamp in broker_timestamps_entries.items():
        latency_entries.append(broker_timestamp - client_timestamps_entries[key])
    return np.mean(latency_entries)


def extract_data(producer_line: str, consumer_line: str):
    id = producer_line.split(' ')[1].split('-')[0]
    total_time_to_produce = float(producer_line.split(' ')[-1])
    mean_producer_throughput = get_mean_throughput(f'{language}/benchmarks-broker/throughput/producer/{id}.csv')
    mean_consumer_throughput = get_mean_throughput(f'{language}/benchmarks-broker/throughput/consumer/{id}.csv')
    mean_producer_latency = get_mean_producer_latency(f'{language}/benchmarks-broker/latency/{id}.csv', f'{language}/benchmarks-client/publish/{id}-{language}.csv')
    mean_consumer_latency = get_mean_consumer_latency(f'{language}/benchmarks-client/receive/{id}-{language}.csv')
    return {
        'mean_producer_throughput': mean_producer_throughput, 
        'mean_consumer_throughput': mean_consumer_throughput, 
        'mean_producer_latency': mean_producer_latency, 
        'mean_consumer_latency': mean_consumer_latency, 
        'total_time_to_produce': total_time_to_produce,
        'total_time_to_consume': float(consumer_line.split(' ')[-1])
    }, id

# for every id:
# - mean throughput on PRODUCER (reqs/s)
# - mean throughput on CONSUMER (reqs/s)
# - mean latency on PRODUCER    (s)
# - mean latency on CONSUMER    (s)
# - total time / transfer       (s)

def write_benchmarks_to_file(benchmarks):
    with open('results.json', 'r+') as f:
        results = dict(json.load(f))
        results[language] = benchmarks
        json_string = json.dumps(results, indent=4)
        f.seek(0)
        f.write(json_string)

def main():
    benchmarks = {}
    with open(extract_thingies_path, 'r') as total_time_file:
        line = total_time_file.readline()
        while line:
            data, id = extract_data(line, total_time_file.readline())
            benchmarks[id] = data
            line = total_time_file.readline()
    write_benchmarks_to_file(benchmarks)


import matplotlib.pyplot as plt

languages = ['python-async', 'python-multithreaded', 'cpp', 'rust']
methodologies = ['one2one', 'one2many', 'many2one', 'many2many']
request_sizes = ['1b', '256b', '1kb', '256kb', '1mb']

# def plot_throughput(id, metric_to_plot_producer, metric_to_plot_consumer):
#     with open('results.json', 'r+') as f:
#         results = dict(json.load(f))
#         left = [1, 2, 3, 4, 5, 6, 7, 8]
#         height = []
#         tick_label = ['py async\nproducer', 'py async\nconsumer', 
#                         'py mt\nproducer', 'py mt\nconsumer', 
#                         'c++\nproducer', 'c++\nconsumer', 
#                         'rust\nproducer', 'rust\nconsumer', 
#                         ]
#         for language in languages:
#             max_throughput_producer = 0
#             max_throughput_consumer = 0
#             for key in results[language].keys():
#                 if id in key:
#                     max_throughput_producer = max(max_throughput_producer, results[language][key][metric_to_plot_producer])
#                     max_throughput_consumer = max(max_throughput_consumer, results[language][key][metric_to_plot_consumer])
#             height.append(max_throughput_producer)
#             height.append(max_throughput_consumer)
#         figure = plt.figure(id, figsize=(9, 6))
#         plt.bar(left, height, tick_label=tick_label, width=0.8, color=['#3643f5', '#d92418'])
#         plt.xlabel('Programming language')
#         plt.ylabel('Throughput (requests/s)')
#         plt.title(f'Throughput values for {id}')
#         figure.show()

def plot_throughput(id, actor_to_plot):
    with open('results.json', 'r+') as f:
        results = dict(json.load(f))
        heights = {
            language: [] for language in languages
        }
        max_height = 0
        for request_size in request_sizes:
            for language in languages:
                max_throughput = 0
                for key in results[language].keys():
                    if f'{id}_{request_size}' in key:
                        max_throughput = max(max_throughput, round(float(results[language][key][f'mean_{actor_to_plot}_throughput']), 2))
                heights[language].append(max_throughput)
                max_height = max(max_height, max_throughput)
        
        x = np.arange(len(request_sizes))
        width = 0.2
        multiplier = 0

        fig, ax = plt.subplots(layout='constrained')
        fig.canvas.manager.set_window_title(f'{id.split("_")[0]}_throughput_{actor_to_plot}')
        for request_size, measurement in heights.items():
            offset = width * multiplier
            rects = ax.bar(x + offset, tuple(measurement), width, label=request_size)
            ax.bar_label(rects, padding=3)
            multiplier += 1
        ax.set_ylabel('Throughput (requests/s)')
        ax.set_xlabel('Message size')
        ax.set_title(f'Throughput values for {id.split("_")[0]}')
        ax.set_xticks(x + width * 3 / 2, request_sizes)
        ax.legend(loc='upper left', ncols=4)
        ax.set_ylim(0, max_height * 120 / 100)
        fig.set_figwidth(14)
        fig.set_figheight(6)
        fig.show()

def plot_throughput_wrapper():
    measurement = 'throughput'
    for methodology in methodologies:
        plot_throughput(f'{methodology}_{measurement}', 'producer')
        plot_throughput(f'{methodology}_{measurement}', 'consumer')
    input()

def plot_latency_by_message_size(id, actor_to_plot):
    with open('results.json', 'r+') as f:
        xs = [[] for _ in languages]
        ys = [[] for _ in languages]
        results = dict(json.load(f))
        for request_size in request_sizes:
            for i, language in enumerate(languages):
                xs[i].append(request_size)
                ys[i].append(results[language][f'{id}_{request_size}'][f'mean_{actor_to_plot}_latency'])
        figure = plt.figure(f'{id}_{actor_to_plot}', figsize=(8, 5))
        for i, language in enumerate(languages):
            plt.plot(xs[i], ys[i], label=language)
        plt.xlabel('Message size')
        plt.ylabel('Latency (s)')
        plt.title(f'Latency values for {id}_{actor_to_plot}')
        plt.legend()
        figure.show()

def plot_latency_by_message_size_wrapper():
    measurement = 'latency_by_message_size'
    for methodology in methodologies:
        plot_latency_by_message_size(f'{methodology}_{measurement}', 'producer')
        plot_latency_by_message_size(f'{methodology}_{measurement}', 'consumer')
    input()

def plot_latency_by_no_of_messages(id, actor_to_plot):
    with open('results.json', 'r+') as f:
        xs = [['1b x 5120', '256b x 20', '1kb x 5120', '256kb x 20', '1mb x 5'] for _ in languages]
        ys = [[] for _ in languages]
        results = dict(json.load(f))
        for request_size in request_sizes:
            for i, language in enumerate(languages):
                ys[i].append(results[language][f'{id}_{request_size}'][f'mean_{actor_to_plot}_latency'])
        figure = plt.figure(f'{id}_{actor_to_plot}', figsize=(8, 5))
        for i, language in enumerate(languages):
            plt.plot(xs[i], ys[i], label=language)
        plt.xlabel('Message size')
        plt.ylabel('Latency (s)')
        plt.title(f'Latency values for {id}_{actor_to_plot}')
        plt.legend()
        figure.show()

def plot_latency_by_no_of_messages_wrapper():
    measurement = 'latency_by_no_of_messages'
    for methodology in methodologies:
        plot_latency_by_no_of_messages(f'{methodology}_{measurement}', 'producer')
        plot_latency_by_no_of_messages(f'{methodology}_{measurement}', 'consumer')
    input()


def plot_total_time(id, actor_to_plot):
    with open('results.json', 'r+') as f:
        results = dict(json.load(f))
        heights = {
            language: [] for language in languages
        }
        max_height = 0
        for request_size in request_sizes:
            for language in languages:
                new_height = round(float(results[language][f'{id}_{request_size}'][f'total_time_to_{actor_to_plot}']), 2)
                heights[language].append(new_height)
                max_height = max(max_height, new_height)
        
        x = np.arange(len(request_sizes))
        width = 0.2
        multiplier = 0

        fig, ax = plt.subplots(layout='constrained')
        fig.canvas.manager.set_window_title(f'{id.split("_")[0]}_total_time_{actor_to_plot}r')
        for request_size, measurement in heights.items():
            offset = width * multiplier
            rects = ax.bar(x + offset, tuple(measurement), width, label=request_size)
            ax.bar_label(rects, padding=3)
            multiplier += 1
        ax.set_ylabel('Time (s)')
        ax.set_xlabel('Message size')
        ax.set_title(f'Total time taken {id.split("_")[0]}')
        ax.set_xticks(x + width * 3 / 2, request_sizes)
        ax.legend(loc='upper left', ncols=4)
        ax.set_ylim(0, max_height * 120 / 100)
        fig.set_figwidth(12)
        fig.set_figheight(5)
        fig.show()

def plot_total_time_wrapper():
    measurement = 'latency_by_message_size'
    for methodology in methodologies:
        plot_total_time(f'{methodology}_{measurement}', 'produce')
        plot_total_time(f'{methodology}_{measurement}', 'consume')
    input()

if __name__ == "__main__":
    # main()
    plot_throughput_wrapper()
    # plot_latency_by_message_size_wrapper()
    # plot_latency_by_no_of_messages_wrapper()
    # plot_total_time_wrapper()
