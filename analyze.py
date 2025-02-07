from pathlib import Path
from typing import Dict, List

import numpy
import pandas as pd


class IPFSSimulationAnalyzer:

    def __init__(self, topology: str):
        self.topology = topology
        self.simulation_dir = Path("data-" + topology)
        self.simulation_df = None
        self.metrics_df = None
        self.topology_df = None
        self.start_timestamp = None
        self.end_timestamp = None
        self.load_data()

    def load_data(self):
        self.simulation_df = pd.read_csv(self.simulation_dir / 'ipfs_simulation.csv')
        self.metrics_df = pd.read_csv(self.simulation_dir / 'metrics.csv')
        self.topology_df = pd.read_csv(self.simulation_dir / 'topology.csv')

        self.simulation_df['timestamp'] = pd.to_datetime(self.simulation_df['timestamp'], utc=True)
        self.metrics_df['timestamp'] = pd.to_datetime(self.metrics_df['timestamp'])
        self.topology_df['timestamp'] = pd.to_datetime(self.topology_df['timestamp'])

        self.start_timestamp = self.topology_df['timestamp'][0]
        self.end_timestamp = self.topology_df['timestamp'][1]

        self.simulation_df = self.normalize_data(self.simulation_df)
        self.metrics_df = self.normalize_data(self.metrics_df)
        self.topology_df = self.normalize_data(self.topology_df)

    def normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            (df['timestamp'] >= self.start_timestamp) & (df['timestamp'] <= self.end_timestamp)
            ].sort_values(by='timestamp')

    def calculate_transfer_metrics(self) -> Dict:
        total_downloads = len(self.simulation_df[self.simulation_df['action'] == 'download'])
        successful_downloads = len(self.simulation_df[
                                       (self.simulation_df['action'] == 'download') &
                                       (self.simulation_df['success'] == True)
                                       ])

        upload_sizes = self.simulation_df[
            (self.simulation_df['action'] == 'upload') &
            (self.simulation_df['success'] == True)
            ]['file_size']

        upload_durations = self.simulation_df[
            (self.simulation_df['action'] == 'upload') &
            (self.simulation_df['success'] == True)
            ]['duration']

        download_sizes = self.simulation_df[
            (self.simulation_df['action'] == 'download') &
            (self.simulation_df['success'] == True)
            ]['file_size']

        download_durations = self.simulation_df[
            (self.simulation_df['action'] == 'download') &
            (self.simulation_df['success'] == True)
            ]['duration']

        return {
            'total_downloads_num': total_downloads,
            'total_uploads_num': len(self.simulation_df[self.simulation_df['action'] == 'upload']),
            'download_success_rate_percent': (
                                                         successful_downloads / total_downloads) * 100 if total_downloads > 0 else 0,
            'avg_upload_size_bytes': upload_sizes.mean(),
            'median_upload_size_bytes': upload_sizes.median(),
            'avg_upload_duration_sec': upload_durations.mean(),
            'median_upload_duration_sec': upload_durations.median(),
            'avg_download_size_bytes': download_sizes.mean(),
            'median_download_size_bytes': download_sizes.median(),
            'avg_download_duration_sec': download_durations.mean(),
            'median_download_duration_sec': download_durations.median(),
        }

    def calculate_performance_metrics(self) -> Dict:
        return {
            'avg_cpu_percent': self.metrics_df['cpu_percent'].mean(),
            'max_cpu_percent': self.metrics_df['cpu_percent'].max(),
            'avg_memory_usage_bytes': self.metrics_df['memory_usage_bytes'].mean(),
            'max_memory_usage_bytes': self.metrics_df['memory_usage_bytes'].max(),
            'avg_network_rx_Bps': self.metrics_df['network_rx_bytes_sec'].mean(),
            'avg_network_tx_Bps': self.metrics_df['network_tx_bytes_sec'].mean()
        }


def format_metric_name(name: str) -> str:
    name = " ".join(name.split("_")[0:-1]).title()

    name = name.replace('Cpu', 'CPU')
    name = name.replace('Avg', 'Average')

    return name


def format_value(name: str, value: float | int) -> str:
    unit_str = name.split("_")[-1].lower()
    match unit_str:
        case "bps":
            unit = "B/s"
        case "bytes":
            unit = "B"
        case "sec":
            unit = "s"
        case "percent":
            unit = "\%"
        case _:
            unit = ""

    if isinstance(value, float):
        if value < 0.01:
            return f"{value:.2e} {unit}"
        elif value < 1:
            return f"{value:.2f} {unit}"
        elif value > 1000000000000:
            return f"{value / 1000000000000:.1f} T{unit}"
        elif value > 1000000000:
            return f"{value / 1000000000:.1f} G{unit}"
        elif value > 1000000:
            return f"{value / 1000000:.1f} M{unit}"
        elif value > 1000:
            return f"{value / 1000:.1f} k{unit}"
        else:
            return f"{value:.1f} {unit}"
    elif isinstance(value, int) or isinstance(value, numpy.int_):
        if value > 1000000000000:
            return f"{value / 1000000000000:.1f} T{unit}"
        elif value > 1000000000:
            return f"{value / 1000000000:.1f} G{unit}"
        elif value > 1000000:
            return f"{value / 1000000:.1f} M{unit}"
        elif value > 1000:
            return f"{value / 1000:.1f} k{unit}"
        else:
            return f"{value} {unit}"
    return f"{value} {unit}"


def generate_topology_comparison(topologies: List[str]) -> str:
    all_metrics = {}
    for topology in topologies:
        analyzer = IPFSSimulationAnalyzer(topology)
        analyzer.load_data()
        transfer_metrics = analyzer.calculate_transfer_metrics()
        performance_metrics = analyzer.calculate_performance_metrics()
        all_metrics[topology] = {**transfer_metrics, **performance_metrics}

    latex_lines = [
        "\\begin{table}",
        "\\centering",
        f"\\begin{{tabular}}{{l{'|c' * len(topologies)}}}",
        "\\hline",
        "Metric & " + " & ".join(topologies).title().replace("Barabasi", "Random") + " \\\\ \\hline"
    ]

    metrics = list(all_metrics[topologies[0]].keys())
    for metric in metrics:
        formatted_name = format_metric_name(metric)
        values = [format_value(metric, all_metrics[topology][metric]) for topology in topologies]
        row = f"{formatted_name} & " + " & ".join(values) + " \\\\"
        latex_lines.append(row)

    latex_lines.extend([
        "\\hline",
        "\\end{tabular}",
        "\\caption{Comparison of IPFS Performance Metrics Across Different Network Topologies}",
        "\\label{tab:topology-comparison}",
        "\\end{table}"
    ])

    return "\n".join(latex_lines)


if __name__ == "__main__":
    topologies = ["normal", "ring", "grid", "full", "barabasi"]
    latex_table = generate_topology_comparison(topologies)
    print(latex_table)

    with open("report/topology_comparison.tex", "w") as f:
        f.write(latex_table)
