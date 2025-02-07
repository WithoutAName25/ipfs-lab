#!/usr/bin/env python3
import datetime as dt
import logging
import os
import time
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests


class MetricsCollector:
    def __init__(self):
        self.cadvisor_url = os.getenv('CADVISOR_URL', 'http://localhost:8080')
        self.project = os.getenv('PROJECT', 'my-cluster')
        self.service_prefixes = os.getenv('SERVICE_PREFIXES', 'ipfs').split(',')
        self.collection_interval = int(os.getenv('COLLECTION_INTERVAL', '5'))
        self.output_file = Path(os.getenv('METRICS_FILE', 'metrics.csv'))
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        self.output_file.unlink(missing_ok=True)

        self.last_timestamps: Dict[str, str] = {}

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('metrics_collector')

    def get_container_specs(self):
        try:
            response = requests.get(f"{self.cadvisor_url}/api/v2.0/spec?type=docker&recursive=true")
            if response.status_code == 200:
                return {
                    cid: spec for cid, spec in response.json().items()
                    if self.project == spec.get('labels', {}).get('com.docker.compose.project', '')
                       and any(
                        spec.get('labels', {}).get('com.docker.compose.service', '').startswith(prefix) for prefix in
                        self.service_prefixes)
                }
            return None
        except requests.RequestException as e:
            self.logger.error(f"Error fetching container specs: {e}")
            return None

    def get_container_stats(self, container_id: str) -> Optional[list]:
        try:
            response = requests.get(
                f"{self.cadvisor_url}/api/v2.0/stats/{container_id}",
                params={'count': self.collection_interval * 2}
            )
            if response.status_code == 200:
                return response.json().get(container_id)
            return None
        except requests.RequestException as e:
            self.logger.error(f"Error fetching stats for {container_id}: {e}")
            return None

    def parse_timestamp(self, ts: str) -> dt.datetime:
        # Truncate nanoseconds to microseconds for strptime compatibility
        ts_micro = ts[:-4] + 'Z' if ts.endswith('Z') else ts
        return dt.datetime.strptime(ts_micro, '%Y-%m-%dT%H:%M:%S.%fZ')

    def calculate_cpu_usage(self, current: dict, previous: dict) -> float:
        if not (current and previous and 'cpu' in current and 'cpu' in previous):
            return 0.0

        cpu_delta = current['cpu']['usage']['total'] - previous['cpu']['usage']['total']
        time_delta = (
                self.parse_timestamp(current['timestamp']) -
                self.parse_timestamp(previous['timestamp'])
        ).total_seconds()

        return (cpu_delta / 1e9) / time_delta * 100 if time_delta > 0 else 0.0

    def calculate_network_rate(self, current: dict, previous: dict) -> tuple:
        if not (current and previous):
            return 0.0, 0.0

        current_net = current.get('network', {}).get('interfaces', [{}])[0]
        previous_net = previous.get('network', {}).get('interfaces', [{}])[0]

        time_delta = (
                self.parse_timestamp(current['timestamp']) -
                self.parse_timestamp(previous['timestamp'])
        ).total_seconds()

        if time_delta <= 0:
            return 0.0, 0.0

        rx_delta = current_net.get('rx_bytes', 0) - previous_net.get('rx_bytes', 0)
        tx_delta = current_net.get('tx_bytes', 0) - previous_net.get('tx_bytes', 0)

        return rx_delta / time_delta, tx_delta / time_delta

    def process_metrics(self, specs: dict) -> list:
        processed_data = []

        for container_id, spec in specs.items():
            stats = self.get_container_stats(container_id)
            if not stats:
                continue

            service_name = spec['labels'].get('com.docker.compose.service', 'unknown')
            last_timestamp = self.last_timestamps.get(container_id)

            valid_stats = [
                stat for stat in stats
                if not last_timestamp or stat['timestamp'] > last_timestamp
            ]
            valid_stats.sort(key=lambda x: x['timestamp'])

            for i in range(1, len(valid_stats)):
                current, previous = valid_stats[i], valid_stats[i - 1]

                cpu_usage = self.calculate_cpu_usage(current, previous)
                rx_rate, tx_rate = self.calculate_network_rate(current, previous)

                processed_data.append({
                    'timestamp': current['timestamp'],
                    'service': service_name,
                    'cpu_percent': cpu_usage,
                    'memory_usage_bytes': current['memory']['usage'],
                    'network_rx_bytes_sec': rx_rate,
                    'network_tx_bytes_sec': tx_rate,
                })

            if valid_stats:
                self.last_timestamps[container_id] = valid_stats[-1]['timestamp']

        return processed_data

    def save_metrics(self, processed_data: list):
        if not processed_data:
            return

        df = pd.DataFrame(processed_data)

        if not self.output_file.exists():
            df.to_csv(self.output_file, index=False)
        else:
            df.to_csv(self.output_file, mode='a', header=False, index=False)

        self.logger.info(f"Appended {len(processed_data)} records to {self.output_file}")

    def run(self):
        self.logger.info(f"Starting metrics collection. Interval: {self.collection_interval}s")
        while True:
            try:
                specs = self.get_container_specs()
                if specs:
                    processed_data = self.process_metrics(specs)
                    self.save_metrics(processed_data)

                time.sleep(self.collection_interval)
            except Exception as e:
                self.logger.error(f"Error in collection cycle: {e}", exc_info=True)
                time.sleep(1)


if __name__ == '__main__':
    collector = MetricsCollector()
    collector.run()
