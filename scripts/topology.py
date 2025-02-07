#!/usr/bin/env python3

import argparse
import csv
import os
import datetime as dt
from pathlib import Path
from random import choices
from typing import List, Any
import socket

import requests


class IPFSTopologyManager:
    def __init__(self, container_names: List[str], reset_file: bool, api_port: int = 5001, api_version: str = 'v0'):
        self.names = container_names
        self.urls = [f'http://{name}:{api_port}/api/{api_version}' for name in container_names]
        self.num_nodes = len(container_names)
        self.log_file = Path(os.getenv('TOPOLOGY_LOG_FILE', 'topology.csv'))
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        if reset_file or not self.log_file.exists():
            with open(self.log_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'action', 'num_nodes', 'status', 'details'])

    def log_execution(self, action: str, status: str = 'completed', details: str = '') -> None:
        with open(self.log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                dt.datetime.now(dt.UTC).isoformat(),
                action,
                self.num_nodes,
                status,
                details
            ])

    def resolve_container_ip(self, index: int) -> str | None:
        try:
            container_name = self.names[index]
            return socket.gethostbyname(container_name)
        except socket.gaierror as e:
            print(f"Failed to resolve IP for container {self.names[index]}: {e}")
            return None

    def get_node_id(self, index: int) -> Any | None:
        try:
            response = requests.post(f'{self.urls[index]}/id')
            return response.json()['ID']
        except Exception as e:
            print(f"Failed to get node ID from {self.names[index]}: {e}")
            return None

    def connect_nodes(self, source_index: int, target_index: int) -> bool:
        try:
            target_ip = self.resolve_container_ip(target_index)
            if not target_ip:
                return False

            target_id = self.get_node_id(target_index)
            if not target_id:
                return False

            response = requests.post(
                f'{self.urls[source_index]}/swarm/connect',
                params={'arg': f'/ip4/{target_ip}/tcp/4001/p2p/{target_id}'}
            )
            if response.status_code == 200:
                print(f"Connected {self.names[source_index]} to {self.names[target_index]}")
                return True
            else:
                print(f"Connection failed with status {response.status_code}. Response: {response.text}")
                return False
        except Exception as e:
            print(f"Failed to connect {self.names[source_index]} to {self.names[target_index]}. Error: {e}")
            return False

    def create_ring_topology(self) -> None:
        print("Creating ring topology...")
        for i in range(len(self.urls)):
            next_node = (i + 1) % len(self.urls)
            self.connect_nodes(i, next_node)

    def create_grid_topology(self) -> None:
        print("Creating grid topology...")
        size = int(len(self.urls) ** 0.5)  # Calculate grid dimensions
        if size * size != len(self.urls):
            raise ValueError("Number of nodes must be a perfect square for grid topology")

        for i in range(size):
            for j in range(size):
                current = i * size + j
                if j < size - 1:
                    self.connect_nodes(current, current + 1)
                if i < size - 1:
                    self.connect_nodes(current, current + size)

    def create_fully_connected_topology(self) -> None:
        print("Creating fully connected topology...")
        for i in range(len(self.urls)):
            for j in range(i + 1, len(self.urls)):
                self.connect_nodes(i, j)

    def create_barabasi_albert_topology(self, m: int = 2) -> None:
        print(f"Creating Barabási-Albert topology with m={m}...")
        if m < 1 or m >= len(self.urls):
            raise ValueError("Parameter 'm' must be >= 1 and less than the number of nodes")

        connected = set()
        connected.add(0)

        for new_node_index in range(1, len(self.urls)):
            targets = list(connected)
            probabilities = [1 / len(targets)] * len(targets)
            selected = choices(targets, probabilities, k=min(m, len(targets)))

            for target_index in selected:
                if self.connect_nodes(new_node_index, target_index):
                    connected.add(new_node_index)

    def read_connection_matrix(self) -> None:
        print("Reading connection matrix...")
        matrix = [[0] * self.num_nodes for _ in range(self.num_nodes)]

        for i in range(self.num_nodes):
            try:
                response = requests.post(f'{self.urls[i]}/swarm/peers?verbose=true')
                if response.status_code == 200:
                    peers = response.json().get("Peers", [])
                    if not peers:
                        continue

                    kad_peers = [
                        peer for peer in peers
                        if any(stream.get("Protocol") == "/ipfs/lan/kad/1.0.0" for stream in peer.get("Streams", []))
                    ]

                    for peer in kad_peers:
                        peer_id = peer.get("Peer")
                        for j in range(self.num_nodes):
                            if self.get_node_id(j) == peer_id:
                                matrix[i][j] = 1
                else:
                    print(f"Failed to get peers for {self.names[i]}: {response.text}")
            except Exception as e:
                print(f"Error reading peers for {self.names[i]}: {e}")

        print("Connection Matrix:")
        for i, row in enumerate(matrix):
            print(" ".join("x" if i == j and val == 0 else str(val) for j, val in enumerate(row)))

def main():
    parser = argparse.ArgumentParser(description='Configure IPFS node topology')
    parser.add_argument('--topology', '-t', choices=['ring', 'grid', 'full', 'barabasi'],
                        help='Topology type to create')
    parser.add_argument('--nodes', '-n', type=int, default=16,
                        help='Number of IPFS nodes (default: 16)')
    parser.add_argument('--matrix', '-m', action='store_true',
                        help='Only print out the connection matrix')

    args = parser.parse_args()

    container_names = [f'ipfs{i}' for i in range(args.nodes)]

    manager = IPFSTopologyManager(container_names, args.topology is not None)

    try:
        if args.matrix:
            manager.read_connection_matrix()
            manager.log_execution('matrix_read')
        elif args.topology:
            if args.topology == 'ring':
                manager.create_ring_topology()
            elif args.topology == 'grid':
                manager.create_grid_topology()
            elif args.topology == 'full':
                manager.create_fully_connected_topology()
            elif args.topology == 'barabasi':
                manager.create_barabasi_albert_topology()
            manager.read_connection_matrix()
            manager.log_execution(f'topology_{args.topology}')
        else:
            print("Error: You must specify --topology, --matrix or --disconnect-all")
            manager.log_execution('invalid_args', 'error', 'No action specified')
    except Exception as e:
        manager.log_execution(
            'error',
            'failed',
            f'{type(e).__name__}: {str(e)}'
        )
        raise


if __name__ == "__main__":
    main()
