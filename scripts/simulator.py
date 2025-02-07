#!/usr/bin/env python3
import asyncio
import csv
import io
import os
import random
from datetime import datetime
from typing import Optional, List

import aiohttp
import numpy as np


class IPFSSimulator:
    def __init__(self, base_port=5001, num_nodes=16, log_file="simulation_log.csv", timeout=30):
        self.nodes = [f"http://ipfs{i}:{base_port}" for i in range(num_nodes)]
        self.log_file = log_file
        self.timeout = timeout
        self._init_log_file()

    def _init_log_file(self):
        with open(self.log_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'node', 'action', 'file_size', 'cid', 'duration', 'success'])

    def _log_action(self, node: str, action: str, file_size: int, cid: str, duration: float, success: bool,
                    start_time: datetime):
        with open(self.log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                start_time.isoformat(),
                node,
                action,
                file_size,
                cid,
                duration,
                success
            ])

    def generate_random_file(self, size_bytes: int) -> bytes:
        chunk_size = 1024 * 1024 * 10
        chunks = []
        remaining_bytes = int(size_bytes)
        while remaining_bytes > 0:
            current_chunk_size = min(chunk_size, remaining_bytes)
            chunks.append(random.randbytes(current_chunk_size))
            remaining_bytes -= current_chunk_size
        return b"".join(chunks)

    async def upload_file(self, session: aiohttp.ClientSession, node_idx: int, file_size: int) -> Optional[str]:
        file_size = int(file_size)
        print(f"Uploading file of size {file_size} to node {node_idx}...")
        node_url = self.nodes[node_idx]
        random_data = self.generate_random_file(file_size)

        start_time_float = asyncio.get_event_loop().time()
        start_time = datetime.now()
        try:
            data = aiohttp.FormData()
            data.add_field('file',
                           io.BytesIO(random_data),
                           filename='random_file')

            async with session.post(
                    f"{node_url}/api/v0/add",
                    data=data,
                    timeout=self.timeout
            ) as response:
                response.raise_for_status()
                result = await response.json()
                cid = result['Hash']
                duration = asyncio.get_event_loop().time() - start_time_float
                self._log_action(f"ipfs{node_idx}", "upload", file_size, cid, duration, True, start_time)
                return cid

        except TimeoutError:
            duration = asyncio.get_event_loop().time() - start_time_float
            self._log_action(f"ipfs{node_idx}", "upload", file_size, "error", duration, False, start_time)
            return None

    async def download_file(self, session: aiohttp.ClientSession, node_idx: int, cid: str) -> Optional[bytes]:
        print(f"Downloading file {cid} from node {node_idx}...")
        node_url = self.nodes[node_idx]
        start_time_float = asyncio.get_event_loop().time()
        start_time = datetime.now()

        try:
            async with session.post(
                    f"{node_url}/api/v0/cat",
                    params={'arg': cid},
                    timeout=self.timeout
            ) as response:
                response.raise_for_status()
                content = await response.read()
                duration = asyncio.get_event_loop().time() - start_time_float
                file_size = len(content)
                self._log_action(f"ipfs{node_idx}", "download", file_size, cid, duration, True, start_time)
                return content

        except TimeoutError:
            duration = asyncio.get_event_loop().time() - start_time_float
            self._log_action(f"ipfs{node_idx}", "download", 0, cid, duration, False, start_time)
            return None

    async def run_operation(self, session: aiohttp.ClientSession, uploaded_files: List[str], mean_size: int,
                            max_size: int) -> None:
        op = random.randint(0, len(uploaded_files) + 1)
        node = random.randint(0, len(self.nodes) - 1)

        if op >= len(uploaded_files):
            size = int(np.random.exponential(mean_size))
            cid = await self.upload_file(session, node, min(size, max_size))
            if cid is not None:
                uploaded_files.append(cid)
        else:
            cid = uploaded_files[op]
            await self.download_file(session, node, cid)

    async def run_simulation(self, seed: int, num_operations: int, mean_size: int,
                             max_size: int, mean_delay: float) -> None:
        random.seed(seed)
        np.random.seed(seed)
        uploaded_files: List[str] = []

        async with aiohttp.ClientSession() as session:
            operations = []
            delay = 0
            for i in range(num_operations):
                delay += np.random.exponential(mean_delay)
                task = asyncio.create_task(self._delayed_operation(delay, session, uploaded_files, mean_size, max_size))
                operations.append(task)

            await asyncio.gather(*operations)

    async def _delayed_operation(self, delay: float, session: aiohttp.ClientSession, uploaded_files: List[str],
                                 mean_size: int, max_size: int) -> None:
        await asyncio.sleep(delay)
        await self.run_operation(session, uploaded_files, mean_size, max_size)


async def main():
    simulator = IPFSSimulator(
        num_nodes=16,
        log_file=os.getenv('SIMULATION_LOG_FILE', 'ipfs_simulation.csv'),
        timeout=30
    )
    await simulator.run_simulation(42, 100, 128 * 1024 * 1024, 512 * 1024 * 1024, 2)


if __name__ == "__main__":
    asyncio.run(main())
