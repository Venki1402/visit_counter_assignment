import hashlib
from typing import List, Dict, Any
from bisect import bisect
from app.logger import logger


class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100):
        """
        Initialize the consistent hash ring

        Args:
            nodes: List of node identifiers (parsed from comma-separated string)
            virtual_nodes: Number of virtual nodes per physical node
        """
        self.virtual_nodes = virtual_nodes
        self.hash_ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []

        # Add each node to the hash ring
        for node in nodes:
            self.add_node(node)

        logger.info(
            f"Initialized consistent hash ring with {len(nodes)} nodes and {virtual_nodes} virtual nodes each"
        )

    def _hash(self, key: str) -> int:
        """
        Generate a hash for a key.
        """
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str) -> None:
        """
        Add a new node to the hash ring

        Args:
            node: Node identifier to add
        """
        # Create virtual nodes for the new physical node
        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node}_{i}"
            hash_val = self._hash(virtual_node_key)

            # Adding to hash ring
            self.hash_ring[hash_val] = node

            # Insert into sorted keys while maintaining order
            index = bisect(self.sorted_keys, hash_val)
            self.sorted_keys.insert(index, hash_val)

        logger.info(
            f"Added node {node} to hash ring with {self.virtual_nodes} virtual nodes"
        )

    def remove_node(self, node: str) -> None:
        """
        Remove a node from the hash ring

        Args:
            node: Node identifier to remove
        """
        # Find all virtual nodes for this physical node
        keys_to_remove = []
        for hash_val, phys_node in self.hash_ring.items():
            if phys_node == node:
                keys_to_remove.append(hash_val)

        # Remove from hash ring and sorted keys
        for hash_val in keys_to_remove:
            self.hash_ring.pop(hash_val)
            self.sorted_keys.remove(hash_val)

        logger.info(
            f"Removed node {node} from hash ring (removed {len(keys_to_remove)} virtual nodes)"
        )

    def get_node(self, key: str) -> str:
        """
        Get the node responsible for the given key

        Args:
            key: The key to look up

        Returns:
            The node responsible for the key
        """
        if not self.sorted_keys:
            raise ValueError("Hash ring is empty")

        # Calculate hash of the key
        hash_val = self._hash(key)

        # Find the first node in the ring that comes after the key's hash
        index = bisect(self.sorted_keys, hash_val)

        # If we went past the end of the ring, wrap around to the first node
        if index >= len(self.sorted_keys):
            index = 0

        # Return the physical node
        return self.hash_ring[self.sorted_keys[index]]
