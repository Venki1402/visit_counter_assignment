import hashlib
from typing import List, Dict, Any
from bisect import bisect

class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100):
        """
        Initialize the consistent hash ring
        
        Args:
            nodes: List of node identifiers (parsed from comma-separated string)
            virtual_nodes: Number of virtual nodes per physical node
        """
        
        # TODO: Initialize the hash ring with virtual nodes
        # 1. For each physical node, create virtual_nodes number of virtual nodes
        # 2. Calculate hash for each virtual node and map it to the physical node
        # 3. Store the mapping in hash_ring and maintain sorted_keys
        self.virtual_nodes = virtual_nodes
        self.hash_ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        for node in nodes:
            self.add_node(node)
    
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
        # TODO: Implement adding a new node
        # 1. Create virtual nodes for the new physical node
        # 2. Update hash_ring and sorted_keys
        
        
        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node}_{i}"
            hash_val = self._hash(virtual_node_key)
            
            # Adding to hash ring
            self.hash_ring[hash_val] = node
            
            # Insert into sorted keys
            index = bisect(self.sorted_keys, hash_val)
            self.sorted_keys.insert(index, hash_val)
        

    def remove_node(self, node: str) -> None:
        """
        Remove a node from the hash ring
        
        Args:
            node: Node identifier to remove
        """
        # TODO: Implement removing a node
        # 1. Remove all virtual nodes for the given physical node
        # 2. Update hash_ring and sorted_keys
        
        keys_to_remove = []
        for hash_val, phys_node in self.hash_ring.items():
            if phys_node == node:
                keys_to_remove.append(hash_val)
                
        for hash_val in keys_to_remove:
            self.hash_ring.pop(hash_val)
            self.sorted_keys.remove(hash_val)
        

    def get_node(self, key: str) -> str:
        """
        Get the node responsible for the given key
        
        Args:
            key: The key to look up
            
        Returns:
            The node responsible for the key
        """
        # TODO: Implement node lookup
        # 1. Calculate hash of the key
        # 2. Find the first node in the ring that comes after the key's hash
        # 3. If no such node exists, wrap around to the first node
        
        hash_val = self._hash(key)
        index = bisect(self.sorted_keys, hash_val)
        
        if index >= len(self.sorted_keys):
            index = 0
            
        return self.hash_ring[self.sorted_keys[index]]
    