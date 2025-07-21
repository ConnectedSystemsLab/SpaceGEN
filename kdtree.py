import heapq
from scipy.spatial import KDTree
from collections import defaultdict, deque

class NearestQueueMap:
    def __init__(self):
        self.point_to_queue = defaultdict(deque)  # Map (x, y) to a queue
        self.kdtree = None                        # KDTree for efficient nearest search
        self.points = []                          # List of points in the tree

    def _update_kdtree(self):
        """Rebuild the KDTree based on the non-empty points."""
        self.points = [point for point, queue in self.point_to_queue.items() if queue]
        if self.points:
            self.kdtree = KDTree(self.points)
        else:
            self.kdtree = None

    def add_point(self, x, y, value, auto_update=True):
        """Add a value to the queue at (x, y)."""
        empty_queue = len(self.point_to_queue[(x, y)]) == 0
        self.point_to_queue[(x, y)].append(value)
        if auto_update and empty_queue:
            self._update_kdtree()

    def get_nearest_non_empty(self, x, y):
        """Find the nearest (x, y) with a non-empty queue."""
        if not self.kdtree:
            return None, None  # No non-empty points
        
        # Query the nearest neighbor
        distance, index = self.kdtree.query((x, y))
        nearest_point = tuple(self.points[index])
        return nearest_point

    def pop_from_nearest(self, x, y):
        """Pop a value from the nearest non-empty queue."""
        nearest_point = self.get_nearest_non_empty(x, y)
        if nearest_point == (None, None):
            return None  # No non-empty queues
        
        value = self.point_to_queue[nearest_point].popleft()
        if not self.point_to_queue[nearest_point]:  # If queue is empty, rebuild KDTree
            self._update_kdtree()
        return value

class NearestQueueMap1D:
    def __init__(self):
        self.point_to_queue = defaultdict(deque)  # Map (x, y) to a queue
        self.kdtree = None                        # KDTree for efficient nearest search
        self.points = []                          # List of points in the tree

    def _update_kdtree(self):
        """Rebuild the KDTree based on the non-empty points."""
        self.points = [point for point, queue in self.point_to_queue.items() if queue]
        if self.points:
            self.kdtree = KDTree(self.points)
        else:
            self.kdtree = None

    def add_point(self, x, value):
        """Add a value to the queue at (x, y)."""
        empty_queue = len(self.point_to_queue[(x, )]) == 0
        self.point_to_queue[(x, )].append(value)
        if empty_queue:
            self._update_kdtree()

    def get_nearest_non_empty(self, x):
        """Find the nearest (x, y) with a non-empty queue."""
        if not self.kdtree:
            return None, None  # No non-empty points
        
        # Query the nearest neighbor
        distance, index = self.kdtree.query((x, ))
        nearest_point = tuple(self.points[index])
        return nearest_point

    def pop_from_nearest(self, x):
        """Pop a value from the nearest non-empty queue."""
        nearest_point = self.get_nearest_non_empty(x,)
        if nearest_point == (None, None):
            return None  # No non-empty queues
        
        value = self.point_to_queue[nearest_point].popleft()
        if not self.point_to_queue[nearest_point]:  # If queue is empty, rebuild KDTree
            self._update_kdtree()
        return value