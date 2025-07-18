# runner_registry.py
import threading
from typing import Dict

running_threads: Dict[int, threading.Thread] = {}