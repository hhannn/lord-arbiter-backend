# runner_registry.py
import threading
from typing import Dict
from threading import Lock

running_threads = {}
running_threads_lock = Lock()

running_threads: Dict[int, threading.Thread] = {}