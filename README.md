# wekalib

A group of Python modules that implement some handy interfaces to Weka Clusters.

class WekaCluster (wekacluster.py) - a high-level abstraction for a weka cluster.  WekaCluster.call_api() is the method for making API calls.  API calls are distributed across cluster backend hosts.  Error checking and retries automatically handled.

class WekaApi (wekaapi.py) - Low-level API calls to a single cluster backend host.  WekaApi.weka_api_command() is the method for making API calls to a particular backend host.

class simul_threads (sthreads.py) - manage threads (standard threading library threads).  Intended for short-duration threads that all run simultaneously.  Queue up many threads, set how many are to run simultaneously, then run all and wait for them.  If more threads are queued than the number it can run simultaneously, it will automatically start new threads as older ones finish.   simul_threads.run() method does not return until all threads are completed.   Useful for sending lots of API calls to the cluster in a batch.  Used by WekaCluster.

class circular_list (circular.py) - implement a circular linked list.

wekatime.py - a handful of functions that translate Weka 'd' timestamps to/from datetime objects and other formats.

class signal_handling (signals.py) - set up signal handlers commonly used. 
