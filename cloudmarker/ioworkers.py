"""Concurrent input/output workers implementation.

This module offers a :func:`run` function to run a specified function in
a large number of threads. Unlike the standard library :mod:`threading`
module of Python, this module lets us run threads on multiple CPUs at
the same time. This is achieved by first creating multiple processes
using the standard library :mod:`multiprocessing` package. These
processes can utilize multiple CPUs. The threads are then launched under
these multiple processes.
"""


import multiprocessing
import os
import threading


def run(input_func, output_func, processes=0, threads=0):
    """Run concurrent input/output workers with specified functions.

    A two-level hierarchy of workers are created using both
    multiprocessing as well as multithreading. At first, ``processes``
    number of worker processes are created. Then within each process
    worker, ``threads`` number of worker threads are created. Thus, in
    total, ``processes * threads`` number of worker threads are created.

    Arguments:
        input_func (callable): A callable which when called yields
            tuples. Each tuple must represent arguments to be passed to
            ``output_func``.
        output_func (callable): A callable that can accept as arguments
            an unpacked tuple yielded by ``input_func``. When called,
            this callable must work on the arguments and return an
            output value. This callable must not return ``None`` for any
            input.
        processes (int): Number of worker processes to run. If
            unspecified or ``0`` or negative integer is specified, then
            the number returned by :func:`os.cpu_count` is used.
        threads (int): Number of worker threads to run in each process.
            If unspecified or ``0`` or negative integer is specified,
            then `5` multiplied by the number returned by
            :func:`os.cpu_count` is used.

    Yields:
        Each output value returned by ``output_func``.

    """
    if processes <= 0:
        processes = os.cpu_count()

    if threads <= 0:
        threads = os.cpu_count() * 5

    in_q = multiprocessing.Queue()
    out_q = multiprocessing.Queue()

    # Create process workers.
    process_workers = []
    for _ in range(processes):
        w = multiprocessing.Process(target=_process_worker,
                                    args=(in_q, out_q, threads,
                                          output_func))
        w.start()
        process_workers.append(w)

    # Get input data for thread workers to work on.
    for args in input_func():
        in_q.put(args)

    # Tell each thread worker that there is no more input to work on.
    for _ in range(processes * threads):
        in_q.put(None)

    # Consume output objects from thread workers and yield them.
    stopped_threads = 0
    while True:
        record = out_q.get()
        if record is None:
            stopped_threads += 1
            if stopped_threads == processes * threads:
                break
            continue
        yield record

    # Wait for process workers to terminate.
    for w in process_workers:
        w.join()


def _process_worker(in_q, out_q, threads, output_func):
    """Process worker."""
    thread_workers = []
    for _ in range(threads):
        w = threading.Thread(target=_thread_worker,
                             args=(in_q, out_q, output_func))
        w.start()
        thread_workers.append(w)
    for w in thread_workers:
        w.join()


def _thread_worker(in_q, out_q, output_func):
    """Thread worker."""
    while True:
        work = in_q.get()
        if work is None:
            out_q.put(None)
            break
        for record in output_func(*work):
            out_q.put(record)
