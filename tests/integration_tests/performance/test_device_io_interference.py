# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Performance benchmark for concurrent multi-device I/O"""

import concurrent.futures
import os
import re
import time

import pytest

import framework.utils_fio as fio
import host_tools.drive as drive_tools
from framework.artifacts import ACPI_GUEST_KERNELS, pin_guest_kernel
from framework.utils import check_output, track_cpu_utilization
from framework.utils_iperf import IPerf3Test, emit_iperf3_metrics

pytestmark = pin_guest_kernel(ACPI_GUEST_KERNELS)

# Size of each scratch block device used in the tests, in MB.
BLOCK_DEVICE_SIZE_MB = 2048

# Seconds for which a workload "warms up" before steady-state measurement.
WARMUP_SEC = 10

# Seconds for which steady-state measurement runs after warmup.
RUNTIME_SEC = 30

# Guest memory. Large enough that the working set is not memory-bound and the
# fio workload actually exercises the block device rather than the page cache.
GUEST_MEM_MIB = 1024

# Block size for fio. 4K matches the existing block performance baseline.
FIO_BLOCK_SIZE = 4096

# Base port for iperf3 servers.
IPERF_BASE_PORT = 5000


BLOCK_MODES = [
    pytest.param(False, False, id="inline-1q"),
    pytest.param(True, False, id="threaded-1q"),
    pytest.param(True, True, id="threaded-multiqueue"),
]


def block_num_queues(vcpus, multiqueue):
    """Use one block queue per vCPU for multiqueue benchmark cases."""
    return vcpus if multiqueue else 1


def assert_block_num_queues(microvm, expected, guest_devices=("vdb",)):
    """Verify that the guest created the configured number of hardware queues."""
    for dev in guest_devices:
        _, stdout, stderr = microvm.ssh.check_output(
            f"find /sys/block/{dev}/mq -mindepth 1 -maxdepth 1 -type d | wc -l"
        )
        assert stderr == ""
        assert int(stdout) == expected


def prepare_for_io(microvm, guest_devices=("vdb",)):
    """Flush and drop caches so block measurements hit the device, not caches"""
    for dev in guest_devices:
        _, _, stderr = microvm.ssh.check_output(
            f"echo 'none' > /sys/block/{dev}/queue/scheduler"
        )
        assert stderr == ""

    # Flush all guest cached data to host, then drop guest FS caches.
    microvm.ssh.check_output("sync")
    microvm.ssh.check_output("echo 3 > /proc/sys/vm/drop_caches")

    # Flush all host cached data to hardware, then drop host FS caches.
    check_output("sync")
    check_output("echo 3 > /proc/sys/vm/drop_caches")


def fio_cmd(microvm, guest_dev, mode, runtime, warmup):
    """Build a libaio fio command targeting a raw guest block device"""
    return fio.build_cmd(
        f"/dev/{guest_dev}",
        BLOCK_DEVICE_SIZE_MB,
        FIO_BLOCK_SIZE,
        mode,
        microvm.vcpus_count,
        fio.Engine.LIBAIO,
        runtime,
        max(warmup, 1),
    )


def fio_out_dir(results_dir, guest_dev):
    """Per-device output: results_dir/device_name/"""
    out = results_dir / guest_dev
    out.mkdir(parents=True, exist_ok=True)
    return out


def run_fio_blocking(microvm, guest_dev, mode, results_dir, runtime, warmup):
    """Run fio in the guest, tracking the VMM CPU usage over the steady window"""
    cmd = fio_cmd(microvm, guest_dev, mode, runtime, warmup)
    test_output_dir = fio_out_dir(results_dir, guest_dev)
    prepare_for_io(microvm, guest_devices=(guest_dev,))

    with concurrent.futures.ThreadPoolExecutor() as executor:
        cpu_future = executor.submit(
            track_cpu_utilization,
            microvm.firecracker_pid,
            runtime,
            omit=warmup,
        )
        rc, _, stderr = microvm.ssh.run(
            f"mkdir -p /tmp/{guest_dev}; cd /tmp/{guest_dev}; {cmd}"
        )
        assert rc == 0, stderr
        assert stderr == ""

        microvm.ssh.scp_get(f"/tmp/{guest_dev}/fio.json", test_output_dir)
        microvm.ssh.scp_get(f"/tmp/{guest_dev}/*.log", test_output_dir)

        return cpu_future.result()


def submit_fio(microvm, executor, guest_dev, mode, results_dir, runtime, warmup):
    """Start fio in the guest on its own host thread; return a Future"""
    cmd = fio_cmd(microvm, guest_dev, mode, runtime, warmup)
    test_output_dir = fio_out_dir(results_dir, guest_dev)

    def _job():
        rc, _, stderr = microvm.ssh.run(
            f"mkdir -p /tmp/{guest_dev}; cd /tmp/{guest_dev}; {cmd}"
        )
        assert rc == 0, stderr
        microvm.ssh.scp_get(f"/tmp/{guest_dev}/fio.json", test_output_dir)
        microvm.ssh.scp_get(f"/tmp/{guest_dev}/*.log", test_output_dir)
        return rc

    return executor.submit(_job)


def emit_fio_metrics(results_dir, metrics, guest_devices=("vdb",)):
    """Parse fio bw/clat logs and emit them as metrics, aggregated across devices"""
    all_bw_reads, all_bw_writes = [], []
    all_clat_reads, all_clat_writes = [], []
    for dev in guest_devices:
        out_dir = results_dir / dev
        bw_reads, bw_writes = fio.process_log_files(out_dir, fio.LogType.BW)
        all_bw_reads.extend(bw_reads)
        all_bw_writes.extend(bw_writes)
        clat_reads, clat_writes = fio.process_log_files(out_dir, fio.LogType.CLAT)
        all_clat_reads.extend(clat_reads)
        all_clat_writes.extend(clat_writes)

    for tup in zip(*all_bw_reads):
        metrics.put_metric("bw_read", sum(tup), "Kilobytes/Second")
    for tup in zip(*all_bw_writes):
        metrics.put_metric("bw_write", sum(tup), "Kilobytes/Second")

    # latency values in fio logs are in nanoseconds, but cloudwatch only supports
    # microseconds as the more granular unit, so need to divide by 1000.
    for tup in zip(*all_clat_reads):
        for value in tup:
            metrics.put_metric("clat_read", value / 1000, "Microseconds")
    for tup in zip(*all_clat_writes):
        for value in tup:
            metrics.put_metric("clat_write", value / 1000, "Microseconds")


def emit_cpu_metrics(cpu_util, metrics):
    """Emit per-thread CPU utilization.
    cpu_utilization_firecracker is the VMM thread"""
    for thread_name, values in cpu_util.items():
        for value in values:
            metrics.put_metric(f"cpu_utilization_{thread_name}", value, "Percent")


def consume_ping_output(ping_output):
    """Yield per-packet RTTs (ms) from ping output."""
    output = ping_output.strip().split("\n")
    assert len(output) > 2

    # Compute percentiles.
    pattern_time = ".+ bytes from .+: icmp_seq=.+ ttl=.+ time=(.+) ms"
    for seq in output:
        time = re.findall(pattern_time, seq)
        if time:
            assert len(time) == 1
            yield float(time[0])


@pytest.mark.nonci
@pytest.mark.timeout(300)
@pytest.mark.parametrize("vcpus", [2, 4], ids=["2vcpu", "4vcpu"])
@pytest.mark.parametrize("threaded,multiqueue", BLOCK_MODES)
@pytest.mark.parametrize("scenario", ["block_only", "net_only", "concurrent"])
def test_block_net_throughput_interference(
    uvm,
    vcpus,
    threaded,
    multiqueue,
    scenario,
    io_engine,
    metrics,
    results_dir,
):
    """Measure block throughput and network throughput, isolated vs concurrent
    * ``block_only``  -> fio random-write to /dev/vdb, nothing else.
    * ``net_only``    -> iperf3 guest->host, nothing else.
    * ``concurrent``  -> both at once, on overlapping steady-state windows

    The block modes compare the inline path, one threaded queue, and one
    threaded queue per vCPU.
    """
    num_queues = block_num_queues(vcpus, multiqueue)
    vm = uvm
    vm.spawn(log_level="Info", emit_metrics=True)
    vm.basic_config(vcpu_count=vcpus, mem_size_mib=GUEST_MEM_MIB)
    vm.add_net_iface()
    # Add a secondary block device for benchmark tests.
    fs = drive_tools.FilesystemFile(
        os.path.join(vm.fsfiles, "scratch"), BLOCK_DEVICE_SIZE_MB
    )
    vm.add_drive(
        "scratch",
        fs.path,
        io_engine=io_engine,
        threaded=threaded,
        num_queues=num_queues,
    )
    vm.start()
    assert_block_num_queues(vm, num_queues)

    metrics.set_dimensions(
        {
            "performance_test": "test_block_net_throughput_interference",
            "io_engine": io_engine,
            "threaded": str(threaded),
            "num_queues": str(num_queues),
            "scenario": scenario,
            **vm.dimensions,
        }
    )

    first_free_cpu = vm.pin_threads(0)

    if scenario == "block_only":
        cpu_util = run_fio_blocking(
            vm, "vdb", fio.Mode.RANDWRITE, results_dir, RUNTIME_SEC, WARMUP_SEC
        )
        emit_fio_metrics(results_dir, metrics)
        emit_cpu_metrics(cpu_util, metrics)
        return

    # Both net_only and concurrent run iperf3. We align fio and iperf on the
    # same total duration (WARMUP + RUNTIME) and omit the same warmup window so
    # their steady-state measurement windows overlap.
    iperf_test = IPerf3Test(
        microvm=vm,
        base_port=IPERF_BASE_PORT,
        runtime=WARMUP_SEC + RUNTIME_SEC,
        omit=WARMUP_SEC,
        mode="g2h",
        num_clients=vm.vcpus_count,
        connect_to=vm.iface["eth0"]["iface"].host_ip,
        payload_length="1024K",
    )

    if scenario == "net_only":
        data = iperf_test.run_test(first_free_cpu)
        emit_iperf3_metrics(metrics, data, WARMUP_SEC)
        return

    # scenario == "concurrent": launch fio in the background, then run iperf so
    # both are active during iperf's measurement window. iperf's own CPU
    # tracking captures the (shared, today) VMM thread saturation.
    prepare_for_io(vm, guest_devices=("vdb",))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        fio_future = submit_fio(
            vm,
            executor,
            "vdb",
            fio.Mode.RANDWRITE,
            results_dir,
            RUNTIME_SEC,
            WARMUP_SEC,
        )
        data = iperf_test.run_test(first_free_cpu)
        # Make sure the block workload finished cleanly.
        assert fio_future.result() == 0

    emit_fio_metrics(results_dir, metrics)
    emit_iperf3_metrics(metrics, data, WARMUP_SEC)


@pytest.mark.nonci
@pytest.mark.timeout(240)
@pytest.mark.parametrize("vcpus", [2, 4], ids=["2vcpu", "4vcpu"])
@pytest.mark.parametrize("threaded,multiqueue", BLOCK_MODES)
@pytest.mark.parametrize("scenario", ["idle", "under_block_load"])
def test_network_latency_under_block_load(
    uvm,
    vcpus,
    threaded,
    multiqueue,
    scenario,
    io_engine,
    metrics,
    results_dir,
):
    """Measure guest->host ping latency with and without a heavy block writer"""
    num_queues = block_num_queues(vcpus, multiqueue)
    vm = uvm
    vm.spawn(log_level="Info", emit_metrics=True)
    vm.basic_config(vcpu_count=vcpus, mem_size_mib=GUEST_MEM_MIB)
    vm.add_net_iface()
    fs = drive_tools.FilesystemFile(
        os.path.join(vm.fsfiles, "scratch"), BLOCK_DEVICE_SIZE_MB
    )
    vm.add_drive(
        "scratch",
        fs.path,
        io_engine=io_engine,
        threaded=threaded,
        num_queues=num_queues,
    )
    vm.start()
    assert_block_num_queues(vm, num_queues)

    metrics.set_dimensions(
        {
            "performance_test": "test_network_latency_under_block_load",
            "io_engine": io_engine,
            "threaded": str(threaded),
            "num_queues": str(num_queues),
            "scenario": scenario,
            **vm.dimensions,
        }
    )

    vm.pin_threads(0)
    host_ip = vm.iface["eth0"]["iface"].host_ip

    # ~150 pings at 200ms spacing ≈ 30s of measurement.
    ping_count = 150
    ping_interval = 0.2

    def _ping():
        _, ping_output, _ = vm.ssh.check_output(
            f"ping -c {ping_count} -i {ping_interval} {host_ip}"
        )
        return list(consume_ping_output(ping_output))

    if scenario == "idle":
        samples = _ping()
    else:
        # Drive a sustained random-write workload (no warmup, so it is already
        # saturating the shared thread when ping starts) and ping during it.
        prepare_for_io(vm, guest_devices=("vdb",))
        # fio runtime must cover the warmup ramp plus the full ping duration,
        # with margin, so the block device stays busy for every ping sample.
        fio_runtime = WARMUP_SEC + int(ping_count * ping_interval) + 20
        cmd = fio_cmd(vm, "vdb", fio.Mode.RANDWRITE, fio_runtime, 0)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            fio_future = executor.submit(
                lambda: vm.ssh.run(f"mkdir -p /tmp/vdb; cd /tmp/vdb; {cmd}")
            )
            # Let fio ramp into steady-state so the block device is genuinely
            # saturating the shared thread before we start measuring latency.
            time.sleep(WARMUP_SEC)
            samples = _ping()
            rc, _, stderr = fio_future.result()
            assert rc == 0, stderr
        vm.ssh.scp_get("/tmp/vdb/fio.json", results_dir)

    assert samples, "ping produced no latency samples"
    for sample in samples:
        metrics.put_metric("ping_latency", sample, "Milliseconds")


@pytest.mark.nonci
@pytest.mark.timeout(300)
@pytest.mark.parametrize("vcpus", [2, 4], ids=["2vcpu", "4vcpu"])
@pytest.mark.parametrize("threaded,multiqueue", BLOCK_MODES)
@pytest.mark.parametrize("scenario", ["single", "dual"])
def test_multi_block_throughput(
    uvm,
    vcpus,
    threaded,
    multiqueue,
    scenario,
    io_engine,
    metrics,
    results_dir,
):
    """Aggregate block throughput across one vs two block devices.
    Compares aggregate ``bw_write`` between ``single`` and ``dual``

    The threaded multiqueue mode creates one worker per vCPU for each scratch
    device.
    """
    num_queues = block_num_queues(vcpus, multiqueue)
    vm = uvm
    vm.spawn(log_level="Info", emit_metrics=True)
    vm.basic_config(vcpu_count=vcpus, mem_size_mib=GUEST_MEM_MIB)
    vm.add_net_iface()

    # Always attach two scratch devices so the guest layout (vdb, vdc) is
    # identical across scenarios; the ``single`` scenario simply leaves vdc idle.
    fs_b = drive_tools.FilesystemFile(
        os.path.join(vm.fsfiles, "scratch_b"), BLOCK_DEVICE_SIZE_MB
    )
    vm.add_drive(
        "scratch_b",
        fs_b.path,
        io_engine=io_engine,
        threaded=threaded,
        num_queues=num_queues,
    )
    fs_c = drive_tools.FilesystemFile(
        os.path.join(vm.fsfiles, "scratch_c"), BLOCK_DEVICE_SIZE_MB
    )
    vm.add_drive(
        "scratch_c",
        fs_c.path,
        io_engine=io_engine,
        threaded=threaded,
        num_queues=num_queues,
    )
    vm.start()
    assert_block_num_queues(vm, num_queues, guest_devices=("vdb", "vdc"))

    metrics.set_dimensions(
        {
            "performance_test": "test_multi_block_throughput",
            "io_engine": io_engine,
            "threaded": str(threaded),
            "num_queues": str(num_queues),
            "scenario": scenario,
            **vm.dimensions,
        }
    )

    vm.pin_threads(0)

    if scenario == "single":
        cpu_util = run_fio_blocking(
            vm, "vdb", fio.Mode.RANDWRITE, results_dir, RUNTIME_SEC, WARMUP_SEC
        )
        emit_fio_metrics(results_dir, metrics)
        emit_cpu_metrics(cpu_util, metrics)
        return

    # scenario == "dual": hammer both devices concurrently, track the VMM thread
    # CPU across the shared steady-state window.
    prepare_for_io(vm, guest_devices=("vdb", "vdc"))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        cpu_future = executor.submit(
            track_cpu_utilization,
            vm.firecracker_pid,
            RUNTIME_SEC,
            omit=WARMUP_SEC,
        )
        fio_b = submit_fio(
            vm,
            executor,
            "vdb",
            fio.Mode.RANDWRITE,
            results_dir,
            RUNTIME_SEC,
            WARMUP_SEC,
        )
        fio_c = submit_fio(
            vm,
            executor,
            "vdc",
            fio.Mode.RANDWRITE,
            results_dir,
            RUNTIME_SEC,
            WARMUP_SEC,
        )
        assert fio_b.result() == 0
        assert fio_c.result() == 0
        cpu_util = cpu_future.result()

    # Aggregate bandwidth across both devices: bw_write is the sum of vdb+vdc,
    # so it can be compared directly against the single-device scenario to see
    # whether a second device adds throughput or just contends for the thread.
    emit_fio_metrics(results_dir, metrics, guest_devices=("vdb", "vdc"))
    emit_cpu_metrics(cpu_util, metrics)
