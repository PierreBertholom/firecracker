# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests that ensure the boot time to init process is within spec."""

import datetime
import os
import re
import time

import pytest

import host_tools.drive as drive_tools
from framework.artifacts import ACPI_GUEST_KERNELS, pin_guest_kernel, pin_rootfs_mode

# Regex for obtaining boot time from some string.

DEFAULT_BOOT_ARGS = (
    "reboot=k panic=1 nomodule 8250.nr_uarts=0"
    " i8042.noaux i8042.nomux i8042.nopnp i8042.dumbkbd swiotlb=noforce cryptomgr.notests"
)

pytestmark = pin_guest_kernel(ACPI_GUEST_KERNELS)


def get_boottime_device_info(vm):
    """Auxiliary function for asserting the expected boot time."""
    boot_time_us = None
    boot_time_cpu_us = None
    timestamps = []

    timestamp_log_regex = (
        r"Guest-boot-time =\s+(\d+) us\s+(\d+) ms,\s+(\d+) CPU us\s+(\d+) CPU ms"
    )

    iterations = 50
    sleep_time_s = 0.1
    for _ in range(iterations):
        timestamps = re.findall(timestamp_log_regex, vm.log_data)
        if timestamps:
            break
        time.sleep(sleep_time_s)
    if timestamps:
        boot_time_us, _, boot_time_cpu_us, _ = timestamps[0]

    assert boot_time_us and boot_time_cpu_us, (
        f"MicroVM did not boot within {sleep_time_s * iterations}s\n"
        f"Firecracker logs:\n{vm.log_data}\n"
        f"Thread backtraces:\n{vm.thread_backtraces}"
    )
    return int(boot_time_us), int(boot_time_cpu_us)


def find_events(log_data):
    """
    Parse events in the Firecracker logs

    Events have this format:

        TIMESTAMP [LOGLEVEL] event_(start|end): EVENT
    """
    ts_fmt = "%Y-%m-%dT%H:%M:%S.%f"
    matches = re.findall(r"(.+) \[.+\] event_(start|end): (.*)", log_data)
    timestamps = {}
    for ts, when, what in matches:
        evt1 = timestamps.setdefault(what, {})
        evt1[when] = datetime.datetime.strptime(ts[:-3], ts_fmt)
    for _, val in timestamps.items():
        val["duration"] = val["end"] - val["start"]
    return timestamps


def get_systemd_analyze_times(microvm):
    """
    Parse systemd-analyze output
    """
    rc, stdout, stderr = microvm.ssh.run("systemd-analyze")
    assert rc == 0, stderr
    assert stderr == ""

    boot_line = stdout.splitlines()[0]
    # The line will look like this:
    # Startup finished in 79ms (kernel) + 231ms (userspace) = 310ms
    # In the regex we capture the time and the unit for kernel, userspace and total values
    pattern = r"Startup finished in ([\d.]*)(ms|s)\s+\(kernel\) \+ ([\d.]*)(ms|s)\s+\(userspace\) = ([\d.]*)(ms|s)\s*"
    kernel, kernel_unit, userspace, userspace_unit, total, total_unit = re.findall(
        pattern, boot_line
    )[0]

    def to_ms(v, unit):
        match unit:
            case "ms":
                return float(v)
            case "s":
                return float(v) * 1000

    kernel = to_ms(kernel, kernel_unit)
    userspace = to_ms(userspace, userspace_unit)
    total = to_ms(total, total_unit)

    return kernel, userspace, total


def launch_vm_with_boot_timer(
    microvm_factory,
    guest_kernel,
    rootfs,
    vcpu_count,
    mem_size_mib,
    pci_enabled,
    boot_from_pmem,
    pool_size=None,
    num_scratch_drives=0,
):
    """Launches a microVM with guest-timer and returns the reported metrics for it"""
    vm = microvm_factory.build(
        guest_kernel, rootfs, pci=pci_enabled, monitor_memory=False
    )
    vm.jailer.extra_args.update({"boot-timer": None})
    vm.spawn()
    if not boot_from_pmem:
        vm.basic_config(
            vcpu_count=vcpu_count,
            mem_size_mib=mem_size_mib,
            boot_args=DEFAULT_BOOT_ARGS + " init=/usr/local/bin/init",
            enable_entropy_device=True,
            pool_size=pool_size,
        )
    else:
        vm.basic_config(
            add_root_device=False,
            vcpu_count=vcpu_count,
            mem_size_mib=mem_size_mib,
            boot_args=DEFAULT_BOOT_ARGS + " init=/usr/local/bin/init rootflags=dax",
            enable_entropy_device=True,
            pool_size=pool_size,
        )
        vm.add_pmem("pmem", rootfs, True, True)

    # Attach extra scratch block devices to stress the boot-time cost of
    # registering many devices (and, with a pool, handing each to a worker).
    # They are only probed at boot, never written, so a tiny size is enough.
    for idx in range(num_scratch_drives):
        fs = drive_tools.FilesystemFile(
            os.path.join(vm.fsfiles, f"scratch_{idx}"), size=8
        )
        vm.add_drive(f"scratch_{idx}", fs.path, io_engine="Sync")

    vm.add_net_iface()
    vm.start()
    vm.pin_threads(0)

    boot_time_us, cpu_boot_time_us = get_boottime_device_info(vm)

    return (vm, boot_time_us, cpu_boot_time_us)


def test_boot_timer(microvm_factory, guest_kernel, rootfs, pci_enabled):
    """Tests that the boot timer device works"""
    launch_vm_with_boot_timer(
        microvm_factory, guest_kernel, rootfs, 1, 128, pci_enabled, False
    )


@pytest.mark.parametrize(
    "vcpu_count,mem_size_mib",
    [(1, 128), (1, 1024), (2, 2048), (4, 4096)],
)
@pin_rootfs_mode("rw")
@pytest.mark.parametrize("boot_from_pmem", [True, False], ids=["PmemBoot", "BlockBoot"])
@pytest.mark.parametrize("pool_size", [0, 2], ids=["pool0", "pool2"])
@pytest.mark.nonci
def test_boottime(
    microvm_factory,
    guest_kernel,
    rootfs,
    vcpu_count,
    mem_size_mib,
    boot_from_pmem,
    pool_size,
    pci_enabled,
    metrics,
):
    """Test boot time with different guest configurations.

    Swept across ``pool_size`` (0 = legacy shared VMM thread, 2 = block devices
    on worker threads) to measure the boot-path cost of pre-spawning the pool
    and handing devices to worker threads.
    """

    for i in range(10):
        vm, boot_time_us, cpu_boot_time_us = launch_vm_with_boot_timer(
            microvm_factory,
            guest_kernel,
            rootfs,
            vcpu_count,
            mem_size_mib,
            pci_enabled,
            boot_from_pmem,
            pool_size=pool_size,
        )

        if i == 0:
            metrics.set_dimensions(
                {
                    "performance_test": "test_boottime",
                    "boot_from_pmem": str(boot_from_pmem),
                    "pool_size": str(pool_size),
                    **vm.dimensions,
                }
            )

        metrics.put_metric(
            "guest_boot_time",
            boot_time_us,
            unit="Microseconds",
        )
        metrics.put_metric(
            "guest_cpu_boot_time",
            cpu_boot_time_us,
            unit="Microseconds",
        )

        events = find_events(vm.log_data)
        build_time = events["build microvm for boot"]["duration"]
        metrics.put_metric("build_time", build_time.microseconds, unit="Microseconds")
        resume_time = events["boot microvm"]["duration"]
        metrics.put_metric("resume_time", resume_time.microseconds, unit="Microseconds")

        kernel, userspace, total = get_systemd_analyze_times(vm)
        metrics.put_metric("systemd_kernel", kernel, unit="Milliseconds")
        metrics.put_metric("systemd_userspace", userspace, unit="Milliseconds")
        metrics.put_metric("systemd_total", total, unit="Milliseconds")

        vm.kill()


def emit_boottime_metrics(vm, metrics, boot_time_us, cpu_boot_time_us):
    """Emit the standard boot-time metrics for a single launched microVM."""
    metrics.put_metric("guest_boot_time", boot_time_us, unit="Microseconds")
    metrics.put_metric("guest_cpu_boot_time", cpu_boot_time_us, unit="Microseconds")

    events = find_events(vm.log_data)
    build_time = events["build microvm for boot"]["duration"]
    metrics.put_metric("build_time", build_time.microseconds, unit="Microseconds")
    resume_time = events["boot microvm"]["duration"]
    metrics.put_metric("resume_time", resume_time.microseconds, unit="Microseconds")

    kernel, userspace, total = get_systemd_analyze_times(vm)
    metrics.put_metric("systemd_kernel", kernel, unit="Milliseconds")
    metrics.put_metric("systemd_userspace", userspace, unit="Milliseconds")
    metrics.put_metric("systemd_total", total, unit="Milliseconds")


# Number of extra scratch block devices attached for the many-device boot test.
NUM_BLOCK_DEVICES = 10


@pin_rootfs_mode("rw")
@pytest.mark.parametrize("pool_size", [0, NUM_BLOCK_DEVICES], ids=["pool0", "pool10"])
@pytest.mark.nonci
def test_boottime_many_block_devices(
    microvm_factory,
    guest_kernel,
    rootfs,
    pool_size,
    pci_enabled,
    metrics,
):
    """Boot time with many block devices, legacy vs one worker thread per device.

    Attaches ``NUM_BLOCK_DEVICES`` scratch block devices (plus the rootfs) and
    measures boot time with ``pool_size`` 0 (all devices on the shared VMM
    thread) vs ``NUM_BLOCK_DEVICES`` (each block device pre-spawned onto its own
    worker thread). Isolates the boot-path cost of the pool as device count
    grows -- worker pre-spawn and the per-device handoff both happen in the
    "build microvm for boot" window.
    """
    for i in range(10):
        vm, boot_time_us, cpu_boot_time_us = launch_vm_with_boot_timer(
            microvm_factory,
            guest_kernel,
            rootfs,
            vcpu_count=2,
            mem_size_mib=1024,
            pci_enabled=pci_enabled,
            boot_from_pmem=False,
            pool_size=pool_size,
            num_scratch_drives=NUM_BLOCK_DEVICES,
        )

        if i == 0:
            metrics.set_dimensions(
                {
                    "performance_test": "test_boottime_many_block_devices",
                    "num_block_devices": str(NUM_BLOCK_DEVICES),
                    "pool_size": str(pool_size),
                    **vm.dimensions,
                }
            )

        emit_boottime_metrics(vm, metrics, boot_time_us, cpu_boot_time_us)
        vm.kill()
