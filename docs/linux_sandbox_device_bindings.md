# Device Pass-Through In The Bubblewrap Sandbox (`linux_sandbox.device_bindings`)

In terminal mode `bubblewrap`, agent commands run inside a bubblewrap sandbox. That sandbox
is deliberately **device-free**: without configuration, not a single host device is visible —
no GPU, no audio, no USB. With `linux_sandbox.device_bindings` in
`local_automation_policy.json` you grant individual devices explicitly, exactly like
additional read and write directories are granted through `additional_read_roots`.

The key applies **to bubblewrap only**. The Docker runtime has different device semantics
and ignores it.

## 1. Why Devices Are Missing By Default

`_build_linux_sandbox_command`
(`interfaces/generic-agent-interface/generic_agent_interface/tools/local_desktop_automation_tools.py`)
builds the sandbox filesystem like this:

```text
--tmpfs /  --dev /dev  --proc /proc  …
```

According to `bwrap --help`, `--dev /dev` means "Mount new dev on DEST" — it mounts a **new,
empty devtmpfs** that `bwrap` itself populates with a fixed set:
`null`, `zero`, `full`, `random`, `urandom`, `tty`, `ptmx`, `pts/`, `shm/` plus the symlinks
`core`, `fd`, `stdin`, `stdout`, `stderr`. Every host device is simply not present. This is
neither a `nodev` problem nor a permission problem.

Typical symptom (measured on a host with NVIDIA driver 580.142):

```text
$ nvidia-smi
NVIDIA-SMI has failed because it couldn't communicate with the NVIDIA driver.

$ strace -f -e trace=openat /usr/bin/nvidia-smi
openat("/dev/nvidiactl", O_RDONLY) = -1 ENOENT (No such file or directory)
```

`ENOENT`, not `EACCES`: the user-space driver stack was fully visible through `/usr`, which is
mounted read-only anyway (`libcuda.so.1`, `libnvidia-ml.so.1`); only the device node was
missing. A bind mount is therefore sufficient.

## 2. What The Key Does

For every entry the Engine emits

```text
--dev-bind-try <path> <path>
```

**after** the `--dev /dev` mount. Both details matter and were verified on a host with
bubblewrap 0.9.0:

- devices placed before `--dev /dev` are hidden again by the fresh devtmpfs,
- `--dev-bind` (without `-try`) aborts the command hard when the path is missing
  (`bwrap: Can't find source path …`). `--dev-bind-try` ignores missing paths because devices
  can disappear at runtime (hotplug, MIG slicing, driver reload).

Missing parent directories in that devtmpfs (for example `/dev/dri`) are created beforehand
with the existing `--dir` helper.

## 3. Configuration

```json
{
  "terminal_runtime_mode": "bubblewrap",
  "linux_sandbox": {
    "device_bindings": [
      "/dev/nvidiactl",
      "/dev/nvidia-modeset",
      "/dev/nvidia-uvm",
      "/dev/nvidia-uvm-tools",
      "/dev/nvidia0",
      "/dev/nvidia-caps"
    ]
  }
}
```

Rules enforced by `normalize_local_automation_device_bindings`:

| Rule | Reason |
| --- | --- |
| absolute path | relative paths would resolve against the policy directory |
| below `/dev` | a device bind bypasses the root/path model, so only inside the device directory |
| not `/dev` itself | binding `/dev` as a whole would replace the sanitized devtmpfs with the complete host device tree |
| no `..` segments, no wildcards, no `~`, `$`, or backtick | the Engine passes them literally to bwrap, where they silently match nothing |
| symlink targets outside `/dev` are rejected | otherwise a symlink below `/dev` would be a way out of the device directory |
| duplicates are removed, order is kept | reproducible bwrap arguments |
| missing paths are allowed | `--dev-bind-try` semantics; entries do not vanish on the next GPU restart |

Invalid entries are dropped and named in a `WARNING` message — the Engine does not fail to
start, but the device obviously stays closed.

**The default is empty.** A fresh installation behaves bit-identically to the state before this
key; device access is always a deliberate operator decision. The policy file is re-read on
every tool call, so a change does not require an Engine restart.

The template `local_automation_policy.example.json` carries the NVIDIA block already written
out and commented. It stays valid JSON: all `_comment_*` keys are documentation and are ignored
by the normalization.

## 4. Templates Per Hardware

Every block below is the same key combined differently. Check beforehand what actually exists
on the host: `ls -l /dev/<area>`.

### NVIDIA / CUDA

x86_64 and aarch64, one GPU. For every additional GPU add `"/dev/nvidia1"`, `"/dev/nvidia2"`, …

```json
"device_bindings": [
  "/dev/nvidiactl",
  "/dev/nvidia-modeset",
  "/dev/nvidia-uvm",
  "/dev/nvidia-uvm-tools",
  "/dev/nvidia0"
]
```

With MIG additionally: `"/dev/nvidia-caps"` (the nodes below
`/proc/driver/nvidia/capabilities/mig/` are already readable through the `/proc` mounted from
the host; only the nodes are missing). Fabric Manager systems: `"/dev/nvidia-fabricmanager"`.

Jetson/Orin (Tegra): depending on the L4T package, multimedia nodes are added, for example
`"/dev/nvhost-gpu"`, `"/dev/nvhost-ctrl-gpu"`, the `"/dev/nvgpu/` directory. `ls /dev/nv*`
shows what applies; CUDA itself needs the list above.

Useful for: building and running llama.cpp/ik_llama.cpp with CUDA, QAT and fine-tuning runs,
faster-whisper, and any `nvidia-smi` proof.

### AMD / Intel (Vulkan, VAAPI, DRM)

```json
"device_bindings": ["/dev/dri"]
```

The directory brings `renderD128…` and `card0…`. **Note the permission case:**
`/dev/dri/renderD128` is usually owned by `root:render` with `0660`. See section 6.

### Audio (ALSA directly)

```json
"device_bindings": ["/dev/snd"]
```

Only for tools that address ALSA directly. PipeWire and PulseAudio work through sockets below
`/run/user/<uid>`; `/run` is mounted read-only, so that needs a separate decision
(section 7).

### USB And Serial Devices

```json
"device_bindings": ["/dev/bus/usb", "/dev/ttyUSB0", "/dev/ttyACM0"]
```

`/dev/bus/usb` is the path used by libusb (and adb). Individual nodes below
`/dev/bus/usb/BBB/DDD` belong to physically connected devices and change when you replug —
for flashing and debug work, the directory is the better fit.

### Further Classes

| Purpose | Entry |
| --- | --- |
| Fuse filesystems | `"/dev/fuse"` |
| Camera / V4L2 | `"/dev/video0"`, `"/dev/media0"` |
| RDMA / InfiniBand | `"/dev/infiniband"` |
| Precise time | `"/dev/ptp0"`, `"/dev/pps0"` |
| I²C, SPI, GPIO | `"/dev/i2c-0"`, `"/dev/spidev0.0"`, `"/dev/gpiochip0"` |

`cat /proc/devices` gives a complete list of the device classes present on the host.

## 5. Finding Out Which Device Is Missing

1. Run the tool inside the sandbox and count the calls:

   ```bash
   strace -f -e trace=openat <tool> 2>&1 | grep ENOENT
   ```

   Every `ENOENT` hit below `/dev/…` is a candidate for `device_bindings`.

2. Check whether the device class exists in the kernel at all: `cat /proc/devices`.
3. Inspect node and mode: `ls -l /dev/<node>`, `stat -c '%a %U:%G %n' /dev/<node>`.
4. Grant it, run the command again.

Do not conclude from absence of evidence: when a tool reports that it finds no driver or no
device, that does not yet mean a node is missing. Example — `torch.cuda.is_available()` also
returns `False` when the installed torch is simply a CPU build (`torch 2.8.0+cpu` has no CUDA
layer at all). Only `strace` or `nvidia-smi -L` separate a device problem from a build problem.

## 6. Permissions And Groups

The sandbox runs with `--unshare-user`. **Supplementary groups are not passed through**, so
sandbox `/dev` entries appear as `nobody:nogroup` inside the sandbox.

| Node type | typical mode | effect of the bind |
| --- | --- | --- |
| NVIDIA (`/dev/nvidia*`) | `0666` | works immediately |
| `/dev/dri/renderD128` | `root:render 0660` | node is visible, `open()` returns `EACCES` |
| `/dev/snd/*` | `root:audio 0660` | same as above |

The countermeasure is host work, not Engine code: a udev rule that sets node groups and
mode, or dropping the device. Opening an audio or DRM node to `0666` is a deliberate decision
on multi-user systems.

## 7. What This Key Deliberately Does Not Do

- **Docker runtime.** `docker_sandbox.py` currently forwards no devices at all
  (`cap_drop=ALL`, `security_opt=no-new-privileges`, runtime `runsc`). gVisor uses different
  device semantics (`nvproxy` instead of a bind mount) and is a task of its own. The key is
  ignored in `docker` configurations, and the mount plan stays unchanged as well.
- **`/sys`.** The sandbox does not mount `/sys`. Tools with CPU topology detection therefore
  run blind, for example:
  `Error in cpuinfo: failed to parse the list of possible processors in /sys/devices/system/cpu/possible`
  (observed on the torch import inside the Engine venv; affects llama.cpp/ik_llama.cpp builds,
  thread pinning, and benchmark comparability). `device_bindings` is the wrong lever here —
  `/sys` carries information about the whole system and belongs in its own, read-only switch.
- **Sound servers and session sockets.** PipeWire/Pulse/D-Bus live below `/run/user/<uid>`;
  `/run` is mounted read-only.
- **The Engine's own GPU.** The Engine runs on the host; only agent commands sit in the sandbox.
- **Runtime selection.** `terminal_runtime_mode` stays as documented; the historical value
  `linux_sandbox` continues to be read as an alias for `bubblewrap`.

## 8. Security

A device bind is a real permission grant and bypasses the root/path model: the key does not
mount a directory, it grants access to kernel subsystems. Therefore:

- The list is **server-side**. `normalize_local_automation_user_policy_payload` persists
  `roots` exclusively from the per-user policy — users can neither add nor remove devices that
  way (covered by a test).
- Only paths below `/dev` are accepted, `/dev` itself is not.
- **Keep it as small as possible.** Especially critical:
  - `/dev/mem`, `/dev/kmem`, `/dev/port` — physical memory, a direct way out of the sandbox.
  - Block devices such as `/dev/nvme0n1`, `/dev/sda` — raw access to whole drives, bypassing
    every root grant.
  - `/dev/dri/card0` (modesetting) and `/dev/nvidia*` are less critical but still grant access
    across every process path.

  Always limit the grant to the minimum set the affected tool needs.

## 9. Verifying

### Without The Engine, Directly On The Host

This checks whether the bind mount and the permissions fit your hardware before you change the
policy (tested with bubblewrap 0.9.0):

```bash
bwrap \
  --dev /dev \
  --dir /dev \
  --dev-bind-try /dev/nvidiactl /dev/nvidiactl \
  --dev-bind-try /dev/nvidia0 /dev/nvidia0 \
  --ro-bind /usr /usr \
  --ro-bind-try /bin /bin --ro-bind-try /sbin /sbin \
  --ro-bind-try /lib /lib --ro-bind-try /lib64 /lib64 \
  --ro-bind-try /etc /etc \
  --proc /proc --tmpfs /tmp \
  -- /usr/bin/nvidia-smi -L
```

Expected: the GPU is listed. If you get `couldn't communicate with the NVIDIA driver`, the
problem is the list or the groups (section 6). If you get `Can't find source path`, then
`--dev-bind` was used instead of `--dev-bind-try`.

With a harmless node instead of the GPU, the mechanism can be checked in general:

```bash
bwrap --dev /dev --dev-bind-try /dev/null /dev/null \
  --ro-bind /usr /usr --ro-bind-try /bin /bin --ro-bind-try /lib /lib \
  --ro-bind-try /lib64 /lib64 --ro-bind-try /etc /etc --proc /proc \
  -- /bin/sh -c 'ls -l /dev/null; echo probe > /dev/null && echo WRITE_OK'
```

Expected: `crw-rw-rw- 1 nobody nogroup 1, 3 … /dev/null` and `WRITE_OK`.

### With The Engine

After setting the key in `local_automation_policy.json` (no Engine restart needed):

```text
ls -l /dev/nvidia* && nvidia-smi -L
```

as an agent command. Without configuration, the previous behaviour stays (`ENOENT`).

### At The Code Level

`tests/test_linux_sandbox_device_bindings_unit.py` covers: the validation and rejection rules,
order and duplicate removal, the persistence round trip, that the per-user policy cannot set
devices, that **no** `--dev-bind-try` arguments are emitted without the key and that the
generated arguments stay identical for an empty list, and that every binding appears **after**
`--dev /dev` while the shared mount plan (the Docker path) stays unchanged.

## 10. Failure Patterns

| Message inside the sandbox | Cause | Step |
| --- | --- | --- |
| `ENOENT` on `/dev/...` in `strace` | node not granted | add an entry to `device_bindings` |
| `EACCES` on a granted node | group permission, not a bind problem | section 6, udev rule on the host |
| driver "not found" but `strace` shows no `/dev` `ENOENT` | the build does not fit (for example a CPU build) | check the tool's build, not the sandbox |
| `bwrap: Can't find source path` | `--dev-bind` with a missing path | the Engine always uses `--dev-bind-try`; run your own experiments with `-try` |
| node visible but tool still blind | `/sys` is missing (topology/device metadata) | section 7 |
| GPU does not work in the sandbox although everything is set | Docker runtime is active | check `terminal_runtime_mode`; Docker ignores the key |
