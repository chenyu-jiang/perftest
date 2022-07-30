import os
import shutil
import subprocess
import argparse

from pkg_resources import require
from scipy.fft import dst

parser = argparse.ArgumentParser(
    description="Execute ib_send_bw tests according to mcconfig."
)
parser.add_argument("-c", "--config", help="mcconfig file", type=str, required=True)
parser.add_argument("-H", "--hostfile", help="hostfile containing server ip addresses", type=str, required=True)
parser.add_argument(
    "-d",
    "--devices",
    help="IB devices to use",
    type=str,
    default="rdmap16s27,rdmap32s27,rdmap144s27,rdmap160s27",
)
parser.add_argument("-x", "--gid-indices", help="GID indices to use", default="0,0,0,0")
parser.add_argument(
    "-b",
    "--bidirectional",
    help="benchmark bidirectional bandwidth. each session will be run in both directions",
    action="store_true",
)
parser.add_argument(
    "-o",
    "--output-prefix",
    help="prefix of output logs, default to bw_send + the name of mcconfig",
    type=str,
)
parser.add_argument(
    "-r", "--rx-depth", help="Size of receive queue, default 512", type=int, default=512
)
parser.add_argument(
    "-t", "--tx-depth", help="Size of receive queue, default 128", type=int, default=128
)
parser.add_argument(
    "-q",
    "--qp",
    help="Number of queue pairs running in the process, default 1",
    type=int,
    default=1,
)

args = parser.parse_args()

if args.output_prefix is None:
    args.output_prefix = "./bw_send_" + args.config.split(".")[0]

BASE_PORT = 15000

devices = args.devices.split(",")
gid_indices = args.gid_indices.split(",")
dev2gid = {}
for idx, dev in enumerate(devices):
    dev2gid[dev] = gid_indices[idx]

hosts = []
with open(args.hostfile, "r") as f:
    for line in f:
        hosts.append(line.strip())

nnodes = len(hosts)

if os.environ.get("HOST_IP", None):
    host_ip = os.environ["HOST_IP"]
else:
    host_ip = subprocess.check_output(["hostname", "-I"]).decode("utf-8").split(" ")[0]

assert host_ip in hosts, f"Host ip {host_ip} not in provided hostfile. If the automatically detected IP is incorrect, please set HOST_IP environment variable."
current_node = hosts.index(host_ip)

def get_node(dev_idx):
    return dev_idx // len(devices)

mc_sessions = []
with open(args.config, "r") as f:
    for idx, line in enumerate(f):
        if line.startswith("#"):
            continue
        line = line.strip()
        if not line:
            continue
        splitted_line = line.split()
        assert len(splitted_line) == 3, f"Invalid line {idx} in mcconfig: {line}"
        src_device, dst_device, _ = splitted_line
        src_device = int(src_device)
        dst_device = int(dst_device)
        assert (
            0 <= src_device < nnodes * len(devices)
        ), f"Invalid source device {src_device} in line {idx} in mcconfig"
        assert (
            0 <= dst_device < nnodes * len(devices)
        ), f"Invalid destination device {dst_device} in line {idx} in mcconfig"
        assert (
            get_node(src_device) != get_node(dst_device)
        ), f"Invalid device pair ({src_device},{dst_device}) in line {idx} in mcconfig. Src and dst device must locate on different nodes."
        mc_sessions.append((src_device, dst_device))

os.environ["N_THREADS"] = str(len(mc_sessions))

def get_node(dev_idx):
    return dev_idx // len(devices)

def get_cmd():
    exec_cmd = shutil.which("ib_send_bw")
    if exec_cmd is None:
        exec_cmd = shutil.which("./ib_send_bw")
        if exec_cmd is None:
            raise Exception("ib_send_bw not found")
    return exec_cmd

def get_params(device, port, gid, out_json, remote=None):
    if remote:
        remote = [remote]
    else:
        remote = []
    if args.bidirectional:
        bi_arg = ["-b", "--report-both"]
    else:
        bi_arg = []
    return (
        [
            "-s",
            "8192",
            "-q",
            str(args.qp),
            "-r",
            str(args.rx_depth),
            "-t",
            str(args.tx_depth),
            "-c",
            "SRD",
            "-n",
            "20000",
            "-N",
            "--out_json",
            f"--out_json_file={out_json}",
        ]
        + bi_arg
        + ["-d", device, "-p", str(port), "-x", gid]
        + remote
        + [":"]
    )


def get_out_json_name(sess_idx, is_client):
    name = args.output_prefix
    if args.bidirectional:
        name += "_bi"
    else:
        name += "_send_only"
    name += f"_sess{sess_idx}_s{mc_sessions[sess_idx][0]}_r{mc_sessions[sess_idx][1]}"
    if args.tx_depth != 128 or args.rx_depth != 512:
        name += f"_td{args.tx_depth}rd{args.rx_depth}"
    if args.qp != 1:
        name += f"_qp{args.qp}"
    name += f"_{'cli' if is_client else 'ser'}.json"
    return name


params = []
for session_idx, (src_idx, dst_idx) in enumerate(mc_sessions):
    src_node = get_node(src_idx)
    dst_node = get_node(dst_idx)
    if src_node == current_node:
        is_client = True
    elif dst_node == current_node:
        is_client = False
    else:
        # irrelavent session
        continue
    remote = None
    if is_client:
        device = devices[src_idx - src_node * len(devices)]
        remote = hosts[dst_node]
    else:
        device = devices[dst_idx - dst_node * len(devices)]
    params += get_params(
        device,
        BASE_PORT + session_idx,
        dev2gid[device],
        get_out_json_name(session_idx, is_client),
        remote=remote,
    )

subprocess.run([get_cmd()] + params)