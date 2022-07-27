import os
import shutil
import subprocess
import argparse

parser = argparse.ArgumentParser(description='Execute ib_send_bw tests according to mcconfig.')
parser.add_argument('-c', '--config', help='mcconfig file', type=str, required=True)
parser.add_argument('-r', '--remote', help='server address', type=str)
parser.add_argument('-d', '--devices', help='IB devices to use', type=str, default="rdmap16s27,rdmap32s27,rdmap144s27,rdmap160s27")
parser.add_argument('-x', '--gid-indices', help='GID indices to use', default="0,0,0,0")
parser.add_argument('-b', '--bidirectional', help='benchmark bidirectional bandwidth', action='store_true')
parser.add_argument('-o', '--output-prefix', help='prefix of output logs, default to bw_send + the name of mcconfig', type=str)

args = parser.parse_args()

if args.output_prefix is None:
    args.output_prefix = "./bw_send_" + args.config.split(".")[0] + ("_bi" if args.bidirectional else "_send_only")

NNODES = 2
BASE_PORT = 15000

is_client = args.remote is not None
devices = args.devices.split(",")
gid_indices = args.gid_indices.split(",")
dev2gid = {}
for idx, dev in enumerate(devices):
    dev2gid[dev] = gid_indices[idx]

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
        assert 0 <= src_device < len(devices), f"Invalid source device {src_device} in line {idx} in mcconfig"
        assert len(devices) <= dst_device < NNODES*len(devices), f"Invalid destination device {dst_device} in line {idx} in mcconfig"
        assert src_device // len(devices) + dst_device // len(devices) == 1, f"Invalid device pair ({src_device},{dst_device}) in line {idx} in mcconfig. Src and dst device must locate on different nodes."
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
        bi_arg = ["-b", "--report-both", "-s", "8192"]
    else:
        bi_arg = ["-s", "8192"]
    return ["-c", "SRD", "-n", "20000", "-N", "--out_json", f"--out_json_file={out_json}"] + bi_arg + ["-d", device, "-p", str(port), "-x", gid] + remote

def get_out_json_name(sess_idx):
    return args.output_prefix + f"_{'bi_' if args.bidirectional else ''}sess{sess_idx}_s{mc_sessions[sess_idx][0]}_r{mc_sessions[sess_idx][1]}_{'cli' if is_client else 'ser'}.json"

params = []
for session_idx, (src_idx, dst_idx) in enumerate(mc_sessions):
    if is_client:
        device = devices[src_idx]
    else:
        device = devices[dst_idx - len(devices)]
    params += get_params(device, BASE_PORT + session_idx, dev2gid[device], get_out_json_name(session_idx), remote=args.remote)

subprocess.run([get_cmd()] + params)