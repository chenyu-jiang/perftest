import os
import shutil
import subprocess
import argparse

PACKET_SIZE = 8192

def add_parser_args(parser):
    # adds common parser args, returns nothing
    parser.add_argument("-c", "--config", help="mcconfig file", type=str, required=True)
    parser.add_argument(
        "-H",
        "--hostfile",
        help="hostfile containing server ip addresses",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-d",
        "--devices",
        help="IB devices to use",
        type=str,
        default="rdmap16s27,rdmap32s27,rdmap144s27,rdmap160s27",
    )
    parser.add_argument("-x", "--gid-indices", help="GID indices to use", default="0,0,0,0")
    parser.add_argument(
        "-p",
        "--base-port",
        help="Base port to use for IB connections. The ith session's port will be base_port + i. Default to 15000.",
        type=int,
        default=15000,
    )
    parser.add_argument(
        "-b",
        "--bidirectional",
        help="benchmark bidirectional bandwidth. each session will be run in both directions",
        action="store_true",
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
    parser.add_argument(
        "-i",
        "--iters",
        help="Number of iterations to run, default 20000",
        type=int,
        default=20000
    )
    parser.add_argument(
        "-l",
        "--log-prefix",
        help="prefix of output json logs, default to bw_send + the name of mcconfig",
        type=str,
    )

def gen_ib_send_args(args, only_for_host=None):
    # args: args from parser
    # only_for_host: if not None, only generate args for this host (used in run_send_bw_wo_mpi.py)
    # returns: list of args to pass to ib_send_bw. Type: List[Str]
    #          If only_for_host is not None, return a Tuple of type (List[Str], Int) 
    #          containing (output args, # local sessions for the host)

    if args.log_prefix is None:
        args.log_prefix = "./bw_send_" + args.config.split(".")[0]

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
            assert get_node(src_device) != get_node(
                dst_device
            ), f"Invalid device pair ({src_device},{dst_device}) in line {idx} in mcconfig. Src and dst device must locate on different nodes."
            mc_sessions.append((src_device, dst_device))


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
                str(PACKET_SIZE),
                "-q",
                str(args.qp),
                "-r",
                str(args.rx_depth),
                "-t",
                str(args.tx_depth),
                "-c",
                "SRD",
                "-n",
                str(args.iters),
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
        name = args.log_prefix
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
    if only_for_host:
        assert (
            only_for_host in hosts
        ), f"Host ip {only_for_host} not in provided hostfile. If the automatically detected IP is incorrect, please set HOST_IP environment variable."

    for current_node, host_ip in enumerate(hosts):
        if only_for_host and host_ip != only_for_host:
            continue
        n_local_sessions = 0
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
                args.base_port + session_idx,
                dev2gid[device],
                get_out_json_name(session_idx, is_client),
                remote=remote,
            )
            if not only_for_host:
                params.append("") # add empty line to separate processes
            n_local_sessions += 1

    return params, n_local_sessions

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate args for ib_send_bw tests according to mcconfig."
    )
    add_parser_args(parser)

    parser.add_argument(
        "-o",
        "--output",
        help="file to store the generated args, defaults to bw_send_<the name of mcconfig>.args",
        type=str,
    )

    args = parser.parse_args()

    if args.output is None:
        args.output = "./bw_send_" + args.config.split(".")[0] + ".args"

    output_args, _ = gen_ib_send_args(args)

    with open(args.output, "w") as f:
        f.write("\n".join(output_args))
        f.write("\n") # we need an extra newline since join will not add \n to last element