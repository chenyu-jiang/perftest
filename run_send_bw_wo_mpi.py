import os
import shutil
import subprocess
import argparse

from gen_send_bw_args import add_parser_args, gen_ib_send_args

def get_cmd():
    if os.environ.get("IB_SEND_BW_EXEC", None):
        exec_cmd = shutil.which(os.environ["IB_SEND_BW_EXEC"])
        if exec_cmd is None:
            raise Exception("IB_SEND_BW_EXEC ({}) not valid.".format(os.environ["IB_SEND_BW_EXEC"]))
    else:
        exec_cmd = shutil.which("ib_send_bw")
        if exec_cmd is None:
            exec_cmd = shutil.which("./ib_send_bw")
            if exec_cmd is None:
                raise Exception("ib_send_bw not found. Consider setting IB_SEND_BW_EXEC environment variable.")
    return exec_cmd

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Manually execute ib_send_bw tests according to mcconfig."
    )

    add_parser_args(parser)

    args = parser.parse_args()

    if os.environ.get("HOST_IP", None):
        host_ip = os.environ["HOST_IP"]
    else:
        host_ip = subprocess.check_output(["hostname", "-I"]).decode("utf-8").split(" ")[0]

    params, n_local_sessions = gen_ib_send_args(args, only_for_host=host_ip)

    os.environ["N_THREADS"] = str(n_local_sessions)

    print([get_cmd()] + params)

    # subprocess.run([get_cmd()] + params)