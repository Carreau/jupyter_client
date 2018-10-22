"""
A kernel manager to submit kernels via slurm.

Unlike other manager submitting kernels via job scheduler requires to wait to
know the schedule host to bind the right ports. """

import os
from asyncio import sleep

from subprocess import run, PIPE

from .ioloop.manager import IOLoopKernelManager

from there import print

import socket


class SlurmKernelManager(IOLoopKernelManager):
    async def start_kernel(self, **kw):
        """Starts a kernel on this host in a separate process.

        If random ports (port=0) are being used, this method must be called
        before the channels are created.

        Parameters
        ----------
        `**kw` : optional
             keyword arguments that are passed down to build the kernel_cmd
             and launching the kernel (e.g. Popen kwargs).
        """
        # write connection file / get default ports
        self.ip = '0.0.0.0'
        self.write_connection_file()

        # save kwargs for use in restart
        self._launch_args = kw.copy()
        # build the Popen cmd
        extra_arguments = kw.pop("extra_arguments", [])
        kernel_cmd = self.format_kernel_cmd(extra_arguments=extra_arguments)
        print("kernel launc command")
        env = kw.pop("env", os.environ).copy()
        if not self.kernel_cmd:
            # If kernel_cmd has been set manually, don't refer to a kernel spec
            # Environment variables from kernel spec are added to os.environ
            env.update(self.kernel_spec.env or {})
        elif self.extra_env:
            env.update(self.extra_env)

        # launch the kernel subprocess
        self.log.debug("Starting kernel: %s", kernel_cmd)
        kernel_cmd.append('--ip="0.0.0.0"')
        kernel_cmd.append('--debug')

        with open("ssubmit.sh", "w") as f:
            f.write("#!/bin/bash\n")
            f.write("#SBATCH --output=debug.log\n")
            f.write("#SBATCH -p debug.q\n")
            f.write("#SBATCH --time=0-00:15:00\n")
            f.write("#SBATCH --mem-per-cpu=1G\n")
            f.write("#SBATCH --ntasks=1\n")
            f.write("#SBATCH --node=1\n")
            f.write('JUPYTER_RUNTIME_DIR="$HOME/xd/"\n')
            f.write('echo Running:' + ' '.join(kernel_cmd)+'\n')
            f.write(' '.join(kernel_cmd))
        r = run("sbatch ssubmit.sh".split(" "), stdout=PIPE, stderr=PIPE)
        jobid = r.stdout.strip().split(b" ")[-1].decode()
        print("JOB ID", jobid)
        ST = None
        while ST != b"R":
            r = run(["squeue", "-j", jobid, "-o", "%.2t %R"], stdout=PIPE)
            ST, NODELIST = [x for x in r.stdout.splitlines()[-1].split(b" ") if x]
            await sleep(1)
            print(f"Job {ST} {NODELIST}")
        print(f"Job is now running {ST.decode()} on {NODELIST.decode()}")
        new_ip = socket.gethostbyname(NODELIST.decode()+'-ib')
        print('setting new ip to ', new_ip)
        self.ip =  new_ip
        #self.write_connection_file()

        self.start_restarter()
        self._connect_control_socket()
