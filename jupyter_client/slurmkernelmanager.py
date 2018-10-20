"""
A kernel manager to submit kernels via slurm.

unlike other manager submiting kernels via jupb scheduler requires to wait to
know the schedule host to bind the the right ports. """

from ioloop.manager import IOLoopManager
from subprocess import run, PIPE


from subprocess import run, PIPE
r = run('sbatch sleep-100'.split(' '), stdout=PIPE, stderr=PIPE)
jobid = r.stdout.strip().split(b' ')[-1].decode()
print('JOB ID', jobid)

ST = None

for i in range(20):
   r = run(['squeue', '-j', jobid, '-o',"%.2t %R"], stdout=PIPE)
   ST,NODELIST = [x for x in r.stdout.splitlines()[-1].split(b' ') if x]
   import time
   print(f'Job {ST} {NODELIST}')
   time.sleep(1)


class SlurmKernelManager(IOLoopManager):


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
        self.write_connection_file()

        # save kwargs for use in restart
        self._launch_args = kw.copy()
        # build the Popen cmd
        extra_arguments = kw.pop('extra_arguments', [])
        kernel_cmd = self.format_kernel_cmd(extra_arguments=extra_arguments)
        print('kernel launc command')
        env = kw.pop('env', os.environ).copy()
        if not self.kernel_cmd:
            # If kernel_cmd has been set manually, don't refer to a kernel spec
            # Environment variables from kernel spec are added to os.environ
            env.update(self.kernel_spec.env or {})
        elif self.extra_env:
            env.update(self.extra_env)

        # launch the kernel subprocess
        self.log.debug("Starting kernel: %s", kernel_cmd)

        with open('ssubmit.sh' ,'w') as f:
            f.write('#!/bin/bash\n')
            f.write('#SBATCH --output=debug.log\n')
            f.write(kernel_cmd)
        r = run('sbatch ssubmit.sh'.split(' '), stdout=PIPE, stderr=PIPE)
        jobid = r.stdout.strip().split(b' ')[-1].decode()
        print('JOB ID', jobid)
        ST = None
        while ST != 'R':
            r = run(['squeue', '-j', jobid, '-o',"%.2t %R"], stdout=PIPE)
            ST,NODELIST = [x for x in r.stdout.splitlines()[-1].split(b' ') if x]
            from asyncio import sleep
            sleep(1)
            print(f'Job {ST} {NODELIST}')
        print('Job is now running {ST.decode()} on {NODELIST.decode()}')

        self.ip = NODELIST.decode()



        self.start_restarter()
        self._connect_control_socket()

