__author__ = "Brian Fulton-Howard, David Lähnemann, Johannes Köster, Christian Meesters"
__copyright__ = (
    "Copyright 2023, Brian Fulton-Howard, ",
    "David Lähnemann, ",
    "Johannes Köster, ",
    "Christian Meesters",
)
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

import os
import re
import subprocess
import time
from dataclasses import dataclass, field
from typing import List, AsyncGenerator, Optional
from collections import Counter
import uuid
import math
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import (CommonSettings, ExecutorSettingsBase)
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_common.exceptions import WorkflowError

@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    submit_cmd: Optional[str] = field(
        default="qsub",
        metadata={
            "help": "Command for submitting jobs",
            "required": True,
        },
    )

# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Define whether your executor plugin implies that there is no shared
    # filesystem (True) or not (False).
    # This is e.g. the case for cloud execution.
    implies_no_shared_fs=False,
    job_deploy_sources=False,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=False,
    auto_deploy_default_storage_provider=False,
    # wait a bit until bjobs has job info available
    init_seconds_before_status_checks=20,
    pass_group_args=True,
)


# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __post_init__(self):
        self.run_uuid = str(uuid.uuid4())
        self._fallback_project_arg = None
        self._fallback_queue = None
        self.sge_config = self.get_sge_config()


    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.

        if job.is_group():
            # use the group name, which is in groupid
            log_folder = f"group_{job.groupid}"
            # get all wildcards of all the jobs in the group and
            # prepend each with job name, as wildcards of same
            # name can contain different values across jobs
            wildcard_dict = {
                f"{j.name}__{k}": v
                for j in job.jobs
                for k, v in j.wildcards_dict.items()
            }
        else:
            log_folder = f"rule_{job.name}"
            wildcard_dict = job.wildcards_dict

        if wildcard_dict:
            wildcard_dict_noslash = {
                k: v.replace("/", "___") for k, v in wildcard_dict.items()
            }
            wildcard_str = "..".join(
                [f"{k}={v}" for k, v in wildcard_dict_noslash.items()]
            )
            wildcard_str_job = ",".join(
                [f"{k}={v}" for k, v in wildcard_dict_noslash.items()]
            )
            jobname = f"Snakemake_{log_folder}:{wildcard_str_job}___({self.run_uuid})"
        else:
            jobname = f"Snakemake_{log_folder}___({self.run_uuid})"
            wildcard_str = "unique"

        sge_logfile = os.path.abspath(
            f".snakemake/sge_logs/{log_folder}/{wildcard_str}/{self.run_uuid}.log"
        )

        os.makedirs(os.path.dirname(sge_logfile), exist_ok=True)

        # generic part of a submission string:
        # we use a run_uuid in the job-name, to allow `--name`-based
        # filtering in the job status checks

        call = f"qsub -o {sge_logfile} -e {sge_logfile} '{jobname}'"


        # Extracting time_min and converting if necessary
        time_min = job.resources.get('time_min')

        # Check if time_min is provided as a string in the format HH:MM:SS
        if isinstance(time_min, str):
            time_sge = time_min  # Use the time as-is
        elif isinstance(time_min, (int, float)):
            # Convert time_min in minutes to HH:MM:SS format
            hours, minutes = divmod(int(time_min), 60)
            seconds = int((time_min - int(time_min)) * 60)
            time_sge = f"{hours:02}:{minutes:02}:{seconds:02}"
        else:
            raise WorkflowError("Invalid time_min format. Expected a string 'HH:MM:SS' or a numeric value representing minutes.")

        # Append time to the call in the format 'HH:MM:SS'
        call += f" -l h_rt={time_sge}"
        
        # Additional job submission logic...
        print(f"Submitting job with h_rt: {time_sge} seconds")

        # Call the get_mem method to get the memory value
        mem_free = self.get_mem(job)

        # Ensure memory is valid before adding it to the submission call
        if mem_free is not None:
            call += f" -l mem_free={mem_free}G"

        call += f" -pe smp {self.get_cpus(job)}"

        
        call += self.get_project_arg(job)
        call += self.get_queue_arg(job)

        exec_job = self.format_job_exec(job)

        # ensure that workdir is set correctly
        call += f" -cwd {self.workflow.workdir_init} '{exec_job}'"
        # and finally the job to execute with all the snakemake parameters
        # TODO do I need an equivalent to --wrap?
        #call += f' "{exec_job}"'
        print(f"SGE submission command: {call}")

        self.logger.debug(f"qsub call: {call}")
        try:
            out = subprocess.check_output(
                call, shell=True, text=True, stderr=subprocess.STDOUT
            ).strip()
        except subprocess.CalledProcessError as e:
            raise WorkflowError(
                f"sge job submission failed. The error message was {e.output}"
            )

        sge_jobid = re.search(r"Job <(\d+)>", out)[1]
        if not sge_jobid:
            raise WorkflowError(
                f"Could not extract sge job ID. The submission message was\n{out}"
            )
        sge_logfile = sge_logfile.replace("%J", sge_jobid)
        self.logger.info(
            f"Job {job.jobid} has been submitted with sge jobid {sge_jobid} "
            f"(log: {sge_logfile})."
        )
        self.report_job_submission(
            SubmittedJobInfo(
                job, external_jobid=sge_jobid, aux={"sge_logfile": sge_logfile}
            )
        )

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> AsyncGenerator[SubmittedJobInfo, None]:
        # Check the status of active jobs.

        # You have to iterate over the given list active_jobs.
        # For jobs that have finished successfully, you have to call
        # self.report_job_success(job).
        # For jobs that have errored, you have to call
        # self.report_job_error(job).
        # Jobs that are still running have to be yielded.
        #
        # For queries to the remote middleware, please use
        # self.status_rate_limiter like this:
        #
        # async with self.status_rate_limiter:
        #    # query remote middleware here
        fail_stati = ("SSUSP", "EXIT", "USUSP")
        # Cap sleeping time between querying the status of all active jobs:
        max_sleep_time = 180

        job_query_durations = []

        status_attempts = 6

        active_jobs_ids = {job_info.external_jobid for job_info in active_jobs}
        active_jobs_seen = set()

        self.logger.debug("Checking job status")

        for i in range(status_attempts):
            async with self.status_rate_limiter:
                (status_of_jobs, job_query_duration) = await self.job_stati_bjobs()
                job_query_durations.append(job_query_duration)
                self.logger.debug(f"status_of_jobs after qsub is: {status_of_jobs}")
                # only take jobs that are still active
                active_jobs_ids_with_current_status = (
                    set(status_of_jobs.keys()) & active_jobs_ids
                )
                active_jobs_seen = (
                    active_jobs_seen | active_jobs_ids_with_current_status
                )
                missing_status_ever = active_jobs_ids - active_jobs_seen
                if missing_status_ever and i > 2:
                    (
                        status_of_jobs_sgeevt,
                        job_query_duration,
                    ) = await self.job_stati_sgeevents()
                    job_query_durations.append(job_query_duration)
                    status_of_jobs.update(status_of_jobs_sgeevt)
                    self.logger.debug(
                        f"status_of_jobs after sge_EVENTS is: {status_of_jobs}"
                    )
                    active_jobs_ids_with_current_status = (
                        set(status_of_jobs.keys()) & active_jobs_ids
                    )
                    active_jobs_seen = (
                        active_jobs_seen | active_jobs_ids_with_current_status
                    )
                missing_status = active_jobs_seen - active_jobs_ids_with_current_status
                missing_status_ever = active_jobs_ids - active_jobs_seen

                self.logger.debug(f"active_jobs_seen are: {active_jobs_seen}")
                if not missing_status and not missing_status_ever:
                    break
            if i >= status_attempts - 1:
                self.logger.warning(
                    f"Unable to get the status of all active_jobs that should be "
                    f"in sge, even after {status_attempts} attempts.\n"
                    f"The jobs with the following job ids were previously seen "
                    "but are no longer reported by bjobs or in sge_EVENTS:\n"
                    f"{missing_status}\n"
                    f"Please double-check with your sge cluster administrator, that "
                    "job accounting is properly set up.\n"
                )

        any_finished = False
        for j in active_jobs:
            if j.external_jobid not in status_of_jobs:
                yield j
                continue
            status = status_of_jobs[j.external_jobid]
            if status == "DONE":
                self.report_job_success(j)
                any_finished = True
                active_jobs_seen.remove(j.external_jobid)
            elif status == "UNKWN":
                # the job probably does not exist anymore, but 'sacct' did not work
                # so we assume it is finished
                self.report_job_success(j)
                any_finished = True
                active_jobs_seen.remove(j.external_jobid)
            elif status in fail_stati:
                msg = (
                    f"sge-job '{j.external_jobid}' failed, sge status is: "
                    f"'{status}'"
                )
                self.report_job_error(j, msg=msg, aux_logs=[j.aux["sge_logfile"]])
                active_jobs_seen.remove(j.external_jobid)
            else:  # still running?
                yield j

        if not any_finished:
            self.next_seconds_between_status_checks = min(
                self.next_seconds_between_status_checks + 10, max_sleep_time
            )
        else:
            self.next_seconds_between_status_checks = None

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.
        if active_jobs:
            # TODO chunk jobids in order to avoid too long command lines
            jobids = " ".join([job_info.external_jobid for job_info in active_jobs])
            try:
                # timeout set to 60, because a scheduler cycle usually is
                # about 30 sec, but can be longer in extreme cases.
                # Under 'normal' circumstances, 'bkill' is executed in
                # virtually no time.
                subprocess.check_output(
                    f"qdel {jobids}",
                    text=True,
                    shell=True,
                    timeout=60,
                    stderr=subprocess.PIPE,
                )
            except subprocess.TimeoutExpired:
                self.logger.warning("Unable to cancel jobs within a minute.")

    async def job_stati_bjobs(self):
        """
        Obtain sge job status of all submitted jobs from bjobs
        """
        uuid = self.run_uuid

        statuses_all = []

        try:
            running_cmd = f"qstat -u '*' -s r -j"
            time_before_query = time.time()
            running = subprocess.check_output(
                running_cmd, shell=True, text=True, stderr=subprocess.PIPE
            )
            query_duration = time.time() - time_before_query
            self.logger.debug(
                "The job status for running jobs was queried "
                f"with command: {running_cmd}\n"
                f"It took: {query_duration} seconds\n"
                f"The output is:\n'{running}'\n"
            )
            if running:
                statuses_all += [tuple(x.split()) for x in running.strip().split("\n")]
        except subprocess.CalledProcessError as e:
            self.logger.error(
                f"The running job status query failed with command: {running_cmd}\n"
                f"Error message: {e.stderr.strip()}\n"
            )
            pass

        res = {x[0]: x[1] for x in statuses_all}

        return (res, query_duration)

    async def job_stati_sgeevents(self):
        """
        Obtain sge job status of all submitted jobs from sge_EVENTS
        """
        uuid = self.run_uuid

        statuses = {
            "0": "NULL",  # State null
            "1": "PEND",  # Job is pending (it has not been dispatched yet).
            "2": "PSUSP",  # Pending job suspended by owner or sge sysadmin.
            "4": "RUN",  # Job is running.
            "8": "SSUSP",  # Running suspended by the system. *
            "16": "USUSP",  # Running job suspended by owner or sge sysadmin.
            "32": "EXIT",  # Job terminated with a non-zero status. **
            "64": "DONE",  # Job has terminated with status 0.
            "128": "PDONE",  # Post job process done successfully.
            "256": "PERR",  # Post job process has an error.
            "512": "WAIT",  # Chunk job waiting its turn to exec.
            "32768": "RUNKWN",  # Stat unknown (remote cluster contact lost).
            "65536": "UNKWN",  # Stat unknown (local cluster contact lost). ***
            "131072": "PROV",  # Job is provisional. ****
        }

        #   * because execution host was overloaded or queue run window closed.
        #   ** may have been aborted due to an execution error
        #      or killed by owner or sge sysadmin.
        #  *** The server batch daemon (sbatchd) on the host on which the job
        #      is processed has lost contact with the master batch daemon
        #      (mbatchd).
        # **** This state shows that the job is dispatched to a standby
        #      power-saved host, and this host is being waken up or started up.

        awk_code = f"""
        awk '
            BEGIN {{
                FPAT = "([^ ]+)|(\\"[^\\"]+\\")"
            }}
            $1 == "\\"JOB_NEW\\"" && $5 == {os.geteuid()} && $42 ~ "{uuid}" {{
                a[$4]
                next
            }}
            $4 in a && $1 == "\\"JOB_STATUS\\"" && $5 != 192 {{
                print $4, $5
            }}
        ' {self.sge_config['sge_EVENTS']}
        """

        statuses_all = []

        try:
            time_before_query = time.time()
            finished = subprocess.check_output(
                awk_code, shell=True, text=True, stderr=subprocess.PIPE
            )
            query_duration = time.time() - time_before_query
            self.logger.debug(
                f"The job status for completed jobs was queried.\n"
                f"It took: {query_duration} seconds\n"
                f"The output is:\n'{finished}'\n"
            )
            if finished:
                codes = [tuple(x.split()) for x in finished.strip().split("\n")]
                statuses_all += [(x, statuses[y]) for x, y in codes]
        except subprocess.CalledProcessError as e:
            self.logger.error(
                f"The finished job status query failed with command: {awk_code}\n"
                f"Error message: {e.stderr.strip()}\n"
            )
            pass

        res = {x[0]: x[1] for x in statuses_all}

        return (res, query_duration)

    def get_cpus(self, job: JobExecutorInterface):
        """
        Gets the sge cpu request amount for the job.
        """
        cpus_total = job.threads
        if job.resources.get("threads"):
            cpus_per_task = job.resources.threads
            if not isinstance(cpus_per_task, int):
                raise WorkflowError(
                    f"cpus_per_task must be an integer, but is {cpus_per_task}"
                )
            cpus_total = cpus_per_task
        # ensure that at least 1 cpu is requested
        # because 0 is not allowed by sge
        return max(1, cpus_total)

    def get_mem(self, job: JobExecutorInterface):
        """
        Gets the SGE memory request amount for the job.
        Converts the memory to gigabytes (GB) from any of the provided units (KB, MB, GB, TB).
        Returns the memory as an integer.
        """
        # Conversion factors for converting various units to GB
        conv_fcts = {"K": 1 / (1024**2),  # KB to GB
                    "M": 1 / 1024,        # MB to GB
                    "G": 1,               # GB is the base unit
                    "T": 1024}            # TB to GB
        
        # Get the memory unit from the SGE config, default to MB (M)
        mem_unit = self.sge_config.get("sge_UNIT_FOR_LIMITS", "M")[0]  # Extract the first character (K, M, G, T)
        
        # Get the conversion factor for the detected memory unit
        conv_fct = conv_fcts.get(mem_unit, 1)  # Default to 1 (GB)

        # Check for various memory fields in the job's resources and convert to GB
        if job.resources.get("mem_kb") and isinstance(job.resources.mem_kb, (int, float)):
            mem_gb = job.resources.mem_kb * conv_fct
        elif job.resources.get("mem_mb") and isinstance(job.resources.mem_mb, (int, float)):
            mem_gb = job.resources.mem_mb * conv_fct
        elif job.resources.get("mem_gb") and isinstance(job.resources.mem_gb, (int, float)):
            mem_gb = job.resources.mem_gb * conv_fct
        elif job.resources.get("mem_tb") and isinstance(job.resources.mem_tb, (int, float)):
            mem_gb = job.resources.mem_tb * conv_fct
        else:
            self.logger.warning(
                "No valid job memory information is given - submitting without memory request. "
                "This might or might not work on your cluster."
            )
            return None  # Return None if there's no valid memory info
        
        # Return the memory as an integer (rounded down)
        return int(mem_gb)



    def get_project_arg(self, job: JobExecutorInterface):
        """
        checks whether the desired project is valid,
        returns a default project, if applicable
        else raises an error - implicetly.
        """
        if job.resources.get("sge_project"):
            # No current way to check if the project is valid
            return f" -P {job.resources.sge_project}"
        else:
            return ""
        """
        else:
            if self._fallback_project_arg is None:
                self.logger.warning("No sge project given, trying to guess.")
                project = self.get_project()
                if project:
                    self.logger.warning(f"Guessed sge project: {project}")
                    self._fallback_project_arg = f" -P {project}"
                else:
                    self.logger.warning(
                        "Unable to guess sge project. Trying to proceed without."
                    )
                    self._fallback_project_arg = ""  # no project specific args for bsub
            return self._fallback_project_arg
        """
    def get_queue_arg(self, job: JobExecutorInterface):
        """
        checks whether the desired queue is valid,
        returns a default queue, if applicable
        else raises an error - implicetly.
        """
        if job.resources.get("sge_queue"):
            queue = job.resources.sge_queue
            print("Here is the queue: ", queue)
        else:
            if self._fallback_queue is None:
                self._fallback_queue = self.get_default_queue(job)
            queue = self._fallback_queue
        if queue:
            return f" -q {queue}"
        else:
            return ""

    def get_project(self):
        """
        tries to deduce the project from recent jobs,
        returns None, if none is found
        """
        cmd = "qacct -j | grep Project"
        try:
            qacct_out = subprocess.check_output(
                cmd, shell=True, text=True, stderr=subprocess.PIPE
            )
            projects = re.findall(r"Project\s+(\S+)", qacct_out)
            counter = Counter(projects)
            return counter.most_common(1)[0][0] if counter else None
        except subprocess.CalledProcessError as e:
            self.logger.warning(
                f"No project was given, not able to get a sge project from qacct: {e.stderr} "
                f"{e.stderr}"
            )
            return None

    def get_default_queue(self, job):
        """
        if no queue is given, checks whether a fallback onto a default
        queue is possible
        """
        if "DEFAULT_QUEUE" in self.sge_config:
            return self.sge_config["DEFAULT_QUEUE"]
        self.logger.warning(
            f"No queue was given for rule '{job}', and unable to find "
            "a default queue."
            " Trying to submit without queue information."
            " You may want to invoke snakemake with --default-resources "
            "'sge_queue=<your default queue>'."
        )
    
        return ""

    @staticmethod
    def get_lsf_config():
        lsf_config_raw = subprocess.run(
            "badmin showconf mbd", shell=True, capture_output=True, text=True
        )

        lsf_config_lines = lsf_config_raw.stdout.strip().split("\n")
        lsf_config_tuples = [tuple(x.strip().split(" = ")) for x in lsf_config_lines]
        lsf_config = {x[0]: x[1] for x in lsf_config_tuples[1:]}
        clusters = subprocess.run(
            "lsclusters", shell=True, capture_output=True, text=True
        )
        lsf_config["LSF_CLUSTER"] = clusters.stdout.split("\n")[1].split()[0]
        lsf_config["LSB_EVENTS"] = (
            f"{lsf_config['LSB_SHAREDIR']}/{lsf_config['LSF_CLUSTER']}"
            + "/logdir/lsb.events"
        )
        lsb_params_file = (
            f"{lsf_config['LSF_CONFDIR']}/lsbatch/"
            f"{lsf_config['LSF_CLUSTER']}/configdir/lsb.params"
        )
        with open(lsb_params_file, "r") as file:
            for line in file:
                if "=" in line and not line.strip().startswith("#"):
                    key, value = line.strip().split("=", 1)
                    if key.strip() == "DEFAULT_QUEUE":
                        lsf_config["DEFAULT_QUEUE"] = value.split("#")[0].strip()
                        break

        lsf_config["LSF_MEMFMT"] = os.environ.get(
            "SNAKEMAKE_LSF_MEMFMT", "percpu"
        ).lower()

        return lsf_config


def walltime_sge_to_generic(w):
    """
    convert old sge walltime format to new generic format
    """
    s = 0
    if type(w) in [int, float]:
        # convert int minutes to hours minutes and seconds
        return w
    elif type(w) is str:
        if re.match(r"^\d+(ms|[smhdw])$", w):
            return w
        elif re.match(r"^\d+:\d+$", w):
            # convert "HH:MM" to hours and minutes
            h, m = map(float, w.split(":"))
        elif re.match(r"^\d+:\d+:\d+$", w):
            # convert "HH:MM:SS" to hours minutes and seconds
            h, m, s = map(float, w.split(":"))
        elif re.match(r"^\d+:\d+\.\d+$", w):
            # convert "HH:MM.XX" to hours minutes and seconds
            h, m = map(float, w.split(":"))
            s = (m % 1) * 60
            m = round(m)
        elif re.match(r"^\d+\.\d+$", w):
            return math.ceil(w)
        elif re.match(r"^\d+$", w):
            return int(w)
        else:
            raise ValueError(f"Invalid walltime format: {w}")
    h = int(h)
    m = int(m)
    s = int(s)
    return math.ceil((h * 60) + m + (s / 60))


def generalize_sge(rules, runtime=True, memory="perthread_to_perjob"):
    """
    Convert sge specific resources to generic resources
    """
    re_mem = re.compile(r"^([0-9.]+)(B|KB|MB|GB|TB|PB|KiB|MiB|GiB|TiB|PiB)$")
    for k in rules._rules.keys():
        res_ = rules._rules[k].rule.resources
        if runtime:
            if "walltime" in res_.keys():
                runtime_ = walltime_sge_to_generic(res_["walltime"])
                del rules._rules[k].rule.resources["walltime"]
            elif "time_min" in res_.keys():
                runtime_ = walltime_sge_to_generic(res_["time_min"])
                del rules._rules[k].rule.resources["time_min"]
            elif "runtime" in res_.keys():
                runtime_ = walltime_sge_to_generic(res_["runtime"])
            else:
                runtime_ = False
            if runtime_:
                rules._rules[k].rule.resources["runtime"] = runtime_
        if memory == "perthread_to_perjob":
            if "mem_mb" in res_.keys():
                mem_ = float(res_["mem_mb"]) * res_["_cores"]
                if mem_ % 1 == 0:
                    rules._rules[k].rule.resources["mem_mb"] = int(mem_)
                else:
                    rules._rules[k].rule.resources["mem_mb"] = mem_
            elif "mem" in res_.keys():
                mem_ = re_mem.match(res_["mem"])
                if mem_:
                    mem = float(mem_[1]) * res_["_cores"]
                else:
                    raise ValueError(
                        f"Invalid memory format: {res_['mem']} in rule {k}"
                    )
                if mem % 1 == 0:
                    mem = int(mem)
                rules._rules[k].rule.resources["mem"] = f"{mem}{mem_[2]}"
        elif memory == "rename_mem_mb_per_cpu":
            if "mem_mb" in res_.keys():
                rules._rules[k].rule.resources["mem_mb_per_cpu"] = res_["mem_mb"]
                del rules._rules[k].rule.resources["mem_mb"]
            elif "mem" in res_.keys():
                raise ValueError(
                    f"Cannot rename resource from 'mem' to 'mem_mb_per_cpu' in rule {k}"
                )