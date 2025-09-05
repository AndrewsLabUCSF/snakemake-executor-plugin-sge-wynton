__author__ = "Rakshya Ujhanthachhen Sharma, Brian Fulton-Howard, David Lähnemann, Johannes Köster, Christian Meesters"
__copyright__ = (
    "Copyright 2024, Rakshya Ujhanthachhen Sharma, ",
    " Brian Fulton-Howard, ",
    "David Lähnemann, ",
    "Johannes Köster, ",
    "Christian Meesters",
)
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

"""
command to run the plugin: snakemake -j 2 --executor sge-wynton --use-conda --use-singularity -F --max-jobs-per-second 1
"""
import os
import re
import subprocess
import time
import asyncio
import xmltodict
from dataclasses import dataclass, field
from typing import List, AsyncGenerator, Optional
from collections import Counter
import uuid
import math
from threading import Lock
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
    # wait a bit until qstat has job info available
    init_seconds_before_status_checks=3500,
    pass_group_args=True,
)

class Executor(RemoteExecutor):
    def __post_init__(self):
        self.run_uuid = str(uuid.uuid4())
        self._fallback_project_arg = None
        self._fallback_queue = None
        self.sge_jobid = None
        self.fallback_cancel_on_missing = False
        # Initialize sge_config
        sge_root = os.getenv('SGE_ROOT')
        sge_cell = os.getenv('SGE_CELL')
        if sge_root and sge_cell:
            self.sge_config = {
                'accounting': f"{sge_root}/{sge_cell}/common/accounting"
            }
        else:
            raise WorkflowError("SGE_ROOT and SGE_CELL environment variables must be set.")

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
            jobname = f"Snakemake_{log_folder}___{wildcard_str_job}___({self.run_uuid})"
        else:
            jobname = f"Snakemake_{log_folder}___({self.run_uuid})"
            wildcard_str = "unique"

        sge_logfile = os.path.abspath(
            f".snakemake/sge_logs/{log_folder}/{wildcard_str}/{self.run_uuid}.log"
        )

        os.makedirs(os.path.dirname(sge_logfile), exist_ok=True)

        call = f"qsub -shell y -o {sge_logfile} -e {sge_logfile} -N '{jobname}'"

        # Extracting time_min and converting if necessary
        time_min = job.resources.get('time_min', 60)
        if time_min is not None:
            try:
                time_sge = walltime_sge_to_generic(time_min)
            except ValueError as e:
                raise WorkflowError(f"Invalid time_min format: {time_min}. Error: {e}")
        else:
            raise WorkflowError("Missing time_min resources for the job.")

        # Check if time_min is provided as a string in the format HH:MM:SS
        #if isinstance(time_min, str):
         #   time_sge = time_min  # Use the time as-is
        #elif isinstance(time_min, (int, float)):
            # Convert time_min in minutes to HH:MM:SS format
         #   hours, minutes = divmod(int(time_min), 60)
          #  seconds = int((time_min - int(time_min)) * 60)
           # time_sge = f"{hours:02}:{minutes:02}:{seconds:02}"
        ##   raise WorkflowError("Invalid time_min format. Expected a string 'HH:MM:SS' or a numeric value representing minutes.")

        # Append time to the call in the format 'HH:MM:SS'
        call += f" -l h_rt={time_sge}"
    

        # Call the get_mem method to get the memory value
        mem_free = self.get_mem(job)

        # Ensure memory is valid before adding it to the submission call
        if mem_free is not None:
            call += f" -l mem_free={mem_free}G"

        call += f" -pe smp {self.get_cpus(job)}"

        
        call += self.get_project_arg(job)
        call += self.get_queue_arg(job)

        exec_job = self.format_job_exec(job).replace("'", r"\'")
        jobscript = self.get_jobscript(job)
        self.write_jobscript(job, jobscript)

        # ensure that workdir is set correctly
        call += f" -cwd {jobscript}"

        # force all temps into /scratch/$USER
        call += " -v TMPDIR=/scratch/$USER,SNAKEMAKE_TMPDIR=/scratch/$USER"

        # and finally the job to execute with all the snakemake parameters
        # TODO do I need an equivalent to --wrap?
        #call += f' "{exec_job}"'

        self.logger.debug(f"qsub call: {call}")

        try:
            out = subprocess.check_output(
                call, shell=True, text=True, stderr=subprocess.STDOUT
            ).strip()
        except subprocess.CalledProcessError as e:
            raise WorkflowError(
                f"sge job submission failed. The error message was {e.output}"
            )
        print(out)

        # *** Corrected extraction of sge_jobid ***
        match = re.search(r"Your job (\d+)", out)
        if match:
            sge_jobid = match.group(1)
        else:
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
        fail_stati = ("EXIT", "Eqw")
        # Cap sleeping time between querying the status of all active jobs:
        max_sleep_time = 600

        job_query_durations = []

        status_attempts = 30

        active_jobs_ids = {job_info.external_jobid for job_info in active_jobs}
        active_jobs_seen = set()

        self.logger.debug("Checking job status")

        for i in range(status_attempts):
            async with self.status_rate_limiter:
                (status_of_jobs, job_query_duration) = await self.job_stati_qstat()
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
                if missing_status_ever and i > 3:
                    self.logger.debug(f"Jobs {missing_status_ever} missing in qstat after {i} attempts, checking qacct now...")

                    (status_of_jobs_qacct, job_query_duration) = await self.job_stati_qacct()
                    job_query_durations.append(job_query_duration)
                    status_of_jobs.update(status_of_jobs_qacct)
                    self.logger.debug(
                        f"status_of_jobs after qacct is: {status_of_jobs}"
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
                if i >= status_attempts - 1 and missing_status_ever:
                    # Check if jobs are still in the queue (qw state)
                    still_queued = {job.external_jobid for job in active_jobs if status_of_jobs.get(job.external_jobid) == "qw"}

                    if still_queued:
                        self.logger.info(f"Jobs still in queue: {still_queued}. Waiting longer instead of canceling...")
                        # Increase the wait time more aggressively for queued jobs.
                        self.next_seconds_between_status_checks = min(
                            (self.next_seconds_between_status_checks or 300) + 300, max_sleep_time
                        )
                        # Skip further processing in this iteration, so we wait longer.
                        continue
                    # Log missing jobs before canceling
                    self.logger.warning(
                        f"Unable to get the status of all active jobs that should be "
                        f"in SGE, even after {status_attempts} attempts.\n"
                        f"The jobs with the following job IDs were previously seen "
                        "but are no longer reported by `qstat` or `qacct`:\n"
                        f"{missing_status_ever}\n"
                        f"Please double-check with your SGE cluster administrator that "
                        "job accounting is properly set up.\n"
                    )

                    # Now cancel only if missing jobs are truly not reported anywhere
                    for j in active_jobs:
                        if j.external_jobid in missing_status_ever:
                            msg = f"SGE job '{j.external_jobid}' status unknown after maximum attempts."
                            self.report_job_error(j, msg=msg, aux_logs=[j.aux['sge_logfile']])

                    # Fallback: if configured, cancel missing jobs instead of just erroring them.
                    if getattr(self, 'fallback_cancel_on_missing', False):
                        self.logger.info(f"Fallback enabled: Canceling missing jobs {missing_status_ever}.")
                        self.cancel_jobs([j for j in active_jobs if j.external_jobid in missing_status_ever])
                    break

        any_finished = False
        for j in active_jobs:
            if j.external_jobid not in status_of_jobs:
                if i < 2:
                    yield j  # Retry in the next iteration
                continue

            status = status_of_jobs[j.external_jobid]
            active_jobs_seen.add(j.external_jobid)
            if status == "DONE":
                self.report_job_success(j)
                any_finished = True
                active_jobs_seen.remove(j.external_jobid)
            elif status in fail_stati:
                msg = f"SGE job '{j.external_jobid}' failed, SGE status is: '{status}'"
                self.report_job_error(j, msg=msg, aux_logs=[j.aux["sge_logfile"]])
                active_jobs_seen.remove(j.external_jobid)
            else:
                yield j

        if not any_finished:
            self.next_seconds_between_status_checks = min(
                self.next_seconds_between_status_checks + 300, max_sleep_time
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
                # Under 'normal' circumstances, 'qdel' is executed in
                # virtually no time.
                subprocess.check_output(
                    f"qdel {jobids}",
                    text=True,
                    shell=True,
                    timeout=1800,
                    stderr=subprocess.PIPE,
                )
                self.logger.info(f"Cancelled jobs {jobids}")
            except subprocess.TimeoutExpired:
                self.logger.warning("Unable to cancel jobs within a minute. Jobs:m {jobids}")

            except subprocess.CalledProcessError as e:
                self.logger.warning(f"Failed to cancel jobs {jobids}. Error: {e.stderr.strip()}")

    async def job_stati_qstat(self):
        """
        Obtain sge job status of all submitted jobs from qstat
        """

        uuid = self.run_uuid
        query_duration = None
        res ={}

        try:
            running_cmd = f"qstat -s pr -u '{os.getenv('USER')}' -xml"
            #print(f"Running command: {running_cmd}")
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
                qstat_dict = xmltodict.parse(running)
                queue_info = qstat_dict.get("queue_info", {})
                running_jobs = queue_info.get("job_list", [])

                # Ensure running_jobs is always treated as a list
                if isinstance(running_jobs, dict):
                    running_jobs = [running_jobs]

                # Handle the pending jobs from job_info
                job_info = qstat_dict.get("job_info", {}).get("job_info", {})
                
                if not job_info:
                    #self.logger.warning("job_info is empty in qstat response.")
                    pending_jobs = []
                else:
                    pending_jobs = job_info.get("job_list", [])

                    # Ensure pending_jobs is always treated as a list
                    if isinstance(pending_jobs, dict):
                        pending_jobs = [pending_jobs]

                # Now concatenate running and pending jobs
                job_list = running_jobs + pending_jobs
                    #job_list = qstat_dict.get("job_info", {}).get("job_info", {}).get("job_list", [])
                await asyncio.sleep(30)
                # If there's only one job, ensure job_list is treated as a list
                #if isinstance(job_list, dict):
                 #   job_list = [job_list]

                qstat_jobs = {x['JB_job_number']: {"name": x["JB_name"], "state": x["@state"]}
                            for x in job_list if x["@state"] in ("r", "qw")}

                # Filter jobs based on UUID in the job name
                res = {k: v["state"] for k, v in qstat_jobs.items() if re.search(uuid, v["name"])}
        except subprocess.CalledProcessError as e:
            self.logger.error(
                f"The running job status query failed with command: {running_cmd}\n"
                f"Error message: {e.stderr.strip()}\n"
            )
        return (res, query_duration)

    async def job_stati_qacct(self):
        """
        Obtain sge job status of all submitted jobs from sge_EVENTS
        """

        statuses = {
        "0": "DONE",  # Job has terminated with status 0.
    }
        def get_status(exit_code):
            return statuses.get(exit_code, "EXIT") 

        #   * because execution host was overloaded or queue run window closed.
        #   ** may have been aborted due to an execution error
        #      or killed by owner or sge sysadmin.
        #  *** The server batch daemon (sbatchd) on the host on which the job
        #      is processed has lost contact with the master batch daemon
        #      (mbatchd).
        # **** This state shows that the job is dispatched to a standby
        #      power-saved host, and this host is being waken up or started up.

        username = os.getenv("USER")

        awk_code = f"""
        tail -n 500000 {self.sge_config['accounting']} | 
        awk 'BEGIN {{
            FS = ":"; OFS = "\\t"
        }} 
        $4 == "{username}" && $5 ~ /{self.run_uuid}/ {{
            print $6, $13
            }}'
        """

        finished = subprocess.check_output(awk_code, shell=True, text=True, stderr=subprocess.PIPE)
        statuses_all = []
        query_duration = None
        try:
            time_before_query = time.time()
            finished = subprocess.check_output(
                awk_code, shell=True, text=True, stderr=subprocess.PIPE
            ).strip()
            query_duration = time.time() - time_before_query
            self.logger.debug(
                f"The job status for completed jobs was queried.\n"
                f"It took: {query_duration} seconds\n"
                f"The output is:\n' finished: {finished}'\n"
            )
            if finished:
                statuses_all = [(p[0], get_status(p[1])) for p in (line.split() for line in finished.split("\n")) if len(p) >= 2]

        except subprocess.CalledProcessError as e:
            self.logger.error(
                f"The finished job status query failed with command: {awk_code}\n"
                f"Error message: {e.stderr.strip()}\n"
            )

        res = {x[0]: x[1] for x in statuses_all if len(x) > 1}
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
        return max(1, cpus_total)

    def get_mem(self, job: JobExecutorInterface):
        """
        Gets the SGE memory request amount for the job.
        Converts the memory to gigabytes (GB) from any of the provided units (KB, MB, GB, TB).
        Returns the memory as an integer.
        """
        # Conversion factors for converting various units to GB
        
        
        # Get the memory unit from the SGE config, default to MB (M)
        #mem_unit = self.sge_config.get("sge_UNIT_FOR_LIMITS", "M")[0]  # Extract the first character (K, M, G, T)
        
        # Get the conversion factor for the detected memory unit
        #conv_fct = conv_fcts.get(mem_unit, 1)  # Default to 1 (GB)
        #TODO: mem_mb_per_cpu conversion 
        # Check for various memory fields in the job's resources and convert to GB
        if job.resources.get("mem_mb") and isinstance(job.resources.mem_mb, (int, float)):
            mem_gb = job.resources.mem_mb / 1024
        else:
            self.logger.warning(
                "No valid job memory information is given - submitting without memory request. "
                "This might or might not work on your cluster."
            )
            return None  
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

    def get_queue_arg(self, job: JobExecutorInterface):
        """
        checks whether the desired queue is valid,
        returns a default queue, if applicable
        else raises an error - implicetly.
        """
        if job.resources.get("sge_queue"):
            queue = job.resources.sge_queue
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
    
def format_job_exec(self, job: JobExecutorInterface):
    """
    Formats the job execution command.
    """
    exec_job = ""
    # Get the execution command for group jobs
    if job.is_group():
        # Ensure that all jobs in the group have an exec_job attribute
        exec_job = " && ".join([j.exec_job for j in job.jobs if hasattr(j, 'exec_job')])
    else:
        pass
    
    return exec_job

def walltime_sge_to_generic(w):
    """
    Convert walltime to the generic HH:MM:SS format required by SGE.
    """
    if isinstance(w, (int, float)):
        # Convert numeric minutes to HH:MM:SS
        h, m = divmod(int(w), 60)
        s = 0
        return f"{h:02}:{m:02}:{s:02}"
    elif isinstance(w, str):
        if re.match(r"^\d+(ms|[smhdw])$", w):
            return w
        elif re.match(r"^\d+:\d+$", w):
            # Convert "HH:MM" to "HH:MM:SS"
            h, m = map(int, w.split(":"))
            s = 0
            return f"{h:02}:{m:02}:{s:02}"
        elif re.match(r"^\d+:\d+:\d+$", w):
            # Already in "HH:MM:SS", return as-is
            return w
        elif re.match(r"^\d+\.\d+$", w):
            # Convert decimal minutes to HH:MM:SS
            total_minutes = float(w)
            h, m = divmod(int(total_minutes), 60)
            s = int((total_minutes - int(total_minutes)) * 60)
            return f"{h:02}:{m:02}:{s:02}"
        elif re.match(r"^\d+$", w):
            # Plain numeric string, treat as minutes
            h, m = divmod(int(w), 60)
            s = 0
            return f"{h:02}:{m:02}:{s:02}"
        else:
            raise ValueError(f"Invalid walltime format: {w}")
    else:
        raise ValueError(f"Unsupported type for walltime: {type(w)}")


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