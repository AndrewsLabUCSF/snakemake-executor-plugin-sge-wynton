__author__ = "Johannes Köster, Manuel Holtgrewe"
__copyright__ = "Copyright 2023, Johannes Köster, Manuel Holtgrewe"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

from dataclasses import dataclass, field
import xml.etree.ElementTree as ET
from time import sleep
import os
import subprocess
import logging
import re
from typing import AsyncGenerator, List, Optional
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import (
    ExecutorSettingsBase,
    CommonSettings,
)
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface

RESOURCE_MAPPING = {
    "qname": ("qname",),
    "hostname": ("hostname",),
    "calendar": ("calendar",),
    "min_cpu_interval": ("min_cpu_interval",),
    "tmpdir": ("tmpdir",),
    "seq_no": ("seq_no",),
    "s_rt": ("s_rt", "soft_runtime", "soft_walltime"),
    "h_rt": ("h_rt", "time", "runtime", "walltime", "time_min"),
    "s_cpu": ("s_cpu", "soft_cpu"),
    "h_cpu": ("h_cpu", "cpu"),
    "s_data": ("s_data", "soft_data"),
    "h_data": ("h_data", "data"),
    "s_stack": ("s_stack", "soft_stack"),
    "h_stack": ("h_stack", "stack"),
    "s_core": ("s_core", "soft_core"),
    "h_core": ("h_core", "core"),
    "s_rss": ("s_rss", "soft_resident_set_size"),
    "h_rss": ("h_rss", "resident_set_size"),
    "slots": ("slots",),
    "s_vmem": ("s_vmem", "soft_memory", "soft_virtual_memory"),
    "h_vmem": ("h_vmem", "mem", "memory", "virtual_memory"),
    "mem_free": ("mem_mb",),
    "s_fsize": ("s_fsize", "soft_file_size"),
    "h_fsize": ("h_fsize", "disk_mb", "file_size", "mem_mib"),
}


@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    submit_cmd: Optional[str] = field(
        default="qsub",
        metadata={
            "help": "Command for submitting jobs",
            "required": True,
        },
    )
    status_cmd: Optional[str] = field(
        default="qstat -xml", metadata={"help": "Command for retrieving job status"}
    )
    cancel_cmd: Optional[str] = field(
        default="qdel",
        metadata={
            "help": "Command for cancelling jobs. Expected to take one or more jobids as arguments."
        },
    )
    cancel_nargs: int = field(
        default=20,
        metadata={
            "help": "Number of jobids to pass to cancel_cmd. If more are given, cancel_cmd will be called multiple times."
        },
    )
    sidecar_cmd: Optional[str] = field(
        default=None, metadata={"help": "Command for sidecar process."}
    )
    default_cwd: Optional[bool] = field(
        default=True,
        metadata={
            "help": "Submit jobs with the -cwd option (run in current working directory)"
        },
    )
    default_pe: Optional[str] = field(
        default=None,
        metadata={"help": "Parallel environment specification, e.g., 'smp 4'"},
    )
    default_mem_mb: Optional[str] = field(
        default=None,
        metadata={"help": "Memory requirement, e.g., 200"},
    )
    default_scratch: Optional[str] = field(
        default=None,
        metadata={"help": "Scratch space requirement, e.g., '50G'"},
    )
    default_time_min: Optional[str] = field(
        default="00:05:00",
        metadata={"help": "Runtime limit, e.g., '00:05:00'"},
    )


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    non_local_exec=True,
    implies_no_shared_fs=False,
    job_deploy_sources=False,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=True,
    auto_deploy_default_storage_provider=False,
)

logger = logging.getLogger(__name__)


class Executor(RemoteExecutor):
    def __post_init__(self):
        if not self.workflow.executor_settings.submit_cmd:
            raise WorkflowError(
                "You have to specify a submit command via --cluster-generic-submit-cmd."
            )

        if (
            not self.workflow.executor_settings.status_cmd
            and not self.workflow.storage_settings.assume_common_workdir
        ):
            raise WorkflowError(
                "If no shared filesystem is used, you have to specify a cluster status command."
            )

        self.sidecar_vars = None
        if self.workflow.executor_settings.sidecar_cmd:
            self._launch_sidecar()
        if (
            not self.workflow.executor_settings.status_cmd
            and not self.workflow.storage_settings.assume_common_workdir
        ):
            raise WorkflowError(
                "If no shared filesystem is used, you have to "
                "specify a cluster status command."
            )

        self.status_cmd_kills = []
        self.external_jobid = {}

    # get_jobfinished_marker and get_jobfailed_marker, generate filenames for temporary marker files that indicate whether a job has finished successfully or failed.
    def get_jobfinished_marker(self, job: JobExecutorInterface) -> str:
        return f".snakemake/tmp/{job.jobid}.finished"

    def get_jobfailed_marker(self, job: JobExecutorInterface) -> str:
        return f".snakemake/tmp/{job.jobid}.failed"

    def run_job(self, job: JobExecutorInterface):
        cwd = os.getcwd()
        # Define the log directory path
        log_dir = os.path.join(cwd, "logs")
        # print(log_dir)
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)

        jobscript = self.get_jobscript(job)
        jobscript_path = os.path.join(log_dir, os.path.basename(jobscript))
        self.write_jobscript(job, jobscript_path)

        jobfinished = self.get_jobfinished_marker(job)
        jobfailed = self.get_jobfailed_marker(job)

        job_info = SubmittedJobInfo(
            job,
            aux={
                "jobscript": jobscript_path,
                "jobfinished": jobfinished,
                "jobfailed": jobfailed,
            },
        )

        if self.workflow.executor_settings.status_cmd:
            ext_jobid = self.dag.incomplete_external_jobid(job)
            if ext_jobid:
                self.logger.info(
                    f"Resuming incomplete job {job.jobid} with external jobid '{ext_jobid}'."
                )
                self.external_jobid.update((f, ext_jobid) for f in job.output)
                self.report_job_submission(
                    SubmittedJobInfo(job, external_jobid=ext_jobid)
                )
                return

        deps = " ".join(
            self.external_jobid[f] for f in job.input if f in self.external_jobid
        )
        try:
            submitcmd = job.format_wildcards(
                self.workflow.executor_settings.submit_cmd, dependencies=deps
            )
        except AttributeError as e:
            raise WorkflowError(str(e), rule=job.rule if not job.is_group() else None)

        resource_options = []
        if self.workflow.executor_settings.default_cwd:
            resource_options.append("-cwd")

        # self.logger.info(f"Job {job.jobid} attributes: {vars(job)}")

        resources = job.resources
        # print(resources)
        self.logger.info(f"Job {job.jobid} resources: {resources}")

        # Use job.threads for parallel environment
        if job.threads:
            resource_options.append(f"-pe smp {job.threads}")

        # Extract resources using RESOURCE_MAPPING and handle unmapped resources
        for resource_key, value in resources.items():
            # Skip adding threads, _cores, and _nodes as unmapped resources for redundancy still need to figure out _cores and _nodes
            if resource_key in ["threads", "_cores", "_nodes"]:
                continue
            # self.logger.info(f"Checking resource_key: {resource_key} with value: {value}")
            if value in ["<TBD>", None, ""]:
                # self.logger.info(f"Skipping resource_key: {resource_key} with invalid value: {value}")
                continue
            mapped = False
            for qsub_option, resource_keys in RESOURCE_MAPPING.items():
                if resource_key in resource_keys:
                    if resource_key == "mem_mb":
                        value = f"{value / 1024:.2f}G"  # Convert MB to GB
                    elif resource_key == "time_min" and isinstance(value, int):
                        # Convert time_min to HH:MM:SS format if necessary
                        value = f"{value // 60:02}:{value % 60:02}:00"
                    resource_option = f"-l {qsub_option}={value}"
                    resource_options.append(resource_option)
                    self.logger.info(
                        f"Adding mapped resource option: {resource_option}"
                    )
                    mapped = True
                    break
            if not mapped:
                resource_option = f"-l {resource_key}={value}"
                resource_options.append(resource_option)
                self.logger.info(f"Adding unmapped resource option: {resource_option}")

        # Handle additional resources directly from job.resources
        if "scratch" in resources:
            resource_options.append(f"-l scratch={resources['scratch']}")
        elif self.workflow.executor_settings.default_scratch:
            resource_options.append(
                f"-l scratch={self.workflow.executor_settings.default_scratch}"
            )

        # Log the resource options
        self.logger.info(
            f"Submitting job {job.jobid} with resource options: {resource_options}"
        )

        resource_str = " ".join(resource_options)
        print(resource_str)
        submitcmd = f"{submitcmd} {resource_str}"

        try:
            env = dict(os.environ)
            if self.sidecar_vars:
                env["SNAKEMAKE_CLUSTER_SIDECAR_VARS"] = self.sidecar_vars

            env.pop("SNAKEMAKE_PROFILE", None)

            self.logger.info(f'Executing command: {submitcmd} "{jobscript_path}"')
            submit_output = subprocess.check_output(
                f'{submitcmd} "{jobscript_path}"',
                shell=True,
                env=env,
            ).decode()

            self.logger.info(f"Job submission output: {submit_output}")

            ext_jobid = re.search(r"Your job (\d+)", submit_output).group(1)
        except subprocess.CalledProcessError as ex:
            msg = f"Error submitting jobscript (exit code {ex.returncode}):\n{ex.output.decode()}"
            self.logger.error(msg)
            self.report_job_error(job_info, msg=msg)
            return

        if ext_jobid:
            job_info.external_jobid = ext_jobid

            self.external_jobid.update((f, ext_jobid) for f in job.output)

            self.logger.info(
                f"Submitted {'group job' if job.is_group() else 'job'} {job.jobid} with external jobid '{ext_jobid}'."
            )

        self.report_job_submission(job_info)

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> AsyncGenerator[SubmittedJobInfo, None]:
        job_ids = [job.external_jobid for job in active_jobs]
        if not job_ids:
            return

        job_id_str = ",".join(job_ids)
        try:
            status_output = subprocess.check_output(
                f"qstat -xml -j {job_id_str}", shell=True
            ).decode()
            root = ET.fromstring(status_output)
            for job in active_jobs:
                job_state = root.find(
                    f".//job_list[JB_job_number='{job.external_jobid}']/state"
                )
                if job_state is not None:
                    state = job_state.text
                    if state in ("r", "t"):
                        yield job
                    elif state in ("Eqw", "dr", "dt"):
                        self.report_job_error(job)
                    else:
                        self.report_job_success(job)
                else:
                    self.report_job_success(job)
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to retrieve job status for jobs: {job_id_str}")
            for job in active_jobs:
                self.report_job_error(job)

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        if active_jobs:
            jobids = " ".join([job.external_jobid for job in active_jobs])
            try:
                subprocess.check_output(f"qdel {jobids}", shell=True)
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Failed to cancel jobs: {jobids}")
