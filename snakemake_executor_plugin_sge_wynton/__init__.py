__author__ = "Johannes Köster, Manuel Holtgrewe"
__copyright__ = "Copyright 2023, Johannes Köster, Manuel Holtgrewe"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

import pandas as pd
from dataclasses import dataclass, field
import xml.etree.ElementTree as ET
from time import sleep
import os
import subprocess
import logging
import re
import shutil
import contextlib
from typing import AsyncGenerator, List, Optional
from snakemake.deployment.conda import Conda
from snakemake.shell import shell
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import (
    ExecutorSettingsBase,
    CommonSettings,
)
import yaml
from snakemake_interface_executor_plugins.workflow import WorkflowExecutorInterface
from snakemake_interface_executor_plugins.logging import LoggerExecutorInterface
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
    "mem_free": ("mem_mb","mem_mib"),
    "s_fsize": ("s_fsize", "soft_file_size"),
    "h_fsize": ("h_fsize", "disk_mb", "file_size", "disk_mib"),
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
cwd = os.getcwd()
print(f"Current working directory: {cwd}")

class Executor(RemoteExecutor):
    """
   setting specific to SGE
    """
    def __post_init__(self):
        self.workflow: WorkflowExecutorInterface
        self.workflow.executor_settings
        if not self.workflow.executor_settings.submit_cmd:
            raise WorkflowError(
                "You have to specify a submit command via --sge-wynton-submit-cmd."
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
    
    def parse_snakefile(self, snakefile_path):
        with open(snakefile_path, 'r') as file:
            content = file.read()
        # Extract global variables
        global_vars = {}
        global_var_pattern = re.compile(r"(\w+)\s*=\s*(.+)")
        for match in global_var_pattern.finditer(content):
            key, value = match.groups()
            try:
                global_vars[key] = eval(value)
            except:
                global_vars[key] = value.strip("'\"")

        rule_pattern = re.compile(r'rule (\w+):([\s\S]*?)(?=^rule|\Z)', re.MULTILINE)
        section_pattern = re.compile(r'(\w+):\s*(\[?[\s\S]*?\]?)\s*[,|\n]')
        #print("Rule pattern: ", rule_pattern)
        #print("Section pattern: ", section_pattern)
        

        rules = []
        
        for match in rule_pattern.finditer(content):
            rule_name = match.group(1)
            rule_content = match.group(2)

            rule = {
                'name': rule_name,
                'sections': {}
            }

            for section_match in section_pattern.finditer(rule_content):
                section_name = section_match.group(1)
                section_value = section_match.group(2).strip()
                try:
                    rule['sections'][section_name] = eval(section_value)
                except:
                    rule['sections'][section_name] = section_value.strip("'")

            rules.append(rule)
            print(f"Rule: {rule}")

        return global_vars, rules
    
    def generate_resource_options(self, job):
        resource_options = []
        resources = job.resources.get("resources", {})

    # Check if job.threads is set and use it; otherwise, default to 1
        if job.threads:
            resource_options.append(f"-pe smp {job.threads}")
        else:
            resource_options.append(f"-pe smp 1")
        for resource_key, value in resources.items():
            if resource_key in ["threads", "_cores", "_nodes", "disk_mib", "mem_mib"]:
                continue
            if value in ["<TBD>", None, ""]:
                continue

            mapped = False
            for qsub_option, resource_keys in RESOURCE_MAPPING.items():
                if resource_key in resource_keys:
                    if resource_key == "mem_mb":
                        value = f"{value / 1024:.2f}G"  # Convert MB to GB
                    elif resource_key == "time_min" and isinstance(value, int):
                        # Convert time_min to HH:MM:SS format if necessary
                        value = f"{value // 60:02}:{value % 60:02}:00"
                    elif resource_key == "disk_mb":
                        value = f"{value / 1024:.2f}G"
                    resource_option = f"-l {qsub_option}={value}"
                    resource_options.append(resource_option)
                    mapped = True
                    break
            if not mapped:
                resource_option = f"-l {resource_key}={value}"
                resource_options.append(resource_option)

        return resource_options
   
    def convert_param_name(self, param_name):
        return param_name.upper()
    

    def prepare_job_script(self, job:JobExecutorInterface, jobscript_path, global_vars):
        #script = job.rule.script or job.rule.shellcmd
        #script_path = os.path.join("workflow", job.rule.script)
        
        #jobscript_content = "#!/bin/bash\n"
        jobscript_content = ""
        jobscript_content += "#$ -S /bin/Rscript\n"
        #jobscript_content += "#$ -S /bin/Rscript\n"
        jobscript_content += "#$ -cwd\n"
        jobscript_content += "#$ -j y\n"
        
        #conda_env_file = job.rule.conda_env if hasattr(job.rule, 'conda_env') else None

        resource_options = self.generate_resource_options(job)
        jobscript_content += ''.join([f"#$ {opt}\n" for opt in resource_options])
        jobscript_content += "#$ -r y\n"
        #jobscript_content += "module load CBI r\n"
        
        #jobscript_content += "set -x\n"
        #jobscript_content += "set +u\n"
        #jobscript_content += f"source {self.workflow.basedir}/{job.rule.conda_env}\n"
        #jobscript_content += "set -e\n"

        # Create dictionaries for input, output, params
        env_vars = {
            "SNAKEMAKE_INPUT": job.input,
            "SNAKEMAKE_OUTPUT": job.output,
            #"SNAKEMAKE_WILDCARDS": job.wildcards_dict,
            #"SNAKEMAKE_THREADS": job.threads,
            #"SNAKEMAKE_RULE": job.rule.name,
            #"SNAKEMAKE_WORKFLOW": self.workflow.workdir_init,
            #"SNAKEMAKE_PARAMS": job.rule.params
            #"SNAKEMAKE_WORKFLOW_BASEDIR": self.workflow.basedir
        }
        print("env_vars: ", env_vars)
        # Add environment variables to the job script
        for prefix, data in env_vars.items():
            if isinstance(data, dict):
                for key, value in data.items():
                    env_var_name = f"{prefix}_{self.convert_param_name(key)}"
                    if isinstance(value, list):
                        joined_values = ",".join(map(str, value))
                        jobscript_content += f"export {env_var_name}={joined_values}\n"
                    else:
                        jobscript_content += f"export {env_var_name}={value}\n"
            elif isinstance(data, list):
                joined_values = ",".join(map(str, data))
                jobscript_content += f"export {prefix}={joined_values}\n"
            else:
                jobscript_content += f"export {prefix}={data}\n"

        # Handle params separately to export each key-value pair as an environment variable
        if hasattr(job.rule, 'params') and job.rule.params:
            if isinstance(job.rule.params, list):
                for key, value in job.rule.params.items():
                    #print("ruleP: ",job.rule.params.items())
                    env_var_name = self.convert_param_name(key)
                    if isinstance(value, pd.Series):
                        value = value.iloc[0]

                    jobscript_content += f"export {env_var_name}={value}\n"
            elif isinstance(job.rule.params, list):
                for idx, value in enumerate(job.rule.params):
                    env_var_name = f"SNAKEMAKE_PARAM_{idx}"
                    jobscript_content += f"export {env_var_name}={value}\n"

        #jobscript_content += f"export SNAKEMAKE_THREADS={job.threads}\n"


        script_or_shell = f"{job.rule.shellcmd}\n" if job.rule.shellcmd else f"{self.workflow.basedir}/{job.rule.script}"

        for var, value in global_vars.items():
            script_or_shell = script_or_shell.replace(f"{{{var}}}", str(value))

        jobscript_content += f"{script_or_shell}\n"
        #jobscript_content += f"cd {self.workflow.workdir_init}\n"


        # Replace placeholders with actual values
        for key, value in job.wildcards_dict.items():
            jobscript_content = jobscript_content.replace(f"{{{key}}}", value)


        # Write the job script to the specified path
        with open(jobscript_path, 'w') as jobscript_file:
            jobscript_file.write(jobscript_content)
            print(f"Job script written to {jobscript_path}")

    def run_job(self, job: JobExecutorInterface):
        cwd = os.getcwd()
        log_dir = os.path.join(cwd, "logs")
        os.makedirs(log_dir, exist_ok=True)

        jobscript = self.get_jobscript(job)
        jobscript_path = os.path.join(job.rule.conda_env, jobscript)

        global_vars, _ = self.parse_snakefile(self.workflow.snakefile)
        # Use job_args in your logic if necessary
        self.prepare_job_script(job, jobscript_path, global_vars)

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

        resource_options = self.generate_resource_options(job)
        resource_str = " ".join(resource_options)
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

        log_output_path = os.path.join(log_dir, f"snakejob.{job.rule.name}.{job.jobid}.sh.o{ext_jobid}")
        log_error_path = os.path.join(log_dir, f"snakejob.{job.rule.name}.{job.jobid}.sh.e{ext_jobid}")

        original_log_output = f"{jobscript}.o{ext_jobid}"
        original_log_error = f"{jobscript}.e{ext_jobid}"

        if os.path.exists(original_log_output):
            shutil.move(original_log_output, log_output_path)
        if os.path.exists(original_log_error):
            shutil.move(original_log_error, log_error_path)

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

