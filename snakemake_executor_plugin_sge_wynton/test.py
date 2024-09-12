import xmltodict

uuid = "nf-PGSCATALOG"

test = """<?xml version='1.0'?>
<job_info  xmlns:xsd="http://arc.liv.ac.uk/repos/darcs/sge/source/dist/util/resources/schemas/qstat/qstat.xsd">
  <queue_info>
  </queue_info>
  <job_info>
    <job_list state="pending">
      <JB_job_number>2928239</JB_job_number>
      <JAT_prio>0.05448</JAT_prio>
      <JB_name>nf-PGSCATALOG_PGSCCALC_PGSCCALC_MAKE_COMPATIBLE_PLINK2_RELABELBIM_(all_chromosome_ALL)</JB_name>
      <JB_owner>rakshyasharma</JB_owner>
      <state>Eqw</state>
      <JB_submission_time>2024-09-04T16:47:08</JB_submission_time>
      <queue_name></queue_name>
      <slots>2</slots>
    </job_list>
    <job_list state="pending">
      <JB_job_number>2944470</JB_job_number>
      <JAT_prio>0.05444</JAT_prio>
      <JB_name>nf-PGSCATALOG_PGSCCALC_PGSCCALC_MAKE_COMPATIBLE_PLINK2_RELABELBIM_(all_chromosome_ALL)</JB_name>
      <JB_owner>rakshyasharma</JB_owner>
      <state>Eqw</state>
      <JB_submission_time>2024-09-05T14:54:02</JB_submission_time>
      <queue_name></queue_name>
      <slots>4</slots>
    </job_list>
    <job_list state="pending">
      <JB_job_number>2944473</JB_job_number>
      <JAT_prio>0.05296</JAT_prio>
      <JB_name>nf-PGSCATALOG_PGSCCALC_PGSCCALC_ANCESTRY_PROJECT_EXTRACT_DATABASE_(1)</JB_name>
      <JB_owner>rakshyasharma</JB_owner>
      <state>Eqw</state>
      <JB_submission_time>2024-09-05T14:54:02</JB_submission_time>
      <queue_name></queue_name>
      <slots>4</slots>
    </job_list>
    <job_list state="pending">
      <JB_job_number>2944475</JB_job_number>
      <JAT_prio>0.05296</JAT_prio>
      <JB_name>nf-PGSCATALOG_PGSCCALC_PGSCCALC_INPUT_CHECK_COMBINE_SCOREFILES</JB_name>
      <JB_owner>rakshyasharma</JB_owner>
      <state>Eqw</state>
      <JB_submission_time>2024-09-05T14:54:02</JB_submission_time>
      <queue_name></queue_name>
      <slots>4</slots>
    </job_list>
    <job_list state="pending">
      <JB_job_number>2945733</JB_job_number>
      <JAT_prio>0.05294</JAT_prio>
      <JB_name>nf-PGSCATALOG_PGSCCALC_PGSCCALC_ANCESTRY_PROJECT_EXTRACT_DATABASE_(1)</JB_name>
      <JB_owner>rakshyasharma</JB_owner>
      <state>Eqw</state>
      <JB_submission_time>2024-09-05T14:58:03</JB_submission_time>
      <queue_name></queue_name>
      <slots>4</slots>
    </job_list>
    <job_list state="pending">
      <JB_job_number>2945734</JB_job_number>
      <JAT_prio>0.05294</JAT_prio>
      <JB_name>nf-PGSCATALOG_PGSCCALC_PGSCCALC_MAKE_COMPATIBLE_PLINK2_RELABELBIM_(all_chromosome_ALL)</JB_name>
      <JB_owner>rakshyasharma</JB_owner>
      <state>Eqw</state>
      <JB_submission_time>2024-09-05T14:58:03</JB_submission_time>
      <queue_name></queue_name>
      <slots>4</slots>
    </job_list>
    <job_list state="pending">
      <JB_job_number>2945735</JB_job_number>
      <JAT_prio>0.05294</JAT_prio>
      <JB_name>nf-PGSCATALOG_PGSCCALC_PGSCCALC_INPUT_CHECK_COMBINE_SCOREFILES</JB_name>
      <JB_owner>rakshyasharma</JB_owner>
      <state>Eqw</state>
      <JB_submission_time>2024-09-05T14:58:03</JB_submission_time>
      <queue_name></queue_name>
      <slots>4</slots>
    </job_list>
  </job_info>
</job_info>"""

xmltodict.parse(test)
qstat_xml = xmltodict.parse(test)['job_info']['job_info']['job_list']
qstat_jobs = {x['JB_job_number']: {"name": x["JB_name"], "state": x["@state"]}
              for x in qstat_xml}
qstat_jobs_sm = {k: v["state"] for k, v in qstat_jobs.items()
                 if re.search(uuid, v["name"])}