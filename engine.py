import os
import sys
import tempfile
import yaml
import datetime
import traceback
from execute_job import execute_job


def engine():
    _pull()
    for project_name in os.listdir('jobs/projects'):
        for submitted_job_name in os.listdir(f'jobs/projects/{project_name}/submitted'):
            if submitted_job_name.endswith('.yaml'):
                # job_id is the file name without the extension
                job_id_desc = submitted_job_name[:-len('.yaml')]
                rndstr = _create_random_string()
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
                job_id = f'{timestamp}_{rndstr}_{job_id_desc}'
                try:
                    _handle_submitted_job(project_name=project_name, submitted_job_id_desc=job_id_desc, submitted_job_id=job_id)
                except Exception as e:
                    print(f'Error handling job {job_id}: {e}')
                    continue


def _handle_submitted_job(project_name: str, submitted_job_id_desc: str, submitted_job_id: str):
    submitted_job_fname = f'jobs/projects/{project_name}/submitted/{submitted_job_id_desc}.yaml'
    running_dirname = f'jobs/projects/{project_name}/running/{submitted_job_id}'
    completed_dirname = f'jobs/projects/{project_name}/completed/{submitted_job_id}'
    failed_dirname = f'jobs/projects/{project_name}/failed/{submitted_job_id}'
    if os.path.exists(running_dirname):
        raise Exception(f'Running job directory already exists: {running_dirname}')
    if os.path.exists(completed_dirname):
        raise Exception(f'Completed job directory already exists: {completed_dirname}')
    if os.path.exists(failed_dirname):
        raise Exception(f'Failed job directory already exists: {failed_dirname}')
    os.makedirs(running_dirname)
    running_job_fname = f'{running_dirname}/job.yaml'
    _move_file(submitted_job_fname, running_job_fname)
    _commit(project_name=project_name, message=f'START {submitted_job_id}')
    try:
        with capture_console_output(f'{running_dirname}/console_out.txt'):
            _run_job(project_name=project_name, job_id=submitted_job_id)
    except Exception:
        _move_dir(running_dirname, failed_dirname)
        trace_txt = traceback.format_exc()
        _write_text_file(f'{failed_dirname}/error.txt', trace_txt)
        _commit(project_name=project_name, message=f'FAIL {submitted_job_id}')
        return
    _move_dir(running_dirname, completed_dirname)
    _commit(project_name=project_name, message=f'COMPLETE {submitted_job_id}')


def _run_job(*, project_name: str, job_id: str):
    running_dirname = f'jobs/projects/{project_name}//running/{job_id}'
    job_fname = f'{running_dirname}/job.yaml'
    console_out_fname = f'{running_dirname}/console_out.txt'
    with open(job_fname) as f:
        job = yaml.safe_load(f)
    job_type = job['type']
    job_params = job.get('params', {})
    with capture_console_output(console_out_fname):
        execute_job(job_type=job_type, job_params=job_params)


def _move_file(src: str, dst: str):
    if not src.startswith('jobs/') or not dst.startswith('jobs/'):
        raise Exception(f'Invalid move: {src} -> {dst}')
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    os.rename(src, dst)


def _move_dir(src: str, dst: str):
    if not src.startswith('jobs/') or not dst.startswith('jobs/'):
        raise Exception(f'Invalid move: {src} -> {dst}')
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    os.rename(src, dst)


def _write_text_file(fname: str, content: str):
    with open(fname, 'w') as f:
        f.write(content)


def _commit(*, project_name: str, message: str):
    _append_to_log(project_name=project_name, message=message)
    _run_shell_script(f'cd jobs && git add . && git commit -m "{project_name}: {message}"')
    _run_shell_script('cd jobs && git push')


def _append_to_log(*, project_name: str, message: str):
    log_fname = f'jobs/projects/{project_name}/log.txt'
    with open(log_fname, 'a') as f:
        f.write(f'{datetime.datetime.now()} {message}\n')


def _pull():
    if not os.path.exists('jobs'):
        _clone_repo()
    else:
        _run_shell_script('cd jobs && git pull')


def _clone_repo():
    repo_url = 'https://github.com/magland/arc1'
    _run_shell_script(f'''git clone {repo_url} jobs && cd jobs && git checkout jobs''')


def _run_shell_script(script):
    with tempfile.TemporaryDirectory() as tmpdir:
        script_fname = f'{tmpdir}/script.sh'
        with open(script_fname, 'w') as f:
            f.write(script)
        os.system(f'bash {script_fname}')


def capture_console_output(fname):
    class ConsoleOutput:
        def __enter__(self):
            self.stdout = sys.stdout
            self.stderr = sys.stderr
            self.file = open(fname, 'w')
            sys.stdout = self.file
            sys.stderr = self.file
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            sys.stdout = self.stdout
            sys.stderr = self.stderr
            self.file.close()

    return ConsoleOutput()


def _create_random_string():
    return os.urandom(4).hex()


if __name__ == "__main__":
    engine()
