def execute_job(*, job_type: str, job_params: dict):
    if job_type == 'echo':
        message = job_params['message']
        print(message)
    elif job_type == 'import_session_from_dandi':
        from import_session_from_dandi import import_session_from_dandi
        nwb_file_id = job_params['nwb_file_id']
        dandiset_id = job_params['dandiset_id']
        dandiset_version = job_params['dandiset_version']
        nwb_file_path = job_params['nwb_file_path']
        nwb_file_url = job_params['nwb_file_url']
        import_session_from_dandi(
            nwb_file_id=nwb_file_id,
            dandiset_id=dandiset_id,
            dandiset_version=dandiset_version,
            nwb_file_path=nwb_file_path,
            nwb_file_url=nwb_file_url,
        )
    else:
        raise Exception(f'Unknown job type: {job_type}')
