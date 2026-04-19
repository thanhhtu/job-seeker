class JobSeekerError(Exception):
    """Base exception for job-seeker errors."""

    pass


class PoolNotInitializedError(JobSeekerError):
    """Raised when database pool is accessed before initialization."""

    pass


class JobNotFoundError(JobSeekerError):
    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Job not found: {job_id}")


class DuplicateJobError(JobSeekerError):
    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Duplicate job: {job_id}")
