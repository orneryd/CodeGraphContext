from datetime import datetime

from codegraphcontext.cli.cli_helpers import _format_cli_exception
from codegraphcontext.cli.cli_helpers import _format_job_failure
from codegraphcontext.core.jobs import JobInfo, JobStatus


def test_format_cli_exception_includes_cause_chain_and_location():
    try:
        try:
            raise ValueError("bad args payload")
        except ValueError as exc:
            raise RuntimeError("indexing failed") from exc
    except RuntimeError as exc:
        details = _format_cli_exception(exc)

    assert (
        "Cause chain: RuntimeError: indexing failed -> ValueError: bad args payload"
        in details
    )
    assert "Traceback location:" in details
    assert "Failing line:" in details


def test_format_job_failure_includes_original_traceback_and_context():
    job = JobInfo(
        job_id="job-1",
        status=JobStatus.FAILED,
        start_time=datetime.now(),
        current_phase="link_calls",
        current_file="/repo/example.py",
        errors=["invalid property value for key 'args'"],
        error_type="CypherSyntaxError",
        error_details="Traceback (most recent call last):\n  File 'graph_builder.py', line 1, in demo\n    raise ValueError('boom')\nValueError: boom\n",
    )

    details = _format_job_failure(job)

    assert "invalid property value for key 'args'" in details
    assert "Error type: CypherSyntaxError" in details
    assert "Job phase: link_calls" in details
    assert "Current item: /repo/example.py" in details
    assert "Original traceback:" in details
    assert "ValueError: boom" in details
