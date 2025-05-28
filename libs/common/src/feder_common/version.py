from importlib.metadata import version
from subprocess import CalledProcessError, run


def get_feder_version() -> str:
    """Return current Feder API version."""

    feder_version = None
    try:
        feder_version = version('feder')
    except Exception:
        pass
    if feder_version is None:
        try:
            result = run(
                ['git', 'describe', 'HEAD'],
                check=True, capture_output=True
            )
            feder_version = 'dev-' + result.stdout.decode()
        except CalledProcessError:
            pass

    return 'unknown' if feder_version is None else feder_version
