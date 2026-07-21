import os
import logging
import sys
import tomllib
from dataclasses import dataclass
from typing import Any

from pandas import NaT, Timedelta
from pandas._libs import NaTType

from feder_common import DataSource


logger = logging.getLogger(__name__)


def validate_path_roots(paths: dict[str, str]) -> None:
    """Validate that configured filesystem roots are distinct and unnested.

    The paths do not need to exist.  Comparisons use absolute, normalized path
    strings so that obvious relative-path aliases are caught without creating
    directories during configuration parsing.
    """
    normalized = {
        name: os.path.normcase(os.path.abspath(os.path.expanduser(path)))
        for name, path in paths.items()
    }

    names = list(normalized)
    for i, left_name in enumerate(names):
        left = normalized[left_name]
        for right_name in names[i + 1:]:
            right = normalized[right_name]
            common = os.path.commonpath([left, right])
            if left == right:
                raise ValueError(
                    f'path roots "{left_name}" and "{right_name}" must be distinct'
                )
            if common == left:
                raise ValueError(
                    f'path root "{right_name}" must not be nested inside "{left_name}"'
                )
            if common == right:
                raise ValueError(
                    f'path root "{left_name}" must not be nested inside "{right_name}"'
                )


@dataclass(frozen=True)
class ConfigRequirements:
    """Configuration sections required by a particular Feder command mode."""

    rabbitmq: bool = True
    ingester_prometheus: bool = True
    mailjet: bool = True
    receiver_queue: bool = False


STRICT_CONFIG_REQUIREMENTS = ConfigRequirements()
FILE_ONLY_CONFIG_REQUIREMENTS = ConfigRequirements(
    rabbitmq=False,
    ingester_prometheus=False,
    mailjet=False,
)
SCHEDULED_RX_CONFIG_REQUIREMENTS = ConfigRequirements(
    rabbitmq=False,
    ingester_prometheus=False,
    mailjet=False,
    receiver_queue=True,
)
RX_CONFIG_REQUIREMENTS = ConfigRequirements(
    rabbitmq=True,
    ingester_prometheus=False,
    mailjet=False,
)
INGEST_CONFIG_REQUIREMENTS = ConfigRequirements(
    rabbitmq=True,
    ingester_prometheus=True,
    mailjet=False,
)
STATE_OF_FEDER_CONFIG_REQUIREMENTS = ConfigRequirements(
    rabbitmq=False,
    ingester_prometheus=False,
    mailjet=True,
)


class Config:
    def __init__(
            self,
            config_file: str | None = None,
            config_text: str | None = None,
            requirements: ConfigRequirements = STRICT_CONFIG_REQUIREMENTS,
    ):
        self.requirements = requirements
        if config_text is None:
            config_file = config_file or os.environ.get('FEDER_CONFIG')
            if config_file is None:
                logger.critical(
                    'missing configuration file (must be given on command line '
                    'or in the FEDER_CONFIG environment variable).'
                )
                sys.exit(1)
            if not os.path.exists(config_file):
                logger.critical('configuration file does not exist.')
                sys.exit(1)

            with open(config_file, 'rb') as fp:
                self.raw = tomllib.load(fp)
        else:
            self.raw = tomllib.loads(config_text)

        logger.debug(self.raw)

        self._missing = []
        try:
            self._init_paths()
            self._init_rabbitmq()
            self._init_monitoring()
            self._init_sources()
        except KeyError as exc:
            logger.critical(exc.args[0])
            sys.exit(1)
        except Exception as exc:
            logger.critical(exc.args[0])
            sys.exit(1)

    def completion_delay(self, source: DataSource) -> Timedelta:
        return self._source_completion_delay[source]

    def data_lag(self, source: DataSource) -> Timedelta:
        return self._source_data_lag[source]

    def bounds(self, source: DataSource) -> list[float] | None:
        return self._source_bounds[source]

    def credentials(self, source: DataSource) -> dict[str, Any]:
        return self._source_credentials[source]

    def prometheus_port(self, source: DataSource) -> int | None:
        return self._source_prometheus_ports.get(source)

    def _init_paths(self):
        self.data_directory: str = self._get_str('paths', 'data-directory')
        self.staging_directory: str = self._get_str('paths', 'staging-directory')
        self.scratch_directory: str = self._get_str('paths', 'scratch-directory')
        self.receiver_queue_directory: str | None = (
            self._get_str('receiver', 'queue-directory')
            if self.requirements.receiver_queue else
            self._get_opt_str('receiver', 'queue-directory')
        )
        roots = {
            'paths/data-directory': self.data_directory,
            'paths/staging-directory': self.staging_directory,
            'paths/scratch-directory': self.scratch_directory,
        }
        if self.receiver_queue_directory is not None:
            roots['receiver/queue-directory'] = self.receiver_queue_directory
        validate_path_roots(roots)

    def _init_rabbitmq(self):
        if not self.requirements.rabbitmq:
            self.rabbitmq_host = None
            self.rabbitmq_port = None
            self.rabbitmq_username = None
            self.rabbitmq_password = None
            return

        self.rabbitmq_host: str = self._get_str('rabbitmq', 'host')
        self.rabbitmq_port: int = self._get_int('rabbitmq', 'port', default=5672)
        self.rabbitmq_username: str = self._get_str('rabbitmq', 'username')
        self.rabbitmq_password: str = self._get_str('rabbitmq', 'password')

    def _init_monitoring(self):
        self.prometheus_scrape_interval: Timedelta = self._get_interval(
            'monitoring', 'prometheus-scrape-interval',
            default=_td(Timedelta('60 seconds'))
        )
        self.ingester_prometheus_port: int | None = self._get_int(
            'ingester', 'prometheus-port'
        ) if self.requirements.ingester_prometheus else self._get_opt_int(
            'ingester', 'prometheus-port'
        )
        self.ingester_export_interval: Timedelta = self._get_interval(
            'ingester', 'export-interval', default=_td(Timedelta('1 hour'))
        )
        self.ingester_finalize_after: Timedelta = self._get_interval(
            'ingester', 'finalize-after', default=_td(Timedelta('12 hours'))
        )
        self.receiver_max_run_duration: Timedelta = self._get_interval(
            'receiver', 'max-run-duration', default=_td(Timedelta('24 hours'))
        )
        if (
                self.receiver_max_run_duration <= Timedelta(0) or
                self.receiver_max_run_duration % Timedelta('1 hour') != Timedelta(0)
        ):
            raise ValueError('configuration value "receiver/max-run-duration" must be a positive whole number of hours')
        self._source_prometheus_ports: dict[DataSource, int | None] = self._get_sources_opt_int('prometheus-port')
        if self.requirements.mailjet:
            self.mailjet_api_key = self._get_str('mailjet', 'api_key')
            self.mailjet_secret_key = self._get_str('mailjet', 'secret_key')
            self.from_email = self._get_str('mailjet', 'from_email')
            self.from_name = self._get_str('mailjet', 'from_name')
            self.to_email = self._get_str('mailjet', 'to_email')
            self.to_name = self._get_str('mailjet', 'to_name')
        else:
            self.mailjet_api_key = None
            self.mailjet_secret_key = None
            self.from_email = None
            self.from_name = None
            self.to_email = None
            self.to_name = None

    def _init_sources(self):
        def_comp_delay: Timedelta = self._get_interval(
            'sources', 'completion-delay', default=_td(Timedelta('15 minutes'))
        )
        def_data_lag: Timedelta = self._get_interval(
            'sources', 'data-lag', default=_td(Timedelta(0))
        )

        self._source_completion_delay: dict[DataSource, Timedelta] = self._get_sources_interval(
            'completion-delay', default=def_comp_delay
        )
        self._source_data_lag: dict[DataSource, Timedelta] = self._get_sources_interval(
            'data-lag', default=def_data_lag
        )
        self._source_bounds: dict[DataSource, list[float] | None] = self._get_sources_bounds(
            'bounds'
        )

        self._source_credentials: dict[DataSource, dict[str, str]] = {}

        for s in [
                DataSource.CONTRAILS_API,
                DataSource.OPENSKY,
                DataSource.OPENSKY_STATE_VECTORS
        ]:
            api_key = self._get_opt_str(['source', str(s)], 'api-key')
            if api_key is not None:
                self._source_credentials[s] = dict(api_key=api_key)

        username = self._get_opt_str(
            ['source', str(DataSource.FLIGHTAWARE)], 'username'
        )
        password = self._get_opt_str(
            ['source', str(DataSource.FLIGHTAWARE)], 'password'
        )
        if username is not None and password is not None:
            self._source_credentials[DataSource.FLIGHTAWARE] = dict(
                username=username, password=password
            )

    def _get(
            self,
            table: list[str], key: str,
            default: Any | None = None,
            missing_ok: bool = False
    ) -> Any:
        try:
            current = self.raw
            for i in table:
                current = current[i]
            value = current[key]
            if value is not None:
                return value
            if missing_ok:
                return None
            raise KeyError('missing')
        except KeyError:
            if default is not None:
                return default
            if missing_ok:
                return None
            raise KeyError(
                f'configuration key "{".".join(table)}/{key}" missing'
            )

    def _type_error(self, table, key):
        raise ValueError(
            f'wrong type for configuration value "{".".join(table)}/{key}"'
        )

    def _get_str(
            self,
            table: str | list[str], key: str,
            default: str | None = None
    ) -> str:
        table = _as_list(table)
        value = self._get(table, key, default)
        if isinstance(value, str):
            return value
        if value is None:
            return ''
        self._type_error(table, key)

    def _get_opt_str(
            self,
            table: str | list[str], key: str,
            default: str | None = None
    ) -> str | None:
        table = _as_list(table)
        value = self._get(table, key, default, missing_ok=True)
        if isinstance(value, str) or value is None:
            return value
        self._type_error(table, key)

    def _get_int(
            self, table: str | list[str], key: str, default: int | None = None
    ) -> int:
        table = _as_list(table)
        value = self._get(table, key, default)
        if isinstance(value, int):
            return value
        self._type_error(table, key)

    def _get_opt_int(self, table: str | list[str], key: str) -> int | None:
        table = _as_list(table)
        value = self._get(table, key, missing_ok=True)
        if isinstance(value, int) or value is None:
            return value
        self._type_error(table, key)

    def _get_opt_list(self, table: str | list[str], key: str) -> list[float] | None:
        table = _as_list(table)
        value = self._get(table, key, missing_ok=True)
        if isinstance(value, list) or value is None:
            return value
        self._type_error(table, key)

    def _get_bool(
            self, table: str | list[str], key: str, default: bool | None = None
    ) -> bool:
        table = _as_list(table)
        value = self._get(table, key, default)
        if isinstance(value, bool):
            return value
        self._type_error(table, key)

    def _get_interval(
            self, table: str | list[str], key: str, default: Timedelta | None = None
    ) -> Timedelta:
        table = _as_list(table)
        value = self._get(table, key, default)
        if not any(isinstance(value, t) for t in [Timedelta, str, int]):
            self._type_error(table, key)
        try:
            if isinstance(value, str):
                value = Timedelta(value)
            elif isinstance(value, int):
                value = Timedelta(seconds=value)
        except Exception:
            value = NaT
        if isinstance(value, NaTType):
            raise ValueError(
                f'failed to convert configuration value "{".".join(table)}/{key}"'
            )
        return value

    def _get_sources_opt_int(self, key: str):
        return {
            s: self._get_opt_int(['source', str(s)], key)
            for s in DataSource
        }

    def _get_sources_interval(self, key: str, default: Timedelta | None = None):
        return {
            s: self._get_interval(['source', str(s)], key, default=default)
            for s in DataSource
        }

    def _get_sources_bounds(self, key: str):
        return {
            s: self._get_opt_list(['source', str(s)], key)
            for s in DataSource
        }


def _td(td: Timedelta | NaTType) -> Timedelta:
    if isinstance(td, NaTType):
        raise ValueError('unexpected NaT!')
    return td

def _as_list(x):
    return [x] if isinstance(x, str) else x
