import logging
import os
import sys
import tomllib
from typing import Any

from pandas import NaT, Timedelta
from pandas._libs import NaTType

from feder.common import DataSource


logger = logging.getLogger(__name__)


class Config:
    def __init__(self, config_file: str | None = None, config_text: str | None = None):
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

    def credentials(self, source: DataSource) -> dict[str, Any]:
        return self._source_credentials[source]

    def prometheus_port(self, source: DataSource) -> int | None:
        return self._source_prometheus_ports.get(source)

    def _init_paths(self):
        self.data_directory: str = self._get_str('paths', 'data-directory')
        self.scratch_directory: str = self._get_str('paths', 'scratch-directory')

    def _init_rabbitmq(self):
        self.rabbitmq_host: str = self._get_str('rabbitmq', 'host')
        self.rabbitmq_port: int = self._get_int('rabbitmq', 'port', default=5672)
        self.rabbitmq_username: str = self._get_str('rabbitmq', 'username')
        self.rabbitmq_password: str = self._get_str('rabbitmq', 'password')

    def _init_monitoring(self):
        self.prometheus_scrape_interval: Timedelta = self._get_interval(
            'monitoring', 'prometheus-scrape-interval',
            default=_td(Timedelta('60 seconds'))
        )
        self.ingester_prometheus_port: int = self._get_int(
            'ingester', 'prometheus-port'
        )
        self._source_prometheus_ports: dict[DataSource, int | None] = self._get_sources_opt_int('prometheus-port')

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

def _td(td: Timedelta | NaTType) -> Timedelta:
    if isinstance(td, NaTType):
        raise ValueError('unexpected NaT!')
    return td

def _as_list(x):
    return [x] if isinstance(x, str) else x
