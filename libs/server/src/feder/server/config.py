import logging
import os
import sys
import tomllib
from typing import Any

from pandas import NaT, Timedelta
from pandas._libs import NaTType

from .sources import (
    CONTRAILS_API_SOURCE_NAME,
    FLIGHTAWARE_SOURCE_NAME,
    OPENSKY_SOURCE_NAME,
    OPENSKY_STATE_VECTOR_SOURCE_NAME,
    SOURCE_NAMES
)


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

    def enabled(self, source: str) -> bool:
        return self._source_enabled[source]

    def completion_delay(self, source: str) -> Timedelta:
        return self._source_completion_delay[source]

    def completion_interval(self, source: str) -> Timedelta:
        return self._source_completion_interval[source]

    def data_lag(self, source: str) -> Timedelta:
        return self._source_data_lag[source]

    def _init_paths(self):
        self.data_directory: str = self._get_str('paths', 'data-directory')
        self.scratch_directory: str = self._get_str('paths', 'scratch-directory')

    def _init_rabbitmq(self):
        self.rabbitmq_host: str = self._get_str('rabbitmq', 'host')
        self.rabbitmq_port: int = self._get_int('rabbitmq', 'port', default=5672)
        self.rabbitmq_username: str = self._get_str('rabbitmq', 'username')
        self.rabbitmq_password: str = self._get_str('rabbitmq', 'password')

    def _init_monitoring(self):
        self.heartbeat_interval: Timedelta = self._get_interval(
            'monitoring', 'heartbeat-interval', default=_td(Timedelta('30 seconds'))
        )
        self.monitoring_from_email: str = self._get_str('monitoring', 'from-email')
        self.monitoring_from_name: str = self._get_str('monitoring', 'from-name')
        self.monitoring_to_email: str = self._get_str('monitoring', 'to-email')
        self.monitoring_to_name: str = self._get_str('monitoring', 'to-name')
        self.monitoring_mail_backend: str = self._get_str('monitoring', 'mail-backend')
        self.monitoring_mailjet_api_key: str = self._get_str('monitoring', 'mailjet-api-key')
        self.monitoring_mailjet_secret_key: str = self._get_str('monitoring', 'mailjet-secret-key')

    def _init_sources(self):
        def_comp_delay: Timedelta = self._get_interval(
            'sources', 'completion-delay', default=_td(Timedelta('15 minutes'))
        )
        def_comp_interval: Timedelta = self._get_interval(
            'sources', 'completion-interval', default=_td(Timedelta('60 seconds'))
        )
        def_data_lag: Timedelta = self._get_interval(
            'sources', 'data-lag', default=_td(Timedelta(0))
        )

        self._source_enabled: dict[str, bool] = self._get_sources_bool('enabled', default=False)
        self._source_completion_delay: dict[str, Timedelta] = self._get_sources_interval(
            'completion-delay', default=def_comp_delay
        )
        self._source_completion_interval: dict[str, Timedelta] = self._get_sources_interval(
            'completion-interval', default=def_comp_interval
        )
        self._source_data_lag: dict[str, Timedelta] = self._get_sources_interval(
            'data-lag', default=def_data_lag
        )

        self._source_credentials: dict[str, Any] = {}

        for s in [
                CONTRAILS_API_SOURCE_NAME,
                OPENSKY_SOURCE_NAME,
                OPENSKY_STATE_VECTOR_SOURCE_NAME
        ]:
            if self._source_enabled[s]:
                api_key = self._get_str(['source', s], 'api-key')
                if api_key is not None:
                    self._source_credentials[s] = dict(api_key=api_key)

        if self._source_enabled[FLIGHTAWARE_SOURCE_NAME]:
            username = self._get_str(['source', FLIGHTAWARE_SOURCE_NAME], 'username')
            password = self._get_str(['source', FLIGHTAWARE_SOURCE_NAME], 'password')
            if username is not None and password is not None:
                self._source_credentials[FLIGHTAWARE_SOURCE_NAME] = dict(
                    username=username, password=password
                )

    def _get(
            self, table: list[str], key: str, default: Any | None = None
    ) -> Any:
        try:
            current = self.raw
            for i in table:
                current = current[i]
            value = current[key]
            if value is None:
                raise KeyError('missing')
            return value
        except KeyError:
            if default is not None:
                return default
            raise KeyError(
                f'configuration key "{".".join(table)}/{key}" missing'
            )

    def _type_error(self, table, key):
        raise ValueError(
            f'wrong type for configuration value "{".".join(table)}/{key}"'
        )

    def _get_str(
            self, table: str | list[str], key: str, default: str | None = None
    ) -> str:
        table = _as_list(table)
        value = self._get(table, key, default)
        if isinstance(value, str):
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

    def _get_sources_bool(self, key: str, default: bool | None = None):
        return {
            s: self._get_bool(['source', s], key, default=default)
            for s in SOURCE_NAMES
        }

    def _get_sources_interval(self, key: str, default: Timedelta | None = None):
        return {
            s: self._get_interval(['source', s], key, default=default)
            for s in SOURCE_NAMES
        }

def _td(td: Timedelta | NaTType) -> Timedelta:
    if isinstance(td, NaTType):
        raise ValueError('unexpected NaT!')
    return td

def _as_list(x):
    return [x] if isinstance(x, str) else x
