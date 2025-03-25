import click

from feder.server import Config

from .sources.contrails_api import ContrailsAPISource
from .sources.csv import CSVSource
from .sources.flightaware import FlightAwareSource
from .sources.opensky import OpenSkySource, OpenSkyStateVectorSource


SOURCES = [
    ContrailsAPISource,
    CSVSource,
    FlightAwareSource,
    OpenSkySource,
    OpenSkyStateVectorSource
]

SOURCES_BY_NAME = {s.NAME: s for s in SOURCES}


@click.command()
@click.argument(
    'source',
    type=click.Choice([s.NAME for s in SOURCES]),
    required=True
)
@click.option(
    '--config', '-c',
    type=click.Path(exists=True, readable=True)
)
@click.option(
    '--input', '-i',
    type=click.Path(exists=True, readable=True)
)
def run(source: str, config: str, input: str | None) -> None:
    print(f'Source: {source}')
    cfg = Config(config)
    source = SOURCES_BY_NAME[source](cfg)
    print(cfg.config_file)
    print(source.check())


if __name__ == '__main__':
    run(auto_envvar_prefix='FEDER')
