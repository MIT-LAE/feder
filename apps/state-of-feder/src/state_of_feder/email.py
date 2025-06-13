import logging
from pathlib import Path
from string import Template

from mailjet_rest import Client

from feder_server import Config

from .data import EmailData


logger = logging.getLogger(__name__)


def send_email(cfg: Config, data: EmailData) -> None:
    auth = (cfg.mailjet_api_key, cfg.mailjet_secret_key)
    mj = Client(auth=auth, version='v3.1')

    from_details = {
        'Email': cfg.from_email,
        'Name': cfg.from_name,
    }
    to_details = [{
        'Email': cfg.to_email,
        'Name': cfg.to_name,
    }]
    message = {
        'From': from_details,
        'To': to_details,
        'Subject': 'State of Feder',
        'TextPart': text_body(data),
        'HTMLPart': html_body(data),
    }

    result = mj.send.create(data={'Messages': [message]})
    if result.status_code == 200:
        logger.info('Email successfully sent')
    else:
        logger.error('Error response from Mailjet API: '
                     f'{result.status_code} ({str(result.json())})')


def text_body(data: EmailData) -> str:
    lines =[
        f'State of Feder: ${data.report_time.isoformat()} UTC',
        '',
        'Available days:',
        ''
        'From         To'
    ] + [
        f'{f.isoformat()}   {t.isoformat()}'
        for f, t in data.available_dates
    ] + [
        '',
        'Small files (< 50Mb):'
    ] + [
        f'{d.isoformat()}  {size / 1024 / 1024:.2f} MB'
        for d, size in data.small_sizes
    ]
    return '\n'.join(lines)


def html_body(data: EmailData) -> str:
    path = Path(__file__).parent / 'state-of-feder.html'
    with open(path) as fp:
        lines = fp.readlines()
    available_date_rows = [
        f'<tr><td>{f.isoformat()}</td><td>{t.isoformat()}</td></tr>'
        for f, t in data.available_dates
    ]
    small_size_rows = [
        f'<tr><td>{f.isoformat()}</td><td>{s // 1024 // 1024}</td></tr>'
        for f, s in data.small_sizes
    ]
    template = Template('\n'.join(lines))
    return template.safe_substitute(
        report_time=data.report_time.strftime('%Y-%m-%d %H:%M:%S'),
        available_date_rows='\n'.join(available_date_rows),
        small_size_rows='\n'.join(small_size_rows)
    )

html_template = Template('')
