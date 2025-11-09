import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'registrar.settings')

import django  # noqa: E402

django.setup()

from django.conf import settings  # noqa: E402
from log_config_loader import setup_logging  # noqa: E402

setup_logging(
    service_name=settings.SERVICE_NAME,
    level=settings.LOG_LEVEL,
    log_format=settings.LOG_FORMAT,
    version=settings.SERVICE_VERSION,
)

from django.core.wsgi import get_wsgi_application  # noqa: E402

application = get_wsgi_application()
