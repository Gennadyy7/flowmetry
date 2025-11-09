import logging

from django.http import JsonResponse
from django.views import View

logger = logging.getLogger(__name__)


class HealthCheckView(View):
    @staticmethod
    def get(_request, *_args, **_kwargs):
        logger.debug('Health check...')
        return JsonResponse({'status': 'ok'})
