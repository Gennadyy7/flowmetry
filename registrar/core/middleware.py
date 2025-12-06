from collections.abc import Callable
from ipaddress import ip_address
import logging
import re

from django.db import connection
from django.http import HttpRequest, HttpResponse
from django.utils.deprecation import MiddlewareMixin

logger = logging.getLogger(__name__)


class AuditAction(str):
    LOGIN = 'login'
    VIEW_METRICS = 'view_metrics'
    EXPORT_DATA = 'export_data'
    VIEW_APPLICATIONS = 'view_applications'
    VIEW_AUDIT = 'view_audit'

    @classmethod
    def from_view_name(cls, view_name: str) -> str:
        for pattern, action in [
            (r'.*metrics.*', cls.VIEW_METRICS),
            (r'.*application.*', cls.VIEW_APPLICATIONS),
            (r'.*audit.*', cls.VIEW_AUDIT),
        ]:
            if re.search(pattern, view_name):
                return action
        return cls.VIEW_METRICS


class AuditContext:
    __slots__ = (
        'user_id',
        'session_key',
        'action',
        'target',
        'ip_address',
        'user_agent',
        'status_code',
    )

    def __init__(
        self,
        user_id: int,
        session_key: str | None,
        action: str,
        target: str | None,
        ip_address: str,
        user_agent: str | None,
        status_code: int,
    ):
        self.user_id = user_id
        self.session_key = session_key
        self.action = action
        self.target = target
        self.ip_address = ip_address
        self.user_agent = user_agent
        self.status_code = status_code


class AuditLogger:
    SQL_INSERT = """
        INSERT INTO audit_log (
            user_id, session_key, action, target,
            ip_address, user_agent, status_code, timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
    """

    @staticmethod
    def _sanitize_ip(ip: str) -> str:
        try:
            if ip.startswith('::ffff:'):
                ip = ip.split(':')[-1]
            ip_address(ip)
            return ip
        except ValueError:
            return '0.0.0.0'

    @classmethod
    def log(cls, context: AuditContext) -> bool:
        try:
            sanitized_ip = cls._sanitize_ip(context.ip_address)
            with connection.cursor() as cursor:
                cursor.execute(
                    cls.SQL_INSERT,
                    [
                        context.user_id,
                        context.session_key[:64] if context.session_key else None,
                        context.action,
                        context.target[:255]
                        if context.target and len(context.target) > 255
                        else context.target,
                        sanitized_ip,
                        context.user_agent[:255]
                        if context.user_agent and len(context.user_agent) > 255
                        else context.user_agent,
                        context.status_code,
                    ],
                )
            return True
        except Exception as e:
            logger.error(
                f'Audit logging failed: user_id={context.user_id}, action={context.action}, error={str(e)}'
            )
            return False


class AuditMiddleware(MiddlewareMixin):
    EXCLUDE_PATHS = (
        r'/health/?$',
        r'/static/',
        r'/media/',
        r'/admin/jsi18n/',
    )

    EXCLUDE_VIEWS = (
        'django.contrib.auth.views.LoginView',
        'django.contrib.auth.views.LogoutView',
    )

    def __init__(self, get_response: Callable[[HttpRequest], HttpResponse]):
        self.get_response = get_response
        self.exclude_patterns = tuple(re.compile(p) for p in self.EXCLUDE_PATHS)
        super().__init__(get_response)

    def _should_skip(self, request: HttpRequest) -> bool:
        if not request.user.is_authenticated:
            return request.path != '/login/'
        for pattern in self.exclude_patterns:
            if pattern.match(request.path):
                return True
        resolver_match = request.resolver_match
        if resolver_match and resolver_match._func_path in self.EXCLUDE_VIEWS:
            return True
        return False

    @staticmethod
    def _get_client_ip(request: HttpRequest) -> str:
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0].strip()
        else:
            ip = request.META.get('REMOTE_ADDR', '127.0.0.1')
        return ip

    @staticmethod
    def _determine_target(request: HttpRequest) -> str | None:
        if 'metrics' in request.path:
            metric_name = request.GET.get('name')
            return f'metric:{metric_name}' if metric_name else 'metrics:list'
        if 'applications' in request.path:
            app_id = request.GET.get('id')
            return f'application:{app_id}' if app_id else 'applications:list'
        return request.path

    def _build_context(
        self, request: HttpRequest, response: HttpResponse
    ) -> AuditContext:
        action = AuditAction.from_view_name(
            request.resolver_match.view_name if request.resolver_match else 'unknown'
        )
        return AuditContext(
            user_id=request.user.id,
            session_key=request.session.session_key,
            action=action,
            target=self._determine_target(request),
            ip_address=self._get_client_ip(request),
            user_agent=request.META.get('HTTP_USER_AGENT'),
            status_code=response.status_code,
        )

    def process_response(
        self, request: HttpRequest, response: HttpResponse
    ) -> HttpResponse:
        try:
            if self._should_skip(request):
                return response
            if not hasattr(request, 'user') or not request.user.is_authenticated:
                return response
            audit_context = self._build_context(request, response)
            AuditLogger.log(audit_context)
        except Exception as e:
            logger.warning(f'Audit middleware failed: {str(e)}')
        return response
