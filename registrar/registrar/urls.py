from django.conf import settings
from django.conf.urls.static import static
from django.urls import path
from django.views.generic import RedirectView

from registrar.admin_config import site
from registrar.views import HealthCheckView

urlpatterns = [
    path('admin/', site.urls),
    path('health/', HealthCheckView.as_view(), name='health'),
    path('', RedirectView.as_view(url='/admin/', permanent=False)),
]

if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
