from django.contrib import admin

from core.models import Application, AuditLog, MetricInfo


@admin.register(AuditLog)
class AuditLogAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'action', 'target', 'ip_address', 'timestamp')
    list_filter = ('action', 'timestamp', 'user')
    search_fields = ('user__username', 'target', 'ip_address')
    readonly_fields = ('id', 'user', 'action', 'target', 'ip_address', 'timestamp')
    ordering = ('-timestamp',)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser


@admin.register(Application)
class ApplicationAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'owner', 'created_at')
    list_display_links = ('id', 'name')
    search_fields = ('name', 'owner__username')
    list_filter = ('created_at', 'owner')
    ordering = ('-created_at',)
    readonly_fields = ('id', 'name', 'owner', 'created_at')

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser


@admin.register(MetricInfo)
class MetricInfoAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'type', 'application', 'created_at')
    list_filter = ('type', 'application', 'created_at')
    search_fields = ('name',)
    readonly_fields = ('id', 'name', 'type', 'attributes', 'application', 'created_at')
    ordering = ('-created_at',)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
