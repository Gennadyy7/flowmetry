from django.contrib import admin

from core.models import AuditLog


@admin.register(AuditLog)
class AuditLogAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'action', 'target', 'ip_address', 'timestamp')
    list_filter = ('action', 'timestamp', 'user')
    search_fields = ('user__username', 'target', 'ip_address')
    readonly_fields = ('id', 'user', 'action', 'target', 'ip_address', 'timestamp')
    ordering = ('-timestamp',)
