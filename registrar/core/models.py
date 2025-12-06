from django.db import models


class AuditLog(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(
        'auth.User', on_delete=models.SET_NULL, null=True, db_column='user_id'
    )
    action = models.TextField()
    target = models.TextField(blank=True, null=True)
    ip_address = models.GenericIPAddressField()
    timestamp = models.DateTimeField()

    class Meta:
        db_table = 'audit_log'
        managed = False
        verbose_name = 'Audit Log'
        verbose_name_plural = 'Audit Logs'
