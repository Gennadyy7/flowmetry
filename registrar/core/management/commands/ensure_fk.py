import logging

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Ensures foreign key constraints on auth_user are created in TimescaleDB'

    def handle(self, *args, **options):
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_application_owner'
            """)
            if cursor.fetchone():
                self.stdout.write(
                    self.style.SUCCESS("FK 'fk_application_owner' already exists.")
                )
                return

            try:
                cursor.execute("""
                    ALTER TABLE application
                    ADD CONSTRAINT fk_application_owner
                    FOREIGN KEY (owner_id) REFERENCES auth_user(id) ON DELETE SET NULL;
                """)
                self.stdout.write(
                    self.style.SUCCESS(
                        '✅ Added FK: application.owner_id → auth_user.id'
                    )
                )
            except Exception as e:
                self.stderr.write(
                    self.style.ERROR(f"⚠️ Failed to add FK 'fk_application_owner': {e}")
                )

            cursor.execute("""
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_audit_log_user'
            """)
            if cursor.fetchone():
                self.stdout.write(
                    self.style.SUCCESS("FK 'fk_audit_log_user' already exists.")
                )
                return

            try:
                cursor.execute("""
                    ALTER TABLE audit_log
                    ADD CONSTRAINT fk_audit_log_user
                    FOREIGN KEY (user_id) REFERENCES auth_user(id) ON DELETE SET NULL;
                """)
                self.stdout.write(
                    self.style.SUCCESS('✅ Added FK: audit_log.user_id → auth_user.id')
                )
            except Exception as e:
                self.stderr.write(
                    self.style.ERROR(f"⚠️ Failed to add FK 'fk_audit_log_user': {e}")
                )
