#!/bin/sh

echo "Collecting static files..."
python registrar/manage.py collectstatic --noinput

echo "Applying migrations..."
python registrar/manage.py migrate --noinput || {
  echo "Migrations failed. Exiting..."
  exit 1
}

echo "Ensuring foreign keys..."
python registrar/manage.py ensure_fk

echo "Starting supervisord..."
exec supervisord -c /etc/supervisor/conf.d/supervisord.conf