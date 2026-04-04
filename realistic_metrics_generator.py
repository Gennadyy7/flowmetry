import time
import random
from datetime import datetime
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.metrics import CallbackOptions, Observation


class RealisticMetricsGenerator:
    def __init__(self, service_name="web-service", endpoint="http://localhost:8001/v1/metrics"):
        self.resource = Resource.create({
            "service.name": service_name,
            "service.version": "1.0.0",
            "host.name": "server-01",
            "environment": "production"
        })

        try:
            self.exporter = OTLPMetricExporter(
                endpoint=endpoint,
            )
            print(f"✅ Используется OTLP экспортер: {endpoint}")
        except Exception as e:
            print(f"Предупреждение: не удалось создать OTLP экспортер: {e}")
            print("Используется ConsoleExporter для тестирования")
            from opentelemetry.sdk.metrics.export import ConsoleMetricExporter
            self.exporter = ConsoleMetricExporter()

        self.reader = PeriodicExportingMetricReader(self.exporter, export_interval_millis=2000)
        self.provider = MeterProvider(resource=self.resource, metric_readers=[self.reader])
        metrics.set_meter_provider(self.provider)
        self.meter = metrics.get_meter("performance-meter")

        self.request_timestamps = []
        self.last_cleanup_time = time.time()
        self.active_sessions = 0
        self.queue_size = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.database_connections = 0
        self.thread_pool_active = 0

        self.simulation_time = time.time()
        self.last_iteration_time = self.simulation_time

        self._create_metrics()

        self.load_level = 1.0  # 0.0 - 2.0 (0 = мин, 1 = норм, 2 = макс)
        self.running = False

    def _create_metrics(self):
        self.http_requests_total = self.meter.create_counter(
            "http_requests_total",
            description="Total number of HTTP requests",
            unit="requests"
        )

        self.http_errors_total = self.meter.create_counter(
            "http_errors_total",
            description="Total number of HTTP errors",
            unit="errors"
        )

        self.database_queries_total = self.meter.create_counter(
            "database_queries_total",
            description="Total number of database queries",
            unit="queries"
        )

        self.cache_hits_total = self.meter.create_counter(
            "cache_hits_total",
            description="Total number of cache hits",
            unit="hits"
        )

        self.cache_misses_total = self.meter.create_counter(
            "cache_misses_total",
            description="Total number of cache misses",
            unit="misses"
        )

        self.bytes_sent_total = self.meter.create_counter(
            "network_bytes_sent_total",
            description="Total bytes sent over network",
            unit="bytes"
        )

        self.bytes_received_total = self.meter.create_counter(
            "network_bytes_received_total",
            description="Total bytes received over network",
            unit="bytes"
        )

        self.error_rate_total = self.meter.create_counter(
            "error_rate_total",
            description="Error rate tracking",
            unit="errors"
        )

        self.slow_requests_total = self.meter.create_counter(
            "slow_requests_total",
            description="Number of slow requests (>1s)",
            unit="requests"
        )

        self.http_request_duration_seconds = self.meter.create_histogram(
            "http_request_duration_seconds",
            description="HTTP request duration",
            unit="seconds"
        )

        self.database_query_duration_seconds = self.meter.create_histogram(
            "database_query_duration_seconds",
            description="Database query duration",
            unit="seconds"
        )

        self.cache_operation_duration_seconds = self.meter.create_histogram(
            "cache_operation_duration_seconds",
            description="Cache operation duration",
            unit="seconds"
        )

        self.active_users_gauge = self.meter.create_observable_gauge(
            "active_users",
            callbacks=[self._get_active_users],
            description="Number of active users",
            unit="users"
        )

        self.cpu_usage_percent = self.meter.create_observable_gauge(
            "cpu_usage_percent",
            callbacks=[self._get_cpu_usage],
            description="CPU usage percentage",
            unit="percent"
        )

        self.memory_usage_bytes = self.meter.create_observable_gauge(
            "memory_usage_bytes",
            callbacks=[self._get_memory_usage],
            description="Memory usage in bytes",
            unit="bytes"
        )

        self.active_sessions_gauge = self.meter.create_observable_gauge(
            "active_sessions",
            callbacks=[self._get_active_sessions],
            description="Number of active sessions",
            unit="sessions"
        )

        self.queue_size_gauge = self.meter.create_observable_gauge(
            "queue_size",
            callbacks=[self._get_queue_size],
            description="Current queue size",
            unit="items"
        )

        self.database_connections_gauge = self.meter.create_observable_gauge(
            "database_connections",
            callbacks=[self._get_database_connections],
            description="Active database connections",
            unit="connections"
        )

        self.thread_pool_active_gauge = self.meter.create_observable_gauge(
            "thread_pool_active",
            callbacks=[self._get_thread_pool_active],
            description="Active threads in pool",
            unit="threads"
        )

        self.requests_per_second_gauge = self.meter.create_observable_gauge(
            "requests_per_second",
            callbacks=[self._get_requests_per_second],
            description="Requests per second",
            unit="rps"
        )

        self.response_size_bytes = self.meter.create_histogram(
            "http_response_size_bytes",
            description="HTTP response size",
            unit="bytes"
        )

        self.upstream_latency_seconds = self.meter.create_histogram(
            "upstream_latency_seconds",
            description="Upstream service latency",
            unit="seconds"
        )

    def _get_active_users(self, options: CallbackOptions):
        peak_hour = datetime.now().hour
        if 9 <= peak_hour <= 17:
            base_users = 100
        elif 18 <= peak_hour <= 22:
            base_users = 150
        else:
            base_users = 30

        value = int(base_users * self.load_level + random.randint(-20, 20) * self.load_level)
        return [Observation(value=max(0, value), attributes={})]

    def _get_cpu_usage(self, options: CallbackOptions):
        time_factor = 1.0
        hour = datetime.now().hour
        if 10 <= hour <= 16:
            time_factor = 1.3

        base_cpu = 20 * self.load_level * time_factor
        variance = 15 * self.load_level
        value = max(0, min(100, int(base_cpu + random.uniform(-variance, variance))))
        return [Observation(value=value, attributes={})]

    def _get_memory_usage(self, options: CallbackOptions):
        base_memory = 500 * 1024 * 1024
        variance = 200 * 1024 * 1024 * self.load_level
        hour_factor = datetime.now().hour / 24.0
        memory_growth = 100 * 1024 * 1024 * hour_factor
        value = int(base_memory + memory_growth + random.uniform(-variance, variance))
        return [Observation(value=value, attributes={})]

    def _get_active_sessions(self, options: CallbackOptions):
        return [Observation(value=int(self.active_sessions), attributes={})]

    def _get_queue_size(self, options: CallbackOptions):
        return [Observation(value=int(self.queue_size), attributes={})]

    def _get_database_connections(self, options: CallbackOptions):
        return [Observation(value=int(self.database_connections), attributes={})]

    def _get_thread_pool_active(self, options: CallbackOptions):
        return [Observation(value=int(self.thread_pool_active), attributes={})]

    def _get_requests_per_second(self, options: CallbackOptions):
        current_time = time.time()

        self.request_timestamps = [
            t for t in self.request_timestamps
            if current_time - t <= 30.0
        ]

        if len(self.request_timestamps) > 1:
            time_window = current_time - min(self.request_timestamps)
            if time_window > 0:
                rps = len(self.request_timestamps) / time_window
            else:
                rps = 0
        else:
            rps = 0

        return [Observation(value=rps, attributes={})]

    def set_load_level(self, level):
        self.load_level = max(0.0, min(2.0, level))
        print(f"Уровень нагрузки установлен: {self.load_level}")

    def _simulate_http_requests(self):
        time_increment = random.uniform(0.05, 0.2)
        self.simulation_time += time_increment

        hour = datetime.now().hour
        time_factor = 1.0
        if 10 <= hour <= 16:
            time_factor = 1.5
        elif 20 <= hour <= 22:
            time_factor = 1.3

        requests_per_cycle = int(10 * self.load_level * time_factor + random.randint(1, 5) * self.load_level)

        for _ in range(requests_per_cycle):
            method = random.choices(
                ["GET", "POST", "PUT", "DELETE", "PATCH"],
                weights=[60, 20, 10, 5, 5]
            )[0]

            paths = [
                "/api/users", "/api/users/{id}", "/api/products", "/api/orders",
                "/api/auth/login", "/api/auth/logout", "/api/search", "/api/upload",
                "/health", "/metrics", "/api/dashboard", "/api/reports"
            ]
            path = random.choice(paths)

            error_weight = 0.01 * self.load_level
            status_code = random.choices(
                [200, 201, 204, 400, 401, 403, 404, 500, 502, 503],
                weights=[70 - error_weight * 10, 8, 2, 3 + error_weight, 2, 1, 5,
                         2 + error_weight * 2, 1, 1 + error_weight]
            )[0]

            self.http_requests_total.add(
                1,
                attributes={
                    "method": method,
                    "status_code": status_code,
                    "path": path,
                    "host": "server-01"
                }
            )

            if status_code >= 400:
                self.http_errors_total.add(
                    1,
                    attributes={
                        "method": method,
                        "status_code": status_code,
                        "path": path,
                        "error_type": "client_error" if status_code < 500 else "server_error"
                    }
                )
                self.error_rate_total.add(1, {"severity": "high" if status_code >= 500 else "medium"})

            if status_code == 500:
                duration = random.uniform(1.0, 5.0)
            elif method == "GET":
                duration = random.uniform(0.01, 0.3)
            elif method == "POST":
                duration = random.uniform(0.1, 1.5)
            else:
                duration = random.uniform(0.1, 2.0)

            self.http_request_duration_seconds.record(
                duration,
                attributes={
                    "method": method,
                    "status_code": status_code,
                    "path": path,
                    "slow_request": str(duration > 1.0)
                }
            )

            if duration > 1.0:
                self.slow_requests_total.add(1, {"threshold": "1s"})

            if status_code == 200:
                if "api/users" in path:
                    response_size = random.randint(1000, 15000)
                elif "api/products" in path:
                    response_size = random.randint(2000, 25000)
                elif path == "/health":
                    response_size = random.randint(100, 500)
                else:
                    response_size = random.randint(500, 8000)
            else:
                response_size = random.randint(100, 2000)

            self.response_size_bytes.record(
                response_size,
                attributes={
                    "method": method,
                    "status_code": status_code,
                    "path": path
                }
            )

            self.bytes_sent_total.add(response_size, {"direction": "outbound"})
            self.bytes_received_total.add(random.randint(100, 3000), {"direction": "inbound"})

            self.request_timestamps.append(time.time())

            self._simulate_business_operations()

    def _simulate_business_operations(self):
        if random.random() < 0.4 * self.load_level:
            self.database_queries_total.add(
                1,
                attributes={
                    "query_type": random.choice(["SELECT", "INSERT", "UPDATE", "DELETE"]),
                    "table": random.choice(["users", "orders", "products", "logs"])
                }
            )

            db_duration = random.uniform(0.01, 1.5)
            self.database_query_duration_seconds.record(
                db_duration,
                attributes={
                    "query_type": "SELECT",
                    "success": random.random() > 0.02
                }
            )

            self.database_connections = max(0, self.database_connections + random.randint(-2, 3))
            self.database_connections = min(50, self.database_connections)

        if random.random() < 0.5 * self.load_level:
            is_hit = random.random() > 0.25
            if is_hit:
                self.cache_hits_total.add(1)
                self.cache_hits += 1
                cache_duration = random.uniform(0.001, 0.03)
            else:
                self.cache_misses_total.add(1)
                self.cache_misses += 1
                cache_duration = random.uniform(0.05, 0.3)

            self.cache_operation_duration_seconds.record(
                cache_duration,
                attributes={
                    "operation": "get",
                    "hit": is_hit
                }
            )

        if random.random() < 0.15:
            self.active_sessions += random.randint(-3, 5)
            self.active_sessions = max(0, self.active_sessions)

        if random.random() < 0.25:
            self.queue_size += random.randint(-2, 4)
            self.queue_size = max(0, self.queue_size)

        if random.random() < 0.35:
            self.thread_pool_active += random.randint(-2, 3)
            self.thread_pool_active = max(0, min(25, self.thread_pool_active))

    def _simulate_upstream_calls(self):
        if random.random() < 0.2 * self.load_level:
            upstream_duration = random.uniform(0.05, 2.5)
            self.upstream_latency_seconds.record(
                upstream_duration,
                attributes={
                    "service": random.choice(
                        ["auth-service", "payment-service", "notification-service", "email-service"]),
                    "success": random.random() > 0.07
                }
            )

    def start(self, duration_minutes=10):
        print(f"🚀 Запуск генератора метрик на {duration_minutes} минут")
        print(f"📊 Уровень нагрузки: {self.load_level}")
        print(f"🕒 Время начала: {datetime.now().strftime('%H:%M:%S')}")

        self.running = True
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)

        try:
            iteration = 0
            while self.running and time.time() < end_time:
                iteration += 1
                elapsed_minutes = (time.time() - start_time) / 60

                if elapsed_minutes < 2:
                    self.set_load_level(0.7)
                elif elapsed_minutes < 4:
                    self.set_load_level(1.2)
                elif elapsed_minutes < 6:
                    self.set_load_level(1.8)
                elif elapsed_minutes < 8:
                    self.set_load_level(1.0)
                else:
                    self.set_load_level(0.5)

                self._simulate_http_requests()
                self._simulate_upstream_calls()

                if iteration % 15 == 0:
                    print(
                        f"⏱️  Прогресс: {elapsed_minutes:.1f} мин "
                        f"из {duration_minutes} мин"
                    )
                    print(f"📈 Активные сессии: {self.active_sessions}, "
                          f"Очередь: {self.queue_size}")

                time.sleep(2.0)

                if iteration % 5 == 0:
                    self.force_flush_metrics()

        except KeyboardInterrupt:
            print("\n🛑 Остановка генератора...")
        finally:
            self.stop()

    def stop(self):
        self.running = False
        self.provider.shutdown()
        print("✅ Генератор остановлен")

    def force_flush_metrics(self):
        try:
            self.provider.force_flush()
            self.reader.force_flush()
            print(f"📤 Метрики отправлены принудительно в "
                  f"{datetime.now().strftime('%H:%M:%S')}")
        except Exception as e:
            print(f"⚠️ Ошибка при принудительной отправке: {e}")


if __name__ == "__main__":
    generator = RealisticMetricsGenerator()

    print("=" * 50)
    print("🎯 Генератор реалистичных метрик для мониторинга")
    print("=" * 50)

    generator.start(duration_minutes=45)

    print("✅ Тест завершен успешно")
