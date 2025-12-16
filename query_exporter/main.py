"""Script entry point."""

import asyncio
import os
import signal
from functools import partial
from pathlib import Path
import typing as t

from aiohttp.web import AppKey, Application, Request, Response, json_response
import click
from prometheus_aioexporter import (
    EXPORTER_APP_KEY,
    Arguments,
    InvalidMetricType,
    MetricConfig,
    MetricsRegistry,
    PrometheusExporterScript,
)
from prometheus_client.metrics import Gauge

from . import __version__
from .config import (
    Config,
    ConfigError,
    load_config,
)
from .executor import QueryExecutor
from .metrics import QUERY_INTERVAL_METRIC_NAME

# The application key to track the QueryExecutor
QUERY_EXECUTOR_APP_KEY: AppKey[QueryExecutor] = AppKey("query-executor")


class QueryExporterScript(PrometheusExporterScript):
    """Periodically run database queries and export results to Prometheus."""

    name = "query-exporter"
    version = __version__
    description = __doc__
    default_port = 9560
    envvar_prefix = "QE"

    def __init__(self) -> None:
        super().__init__()
        self.config_paths: list[Path] = []
        self._pid_file: Path | None = None
        self._reload_lock = asyncio.Lock()

    def command_line_parameters(self) -> list[click.Parameter]:
        return [
            click.Option(
                ["--check-only"],
                type=bool,
                help="only check configuration, don't run the exporter",
                is_flag=True,
                show_default=True,
                show_envvar=True,
            ),
            click.Option(
                ["--config"],
                type=click.Path(
                    exists=True,
                    dir_okay=False,
                    path_type=Path,
                ),
                help="configuration file",
                multiple=True,
                default=[Path("config.yaml")],
                show_default=True,
                show_envvar=True,
            ),
            click.Option(
                ["--pid-file"],
                type=click.Path(dir_okay=False, path_type=Path),
                help="write the exporter PID to this file (for signal-based reload)",
                show_envvar=True,
            ),
        ]

    def configure(self, args: Arguments) -> None:
        self.config_paths = list(args.config)
        self._pid_file = args.pid_file
        self.config = self._load_config(self.config_paths)
        if args.check_only:
            self.logger.info("configuration valid")
            raise SystemExit(0)
        self.create_metrics(self.config.metrics.values())
        self._set_static_metrics(self.config, self.registry)
        self._write_pid_file()

    async def on_application_startup(
        self, application: Application
    ) -> None:  # pragma: nocover
        query_executor = QueryExecutor(self.config, self.registry, self.logger)
        exporter = application[EXPORTER_APP_KEY]
        exporter.set_metric_update_handler(
            partial(self._update_handler, query_executor)
        )
        application[QUERY_EXECUTOR_APP_KEY] = query_executor
        self._register_reload_routes(application)
        self._install_signal_handlers(application)
        await query_executor.start()

    async def on_application_shutdown(
        self, application: Application
    ) -> None:  # pragma: nocover
        await application[QUERY_EXECUTOR_APP_KEY].stop()
        self._cleanup_pid_file()

    async def _update_handler(
        self, query_executor: QueryExecutor, metrics: list[MetricConfig]
    ) -> None:  # pragma: nocover
        """Run queries with no specified schedule on each request."""
        await query_executor.run_aperiodic_queries()
        query_executor.clear_expired_series()

    def _load_config(
        self, paths: list[Path], *, exit_on_error: bool = True
    ) -> Config:
        """Load the application configuration."""
        try:
            return load_config(paths, self.logger)
        except (InvalidMetricType, ConfigError) as error:
            self.logger.error("configuration invalid", error=str(error))
            if isinstance(error, ConfigError):
                for details in error.details:
                    self.logger.error("configuration invalid", **details)
            if exit_on_error:
                raise SystemExit(1)
            raise

    async def _handle_reload(self, request: Request) -> Response:
        success, message = await self._perform_reload(
            request.app, reason="http"
        )
        status = 200 if success else 400
        return json_response(
            {"success": success, "message": message}, status=status
        )

    async def _perform_reload(
        self, application: Application, *, reason: str
    ) -> tuple[bool, str]:
        if self._reload_lock.locked():
            return False, "reload already in progress"

        async with self._reload_lock:
            self.logger.info("reload requested", reason=reason)
            try:
                new_config = self._load_config(
                    self.config_paths, exit_on_error=False
                )
            except Exception as error:
                return False, f"invalid configuration: {error}"

            try:
                new_registry = MetricsRegistry()
                new_registry.create_metrics(new_config.metrics.values())
                self._set_static_metrics(new_config, new_registry)
                new_executor = QueryExecutor(
                    new_config, new_registry, self.logger
                )
            except Exception as error:  # pragma: nocover - defensive
                self.logger.error("reload failed", error=str(error))
                return False, f"failed to apply configuration: {error}"

            exporter = application[EXPORTER_APP_KEY]
            old_executor = application.get(QUERY_EXECUTOR_APP_KEY)
            if old_executor:
                await old_executor.stop()

            # Swap runtime components
            self.registry = new_registry
            exporter.registry = new_registry
            exporter.set_metric_update_handler(
                partial(self._update_handler, new_executor)
            )
            application[QUERY_EXECUTOR_APP_KEY] = new_executor
            self.config = new_config
            await new_executor.start()
            self.logger.info("reload completed", reason=reason)
            return True, "configuration reloaded"

    def _register_reload_routes(self, application: Application) -> None:
        application.router.add_get("/reload", self._handle_reload)
        application.router.add_post("/reload", self._handle_reload)

    def _install_signal_handlers(self, application: Application) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: nocover
            return

        for sig in (signal.SIGHUP, signal.SIGUSR1):
            try:
                loop.add_signal_handler(
                    sig,
                    lambda sig=sig: asyncio.create_task(
                        self._perform_reload(
                            application, reason=f"signal:{sig.name}"
                        )
                    ),
                )
            except (NotImplementedError, RuntimeError):
                self.logger.warning(
                    "signal handler not installed",
                    signal=getattr(sig, "name", sig),
                )

    def _write_pid_file(self) -> None:
        if not self._pid_file:
            return
        self._pid_file.parent.mkdir(parents=True, exist_ok=True)
        self._pid_file.write_text(str(os.getpid()))
        self.logger.info("pid file written", path=str(self._pid_file))

    def _cleanup_pid_file(self) -> None:
        if self._pid_file and self._pid_file.exists():
            try:
                self._pid_file.unlink()
            except OSError:
                self.logger.warning(
                    "failed to remove pid file", path=str(self._pid_file)
                )

    def _set_static_metrics(
        self, config: Config, registry: MetricsRegistry
    ) -> None:
        query_interval_metric = t.cast(
            Gauge, registry.get_metric(QUERY_INTERVAL_METRIC_NAME)
        )
        for query in config.queries.values():
            if query.interval:
                query_interval_metric.labels(query=query.name).set(
                    query.interval
                )


script = QueryExporterScript()
