"""Alert manager for sending alerts to AlertManager."""

import asyncio
import json
import re
import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
from urllib.parse import quote, urljoin

import aiohttp
import structlog

from .db import (
    MetricResult,
)


class AlertState:
    """Track the state of an alert instance."""
    
    def __init__(self, alert_key: str):
        self.alert_key = alert_key
        self.start_time: Optional[datetime] = None
        self.last_active: Optional[datetime] = None
        self.active = False
        self.sent = False
        
    def update(self, active: bool, current_time: datetime) -> None:
        """Update alert state."""
        self.last_active = current_time
        
        if active and not self.active:
            # Becoming active
            self.start_time = current_time
            self.active = True
            self.sent = False
        elif not active and self.active:
            # Becoming inactive
            self.active = False
            self.start_time = None
            self.sent = False
        elif active and self.active:
            # Remaining active - check if duration exceeds 30 minutes
            # if self.start_time and (current_time - self.start_time).total_seconds() >= 30 * 60:
            #     # Reset for next alert cycle
            #     self.start_time = current_time
            self.sent = False


class AlertManager:
    """Client for AlertManager API."""

    def __init__(
        self, 
        url: str, 
        logger: Optional[structlog.stdlib.BoundLogger] = None
    ):
        self.url = url.rstrip('/')
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logger or structlog.get_logger()
        self._timeout = aiohttp.ClientTimeout(total=30)

    async def start(self) -> None:
        """Initialize the HTTP session."""
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=self._timeout)

    async def stop(self) -> None:
        """Close the HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None

    async def send_alerts(self, alerts: List[Dict[str, Any]]) -> bool:
        """Send alerts to AlertManager."""
        if not self.url:
            self.logger.debug("AlertManager URL not configured, skipping alert sending")
            return True

        if not self.session:
            await self.start()

        url = urljoin(self.url, '/api/v2/alerts')
        alert_count = len(alerts)
        alert_names = [alert['labels'].get('alertname', 'unknown') for alert in alerts]
        
        try:
            self.logger.info(
                "Sending alerts to AlertManager",
                url=url,
                count=alert_count,
                alert_names=alert_names
            )
            self.logger.debug(f"[AlertManager] Sending {alert_count} alert(s) to {url}")
            self.logger.debug(f"[AlertManager] Alert names: {', '.join(alert_names)}")
            async with self.session.post(
                url, 
                json=alerts,
                headers={'Content-Type': 'application/json'}
            ) as response:
                response_text = await response.text()
                if response.status == 200:
                    self.logger.info(
                        "Alerts sent successfully to AlertManager",
                        url=url,
                        count=alert_count,
                        alert_names=alert_names,
                        status=response.status
                    )
                    self.logger.debug(f"[AlertManager] ✓ Successfully sent {alert_count} alert(s) to AlertManager")
                    self.logger.debug(f"[AlertManager] Response status: {response.status}")
                    return True
                else:
                    self.logger.error(
                        "Failed to send alerts to AlertManager",
                        url=url,
                        status=response.status,
                        response=response_text,
                        count=alert_count,
                        alert_names=alert_names
                    )
                    self.logger.debug(f"[AlertManager] ✗ Failed to send alerts to AlertManager")
                    self.logger.debug(f"[AlertManager] Response status: {response.status}")
                    self.logger.debug(f"[AlertManager] Response body: {response_text[:500]}")  # 限制长度避免过长
                    return False
        except aiohttp.ClientError as e:
            error_msg = f"Network error sending alerts to AlertManager: {str(e)}"
            self.logger.error(
                error_msg,
                url=url,
                error=str(e),
                error_type=type(e).__name__,
                count=alert_count
            )
            self.logger.debug(f"[AlertManager] ✗ Network error: {str(e)}")
            return False
        except Exception as e:
            error_msg = f"Unexpected error sending alerts to AlertManager: {str(e)}"
            self.logger.error(
                error_msg,
                url=url,
                error=str(e),
                error_type=type(e).__name__,
                count=alert_count,
                traceback=True
            )
            self.logger.debug(f"[AlertManager] ✗ Unexpected error: {str(e)}")
            return False


class AlertGenerator:
    """Generate alerts from query results based on result existence and duration tracking.
    
    Alerts are triggered when query results contain data (non-empty results).
    If a query returns empty set, no alert will be sent.
    """

    def __init__(
        self, 
        alert_manager: AlertManager, 
        alert_configs: Dict[str, Any],
        logger: Optional[structlog.stdlib.BoundLogger] = None
    ):
        self.alert_manager = alert_manager
        self.alert_configs = alert_configs
        self.logger = logger or structlog.get_logger()
        
        # Track alert states: {alert_key: AlertState}
        self.alert_states: Dict[str, AlertState] = {}

    def generate_alerts_from_results(
        self, 
        query_name: str, 
        alert_names: List[str], 
        results: List[Dict[str, Any]],  # 这里已经是字典列表
        database_labels: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """Generate alerts from query results with condition evaluation."""
        alerts = []
        current_time = datetime.utcnow()
        self.logger.debug(f"[AlertGenerator] generate_alerts_from_results alert_names: {alert_names}")
        self.logger.debug(f"[AlertGenerator] generate_alerts_from_results results: {results}")
        self.logger.debug(f"[AlertGenerator] generate_alerts_from_results database_labels: {database_labels}")
        self.logger.debug(f"[AlertGenerator] generate_alerts_from_results query_name: {query_name}")
        self.logger.debug(f"[AlertGenerator] generate_alerts_from_results alert_configs: {self.alert_configs}")
        for alert_name in alert_names:
            alert_config = self.alert_configs.get(alert_name)
            if not alert_config:
                self.logger.warning(
                    "Alert configuration not found", 
                    alert_name=alert_name,
                    query=query_name
                )
                continue

            for result in results:
                # Check if alert condition is met, if has result value
                is_active = self._evaluate_alert_condition(alert_config, result)
                self.logger.debug(f"[AlertGenerator] _evaluate_alert_condition is_active: {is_active}")
                # Create unique key for this alert instance
                alert_key = self._create_alert_key(alert_name, result, database_labels)
                self.logger.debug(f"[AlertGenerator] _create_alert_key alert_key: {alert_key}")
                # Update alert state
                alert_state = self._update_alert_state(alert_key, is_active, current_time)
                self.logger.debug(f"[AlertGenerator] _update_alert_state alert_state: {alert_state}")
                # Check if alert should be sent based on duration
                should_send = self._should_send_alert(alert_config, alert_state, current_time)
                self.logger.debug(f"[AlertGenerator] _should_send_alert should_send: {should_send}")
                if should_send and not alert_state.sent:
                    alert = self._create_alert(
                        alert_name, 
                        alert_config, 
                        result,  # 这里传递的是字典
                        database_labels,
                        query_name,
                        alert_state
                    )
                    if alert:
                        alerts.append(alert)
                        # alert_state.sent = True
                        self.logger.debug(
                            "Alert triggered",
                            alert_name=alert_name,
                            query=query_name,
                            duration=self._get_duration_seconds(alert_state.start_time, current_time),
                            condition_met=is_active
                        )
        
        return alerts

    def _evaluate_alert_condition(self, alert_config: Dict[str, Any], result: Dict[str, Any]) -> bool:
        """Evaluate if alert should be triggered based on result existence.
        
        Returns True if result has data (not empty), False otherwise.
        If query returns empty set, no alert will be sent.
        If query returns data, alert will be sent.
        """
        try:
            self.logger.debug(f"[AlertGenerator] _evaluate_alert_condition alert_config: {alert_config}")
            self.logger.debug(f"[AlertGenerator] _evaluate_alert_condition result: {result}")
            
            # Check if result has 'value' field
            if 'value' not in result:
                self.logger.debug(
                    "Result missing 'value' field, skipping alert evaluation",
                    result_keys=list(result.keys())
                )
                return False
            
            value = result['value']
            
            # If value is None, don't trigger alert
            if value is None:
                self.logger.debug("Result value is None, skipping alert evaluation")
                return False
            
            # If we have a value (even if it's 0 or empty string), trigger alert
            # The presence of data means the query returned results
            self.logger.debug(
                "Result has value, alert condition met",
                value=value,
                value_type=type(value).__name__
            )
            return True
            
        except Exception as e:
            self.logger.error(
                "Failed to evaluate alert condition",
                error=str(e),
                alert_config=alert_config,
                result=result
            )
            return False


    
    def _create_alert_key(self, alert_name: str, result_labels: Dict[str, Any], database_labels: Dict[str, str]) -> str:
        """Create a unique key for an alert instance."""
        # Use labels to create unique key for this alert instance
        label_parts = []
        
        # Add database labels
        for key, value in sorted(database_labels.items()):
            label_parts.append(f"{key}")
            
        # Add result labels
        for key, value in sorted(result_labels.items()):
            # 跳过指标值字段
            if key == 'value' or (isinstance(value, (int, float)) and key not in ['xxxx', 'yyyy']):
                continue
            if isinstance(value, (str, int, float)):
                label_parts.append(f"{key}")
                
        return f"{alert_name}:{':'.join(label_parts)}"
    
    def _update_alert_state(self, alert_key: str, is_active: bool, current_time: datetime) -> AlertState:
        """Update or create alert state."""
        if alert_key not in self.alert_states:
            self.alert_states[alert_key] = AlertState(alert_key)
            
        alert_state = self.alert_states[alert_key]
        alert_state.update(is_active, current_time)
        
        return alert_state

    def _should_send_alert(self, alert_config: Dict[str, Any], alert_state: AlertState, current_time: datetime) -> bool:
        """Check if alert should be sent based on duration."""
        if not alert_state.active or alert_state.start_time is None:
            return False
            
        # Parse duration from alert config (e.g., "10m", "1h", "30s")
        # Support both 'for' (YAML key) and 'for_duration' (internal model key)
        duration_raw = alert_config.get('for') or alert_config.get('for_duration') or '0m'
        # Ensure string for parsing
        duration_str = str(duration_raw)
        required_duration = self._parse_duration(duration_str)
        self.logger.debug(f"[AlertGenerator] _should_send_alert required_duration: {required_duration}")
        
        actual_duration = self._get_duration_seconds(alert_state.start_time, current_time)
        self.logger.debug(f"[AlertGenerator] _should_send_alert actual_duration: {actual_duration}")
        self.logger.info(
                "duration alert",
                required_duration=required_duration,
                actual_duration=actual_duration
            )
        self.logger.debug(f"[AlertGenerator] _should_send_alert actual_duration >= required_duration: {actual_duration >= required_duration}")
        return actual_duration >= required_duration

    def _parse_duration(self, duration_str: str) -> int:
        """Parse duration string to seconds."""
        try:
            duration_str = duration_str.strip().lower()
            
            if duration_str.endswith('s'):
                return int(duration_str[:-1])
            elif duration_str.endswith('m'):
                return int(duration_str[:-1]) * 60
            elif duration_str.endswith('h'):
                return int(duration_str[:-1]) * 3600
            elif duration_str.endswith('d'):
                return int(duration_str[:-1]) * 86400
            else:
                # Assume minutes if no unit specified
                return int(duration_str) * 60
                
        except (ValueError, TypeError):
            self.logger.warning("Invalid duration format, using 0", duration=duration_str)
            return 0

    def _get_duration_seconds(self, start: datetime, end: datetime) -> float:
        """Get duration in seconds between two datetimes."""
        return (end - start).total_seconds()

    def _create_alert(
        self, 
        alert_name: str, 
        alert_config: Dict[str, Any], 
        result: Dict[str, Any],
        database_labels: Dict[str, str],
        query_name: str,
        alert_state: AlertState
    ) -> Optional[Dict[str, Any]]:
        """Create a single alert from result data."""
        try:
            self.logger.debug(
                "Creating alert",
                alert_name=alert_name,
                alert_config=alert_config,
                result=result,
                database_labels=database_labels,
                query_name=query_name,
                alert_state=alert_state
            )
            
            # 合并标签：数据库标签 + 告警配置标签 + 查询结果标签
            labels = database_labels.copy()
            self.logger.debug(f"[AlertGenerator] _create_alert database_labels: {labels}")
            
            # 正确处理 alert_config 中的 labels
            alert_config_labels = alert_config.get('labels', {})
            if isinstance(alert_config_labels, list):
                # 如果 labels 是列表，将其转换为字典，从查询结果中获取对应的值
                labels_dict = {}
                for label_key in alert_config_labels:
                    if label_key in result:
                        labels_dict[label_key] = str(result[label_key])
                labels.update(labels_dict)
            elif isinstance(alert_config_labels, dict):
                # 如果 labels 已经是字典，直接使用
                labels.update(alert_config_labels)
            
            # 从查询结果中提取其他标签字段
            for key, value in result.items():
                # 跳过指标值字段和已经处理过的标签字段
                if key == 'value' or key == 'metric' or key in labels:
                    continue
                # 只处理字符串、数字类型的值作为标签
                if isinstance(value, (str, int, float)):
                    labels[key] = str(value)
            
            # 设置必需的标签
            labels['alertname'] = alert_name
            labels['severity'] = alert_config.get('severity', 'warning')
            labels['query'] = query_name

            # 构建注解
            annotations = alert_config.get('annotations', {}).copy()
            if 'summary' not in annotations:
                summary_template = alert_config.get('summary', alert_name)
                # Format summary using Prometheus-style templating
                annotations['summary'] = self._format_template(summary_template, result, labels)
            if 'description' not in annotations:
                description_template = alert_config.get('description', '')
                # Format description using Prometheus-style templating
                annotations['description'] = self._format_template(description_template, result, labels)
            
            # 获取指标值
            value = result.get('value')
            annotations['value'] = str(value) if value is not None else 'unknown'
            
            # Add duration information
            if alert_state.start_time:
                duration_seconds = self._get_duration_seconds(alert_state.start_time, datetime.utcnow())
                annotations['duration'] = f"{duration_seconds:.0f}s"
            
            # Add updatedAt timestamp to track when alert was last sent
            current_time = datetime.utcnow()
            annotations['updatedAt'] = current_time.isoformat() + 'Z'

            # Get generatorURL from alert config, with fallback to default
            generator_url = alert_config.get('generatorURL')
            if generator_url:
                # Format generatorURL with template variables
                generator_url = self._format_generator_url(generator_url, labels)
            else:
                # Default generatorURL if not configured
                generator_url = f'https://grafana-infra.stepfun-inc.com'

            alert = {
                'labels': labels,
                'annotations': annotations,
                'startsAt': alert_state.start_time.isoformat() + 'Z' if alert_state.start_time else current_time.isoformat() + 'Z',
                'generatorURL': generator_url
            }

            self.logger.debug(
                "Alert created successfully",
                alert_name=alert_name,
                labels=labels,
                annotations=annotations
            )
            
            return alert

        except Exception as e:
            self.logger.error(
                "Failed to create alert", 
                alert_name=alert_name,
                error=str(e),
                result=result,
                alert_config=alert_config,
                traceback=True  # 这会显示完整的堆栈跟踪
            )
            return None
    
    def _format_template(self, template: str, result: Dict[str, Any], labels: Dict[str, str]) -> str:
        """Format template using Prometheus-style Go templating syntax.
        
        Supports:
        - {{ $labels.variable_name }} - access values from labels dict
        - {{ $result.variable_name }} - access values from result dict
        - {{ .variable_name }} - shorthand for result (for compatibility)
        
        Args:
            template: Template string with Prometheus-style placeholders like {{ $labels.job_name }}
            result: Query result dictionary containing field values
            labels: Labels dictionary containing label values
            
        Returns:
            Formatted string with template placeholders replaced
        """
        if not template:
            return template
        
        # Find all template blocks in format {{ ... }}
        pattern = r'\{\{\s*([^}]+)\s*\}\}'
        matches = re.finditer(pattern, template)
        
        formatted = template
        # Process matches in reverse order to maintain correct indices
        for match in reversed(list(matches)):
            full_match = match.group(0)  # {{ ... }}
            expr = match.group(1).strip()  # content inside {{ }}
            
            value = None
            
            # Parse expression: $labels.variable_name, $result.variable_name, or .variable_name
            if expr.startswith('$labels.'):
                # {{ $labels.variable_name }}
                var_name = expr[8:].strip()  # Remove '$labels.'
                value = labels.get(var_name)
            elif expr.startswith('$result.'):
                # {{ $result.variable_name }}
                var_name = expr[8:].strip()  # Remove '$result.'
                value = result.get(var_name)
            elif expr.startswith('.'):
                # {{ .variable_name }} - shorthand for result
                var_name = expr[1:].strip()  # Remove '.'
                value = result.get(var_name)
            else:
                # Try to resolve as simple variable name (check result first, then labels)
                var_name = expr.strip()
                value = result.get(var_name) if var_name in result else labels.get(var_name)
            
            # Replace template block with actual value or empty string if not found
            if value is not None:
                formatted = formatted[:match.start()] + str(value) + formatted[match.end():]
            else:
                # Keep original template if variable not found (or replace with empty string)
                # Following Prometheus behavior, we'll keep it as-is for debugging
                self.logger.debug(
                    "Template variable not found",
                    expression=expr,
                    available_labels=list(labels.keys()),
                    available_result_keys=list(result.keys())
                )
        
        return formatted
    
    def _format_generator_url(
        self, 
        template: str, 
        labels: Dict[str, str]
    ) -> str:
        """Format generatorURL template with placeholders.
        
        Supports:
        - {{ $labels.variable_name }} - access values from labels dict
        - {{ $variable_name }} - shorthand for labels (e.g., {{ $owner }} -> labels['owner'])
        
        Args:
            template: Generator URL template string
            labels: Labels dictionary containing label values
            
        Returns:
            Formatted URL with all placeholders replaced
        """
        if not template:
            return template
        
        # Find all template blocks in format {{ ... }}
        pattern = r'\{\{\s*([^}]+)\s*\}\}'
        matches = re.finditer(pattern, template)
        
        formatted = template
        # Process matches in reverse order to maintain correct indices
        for match in reversed(list(matches)):
            full_match = match.group(0)  # {{ ... }}
            expr = match.group(1).strip()  # content inside {{ }}
            
            value = None
            
            # Parse expression
            if expr.startswith('$labels.'):
                # {{ $labels.variable_name }}
                var_name = expr[8:].strip()  # Remove '$labels.'
                value = labels.get(var_name)
            elif expr.startswith('$') and not expr.startswith('$labels.') and not expr.startswith('$result.'):
                # {{ $variable_name }} - shorthand for labels (e.g., {{ $owner }})
                var_name = expr[1:].strip()  # Remove '$'
                value = labels.get(var_name)
            else:
                # Try to resolve as simple variable name from labels
                var_name = expr.strip()
                value = labels.get(var_name)
            
            # Replace template block with actual value or keep original if not found
            if value is not None:
                # URL encode the value to handle special characters in query parameters
                formatted = formatted[:match.start()] + quote(str(value), safe='') + formatted[match.end():]
            else:
                # Keep original template if variable not found (for debugging)
                self.logger.debug(
                    "GeneratorURL template variable not found",
                    expression=expr,
                    available_labels=list(labels.keys())
                )
        
        return formatted
    
    def cleanup_expired_states(self, max_age_seconds: int = 3600) -> None:
        """Clean up expired alert states to prevent memory leaks."""
        current_time = datetime.utcnow()
        expired_keys = []
        
        for key, state in self.alert_states.items():
            if state.last_active and self._get_duration_seconds(state.last_active, current_time) > max_age_seconds:
                expired_keys.append(key)
                
        for key in expired_keys:
            del self.alert_states[key]
            
        if expired_keys:
            self.logger.debug("Cleaned up expired alert states", count=len(expired_keys))