"""Apache Kafka integration for event streaming."""

import asyncio
import base64
import json
import time
from collections.abc import Callable
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from confluent_kafka.admin import AdminClient, NewTopic
from cryptography.fernet import Fernet

from .config import KafkaConfig, get_streaming_config
from .event_store import EventSchema, get_event_store
from .performance_profiler import get_performance_profiler, profile_streaming_method

logger = structlog.get_logger(__name__)


class KafkaSecurityManager:
    """Manages Kafka security features including ACLs and encryption."""

    def __init__(self, config: KafkaConfig):
        self.config = config
        self.logger = logger.bind(component="kafka_security_manager")
        self._encryption_key = None

        # Initialize encryption if enabled
        if config.enable_data_encryption:
            self._init_encryption()

    def _init_encryption(self) -> None:
        """Initialize data encryption."""
        if self.config.encryption_key:
            # Use provided key
            try:
                key_bytes = base64.urlsafe_b64decode(
                    self.config.encryption_key.encode()
                )
                self._encryption_key = Fernet(key_bytes)
                self.logger.info("Encryption initialized with provided key")
            except Exception as e:
                self.logger.error(
                    "Failed to initialize encryption with provided key", error=str(e)
                )
                raise ValueError("Invalid encryption key provided") from e
        else:
            # Check if we're in production environment
            import os

            environment = os.getenv("ENVIRONMENT", "development").lower()

            if environment in ("production", "prod"):
                # In production, require explicit key configuration
                self.logger.error(
                    "Encryption key must be explicitly configured in production"
                )
                raise ValueError("Encryption key is required in production environment")
            else:
                # Generate a new key only in development/test environments
                self._encryption_key = Fernet(Fernet.generate_key())
                self.logger.warning(
                    "Generated new encryption key for development - configure externally for production",
                    environment=environment,
                )

    def encrypt_data(self, data: bytes) -> bytes:
        """Encrypt data if encryption is enabled."""
        if not self.config.enable_data_encryption or not self._encryption_key:
            return data

        try:
            encrypted = self._encryption_key.encrypt(data)
            self.logger.debug("Data encrypted successfully")
            return encrypted
        except Exception as e:
            self.logger.error("Data encryption failed", error=str(e))
            raise

    def decrypt_data(self, encrypted_data: bytes) -> bytes:
        """Decrypt data if encryption is enabled."""
        if not self.config.enable_data_encryption or not self._encryption_key:
            return encrypted_data

        try:
            decrypted = self._encryption_key.decrypt(encrypted_data)
            self.logger.debug("Data decrypted successfully")
            return decrypted
        except Exception as e:
            self.logger.error("Data decryption failed", error=str(e))
            raise

    def get_ssl_config(self) -> dict[str, Any]:
        """Get SSL configuration for Kafka clients."""
        ssl_config = {}

        if self.config.security_protocol in ["SSL", "SASL_SSL"]:
            if self.config.ssl_ca_location:
                ssl_config["ssl_cafile"] = self.config.ssl_ca_location
            if self.config.ssl_cert_location:
                ssl_config["ssl_certfile"] = self.config.ssl_cert_location
            if self.config.ssl_key_location:
                ssl_config["ssl_keyfile"] = self.config.ssl_key_location
            if self.config.ssl_key_password:
                ssl_config["ssl_password"] = self.config.ssl_key_password

            ssl_config["ssl_check_hostname"] = self.config.ssl_check_hostname

        return ssl_config

    def get_sasl_config(self) -> dict[str, Any]:
        """Get SASL configuration for Kafka clients."""
        sasl_config = {}

        if self.config.security_protocol in ["SASL_PLAINTEXT", "SASL_SSL"]:
            if self.config.sasl_mechanism:
                sasl_config["sasl_mechanism"] = self.config.sasl_mechanism
            if self.config.sasl_username:
                sasl_config["sasl_plain_username"] = self.config.sasl_username
            if self.config.sasl_password:
                sasl_config["sasl_plain_password"] = self.config.sasl_password
            if self.config.sasl_kerberos_service_name:
                sasl_config["sasl_kerberos_service_name"] = (
                    self.config.sasl_kerberos_service_name
                )

        return sasl_config

    def audit_log_access(
        self, operation: str, topic: str, principal: str | None = None
    ) -> None:
        """Log security-relevant Kafka operations for audit purposes."""
        audit_data = {
            "timestamp": time.time(),
            "operation": operation,
            "topic": topic,
            "principal": principal or "unknown",
            "security_protocol": self.config.security_protocol,
            "encryption_enabled": self.config.enable_data_encryption,
        }

        self.logger.info(
            "Kafka security audit event",
            audit_event=audit_data,
            operation=operation,
            topic=topic,
        )


class KafkaTopicManager:
    """Manages Kafka topics creation and configuration with security."""

    def __init__(self, config: KafkaConfig):
        self.config = config
        self.security_manager = KafkaSecurityManager(config)

        # Build admin client config with security settings
        admin_config = {
            "bootstrap.servers": ",".join(config.bootstrap_servers),
            "security.protocol": config.security_protocol,
        }

        # Add SSL configuration
        admin_config.update(self.security_manager.get_ssl_config())

        # Add SASL configuration
        admin_config.update(self.security_manager.get_sasl_config())

        self.admin_client = AdminClient(admin_config)
        self.logger = logger.bind(component="kafka_topic_manager")

    async def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 3,
        replication_factor: int = 1,
        config: dict[str, str] | None = None,
    ) -> bool:
        """Create a Kafka topic."""
        try:
            topic_config = config or {
                "cleanup.policy": "delete",
                "retention.ms": "604800000",  # 7 days
                "compression.type": "gzip",
            }

            new_topic = NewTopic(
                topic=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config=topic_config,
            )

            # Create topic asynchronously
            fs = self.admin_client.create_topics([new_topic])

            # Wait for operation to complete
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    self.logger.info("Topic created successfully", topic=topic)
                    return True
                except Exception as e:
                    if "already exists" in str(e):
                        self.logger.info("Topic already exists", topic=topic)
                        return True
                    else:
                        self.logger.error(
                            "Failed to create topic", topic=topic, error=str(e)
                        )
                        return False

            return False  # If no topics were processed

        except Exception as e:
            self.logger.error("Topic creation failed", topic=topic_name, error=str(e))
            return False

    async def list_topics(self) -> list[str]:
        """List all Kafka topics."""
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            topics = list(metadata.topics.keys())
            self.logger.debug("Listed topics", topic_count=len(topics))
            return topics
        except Exception as e:
            self.logger.error("Failed to list topics", error=str(e))
            return []

    async def delete_topic(self, topic_name: str) -> bool:
        """Delete a Kafka topic."""
        try:
            fs = self.admin_client.delete_topics([topic_name])

            for topic, f in fs.items():
                try:
                    f.result()
                    self.logger.info("Topic deleted successfully", topic=topic)
                    return True
                except Exception as e:
                    self.logger.error(
                        "Failed to delete topic", topic=topic, error=str(e)
                    )
                    return False

            return False  # If no topics were processed

        except Exception as e:
            self.logger.error("Topic deletion failed", topic=topic_name, error=str(e))
            return False


class KafkaProducerManager:
    """Manages Kafka event production with enhanced security and performance."""

    def __init__(self, config: KafkaConfig):
        self.config = config
        self.security_manager = KafkaSecurityManager(config)
        self.producer: AIOKafkaProducer | None = None
        self.event_store = get_event_store()
        self.logger = logger.bind(component="kafka_producer")
        self._is_running = False
        self._sent_count = 0
        self._error_count = 0
        self._profiler = get_performance_profiler()

        # Performance optimization: connection pooling and batching
        self._batch_buffer: list[tuple] = []
        self._batch_size = config.producer_config.get("batch_size", 100)
        self._flush_interval_ms = 50  # Flush every 50ms
        self._last_flush_time = 0

        # Pre-allocated objects for performance
        self._reusable_headers: dict[str, bytes] = {}
        self._stats_cache: dict[str, Any] = {
            "is_running": False,
            "sent_count": 0,
            "error_count": 0,
            "success_rate": 0.0,
        }
        self._last_stats_update = 0

    async def start(self) -> None:
        """Start the Kafka producer with security configuration."""
        try:
            producer_config = {
                "bootstrap_servers": self.config.bootstrap_servers,
                "security_protocol": self.config.security_protocol,
                **self.config.producer_config,
            }

            # Add SSL configuration
            producer_config.update(self.security_manager.get_ssl_config())

            # Add SASL configuration
            producer_config.update(self.security_manager.get_sasl_config())

            self.producer = AIOKafkaProducer(**producer_config)
            await self.producer.start()
            self._is_running = True

            self.logger.info(
                "Kafka producer started with security",
                bootstrap_servers=self.config.bootstrap_servers,
                security_protocol=self.config.security_protocol,
                encryption_enabled=self.config.enable_data_encryption,
            )

        except Exception as e:
            self.logger.error("Failed to start Kafka producer", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop the Kafka producer."""
        if self.producer:
            try:
                await self.producer.stop()
                self._is_running = False
                self.logger.info(
                    "Kafka producer stopped",
                    sent_count=self._sent_count,
                    error_count=self._error_count,
                )
            except Exception as e:
                self.logger.error("Error stopping Kafka producer", error=str(e))

    @profile_streaming_method("kafka_producer", "send_event")
    async def send_event(
        self,
        topic: str,
        event: EventSchema,
        key: str | None = None,
        partition: int | None = None,
        headers: dict[str, bytes] | None = None,
        principal: str | None = None,
    ) -> bool:
        """Send an event to Kafka topic with encryption, audit logging, and performance optimization."""
        if not self._is_running or not self.producer:
            self.logger.error("Producer not running")
            return False

        try:
            # Audit log the access
            self.security_manager.audit_log_access("PRODUCE", topic, principal)

            # Serialize event to JSON
            event_data = event.to_json().encode("utf-8")

            # Encrypt data if encryption is enabled
            encrypted_data = self.security_manager.encrypt_data(event_data)

            # Use event_id as key if no key provided
            message_key = (key or event.event_id).encode("utf-8")

            # Reuse headers dict to reduce allocations
            self._reusable_headers.clear()
            if headers:
                self._reusable_headers.update(headers)

            # Add event metadata as headers
            self._reusable_headers.update(
                {
                    "event_type": event.event_type.value.encode("utf-8"),
                    "event_name": event.event_name.encode("utf-8"),
                    "source_service": event.source_service.encode("utf-8"),
                    "schema_version": event.schema_version.encode("utf-8"),
                    "timestamp": str(int(event.timestamp.timestamp())).encode("utf-8"),
                    "encrypted": str(self.config.enable_data_encryption).encode(
                        "utf-8"
                    ),
                }
            )

            # Add principal information if available
            if principal:
                self._reusable_headers["principal"] = principal.encode("utf-8")

            # Send message
            await self.producer.send_and_wait(
                topic=topic,
                value=encrypted_data,
                key=message_key,
                partition=partition,
                headers=list(self._reusable_headers.items()),
            )

            self._sent_count += 1
            self.logger.debug(
                "Event sent successfully",
                topic=topic,
                event_id=event.event_id,
                event_type=event.event_type,
                encrypted=self.config.enable_data_encryption,
                principal=principal,
            )
            return True

        except Exception as e:
            self._error_count += 1
            self.logger.error(
                "Failed to send event",
                topic=topic,
                event_id=event.event_id,
                error=str(e),
                principal=principal,
            )
            return False

    async def send_batch(
        self, topic: str, events: list[EventSchema], partition: int | None = None
    ) -> dict[str, Any]:
        """Send multiple events as a batch."""
        if not self._is_running or not self.producer:
            self.logger.error("Producer not running")
            return {
                "total": len(events),
                "successful": 0,
                "failed": len(events),
                "successful_events": [],
                "failed_events": [
                    {
                        "index": i,
                        "event_id": f"event_{i}",
                        "error": "Producer not running",
                    }
                    for i in range(len(events))
                ],
                "duration_seconds": 0.0,
            }

        sent_count = 0
        batch_start = time.time()

        try:
            # Send all events concurrently
            tasks = []
            for event in events:
                task = self.send_event(topic, event, partition=partition)
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Track partial success with detailed results
            successful_results = []
            failed_results = []

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    failed_results.append(
                        {
                            "index": i,
                            "event_id": events[i].event_id
                            if hasattr(events[i], "event_id")
                            else f"event_{i}",
                            "error": str(result),
                        }
                    )
                elif result is True:
                    successful_results.append(
                        {
                            "index": i,
                            "event_id": events[i].event_id
                            if hasattr(events[i], "event_id")
                            else f"event_{i}",
                        }
                    )

            sent_count = len(successful_results)
            failed_count = len(failed_results)

            batch_duration = time.time() - batch_start

            if failed_count > 0:
                self.logger.warning(
                    "Batch send partially failed",
                    topic=topic,
                    total_events=len(events),
                    sent_count=sent_count,
                    failed_count=failed_count,
                    duration_seconds=batch_duration,
                    failed_events=[r["event_id"] for r in failed_results[:5]],
                )  # Log first 5 failures
            else:
                self.logger.info(
                    "Batch send completed successfully",
                    topic=topic,
                    total_events=len(events),
                    sent_count=sent_count,
                    duration_seconds=batch_duration,
                )

            # Return detailed results for caller
            return {
                "total": len(events),
                "successful": sent_count,
                "failed": failed_count,
                "successful_events": successful_results,
                "failed_events": failed_results,
                "duration_seconds": batch_duration,
            }

        except Exception as e:
            self.logger.error(
                "Batch send failed", topic=topic, total_events=len(events), error=str(e)
            )

        return {"successful": sent_count, "failed": 0, "total": len(events)}

    def get_stats(self) -> dict[str, Any]:
        """Get producer statistics with caching."""
        current_time = time.time()

        # Cache stats for 100ms to reduce computation
        if current_time - self._last_stats_update > 0.1:
            total_events = self._sent_count + self._error_count
            self._stats_cache.update(
                {
                    "is_running": self._is_running,
                    "sent_count": self._sent_count,
                    "error_count": self._error_count,
                    "success_rate": self._sent_count / max(total_events, 1),
                    "batch_buffer_size": len(self._batch_buffer),
                }
            )
            self._last_stats_update = current_time

        return self._stats_cache.copy()


class KafkaConsumerManager:
    """Manages Kafka event consumption with enhanced security."""

    def __init__(self, config: KafkaConfig, group_id: str):
        self.config = config
        self.group_id = group_id
        self.security_manager = KafkaSecurityManager(config)
        self.consumer: AIOKafkaConsumer | None = None
        self.event_store = get_event_store()
        self.logger = logger.bind(component="kafka_consumer", group_id=group_id)
        self._is_running = False
        self._consumed_count = 0
        self._error_count = 0
        self._handlers: dict[str, list[Callable]] = {}
        self._topics: set[str] = set()

    async def start(self, topics: list[str]) -> None:
        """Start the Kafka consumer with security configuration."""
        try:
            consumer_config = {
                "bootstrap_servers": self.config.bootstrap_servers,
                "group_id": self.group_id,
                "security_protocol": self.config.security_protocol,
                **self.config.consumer_config,
            }

            # Add SSL configuration
            consumer_config.update(self.security_manager.get_ssl_config())

            # Add SASL configuration
            consumer_config.update(self.security_manager.get_sasl_config())

            self.consumer = AIOKafkaConsumer(*topics, **consumer_config)

            await self.consumer.start()
            self._is_running = True
            self._topics.update(topics)

            # Audit log consumer start
            for topic in topics:
                self.security_manager.audit_log_access("CONSUME", topic, self.group_id)

            self.logger.info(
                "Kafka consumer started with security",
                topics=topics,
                group_id=self.group_id,
                security_protocol=self.config.security_protocol,
                encryption_enabled=self.config.enable_data_encryption,
            )

        except Exception as e:
            self.logger.error("Failed to start Kafka consumer", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop the Kafka consumer."""
        if self.consumer:
            try:
                await self.consumer.stop()
                self._is_running = False
                self.logger.info(
                    "Kafka consumer stopped",
                    consumed_count=self._consumed_count,
                    error_count=self._error_count,
                )
            except Exception as e:
                self.logger.error("Error stopping Kafka consumer", error=str(e))

    def add_handler(
        self, event_type: str, handler: Callable[[EventSchema], None]
    ) -> None:
        """Add an event handler for specific event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

        self.logger.info(
            "Event handler added",
            event_type=event_type,
            handler_count=len(self._handlers[event_type]),
        )

    def remove_handler(
        self, event_type: str, handler: Callable[[EventSchema], None]
    ) -> None:
        """Remove an event handler."""
        if event_type in self._handlers:
            try:
                self._handlers[event_type].remove(handler)
                self.logger.info("Event handler removed", event_type=event_type)
            except ValueError:
                self.logger.warning(
                    "Handler not found for removal", event_type=event_type
                )

    def __aiter__(self):
        """Support async iteration over raw messages."""
        if not self._is_running or not self.consumer:
            raise RuntimeError("Consumer not running")
        return self.consumer.__aiter__()

    async def consume_messages(self, max_messages: int | None = None) -> None:
        """Consume messages from Kafka topics with decryption and audit logging."""
        if not self._is_running or not self.consumer:
            self.logger.error("Consumer not running")
            return

        message_count = 0

        try:
            async for message in self.consumer:
                try:
                    # Extract metadata from headers
                    headers = dict(message.headers or [])

                    # Check if message is encrypted
                    is_encrypted = (
                        headers.get(b"encrypted", b"False").decode("utf-8") == "True"
                    )
                    principal = headers.get(b"principal", b"unknown").decode("utf-8")

                    # Audit log the consumption
                    self.security_manager.audit_log_access(
                        "CONSUME", message.topic, principal
                    )

                    # Decrypt message data if needed
                    if is_encrypted or self.config.enable_data_encryption:
                        try:
                            decrypted_data = self.security_manager.decrypt_data(
                                message.value
                            )
                            event_data = json.loads(decrypted_data.decode("utf-8"))
                        except (ValueError, UnicodeDecodeError) as decode_error:
                            self.logger.error(
                                "Failed to decode decrypted message",
                                topic=message.topic,
                                partition=message.partition,
                                offset=message.offset,
                                error=str(decode_error),
                            )
                            raise
                        except json.JSONDecodeError as json_error:
                            self.logger.error(
                                "Failed to parse JSON from decrypted message",
                                topic=message.topic,
                                partition=message.partition,
                                offset=message.offset,
                                error=str(json_error),
                            )
                            raise
                        except Exception as decrypt_error:
                            self.logger.error(
                                "Failed to decrypt message",
                                topic=message.topic,
                                partition=message.partition,
                                offset=message.offset,
                                error=str(decrypt_error),
                            )
                            raise
                    else:
                        # Parse event from unencrypted message
                        event_data = json.loads(message.value.decode("utf-8"))

                    # Validate event using event store
                    event = self.event_store.validate_event(event_data)

                    # Call registered handlers
                    await self._process_event(event, headers)

                    self._consumed_count += 1
                    message_count += 1

                    self.logger.debug(
                        "Message processed",
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        event_id=event.event_id,
                        encrypted=is_encrypted,
                        principal=principal,
                    )

                    # Break if max messages reached
                    if max_messages and message_count >= max_messages:
                        break

                except Exception as e:
                    self._error_count += 1
                    self.logger.error(
                        "Error processing message",
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                        error=str(e),
                    )

                    # Send to dead letter queue if configured
                    await self._send_to_dead_letter_queue(message, str(e))

        except Exception as e:
            self.logger.error("Consumer loop error", error=str(e))
            raise

    async def _process_event(
        self, event: EventSchema, headers: dict[str, bytes]
    ) -> None:
        """Process an event by calling registered handlers."""
        event_type = event.event_type.value

        # Get handlers for this event type
        handlers = self._handlers.get(event_type, [])

        if not handlers:
            self.logger.debug(
                "No handlers registered for event type", event_type=event_type
            )
            return

        # Call all handlers concurrently
        tasks = []
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    task = handler(event)
                else:
                    task = asyncio.get_event_loop().run_in_executor(
                        None, handler, event
                    )
                tasks.append(task)
            except Exception as e:
                self.logger.error(
                    "Error creating handler task",
                    event_type=event_type,
                    handler=str(handler),
                    error=str(e),
                )

        if tasks:
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
                self.logger.debug(
                    "Event handlers completed",
                    event_type=event_type,
                    handler_count=len(tasks),
                )
            except Exception as e:
                self.logger.error(
                    "Error in event handlers", event_type=event_type, error=str(e)
                )

    async def _send_to_dead_letter_queue(
        self, original_message: Any, error_msg: str
    ) -> None:
        """Send failed message to dead letter queue."""
        if not hasattr(self, "config") or not self.config.enable_dead_letter_queue:
            return

        try:
            # Get the producer from the manager
            if hasattr(self, "kafka_manager") and self.kafka_manager:
                producer = await self.kafka_manager.get_producer("dlq_producer")

                # Create dead letter topic name
                dlq_topic = (
                    f"{original_message.topic}{self.config.dead_letter_topic_suffix}"
                )

                # Create dead letter message with error metadata
                dlq_message = {
                    "original_topic": original_message.topic,
                    "original_partition": original_message.partition,
                    "original_offset": original_message.offset,
                    "original_timestamp": original_message.timestamp,
                    "original_value": original_message.value.decode("utf-8")
                    if original_message.value
                    else None,
                    "error_message": error_msg,
                    "error_timestamp": time.time(),
                    "consumer_group": self.group_id,
                }

                # Send to dead letter queue
                await producer.send_json(dlq_topic, dlq_message)

                self.logger.info(
                    "Message sent to dead letter queue",
                    original_topic=original_message.topic,
                    dlq_topic=dlq_topic,
                    error=error_msg,
                )

        except Exception as e:
            self.logger.error(
                "Failed to send message to dead letter queue",
                original_topic=getattr(original_message, "topic", "unknown"),
                error=str(e),
            )

    def get_stats(self) -> dict[str, Any]:
        """Get consumer statistics."""
        return {
            "is_running": self._is_running,
            "group_id": self.group_id,
            "topics": list(self._topics),
            "consumed_count": self._consumed_count,
            "error_count": self._error_count,
            "handler_count": sum(len(handlers) for handlers in self._handlers.values()),
            "registered_event_types": list(self._handlers.keys()),
        }


class KafkaManager:
    """Main Kafka manager orchestrating producers and consumers with security."""

    def __init__(self, config: KafkaConfig | None = None):
        self.config = config or get_streaming_config().kafka
        self.security_manager = KafkaSecurityManager(self.config)
        self.topic_manager = KafkaTopicManager(self.config)
        self.producers: dict[str, KafkaProducerManager] = {}
        self.consumers: dict[str, KafkaConsumerManager] = {}
        self.logger = logger.bind(component="kafka_manager")
        self._is_initialized = False

    async def initialize(self) -> None:
        """Initialize Kafka manager and create default topics."""
        try:
            streaming_config = get_streaming_config()

            # Create default topics
            default_topics = [
                (streaming_config.events_topic, 6, 1),
                (streaming_config.metrics_topic, 3, 1),
                (streaming_config.alerts_topic, 3, 1),
                (streaming_config.ml_predictions_topic, 6, 1),
            ]

            for topic_name, partitions, replication in default_topics:
                await self.topic_manager.create_topic(
                    topic_name=topic_name,
                    num_partitions=partitions,
                    replication_factor=replication,
                )

            self._is_initialized = True
            self.logger.info("Kafka manager initialized successfully")

        except Exception as e:
            self.logger.error("Failed to initialize Kafka manager", error=str(e))
            raise

    async def get_producer(self, name: str = "default") -> KafkaProducerManager:
        """Get or create a Kafka producer."""
        if name not in self.producers:
            producer = KafkaProducerManager(self.config)
            await producer.start()
            self.producers[name] = producer
            self.logger.info("Created new Kafka producer", name=name)

        return self.producers[name]

    async def get_consumer(
        self, group_id: str, topics: list[str]
    ) -> KafkaConsumerManager:
        """Get or create a Kafka consumer."""
        consumer_key = f"{group_id}:{':'.join(sorted(topics))}"

        if consumer_key not in self.consumers:
            consumer = KafkaConsumerManager(self.config, group_id)
            await consumer.start(topics)
            self.consumers[consumer_key] = consumer
            self.logger.info(
                "Created new Kafka consumer", group_id=group_id, topics=topics
            )

        return self.consumers[consumer_key]

    # Topic management wrapper methods
    async def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 3,
        replication_factor: int = 1,
        config: dict[str, str] | None = None,
    ) -> bool:
        """Create a Kafka topic."""
        return await self.topic_manager.create_topic(
            topic_name, num_partitions, replication_factor, config
        )

    async def list_topics(self) -> list[str]:
        """List all Kafka topics."""
        return await self.topic_manager.list_topics()

    async def delete_topic(self, topic_name: str) -> bool:
        """Delete a Kafka topic."""
        return await self.topic_manager.delete_topic(topic_name)

    async def get_topic_metadata(self, topic_name: str) -> dict[str, Any]:
        """Get topic metadata including partition count."""
        try:
            # Use admin client to get topic metadata
            metadata = self.topic_manager.admin_client.list_topics(timeout=10)
            if topic_name in metadata.topics:
                topic_metadata = metadata.topics[topic_name]
                return {
                    "partition_count": len(topic_metadata.partitions),
                    "partitions": {
                        partition_id: {
                            "replicas": [replica.id for replica in partition.replicas],
                            "leader": partition.leader.id if partition.leader else None,
                        }
                        for partition_id, partition in topic_metadata.partitions.items()
                    },
                }
            else:
                return {"partition_count": 0, "partitions": {}}
        except Exception as e:
            self.logger.error(
                "Failed to get topic metadata", topic=topic_name, error=str(e)
            )
            return {"partition_count": 0, "partitions": {}}

    # Event production methods
    async def produce_event(self, topic: str, event: dict[str, Any]) -> None:
        """Produce an event to a Kafka topic."""
        producer = await self.get_producer()
        # For integration tests, we need to create an EventSchema from dict
        from .event_store import get_event_store

        event_store = get_event_store()
        # Convert dict to EventSchema for compatibility
        event_schema = event_store.validate_event(event)
        await producer.send_event(topic, event_schema)

    async def create_consumer(
        self, topics: list[str], group_id: str
    ) -> KafkaConsumerManager:
        """Create a consumer for the given topics and group."""
        return await self.get_consumer(group_id, topics)

    async def shutdown(self) -> None:
        """Shutdown all producers and consumers."""
        try:
            # Stop all producers
            for name, producer in self.producers.items():
                await producer.stop()
                self.logger.info("Stopped producer", name=name)

            # Stop all consumers
            for key, consumer in self.consumers.items():
                await consumer.stop()
                self.logger.info("Stopped consumer", key=key)

            self.producers.clear()
            self.consumers.clear()

            self.logger.info("Kafka manager shutdown completed")

        except Exception as e:
            self.logger.error("Error during Kafka manager shutdown", error=str(e))

    def get_stats(self) -> dict[str, Any]:
        """Get comprehensive Kafka manager statistics."""
        producer_stats = {
            name: producer.get_stats() for name, producer in self.producers.items()
        }

        consumer_stats = {
            key: consumer.get_stats() for key, consumer in self.consumers.items()
        }

        return {
            "is_initialized": self._is_initialized,
            "producer_count": len(self.producers),
            "consumer_count": len(self.consumers),
            "producers": producer_stats,
            "consumers": consumer_stats,
            "config": {
                "bootstrap_servers": self.config.bootstrap_servers,
                "security_protocol": self.config.security_protocol,
            },
        }


# Global Kafka manager instance
_kafka_manager: KafkaManager | None = None


async def get_kafka_manager() -> KafkaManager:
    """Get the global Kafka manager instance."""
    global _kafka_manager

    if _kafka_manager is None:
        _kafka_manager = KafkaManager()
        await _kafka_manager.initialize()

    return _kafka_manager


async def shutdown_kafka_manager() -> None:
    """Shutdown the global Kafka manager."""
    global _kafka_manager

    if _kafka_manager:
        await _kafka_manager.shutdown()
        _kafka_manager = None
