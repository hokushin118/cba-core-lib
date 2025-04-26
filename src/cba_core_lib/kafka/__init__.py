"""
Package: kafka.

CBA Core Library - Kafka Utilities.
"""
from __future__ import annotations

from cba_core_lib.kafka.consumer import KafkaConsumerManager
from cba_core_lib.kafka.producer import KafkaProducerManager
from cba_core_lib.kafka.utils import (
    safe_string_serializer,
    safe_json_serializer
)
from .configs import (
    SecurityProtocol,
    AutoOffsetReset,
    KafkaConsumerConfig,
    KafkaProducerConfig
)

__all__ = [
    'SecurityProtocol',
    'AutoOffsetReset',
    'KafkaConsumerConfig',
    'KafkaProducerConfig',
    'KafkaConsumerManager',
    'KafkaProducerManager',
    'safe_string_serializer',
    'safe_json_serializer',
]
