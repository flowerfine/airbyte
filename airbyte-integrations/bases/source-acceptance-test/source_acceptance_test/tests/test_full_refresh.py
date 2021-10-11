#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#
from functools import partial

import pytest
from airbyte_cdk.models import Type
from source_acceptance_test.base import BaseTest
from source_acceptance_test.utils import ConnectorRunner, full_refresh_only_catalog, serialize


@pytest.mark.default_timeout(20 * 60)
class TestFullRefresh(BaseTest):
    def test_sequential_reads(
        self, inputs, connector_config, configured_catalog, docker_runner: ConnectorRunner, detailed_logger
    ):
        ignored_fields = getattr(inputs, "ignored_fields") or {}
        print(ignored_fields)
        # configured_catalog = full_refresh_only_catalog(configured_catalog)
        # output = docker_runner.call_read(connector_config, configured_catalog)
        # records_1 = [message.record.data for message in output if message.type == Type.RECORD]

        # output = docker_runner.call_read(connector_config, configured_catalog)
        # records_2 = [message.record.data for message in output if message.type == Type.RECORD]

        # serializer = partial(serialize, exclude_fields=ignored_fields)
        # output_diff = set(map(serializer, records_1))
        # output_diff = set(map(serializer, records_1)) - set(map(serializer, records_2))
        # if output_diff:
        #     msg = "The two sequential reads should produce either equal set of records or one of them is a strict subset of the other"
        #     detailed_logger.info(msg)
        #     detailed_logger.log_json_list(output_diff)
        #     pytest.fail(msg)
