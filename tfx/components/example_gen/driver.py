# Lint as: python2, python3
# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Generic TFX ExampleGen custom driver."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import os
from typing import Any, Dict, List, Optional, Text
from absl import logging
from tfx import types
from tfx.components.base import base_driver
from tfx.components.example_gen import utils
from tfx.orchestration import data_types
from tfx.orchestration import metadata
from tfx.orchestration.portable import base_driver as ir_base_driver
from tfx.proto import example_gen_pb2
from tfx.proto.orchestration import driver_output_pb2
from tfx.proto.orchestration import pipeline_pb2
from google.protobuf import json_format


class Driver(base_driver.BaseDriver, ir_base_driver.BaseDriver):
  """Custom driver for ExampleGen.

  This driver supports file based ExampleGen, e.g., for CsvExampleGen and
  ImportExampleGen.
  """

  def __init__(self,
               metadata_handler: metadata.Metadata,
               pipeline_info: Optional[pipeline_pb2.PipelineInfo] = None,
               pipeline_node: Optional[pipeline_pb2.PipelineNode] = None):
    base_driver.BaseDriver.__init__(self, metadata_handler)
    ir_base_driver.BaseDriver.__init__(
        self, metadata_handler, pipeline_info, pipeline_node)

  def resolve_exec_properties(
      self,
      exec_properties: Dict[Text, Any],
      pipeline_info: data_types.PipelineInfo,
      component_info: data_types.ComponentInfo,
  ) -> Dict[Text, Any]:
    """Overrides BaseDriver.resolve_exec_properties()."""
    del pipeline_info, component_info

    input_config = example_gen_pb2.Input()
    json_format.Parse(exec_properties[utils.INPUT_CONFIG_KEY], input_config)

    input_base = exec_properties[utils.INPUT_BASE_KEY]
    logging.debug('Processing input %s.', input_base)

    # Note that this function updates the input_config.splits.pattern.
    fingerprint, span, version = utils.calculate_splits_fingerprint_span_and_version(
        input_base, input_config.splits)

    exec_properties[utils.INPUT_CONFIG_KEY] = json_format.MessageToJson(
        input_config, sort_keys=True, preserving_proto_field_name=True)
    exec_properties[utils.SPAN_PROPERTY_NAME] = span
    exec_properties[utils.VERSION_PROPERTY_NAME] = version
    exec_properties[utils.FINGERPRINT_PROPERTY_NAME] = fingerprint

    return exec_properties

  def _prepare_output_artifacts(
      self,
      input_artifacts: Dict[Text, List[types.Artifact]],
      output_dict: Dict[Text, types.Channel],
      exec_properties: Dict[Text, Any],
      execution_id: int,
      pipeline_info: data_types.PipelineInfo,
      component_info: data_types.ComponentInfo,
  ) -> Dict[Text, List[types.Artifact]]:
    """Overrides BaseDriver._prepare_output_artifacts()."""
    del input_artifacts

    example_artifact = output_dict[utils.EXAMPLES_KEY].type()
    base_output_dir = os.path.join(pipeline_info.pipeline_root,
                                   component_info.component_id)

    example_artifact.uri = base_driver._generate_output_uri(  # pylint: disable=protected-access
        base_output_dir, utils.EXAMPLES_KEY, execution_id)
    example_artifact.set_string_custom_property(
        utils.FINGERPRINT_PROPERTY_NAME,
        exec_properties[utils.FINGERPRINT_PROPERTY_NAME])
    example_artifact.set_string_custom_property(
        utils.SPAN_PROPERTY_NAME,
        str(exec_properties[utils.SPAN_PROPERTY_NAME]))
    # TODO(b/162622803): add default behavior for when version spec not present.
    if exec_properties[utils.VERSION_PROPERTY_NAME]:
      example_artifact.set_string_custom_property(
          utils.VERSION_PROPERTY_NAME,
          str(exec_properties[utils.VERSION_PROPERTY_NAME]))

    base_driver._prepare_output_paths(example_artifact)  # pylint: disable=protected-access

    return {utils.EXAMPLES_KEY: [example_artifact]}

  def run(self, input_dict: Dict[Text, List[types.Artifact]],
          output_dict: Dict[Text, List[types.Artifact]],
          exec_properties: Dict[Text, Any]) -> driver_output_pb2.DriverOutput:
    if not self._pipeline_info or not self._pipeline_node:
      raise ValueError(
          'pipeline_info and pipeline_node must be supplied in the IR mode.')

    if utils.INPUT_CONFIG_KEY not in self._pipeline_node.parameters.parameters:
      raise ValueError('input_config must be provided to example_gen')
    input_config = example_gen_pb2.Input()
    json_format.Parse(
        self._pipeline_node.parameters.parameters[
            utils.INPUT_CONFIG_KEY].field_value.string_value, input_config)
    if utils.INPUT_BASE_KEY not in self._pipeline_node.parameters.parameters:
      raise ValueError('input_base must be provided to example_gen')
    input_base = (
        self._pipeline_node.parameters.parameters[
            utils.INPUT_BASE_KEY].field_value.string_value)
    logging.debug('Processing input %s.', input_base)

    # Note that this function updates the input_config.splits.pattern.
    fingerprint, span, version = (
        utils.calculate_splits_fingerprint_span_and_version(
            input_base, input_config.splits))

    # Populate exec_properties
    result = driver_output_pb2.DriverOutput()
    result.exec_properties[
        utils.INPUT_CONFIG_KEY].string_value = json_format.MessageToJson(
            input_config, sort_keys=True, preserving_proto_field_name=True)
    result.exec_properties[utils.SPAN_PROPERTY_NAME].int_value = span
    result.exec_properties[utils.VERSION_PROPERTY_NAME].int_value = version
    result.exec_properties[
        utils.FINGERPRINT_PROPERTY_NAME].string_value = fingerprint

    # Populate output_dict
    output_example = copy.deepcopy(
        output_dict[utils.EXAMPLES_KEY][0].mlmd_artifact)
    output_example.custom_properties[
        utils.FINGERPRINT_PROPERTY_NAME].string_value = fingerprint
    output_example.custom_properties[utils.SPAN_PROPERTY_NAME].int_value = span
    if version:
      output_example.custom_properties[
          utils.VERSION_PROPERTY_NAME].int_value = version
    result.output_artifacts[utils.EXAMPLES_KEY].artifacts.append(
        output_example)
    return result
