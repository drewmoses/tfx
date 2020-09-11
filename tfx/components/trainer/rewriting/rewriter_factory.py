# Lint as: python3
# Copyright 2020 Google LLC. All Rights Reserved.
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
"""Factory for instantiating rewriters.

NOTE: For your rewriter to be instanitated, please include it as an import and
a constant for ease of invoking the new rewriter.
"""

import importlib
from typing import Text

from absl import logging

from tfx.components.trainer.rewriting import rewriter

TFLITE_REWRITER = 'TFLiteRewriter'
TFJS_REWRITER = 'TFJSRewriter'


class _RewriterFactory:
  """Factory class for rewriters."""
  _rewriters_loaded = False

  @classmethod
  def _load_rewriters(cls):
    """Load all rewriters."""
    if cls._rewriters_loaded:
      return
    # By importing subclasses of BaseRewriter, we can access them later via
    # BaseRewriter.__subclasses__().
    importlib.import_module('tfx.components.trainer.rewriting.tflite_rewriter')
    try:
      importlib.import_module('tensorflowjs')
    except ImportError:
      logging.info('tensorflowjs is not installed. Skipping tfjs_rewriter. '
                   'Please install [tfjs] extra dependencies to use '
                   'tfjs_rewriter.')
    else:
      importlib.import_module('tfx.components.trainer.rewriting.tfjs_rewriter')
    cls._rewriters_loaded = True

  @classmethod
  def get_rewriter_cls(cls, rewriter_type: Text):
    cls._load_rewriters()
    for c in rewriter.BaseRewriter.__subclasses__():
      if (c.__name__.lower()) == rewriter_type.lower():
        return c
    raise ValueError('Failed to find rewriter: {}'.format(rewriter_type))


def create_rewriter(rewriter_type: Text, *args,
                    **kwargs) -> rewriter.BaseRewriter:
  """Instantiates a new rewriter with the given type and constructor arguments.

  Args:
    rewriter_type: The rewriter subclass to instantiate (can be all lowercase).
    *args: Positional initialization arguments to pass to the rewriter.
    **kwargs: Keyward initialization arguments to pass to the rewriter.

  Returns:
    The instantiated rewriter.
  Raises:
    ValueError: If unable to instantiate the rewriter.
  """
  return _RewriterFactory.get_rewriter_cls(rewriter_type)(*args, **kwargs)
