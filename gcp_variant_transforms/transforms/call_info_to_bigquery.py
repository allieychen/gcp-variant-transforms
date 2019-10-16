# Copyright 2019 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, Union  # pylint: disable=unused-import

import apache_beam as beam

from gcp_variant_transforms.beam_io import vcf_header_io  # pylint: disable=unused-import
from gcp_variant_transforms.libs import call_info_table_schema_generator
from gcp_variant_transforms.libs import hashing_util


class ConvertCallInfoToRow(beam.DoFn):
  """Extracts call info from `VcfHeader` and converts it to a BigQuery row."""

  def process(self, vcf_header):
    # type: (vcf_header_io.VcfHeader) -> Dict[str, Union[int, str]]
    for sample in vcf_header.samples:
      call_id = hashing_util.generate_unsigned_hash_code(
          vcf_header.file_path + sample, max_hash_value=pow(2, 63))
      row = {
          call_info_table_schema_generator.CALL_ID: call_id,
          call_info_table_schema_generator.CALL_NAME: sample,
          call_info_table_schema_generator.FILE_PATH: vcf_header.file_path
      }
      yield row


class CallInfoToBigQuery(beam.PTransform):
  """Writes call info to BigQuery."""

  def __init__(self, output_table_prefix, append=False):
    # type: (str, Dict[str, str], bool) -> None
    """Initializes the transform.

    Args:
      output_table_prefix: The prefix of the output BigQuery table.
      append: If true, existing records in output_table will not be
        overwritten. New records will be appended to those that already exist.
    """
    self._output_table = ''.join([
        output_table_prefix, call_info_table_schema_generator.TABLE_SUFFIX])
    self._append = append
    self._schema = call_info_table_schema_generator.generate_schema()

  def expand(self, pcoll):
    return (pcoll
            | 'ConvertCallInfoToBigQueryTableRow' >> beam.ParDo(
                ConvertCallInfoToRow())
            | 'WriteCallInfoToBigQuery' >> beam.io.WriteToBigQuery(
                self._output_table,
                schema=self._schema,
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                    if self._append
                    else beam.io.BigQueryDisposition.WRITE_TRUNCATE),
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS))
