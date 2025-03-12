# Copyright 2025 Exactpro (Exactpro Systems Limited)
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import logging
import os
import time
from pathlib import Path

from th2_common.schema.factory.common_factory import CommonFactory
from th2_grpc_common.common_pb2 import Direction

from custom import support_functions as sf

SUPPORTED_IMAGE_EXTENSIONS = ['png', 'webp']


def verify_args(args):
    if not os.path.dirname(os.path.realpath(args.config)):
        raise ValueError(f"'{args.config}' path for th2 configs doesn't exist or isn't directory")

    for image_file in args.image_files:
        image_path: Path = Path(image_file)
        if not (image_path.exists() and image_path.is_file()):
            raise ValueError(f"'{image_file}' path for image doesn't exist or isn't file")

        image_path_extension = image_path.suffix.lstrip('.').lower()
        if image_path_extension not in SUPPORTED_IMAGE_EXTENSIONS:
            raise ValueError(
                f"'{image_file}' path for image hasn't got supported extension {SUPPORTED_IMAGE_EXTENSIONS}")

    return args


def scenario(factory: CommonFactory, image_files: list[str], session_alias: str):
    event_router = factory.event_batch_router
    message_router = factory.message_group_batch_router

    scenario_id = 1
    sequence = time.time_ns()

    root_event_id = sf.create_event_id(factory)
    sf.submit_event(
        estore=event_router,
        event_batch=sf.create_event_batch(
            report_name=f"[TS_{scenario_id}] send image as message",
            event_id=root_event_id,
            parent_id=None
        )
    )

    for image_file in image_files:
        sequence += 1
        message_id = sf.create_message_id(factory, session_alias, Direction.FIRST, sequence)
        image_path: Path = Path(image_file)
        image_bytes = image_path.read_bytes()
        image_path_extension = image_path.suffix.lstrip('.').lower()

        sf.submit_message(
            message_router, sf.create_message_group_batch(
                sf.create_raw_message(message_id, image_bytes, protocol=f"image/{image_path_extension}")
            )
        )

        event_id = sf.create_event_id(factory)
        sf.submit_event(
            estore=event_router,
            event_batch=sf.create_event_batch(
                report_name=f"Image '{image_path.name}'",
                etype='UploadImage',
                event_id=event_id,
                parent_id=root_event_id,
                attached_message_ids=[message_id]
            )
        )


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Publish image as th2 message')
        parser.add_argument('--config', required=True, type=str, help='directory with th2 config files')
        parser.add_argument('--image-files', required=True, nargs='+', type=str,
                            help='image files in png or webp format and appropriate extension')
        parser.add_argument('--session-alias', type=str, default='screenshot',
                            help='th2 session alias for raw message publishing')

        args = verify_args(parser.parse_args())

        factory = CommonFactory(config_path=args.config)
        try:
            scenario(factory, args.image_files, args.session_alias)
        finally:
            factory.close()
    except Exception as e:
        logging.error(e)
        raise e
