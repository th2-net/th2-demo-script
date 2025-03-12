# Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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

from __future__ import print_function

import copy
import logging
import random
import time
import uuid
from pathlib import Path

from google.protobuf.timestamp_pb2 import Timestamp
from th2_common.schema.factory.common_factory import CommonFactory
from th2_common.schema.message.message_router import MessageRouter
from th2_grpc_act_template.act_service import ActService
from th2_grpc_act_template.act_template_pb2 import PlaceMessageRequest
from th2_grpc_check1 import check1_pb2
from th2_grpc_check1.check1_service import Check1Service
from th2_grpc_common.common_pb2 import (ValueFilter, FilterOperation, MessageMetadata, MessageFilter, RootMessageFilter,
                                        ConnectionID, EventID, ListValue, Value, Message,
                                        ListValueFilter, MessageID, Event, EventBatch, RawMessage, MessageGroupBatch,
                                        MessageGroup, AnyMessage, RawMessageMetadata, Direction)


# -----------Connection functions
def connect(config_path, tries=3):
    try:
        logging.info('Trying to connect...')
        factory = CommonFactory(config_path=config_path, logging_config_filepath=Path("configs/log4py.conf"))
        grpc_router = factory.grpc_router
        act = grpc_router.get_service(ActService)
        check = grpc_router.get_service(Check1Service)
        estore = factory.event_batch_router
        logging.info('Connection established.')
        return {'act': act,
                'check': check,
                'estore': estore,
                'factory': factory}
    except Exception as e:
        if tries > 0:
            logging.error('Unable to connect.')
            logging.error(str(e))
            logging.info('Retry in 3...')
            print(f'Unable to connect: \n {str(e)}')
            time.sleep(3)
            connect(config_path, tries-1)
        else:
            raise


# -------estore functions
def submit_event(estore, event_batch):
    logging.debug(f'Event content:{str(event_batch)}')
    response = estore.send(event_batch)
    logging.debug(f'Estore response content:{str(response)}')


def to_msg_body(string):
    return bytes('[{"type":"message","data":"' + string + '" } ]', 'utf-8')


def create_event_id(factory):
    start_timestamp = Timestamp()
    start_timestamp.GetCurrentTime()
    book_name = factory.box_configuration.book_name
    scope = factory.box_configuration.box_name
    return EventID(id=str(uuid.uuid1()), book_name=book_name, scope=scope, start_timestamp=start_timestamp)


def store_event(factory, name, event_id=None, parent_id=None, body=b"", status='SUCCESS', etype=''):
    new_event_id = event_id
    if new_event_id is None:
        new_event_id = create_event_id(factory['factory'])
    submit_event(
        estore=factory['estore'],
        event_batch=create_event_batch(
            report_name=name,
            etype=etype,
            status=status,
            event_id=new_event_id,
            parent_id=parent_id,
            body=body))
    return new_event_id


def create_event_batch(report_name, event_id, parent_id=None, status='SUCCESS', body=b"", etype='',
                       attached_message_ids: list[MessageID] = None):
    current_timestamp = Timestamp()
    current_timestamp.GetCurrentTime()
    logging.info(f'Storing event {report_name}...')
    event = Event(
        id=event_id,
        name=report_name,
        status=status,
        body=body,
        type=etype,
        end_timestamp=current_timestamp,
        parent_id=parent_id
    )
    if attached_message_ids:
        for attached_message_id in attached_message_ids:
            event.attached_message_ids.append(attached_message_id)

    return EventBatch(events=[event])


# -------mstore functions
def submit_message(mstore: MessageRouter, message_batch: MessageGroupBatch):
    logging.debug(f'Message content:{str(message_batch)}')
    response = mstore.send(message_batch, 'raw')
    logging.debug(f'Mstore response content:{str(response)}')


def create_message_id(factory: CommonFactory, session_alias: str, direction: Direction, sequence: int):
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    book_name = factory.box_configuration.book_name
    return MessageID(book_name=book_name, connection_id=ConnectionID(session_alias=session_alias), direction=direction,
                     timestamp=timestamp, sequence=sequence)


def create_raw_message(message_id: MessageID, body: bytes, properties: dict[str, str] = None,
                       protocol: str = None) -> RawMessage:
    metadata = RawMessageMetadata(id=message_id)
    if properties:
        for key, value in properties.items():
            metadata.properties[key] = value
    if protocol:
        metadata.protocol = protocol
    return RawMessage(metadata=metadata, body=body)


def create_message_group_batch(*messages: RawMessage) -> MessageGroupBatch:
    return MessageGroupBatch(
        groups=[
            MessageGroup(messages=[
                AnyMessage(raw_message=message) for message in messages
            ])
        ])


# -------------act functions
def placeOrderFIX(act, place_message_request):
    logging.info('Sending request to act...')
    logging.debug(str(place_message_request))
    try:
        act_response = act.placeOrderFIX(place_message_request)
    except Exception as e:
        logging.error('FATAL ERROR. Unable to proceed.')
        logging.error(str(e))
        # raise SystemExit
    if act_response.status.status == 0:
        logging.info('Request submitted. Response received.')
    else:
        logging.error(f'Request submitted. Act rule executed incorrectly{str(act_response.status)}.')

    return act_response


def sendMessage(act, place_message_request):
    logging.info('Sending request to act...')
    logging.debug(str(place_message_request))
    try:
        act_response = act.sendMessage(place_message_request)
    except Exception as e:
        logging.error('FATAL ERROR. Unable to proceed.')
        logging.error(str(e))
        # raise SystemExit
    if act_response.status.status == 0:
        logging.info('Request submitted. Response received.')
    else:
        logging.error(f'Request submitted. Act rule executed incorrectly{str(act_response.status)}.')

    return act_response


def get_field_value_from_act_response(response, field):
    return response.response_message.fields[field].simple_value


def create_message_object(factory, msg_type, fields, session_alias):
    fields = copy.deepcopy(fields)
    for field in fields:
        if field == 'TradingParty' and isinstance(fields[field], list):
            fields[field] = wrap_into_trading_party("value", fields[field]),
        if isinstance(fields[field], str) or isinstance(fields[field], int) or isinstance(fields[field], float):
            fields[field] = Value(simple_value=str(fields[field]))
    book_name = factory['factory'].box_configuration.book_name
    msg_timestamp = Timestamp()
    msg_timestamp.GetCurrentTime()

    return Message(
        metadata=MessageMetadata(
            message_type=msg_type,
            id=MessageID(
                connection_id=ConnectionID(session_alias=session_alias),
                timestamp=msg_timestamp,
                book_name=book_name,
                direction='SECOND',
                sequence=1
            )),
        fields=fields)


# --------check functions
def submitCheckSequenceRule(check, check_sequence_rule_request):
    logging.info('Sending CheckSequenceRuleRequest to check...')
    logging.debug(str(check_sequence_rule_request))
    try:
        check_response = check.submitCheckSequenceRule(check_sequence_rule_request)
    except Exception as e:
        logging.error('FATAL ERROR. Unable to proceed.')
        logging.error(str(e))
        # raise SystemExit
    if check_response.status.status == 0:
        logging.info('Request submitted. Response received.')
    else:
        logging.error(f'Request submitted. Check executed incorrectly{str(check_response.status)}.')

    return check_response


def submitCheckRule(check, check_rule_request):
    logging.info('Sending CheckRuleRequest to check...')
    logging.debug(str(check_rule_request))
    try:
        check_response = check.submitCheckRule(check_rule_request)
    except Exception as e:
        logging.error('FATAL ERROR. Unable to proceed.')
        logging.error(str(e))
        # raise SystemExit
    if check_response.status.status == 0:
        logging.info('Request submitted. Response received.')
    else:
        logging.error(f'Request submitted. Check executed incorrectly{str(check_response.status)}.')

    return check_response


def createCheckpoint(check, parent_id):
    logging.info('Sending Checkpoint request to check...')
    try:
        check_response = check.createCheckpoint(check1_pb2.CheckpointRequest(parent_event_id=parent_id))
    except Exception as e:
        logging.error('FATAL ERROR. Unable to proceed.')
        logging.error(str(e))
        # raise SystemExit
    if check_response.status.status == 0:
        logging.info('Request submitted. Response received.')
    else:
        logging.error(f'Request submitted. Check executed incorrectly{str(check_response.status)}.')

    return check_response


def create_filter_object(msg_type, fields, key_fields_list):
    fields = copy.deepcopy(fields)
    for field in fields:
        if fields[field] == '*':
            fields[field] = ValueFilter(operation=FilterOperation.NOT_EMPTY)
        if field == 'TradingParty' and isinstance(fields[field], list):
            fields[field] = wrap_into_trading_party("filter", fields[field]),
        if isinstance(fields[field], str) or isinstance(fields[field], int) or isinstance(fields[field], float):
            if field in key_fields_list:
                fields[field] = ValueFilter(simple_filter=str(fields[field]), key=True)
            else:
                fields[field] = ValueFilter(simple_filter=str(fields[field]))
        if isinstance(fields[field], dict):
            fields[field] = wrap_filter(fields[field])
        if isinstance(fields[field], list):
            flist = []
            for rgroup in fields[field]:
                flist.append(wrap_filter(rgroup))
            fields[field] = ValueFilter(list_filter=(ListValueFilter(values=flist)))
    return RootMessageFilter(messageType=msg_type, message_filter=MessageFilter(fields=fields))


def wrap_filter(fields):
    fields = copy.deepcopy(fields)
    for field in fields:
        if fields[field] == '*':
            fields[field] = ValueFilter(operation=FilterOperation.NOT_EMPTY)
        if isinstance(fields[field], str) or isinstance(fields[field], int) or isinstance(fields[field], float):
            fields[field] = ValueFilter(simple_filter=str(fields[field]))
        if isinstance(fields[field], dict):
            fields[field] = wrap_filter(fields[field])
        if isinstance(fields[field], list):
            flist = []
            for rgroup in fields[field]:
                flist.append(wrap_filter(rgroup))
            fields[field] = ValueFilter(list_filter=(ListValueFilter(values=flist)))

    return ValueFilter(message_filter=(MessageFilter(fields=fields)))


def create_chain_id():
    return check1_pb2.ChainID(id=str(uuid.uuid1()))


def generate_client_order_id(length):
    return random.randint(10 ** (length - 1), (10 ** length) - 1)


# I think following action should be reworked for dynamic filling of the header based on msgType and service.
def create_header_field(fields):
    for field in fields:
        if fields[field] == '*':
            fields[field] = ValueFilter(operation=FilterOperation.NOT_EMPTY)
        if isinstance(fields[field], str) or isinstance(fields[field], int) or isinstance(fields[field], float):
            fields[field] = ValueFilter(simple_filter=fields[field])
    header = ValueFilter(message_filter=MessageFilter(fields=fields))
    return header


def create_prefilter_msgtype_is_not(value):
    return check1_pb2.PreFilter(fields={'header': create_header_field(
        {'MsgType': ValueFilter(simple_filter=value, operation=FilterOperation.NOT_EQUAL)})})


def create_prefilter_msgtype_is(value):
    return check1_pb2.PreFilter(
        fields={'header': create_header_field({'MsgType': ValueFilter(simple_filter=value)})})


def create_prefilter_msgtype_is_with_additional_fields(message_type, fields):
    for field in fields:
        fields[field] = ValueFilter(simple_filter=fields[field])
    if isinstance(message_type, str):
        message_type = ValueFilter(simple_filter=message_type)
    return check1_pb2.PreFilter(
        fields={
            **fields,
            'header': create_header_field({'MsgType': message_type})})


# ----------TODO: we need universal wrapper someday
def wrap_into_target_party(value_type, repeating_groups):
    repeating_groups = copy.deepcopy(repeating_groups)
    if value_type.upper() == 'VALUE':
        for gid in range(len(repeating_groups)):

            for field in repeating_groups[gid]:
                repeating_groups[gid][field] = Value(simple_value=repeating_groups[gid][field])

            repeating_groups[gid] = Value(
                message_value=(Message(
                    metadata=MessageMetadata(message_type='TargetParty_NoTargetPartyIDs'),
                    fields=repeating_groups[gid])))
        repeating_groups = Value(
            message_value=Message(fields={
                'NoTargetPartyIDs': Value(list_value=(ListValue(values=repeating_groups)))
            }))
        return repeating_groups

    elif value_type.upper() == 'FILTER':
        for gid in range(len(repeating_groups)):

            for field in repeating_groups[gid]:
                repeating_groups[gid][field] = ValueFilter(simple_filter=repeating_groups[gid][field])

            repeating_groups[gid] = ValueFilter(
                message_filter=(MessageFilter(fields=repeating_groups[gid])))
        repeating_groups = ValueFilter(
            message_filter=MessageFilter(fields={
                'NoTargetPartyIDs': ValueFilter(list_filter=ListValueFilter(
                    values=repeating_groups))
            }))
        return repeating_groups
    else:
        print("Incorrect value type for TradingParty. Only filter or value available")
        # raise SystemExit


def wrap_into_trading_party(value_type, repeating_groups):
    repeating_groups = copy.deepcopy(repeating_groups)
    if value_type.upper() == 'VALUE':
        for gid in range(len(repeating_groups)):

            for field in repeating_groups[gid]:
                repeating_groups[gid][field] = Value(simple_value=repeating_groups[gid][field])

            repeating_groups[gid] = Value(
                message_value=(Message(
                    metadata=MessageMetadata(message_type='TradingParty_NoPartyIDs'),
                    fields=repeating_groups[gid])))
        repeating_groups = Value(
            message_value=Message(fields={
                'NoPartyIDs': Value(list_value=(ListValue(values=repeating_groups)))
            }))
        return repeating_groups

    elif value_type.upper() == 'FILTER':
        for gid in range(len(repeating_groups)):

            for field in repeating_groups[gid]:
                repeating_groups[gid][field] = ValueFilter(simple_filter=repeating_groups[gid][field])

            repeating_groups[gid] = ValueFilter(
                message_filter=(MessageFilter(fields=repeating_groups[gid])))
        repeating_groups = ValueFilter(
            message_filter=MessageFilter(fields={
                'NoPartyIDs': ValueFilter(list_filter=ListValueFilter(values=repeating_groups))
            }))
        return repeating_groups
    else:
        print("Incorrect value type for TradingParty. Only filter or value available")
        # raise SystemExit


def to_msg_body(string):
    return bytes('[{"data":"' + string + '", "type":"message" } ]', 'utf-8')


def wrap_into_no_related_sym(value_type, repeating_groups):
    repeating_groups = copy.deepcopy(repeating_groups)
    if value_type.upper() == 'VALUE':
        for field in repeating_groups:

            if isinstance(repeating_groups[field], str) \
                    or isinstance(repeating_groups[field], int) \
                    or isinstance(repeating_groups[field], float):
                repeating_groups[field] = Value(simple_value=repeating_groups[field])

        repeating_groups = Value(
            message_value=Message(
                fields={'NoRelatedSym': Value(
                    list_value=(ListValue(values=[
                        Value(message_value=(Message(
                            metadata=MessageMetadata(message_type='NoRelatedSymGrp_NoRelatedSym'),
                            fields=repeating_groups)))])))}))
        return repeating_groups
    elif value_type.upper() == 'FILTER':
        for field in repeating_groups:

            if isinstance(repeating_groups[field], str) \
                    or isinstance(repeating_groups[field], int) \
                    or isinstance(repeating_groups[field], float):
                repeating_groups[field] = ValueFilter(simple_filter=repeating_groups[field])

        repeating_groups = ValueFilter(
            message_filter=MessageFilter(
                fields={'NoRelatedSym': ValueFilter(
                    list_filter=(ListValueFilter(values=[
                        ValueFilter(message_filter=(MessageFilter(
                            fields=repeating_groups)))])))}))
        return repeating_groups
    else:
        print("Incorrect value type for NoRelatedSym. Only filter or value available")
        # raise SystemExit


def request_security_status(instrument, session_alias, event_id, factory):
    # SecurityStatusRequest parameters
    sec_status_request = {
        'SecurityID': instrument,
        'SecurityIDSource': '8',
        'SecurityStatusReqID': instrument
    }
    # Sending message to act
    sendMessage(
        act=factory['act'],
        place_message_request=PlaceMessageRequest(
            description=f'Request SecurityStatus for {instrument}',
            parent_event_id=event_id,
            message=create_message_object(factory=factory,
                                          msg_type='SecurityStatusRequest',
                                          fields=sec_status_request,
                                          session_alias=session_alias)))
