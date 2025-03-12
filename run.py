# Copyright 2021-2025 Exactpro (Exactpro Systems Limited)
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

import datetime
import logging
import pickle
import subprocess
import time

import yaml

from scenarios.AggressiveIOC_Traded_against_TwoOrders_partially_and_Cancelled import fix_run
from custom import support_functions as sf

# IMPORT REFDATA FROM FILES
with open('configs/instruments.refdata') as f:
    instruments = yaml.load(f, Loader=yaml.FullLoader)
with open('configs/firms.refdata') as f:
    firms = yaml.load(f, Loader=yaml.FullLoader)


def scenario(factory, parent=None):
    # Storing EventID object of root Event.
    report_id = sf.create_event_id(factory['factory'])

    # Initialize chain_id for script
    ver1_chain = None
    ver2_chain = None
    scenario_id = 1
    # Sending request to estore. Creation of the root Event for all cases performed.
    sf.submit_event(
        estore=factory['estore'],
        event_batch=sf.create_event_batch(
            report_name=f"[TS_{scenario_id}]Aggressive IOC vs two orders: second order's price is more favorable than first",
            event_id=report_id,
            parent_id=parent))

    # Getting case participants from refdata
    trader1 = firms[0]['Traders'][0]['TraderName']
    trader1_firm = firms[0]['FirmName']
    trader1_fix = firms[0]['Traders'][0]['TraderConnection']
    trader2 = firms[1]['Traders'][0]['TraderName']
    trader2_firm = firms[1]['FirmName']
    trader2_fix = firms[1]['Traders'][0]['TraderConnection']

    case_id = 0
    # Execution of case for every instrument in refdata
    for instrument in instruments:
        case_id += 1
        ver1_chain, ver2_chain = fix_run.aggressive_ioc_traded_against_two_orders_partially_and_then_cancelled(
            f"Case[TC_{scenario_id}.{case_id}]: "
            f"Trader {trader1} vs trader {trader2} for instrument {instrument['SecurityID']}",
            report_id, {
                'case_id': sf.create_event_id(factory['factory']),
                'Instrument': instrument['SecurityID'],
                'Order1Price': instrument['Price'],
                'Order1Qty': 30,
                'Order2Price': instrument['Price'] + 1,
                'Order2Qty': 10,
                'Order3Price': instrument['Price'] - 1,
                'Order3Qty': 100,
                'trader1': trader1,
                'trader1_firm': trader1_firm,
                'trader1_fix': trader1_fix,
                'trader2': trader2,
                'trader2_firm': trader2_firm,
                'trader2_fix': trader2_fix,
                'ver1_chain': ver1_chain,
                'ver2_chain': ver2_chain
            }, factory)


if __name__ == '__main__':
    logging.basicConfig(filename=time.asctime().replace(':', '-') + ' script.log',
                        level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # Creation of grpc channels and instances of act, check1 and estore stubs.
    factory = sf.connect(config_path="./configs/")

    try:
        start_datetime = datetime.datetime.utcnow()
        scenario(factory)
        time.sleep(10)
        finish_datetime = datetime.datetime.utcnow()

        print(F"start datetime: {start_datetime}")
        print(F"finish datetime: {finish_datetime}")

        print(F"Data Services - start")
        with open('scenarios/data_services/start_datetime.pickle', 'wb') as f:
            pickle.dump(start_datetime, f)
        with open('scenarios/data_services/finish_datetime.pickle', 'wb') as f:
            pickle.dump(finish_datetime, f)

        with subprocess.Popen('jupyter notebook scenarios/data_services/notebook.ipynb'.split()) as p:
            x = None
            time.sleep(10)
            while x not in ['Y', 'y']:
                x = input("Enter Y/y to close DataServices and finish demo script: ")
            p.kill()

    finally:
        factory['factory'].close()
