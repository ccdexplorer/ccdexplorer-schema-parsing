import io

from ccdexplorer_fundamentals.enums import NET
from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from ccdexplorer_fundamentals.GRPCClient.CCD_Types import CCD_BlockItemSummary
from ccdexplorer_fundamentals.GRPCClient.types_pb2 import VersionedModuleSource
from ccdexplorer_fundamentals.mongodb import Collections, MongoDB, MongoMotor
from ccdexplorer_fundamentals.tooter import Tooter
from rich import print
from schema_parsing_python import Schema
from pymongo import DESCENDING, ASCENDING

grpcclient = GRPCClient()
tooter = Tooter()

mongodb = MongoDB(tooter)
motormongo = MongoMotor(tooter)

block_hash = "last_final"
# module_ref = "2826a0cf9516115386fd2c8587ab2a179fdc9b40203143044de13fdacf02733e"
# instance = "<8219,0>"

module_ref = "2c484449e32d435dcaa3921b7c04b395b3b4b8b25b1ccdc3d3002322dd04edcb"
instance = "<8527,0>"


result = mongodb.testnet[Collections.helpers].find_one(
    {"_id": "schema_parsing_last_processed_block"}
)
schema_parsing_last_processed_block = result["height"]


ms: VersionedModuleSource = grpcclient.get_module_source_original_classes(
    module_ref, block_hash, net=NET.TESTNET
)
schema = Schema(ms.v1.value, 1) if ms.v1 else Schema(ms.v0.value, 0)


pipeline = [
    {
        "$match": {"impacted_address_canonical": {"$eq": instance}},
    },
    {  # this filters out account rewards, as they are special events
        "$match": {"tx_hash": {"$exists": True}},
    },
    # {"$sort": {"block_height": DESCENDING}},
    {"$skip": 0},
    {"$limit": 100},
    {"$project": {"tx_hash": 1}},
]
result = list(mongodb.testnet[Collections.impacted_addresses].aggregate(pipeline))
all_txs_hashes = [x["tx_hash"] for x in result]

int_result = list(
    mongodb.testnet[Collections.transactions]
    .find({"_id": {"$in": all_txs_hashes}})
    .sort("block_info.height", ASCENDING)
)
tx_result = [CCD_BlockItemSummary(**x) for x in int_result]


# result = mongodb.testnet[Collections.transactions].find_one(tx_hash)
# if result:
#     tx = CCD_BlockItemSummary(**result)

for index, tx in enumerate(tx_result):
    effects = tx.account_transaction.effects
    if tx.account_transaction.effects.contract_update_issued:
        for effect in effects.contract_update_issued.effects:
            if effect.updated:
                logged_events = []
                for event in effect.updated.events:

                    event_json = schema.event_to_json(
                        "track_and_trace", bytes.fromhex(event)
                    )
                    print(f"{tx.hash[:4]}: {event_json}")
    else:
        print(f"{tx.hash[:4]}: contract_initialized.")
