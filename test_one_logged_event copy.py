import io

from ccdexplorer_fundamentals.enums import NET
from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from ccdexplorer_fundamentals.GRPCClient.CCD_Types import CCD_BlockItemSummary
from ccdexplorer_fundamentals.GRPCClient.types_pb2 import VersionedModuleSource
from ccdexplorer_fundamentals.mongodb import Collections, MongoDB, MongoMotor
from ccdexplorer_fundamentals.tooter import Tooter
from rich import print
from schema_parsing_python import Schema

grpcclient = GRPCClient()
tooter = Tooter()

mongodb = MongoDB(tooter)
motormongo = MongoMotor(tooter)

block_hash = "last_final"
module_ref = "2826a0cf9516115386fd2c8587ab2a179fdc9b40203143044de13fdacf02733e"
tx_hash = "10fff86cf27b43582314f6ff79c8d8e79c98668bba6a8fe95048f0aec729c17e"

ms: VersionedModuleSource = grpcclient.get_module_source_original_classes(
    module_ref, block_hash, net=NET.TESTNET
)
schema = Schema(ms.v1.value, 1) if ms.v1 else Schema(ms.v0.value, 0)
result = mongodb.testnet[Collections.transactions].find_one(tx_hash)
if result:
    tx = CCD_BlockItemSummary(**result)

logged_event_hex = tx.account_transaction.effects.contract_update_issued.effects[
    0
].updated.events[0]

event_json = schema.event_to_json("track_and_trace", bytes.fromhex(logged_event_hex))
print(event_json)
