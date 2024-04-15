from ccdexplorer_fundamentals.enums import NET
from ccdexplorer_fundamentals.GRPCClient import GRPCClient, wadze
from ccdexplorer_fundamentals.GRPCClient.types_pb2 import VersionedModuleSource

from schema_parsing import Schema
import io

grpcclient = GRPCClient()

block_hash = "last_final"
module_ref = "2826a0cf9516115386fd2c8587ab2a179fdc9b40203143044de13fdacf02733e"

ms: VersionedModuleSource = grpcclient.get_module_source(
    module_ref, block_hash, net=NET.TESTNET
)
bs = io.BytesIO(bytes.fromhex(ms.v1))

module = wadze.parse_module(bs.read())
# f = open("/path/to/module", "rb").read()
schema = Schema(module["code"])
# to convert the event which is serialized as [1,2,3] for the contract "test"
# to json
event_json = schema.event_to_json("test", [1, 2, 2])
