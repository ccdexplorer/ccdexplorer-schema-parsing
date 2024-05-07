from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from ccdexplorer_fundamentals.mongodb import (
    Collections,
    CollectionsUtilities,
    MongoDB,
    MongoMotor,
)
from ccdexplorer_fundamentals.tooter import Tooter
from pymongo.collection import Collection

from .get_logged_events import GetLoggedEvents as _get_logged_events


class SchemaParser(
    _get_logged_events,
):
    def __init__(
        self,
        grpcclient: GRPCClient,
        tooter: Tooter,
        mongodb: MongoDB,
        motormongo: MongoMotor,
        net: str,
    ):
        self.grpcclient = grpcclient
        self.tooter = tooter
        self.mongodb = mongodb
        self.motormongo = motormongo
        self.net = net
        self.source_module_ref_to_schema_cache = {}
        self.contract_address_to_module_refs_cache = {}
        self.mainnet: dict[Collections, Collection] = self.mongodb.mainnet
        self.testnet: dict[Collections, Collection] = self.mongodb.testnet
        self.utilities: dict[CollectionsUtilities, Collection] = self.mongodb.utilities

        self.motor_mainnet: dict[Collections, Collection] = self.motormongo.mainnet
        self.motor_testnet: dict[Collections, Collection] = self.motormongo.testnet

        self.db: dict[Collections, Collection] = (
            self.mainnet if self.net == "mainnet" else self.testnet
        )
