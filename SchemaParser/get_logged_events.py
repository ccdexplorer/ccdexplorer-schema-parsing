from ccdexplorer_fundamentals.enums import NET
from ccdexplorer_fundamentals.GRPCClient import GRPCClient
from ccdexplorer_fundamentals.cis import (
    CIS,
    itemStatusChangedEvent,
    itemCreatedEvent,
    StandardIdentifiers,
    LoggedEvents,
)

from ccdexplorer_fundamentals.GRPCClient.CCD_Types import (
    CCD_BlockItemSummary,
    CCD_ContractAddress,
    CCD_ContractTraceElement,
)
from ccdexplorer_fundamentals.GRPCClient.types_pb2 import VersionedModuleSource
from ccdexplorer_fundamentals.mongodb import Collections, MongoDB, MongoMotor
from ccdexplorer_fundamentals.tooter import Tooter
from rich import print
from rich.progress import track
from pymongo import ReplaceOne

# from schema_parsing_python import Schema
from pymongo import ASCENDING
from pymongo.collection import Collection

from ccdexplorer_schema_parser.Schema import Schema

block_hash = "last_final"
# module_ref = "2826a0cf9516115386fd2c8587ab2a179fdc9b40203143044de13fdacf02733e"
# instance = "<8219,0>"

module_ref = "2c484449e32d435dcaa3921b7c04b395b3b4b8b25b1ccdc3d3002322dd04edcb"
instance = "<8527,0>"


# result = mongodb.testnet[Collections.helpers].find_one(
#     {"_id": "schema_parsing_last_processed_block"}
# )
# schema_parsing_last_processed_block = result["height"]


class GetLoggedEvents:
    def source_module_refs_from_instances(self):
        print("source_module_refs_from_instances...")
        result = self.db[Collections.instances].find({})
        for instance in list(result):
            if instance.get("v0"):
                self.contract_address_to_module_refs_cache[instance["_id"]] = {
                    "source_module_ref": instance["v0"]["source_module"],
                    "module_name": instance["v0"]["name"][5:],
                }
            else:
                self.contract_address_to_module_refs_cache[instance["_id"]] = {
                    "source_module_ref": instance["v1"]["source_module"],
                    "module_name": instance["v1"]["name"][5:],
                }

        return self.contract_address_to_module_refs_cache

    def get_schema_from_source(self, module_ref: str, net: str):
        self.source_module_ref_to_schema_cache: dict
        self.grpcclient: GRPCClient
        if not self.source_module_ref_to_schema_cache.get(module_ref):
            ms: VersionedModuleSource = (
                self.grpcclient.get_module_source_original_classes(
                    module_ref, "last_final", net=NET(net)
                )
            )
            schema = Schema(ms.v1.value, 1) if ms.v1 else Schema(ms.v0.value, 0)
            self.source_module_ref_to_schema_cache[module_ref] = schema
        else:
            schema = self.source_module_ref_to_schema_cache[module_ref]
        return schema

    def formulate_tnt_logged_event(
        self,
        tx: CCD_BlockItemSummary,
        tag_: int,
        result: itemCreatedEvent | itemStatusChangedEvent,
        effect_index: int,
        contract_address: CCD_ContractAddress,
        event: str,
        event_index: int,
    ):
        if tag_ in [236, 237]:
            _id = f"{tx.block_info.height}-{contract_address.to_str()}-{result.item_id}-{event}-{effect_index}-{event_index}"
            if result:
                result_dict = result.model_dump()
            else:
                result_dict = {}

            d = {
                "_id": _id,
                "logged_event": event,
                "result": result_dict,
                "event_type": LoggedEvents(tag_).name,
                "block_height": tx.block_info.height,
                "tx_hash": tx.hash,
                "tx_index": tx.index,
                "effect_index": effect_index,
                "event_index": event_index,
                "item_id": result.item_id,
                "item_address": f"{contract_address.to_str()}-{result.item_id}",
                "contract": contract_address.to_str(),
                "date": f"{tx.block_info.slot_time:%Y-%m-%d}",
                "timestamp": tx.block_info.slot_time,
            }
            return ReplaceOne(
                {"_id": _id},
                replacement=d,
                upsert=True,
            )

        else:
            return None

    def check_cis_6(
        self,
        cis: CIS,
        cis_6_contracts: dict[bool],
        contract_address: CCD_ContractAddress,
    ):
        if contract_address.to_str() not in cis_6_contracts:
            cis_6_contracts[contract_address.to_str()] = cis.supports_standard(
                StandardIdentifiers.CIS_6
            )

        return cis_6_contracts[contract_address.to_str()]

    def get_logged_events(self):
        print("get_logged_events...")
        cis_6_contracts: dict[bool] = {}
        self.db: dict[Collections, Collection]
        contract_address_to_module_refs_cache = self.source_module_refs_from_instances()
        result = self.db[Collections.helpers].find_one(
            {"_id": "schema_parsing_last_processed_block"}
        )
        schema_parsing_last_processed_block = result["height"]

        pipeline = [
            {
                "$match": {
                    "account_transaction.effects.contract_update_issued": {
                        "$exists": True
                    }
                },
            },
            {
                "$match": {
                    "block_info.height": {"$gt": schema_parsing_last_processed_block}
                },
            },
            {"$sort": {"block_height": ASCENDING}},
            {"$limit": 1},
        ]
        result = list(self.db[Collections.transactions].aggregate(pipeline))
        tx_result = [CCD_BlockItemSummary(**x) for x in result]

        for tx in tx_result:
            logged_events = []
            effects = tx.account_transaction.effects
            if not tx.account_transaction.effects.contract_update_issued:
                break
            for effect_index, effect in enumerate(
                effects.contract_update_issued.effects
            ):
                effect: CCD_ContractTraceElement
                if not effect.updated:
                    break
                contract_address = effect.updated.address.to_str()

                source_module_ref = contract_address_to_module_refs_cache.get(
                    contract_address
                )["source_module_ref"]
                source_module_name = contract_address_to_module_refs_cache.get(
                    contract_address
                )["module_name"]
                ci = CIS(
                    self.grpcclient,
                    effect.updated.address.index,
                    effect.updated.address.subindex,
                    f"{source_module_name}.supports",
                    NET(self.net),
                )
                supports_cis_6 = self.check_cis_6(
                    ci, cis_6_contracts, effect.updated.address
                )
                # print(effect.updated.address.to_str(), supports_cis_6)

                if not supports_cis_6:
                    break
                for event_index, event in enumerate(effect.updated.events):

                    schema = self.get_schema_from_source(source_module_ref, self.net)
                    try:
                        tag, result = ci.process_tnt_log_event(event)
                        if tag == 236:
                            event_json = schema.event_to_json(
                                source_module_name,
                                bytes.fromhex(event),
                            )

                            result: itemStatusChangedEvent
                            result.new_status = list(
                                event_json["ItemStatusChanged"][0]["new_status"].keys()
                            )[0]
                            possible_logged_event = self.formulate_tnt_logged_event(
                                tx,
                                tag,
                                result,
                                effect_index,
                                effect.updated.address,
                                event,
                                event_index,
                            )

                            print(f"{tx.hash}: {contract_address} | {result}")
                        if tag == 237:
                            event_json = schema.event_to_json(
                                source_module_name,
                                bytes.fromhex(event),
                            )

                            result: itemCreatedEvent
                            result.initial_status = list(
                                event_json["ItemCreated"][0]["initial_status"].keys()
                            )[0]
                            possible_logged_event = self.formulate_tnt_logged_event(
                                tx,
                                tag,
                                result,
                                effect_index,
                                effect.updated.address,
                                event,
                                event_index,
                            )
                            print(f"{tx.hash}: {contract_address} | {result}")
                        if possible_logged_event:
                            logged_events.append(possible_logged_event)
                    except ValueError:
                        pass
                        # print(
                        #     f"{tx.hash[:4]}: Unable to get schema from the module: No schema found in the module"
                        # )
            if len(logged_events) > 0:
                _ = self.db[Collections.tnt_logged_events].bulk_write(logged_events)
